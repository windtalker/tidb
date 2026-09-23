# Materialized View Refresh PR Split Plan

## Purpose

The `mv_pr5_refresh_for_master` branch currently ports the manual materialized-view
refresh implementation onto `origin/master`. The branch is functionally validated,
but its diff is too large to review and merge as one pull request:

```text
94 files changed
31,797 insertions
12,710 deletions
```

The largest generated or feature-heavy files are:

```text
pkg/parser/parser.go                         +12,685 / -12,563
pkg/executor/materialized_view.go             +3,326
pkg/planner/mview/mview.go                     +2,448
pkg/executor/mviewdeltamergeagg/exec.go       +1,870
pkg/executor/mviewdeltamergeagg/exec_test.go  +2,005
```

The current history is not a suitable PR split. The first port commit,
`af4b52a80b7`, contains most of the implementation in one 86-file change. The
later fixes are small, but cherry-picking the existing commits would still leave
one monolithic PR.

This document defines a stacked-PR sequence. Each PR has one primary ownership
boundary, can be validated independently, and is merged before the next PR is
rebased onto the updated master.

## Dependency Graph

```text
PR1  Parser SQL surface
  |
  v
PR2  Shadow metadata and DDL cutover
  |
  +------> PR3  Refresh planner
                    |
                    v
             PR4  Delta merge executor
                    |
                    v
             PR5  Refresh runtime and observability
                    |
                    v
             PR6  Integration tests and documentation
```

The parser PR is the first dependency because every later runtime layer consumes
the refresh AST. The shadow/cutover PR and planner PR both provide contracts used
by the runtime. The delta executor PR depends on planner physical operators. The
runtime PR consumes all of those layers and must include the observability pieces
that its control flow directly calls.

## PR1: Parser SQL Surface

### Intent

Add the refresh SQL grammar and AST without adding refresh execution behavior.

### Scope

- `pkg/parser/parser.y`
- Generated `pkg/parser/parser.go`
- `pkg/parser/ast/ast.go`
- `pkg/parser/ast/misc.go`
- `pkg/parser/ast/sem.go`
- `pkg/parser/ast/visitor_codegen/generator.go`
- `pkg/parser/ast/visitor_inplace_generated.go`
- Parser visitor and keyword tests
- `pkg/parser/keywords.go`
- `pkg/parser/misc.go`

The grammar covers `REFRESH MATERIALIZED VIEW`, complete and fast modes,
`WITH ASYNC MODE`, `DRY RUN`, `WITH PROFILE`, and
`CANCEL MATERIALIZED VIEW REFRESH JOB`.

`pkg/parser/parser.go` is generated output. Its large diff is expected because
adding grammar tokens changes the generated token and parse tables. It should be
reviewed as generated output and never manually edited to reduce the diff.

### Validation

```bash
cd pkg/parser
go test ./... -count=1
```

Run the repository parser-generation checks after updating `parser.y`; keep the
generated output in the same PR as its source grammar.

## PR2: Shadow Metadata and DDL Cutover

### Intent

Introduce the protected shadow table metadata and the DDL jobs needed to create a
shadow table and atomically cut it over to the materialized-view name. This PR does
not add the full refresh execution lifecycle.

### Scope

- `pkg/meta/model/{job.go,job_args.go,table.go,bdr.go}`
- `pkg/ddl/create_table.go`
- `pkg/ddl/delete_range.go`
- `pkg/ddl/executor.go`
- `pkg/ddl/job_worker.go`
- `pkg/ddl/jobsubmit/submit.go`
- `pkg/ddl/materialized_view.go`
- `pkg/ddl/notifier/events.go`
- `pkg/ddl/rollingback.go`
- `pkg/ddl/sanity_check.go`
- `pkg/ddl/schema_version.go`
- `pkg/ddl/table.go`
- `pkg/ddl/schematracker/**`
- `pkg/infoschema/builder.go`
- `pkg/statistics/handle/ddl/subscriber.go`

Tests should remain with their owning package. They cover action types and job
arguments, shadow-table protection, job table IDs, schema diff construction,
InfoSchema V1/V2 cutover, and old/new statistics metadata.

Two correctness fixes belong in this PR rather than in the later runtime PR:

1. `ActionMViewRefreshOutOfPlaceCutover` uses the generic InfoSchema V2 action
   path. The old V1-specific path reports an unknown database when V2 is enabled.
2. The statistics DDL subscriber handles the cutover event as a truncate-like
   old/new physical-table replacement instead of treating it as an unknown event.

### Validation

```bash
./tools/check/failpoint-go-test.sh pkg/ddl -count=1
./tools/check/failpoint-go-test.sh pkg/ddl/schematracker -count=1
go test ./pkg/infoschema -run 'TestApply.*MaterializedView.*' -tags=intest,deadlock -count=1
go test ./pkg/statistics/handle/ddl -tags=intest,deadlock -count=1
```

## PR3: Refresh Planner

### Intent

Add the logical and physical refresh planning layer without implementing the
executor that consumes the resulting plans.

### Scope

- `pkg/planner/mview/**`
- `pkg/planner/core/mview_refresh_builder.go`
- `pkg/planner/core/mview_refresh_lookup.go`
- `pkg/planner/core/common_plans.go`
- `pkg/planner/core/logical_plan_builder.go`
- `pkg/planner/core/planbuilder.go`
- `pkg/planner/core/point_get_plan.go`
- `pkg/planner/core/preprocess.go`
- `pkg/planner/core/util.go`
- `pkg/planner/planctx/context.go`
- `pkg/planner/plannersession/context.go`
- `pkg/util/plancodec/id.go`

Tests cover fast/bounded refresh derivation, statistics planning, shadow-table
readability in preprocessing, and refresh plan lookup/building.

### Validation

```bash
go test ./pkg/planner/mview -tags=intest,deadlock -count=1
go test ./pkg/planner/core \
  -run '^(TestPreprocessMViewShadowReadable|TestCheckMViewUpdatable|TestCheckMViewShadowReadable)$' \
  -tags=intest,deadlock -count=1
```

The full `pkg/planner/core` package currently has a local baseline failure in
`TestHandleFineGrainedShuffle` (`expected 16, actual 0`) that reproduces on a clean
`origin/master` checkout. It is not a refresh-planner regression and should be
recorded separately from this PR's targeted validation.

## PR4: Delta Merge Executor

### Intent

Implement the executor-side operators for fast refresh and delta merge plans.
Keep the complete refresh lifecycle out of this PR.

### Scope

- `pkg/executor/mviewdeltamergeagg/**`
- `pkg/executor/mview_delta_merge_agg_builder.go`
- `pkg/executor/internal/util/touched_rows.go`
- `pkg/util/chunk/column.go`
- `pkg/util/execdetails/runtime_stats.go`
- Related `BUILD.bazel` files

`MViewCompleteDeltaApplyExec` currently lives in the large
`pkg/executor/materialized_view.go` file. Before constructing this PR, move that
operator into a new dedicated executor file under `pkg/executor/` as a mechanical
refactor. Keep that move separate from behavior changes so PR4 remains easy to
review.

The refresh executor builder that dispatches the top-level refresh statement stays
in PR5, where the complete runtime lifecycle is introduced.

### Validation

```bash
go test ./pkg/executor/mviewdeltamergeagg -tags=intest,deadlock -count=1
```

## PR5: Refresh Runtime and Observability

### Intent

Add the complete refresh lifecycle using the parser, DDL, planner, and delta
executor contracts from the previous PRs.

### Scope

- `pkg/executor/materialized_view.go`
- `pkg/executor/mview_refresh_executor_builder.go`
- `pkg/executor/mv_refresh_observability.go`
- `pkg/executor/builder.go`
- `pkg/executor/adapter.go`
- `pkg/executor/adapter_slow_log.go`
- `pkg/executor/grant.go`
- `pkg/session/session.go`
- `pkg/sessionctx/variable/session.go`
- `pkg/kv/option.go`
- `pkg/metrics/materialized_view.go`
- `pkg/metrics/executor.go`
- `pkg/metrics/metrics.go`
- Refresh executor unit tests

This PR includes complete in-place, fast, complete delta-apply, and complete
out-of-place refresh execution, including advisory locks, refresh info/history,
cancel handling, shadow-table build, cutover invocation, session maintenance
variables, slow-log fallback text, and statement results.

### Observability Boundary

Observability cannot be deferred as a whole. The runtime directly depends on the
following symbols and metrics:

- `newMVRefreshStepSet`
- `observeMVRefreshStep`
- `emitMVRefreshStepPlanRows`
- refresh step observer and collector types
- `AffectedRowsCounterRefreshMV`
- `MVServiceRefreshScheduleDurationHistogram`

Therefore the complete `mv_refresh_observability.go` implementation and metric
definitions should land with PR5. Runtime code should use the final observer and
metrics from its first version; it should not contain a temporary no-op or duplicate
observability implementation.

If PR5 still needs to be reduced, split the existing observability file mechanically
into two new files under `pkg/executor/` instead of rewriting behavior:

- The PR5 file contains step types, collector, step set, observer callbacks, and
  plan-row collection.
- The PR6 file contains DRY RUN/PROFILE executors and output formatting.

The default recommendation is to keep the file whole in PR5 and leave only final
surface assertions, integration coverage, and documentation for PR6.

### Validation

```bash
./tools/check/failpoint-go-test.sh pkg/executor \
  -run 'Test(RestoreRefreshMaterializedViewStmtForSlowLog|BuildMVRefreshOutOfPlaceShadowTableInfoSetsSource)$' \
  -count=1
```

Add targeted tests for each runtime mode before opening the PR. Package tests that
use failpoints must run through `failpoint-go-test.sh`.

## PR6: Integration Tests and Documentation

### Intent

Finish the stack with end-to-end coverage, final observability output assertions,
and the split-plan documentation. This PR should not introduce a second runtime
implementation.

### Scope

- `tests/integrationtest/t/executor/mview_refresh.test`
- `tests/integrationtest/r/executor/mview_refresh.result`
- Remaining refresh observability/profile/dry-run assertions
- `docs/agents/mview-refresh-port-plan.md` updates
- This split-plan document

### Validation

```bash
cd tests/integrationtest
./run-tests.sh -r executor/mview_refresh
```

The current integration suite has 50 passing cases. Result files should be
regenerated by the integration-test runner and reviewed for minimal, expected
changes. Trailing tabs in the `DESC` output represent an empty final `Extra`
column and should not be removed manually.

## Branching and Merge Procedure

Keep the current final branch as a source snapshot before reconstructing the stack:

```bash
git tag mv-refresh-port-monolithic-final 15798f0d919
```

Create each PR branch from the previous PR branch. Do not cherry-pick
`af4b52a80b7` as a whole. Reconstruct each incremental diff by path and, for files
that mix multiple layers, by hunk. The main mixed files are:

- `pkg/ddl/materialized_view.go`
- `pkg/executor/materialized_view.go`
- `pkg/executor/mv_refresh_observability.go`

Use `mv_pr5_refresh_for_master` as the implementation reference while reconstructing
the stack. After the complete refresh port is ready, double-check the final port
against `cp_mv_for_master` before submission, and resolve any unintended behavioral
or diff differences.

After a PR merges, rebase the next stacked branch onto the new master and check the
result with `git range-diff`. Keep generated Bazel metadata with the code that
requires it; do not create a standalone BUILD-only PR.

Every PR contains Go additions, import changes, or new tests, so run
`make bazel_prepare` for each branch. Code PRs also use `make lint` and the scoped
tests listed above.

## Explicit Scope Exclusions

The following features are outside this refresh stack and should not be pulled into
these PRs:

- `pkg/mvservice` background service and scheduling framework
- SHOW/COMPARE/status presentation features
- Later product-feature PR6/PR7 service alerting, cleanup, and background scheduling work
- Unrelated parser or planner refactors
- Unrelated generated-code churn

The goal is for PR5 to be a complete refresh runtime with its real observability
dependencies, while this stack's PR6 only validates and documents the finished
behavior.
