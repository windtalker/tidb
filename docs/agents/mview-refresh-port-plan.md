# Port MV Refresh To Master

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date.

Reference: `PLANS.md` at the repository root.

## Purpose / Big Picture

After this change, the PR4 master-port branch will support manual materialized-view refresh using the current master planner and executor APIs. Complete, fast, and bounded refresh paths will update the materialized-view data and refresh metadata, while refresh cancellation, history, metrics, schedule timing, and refresh-specific observability follow the source behavior. MLog purge and its existing cancellation monitor remain intact.

The port is limited to PR5 from `cp_mv_for_master_base`'s tracking document. SHOW/COMPARE/status presentation remains PR6, and the background MV service, scheduling loop, alert checker, and cleanup remain PR7.

## Progress

- [x] (2026-09-12) Created `mv_pr5_refresh_for_master` from the current PR4 tip `c8ec004ee3`.
- [x] (2026-09-12) Confirmed the PR5 boundary in `docs/note/materialized_view/mv_master_port_tracking.md` on `cp_mv_for_master_base`.
- [x] (2026-09-12) Ported the manual/complete refresh execution lifecycle and metadata/history writes.
- [x] (2026-09-12) Ported fast and bounded refresh planning and delta-merge execution dependencies.
- [x] (2026-09-12) Ported refresh observability, metrics, slow-log fields, and schedule duration handling.
- [x] (2026-09-12) Added focused refresh regression coverage without importing PR6/PR7 behavior.
- [x] (2026-09-12) Added the out-of-place refresh methods to `ddl.Executor`, `Checker`, and `SchemaTracker` so tracker-backed execution remains interface-complete.
- [x] (2026-09-12) Regenerated Bazel metadata and completed scoped compilation/tests, vet, lint, and diff review.
- [x] (2026-09-13) Added the out-of-place cutover statistics-subscriber branch and a regression test covering old/new stats metadata.
- [x] (2026-09-13) Restored the direct preprocessor shadow-table readability check and a regression test that stops at preprocessing.

## Surprises & Discoveries

- The source `pkg/executor/materialized_view.go` aggregates refresh, compare, cancel, purge, and service helpers. Copying it wholesale would import PR6/PR7 code and duplicate the current standalone purge implementation.
- The current PR4 branch already contains shared task-cancel monitoring and MLog purge helpers. Refresh must reuse compatible helpers or add refresh-specific wrappers without changing purge behavior.
- The final-diff shadow-table audit found that the statistics DDL subscriber does not handle `ActionMViewRefreshOutOfPlaceCutover`; this is a production gap because cutover publishes that event and the subscriber asserts on unknown events.
- The final diff also adds a direct `CheckMViewShadowReadable` call in planner preprocessing. The current branch checks the same condition in the logical-plan and point-get builders, but the preprocessor call itself is still missing and should be restored for path consistency.
- Several final-diff shadow/cutover regression tests are not present in the current branch. They are listed below so future refresh ports do not treat the existing focused tests as complete lifecycle coverage.

## Decision Log

- Decision: Create a dedicated `mv_pr5_refresh_for_master` branch from the current PR4 tip.
  Rationale: The requested sequence is cumulative; PR5 must include PR4's purge implementation and fixes.
  Date/Author: 2026-09-12 / Codex.
- Decision: Port refresh by semantic slices instead of copying the source aggregate executor file.
  Rationale: The source file also contains PR6 compare/show behavior and PR7 service integrations that are explicitly out of scope.
  Date/Author: 2026-09-12 / Codex.
- Decision: Include refresh observability in PR5, including refresh history/metrics, step timing/rows, slow-log integration, and schedule duration; leave service observability/alerts to PR7.
  Rationale: This is the explicit PR5 scope in the master-port tracking document.
  Date/Author: 2026-09-12 / Codex.
- Decision: Treat the statistics subscriber cutover branch as a blocking PR5 parity gap, and treat the preprocessor shadow-read call as a defensive consistency gap.
  Rationale: `ActionMViewRefreshOutOfPlaceCutover` is emitted by the DDL worker and must update old/new physical-table statistics; the downstream planner checks reduce, but do not eliminate, the risk from omitting the preprocessor check.
  Date/Author: 2026-09-13 / Codex.
- Decision: Keep the `pkg/mvservice` cutover case outside PR5.
  Rationale: The final branch contains the whole MV service subsystem, while PR5 is limited to manual refresh and refresh observability. The missing service package is a later-scope difference, not a shadow-table core omission.
  Date/Author: 2026-09-13 / Codex.

## Known Gaps From Final-Diff Shadow Audit

The comparison source is `remotes/xufei/cp_mv_for_master`, compared against the current
`mv_pr5_refresh_for_master` worktree (including its uncommitted PR5 changes).

### Resolved production gaps

- **Resolved (2026-09-13):** `ActionMViewRefreshOutOfPlaceCutover` now follows the
  same new-physical-ID initialization and old-physical-ID delayed-deletion flow as
  truncate in `pkg/statistics/handle/ddl/subscriber.go`. The regression test verifies
  both stats-meta records and the old table's historical schema-change record.
- **Resolved (2026-09-13):** `pkg/planner/core/preprocess.go` directly applies
  `CheckMViewShadowReadable` for `SELECT` table resolution. The regression test calls
  `Preprocess` without invoking later plan builders and verifies that user reads are
  rejected.

### Missing focused tests

The following final-diff tests or assertions have no equivalent found in the current
branch:

- `TestBuildMViewRefreshOutOfPlaceCutoverInvolvingSchemaInfo`;
- cutover constructor/getter assertions in `TestMVAlterEventConstructors`;
- `TestMViewRefreshOutOfPlaceCutoverEventPublished`;
- `TestBuildMVRefreshOutOfPlaceBuildSQLImportOptions`;
- `TestGetRefreshMaterializedViewCompleteOutOfPlaceCutoverArgs`;
- `TestCreateMaterializedViewLogRejectShadowTable`;
- `TestMaterializedViewRefreshOutOfPlaceObserveLoadShadowPlanUsesBuildSQL`;
- `TestMaterializedViewRefreshCompleteOutOfPlaceStatementResult`;
- `TestMaterializedViewRefreshCompleteOutOfPlacePreservesPreviousCommitTSOSnapshot`;
- `TestMaterializedViewRefreshCompleteOutOfPlaceCutoverBasic`;
- `TestMaterializedViewRefreshCompleteOutOfPlaceCutoverFailureRollsBackRefreshInfo`;
- `TestMaterializedViewRefreshCompleteOutOfPlaceShadowTableProtected`;
- `TestMaterializedViewRefreshCompleteOutOfPlaceBuildFailureCleansShadow`;
- `TestMaterializedViewRefreshCompleteOutOfPlaceCancelWatcherStopsBeforeCreateShadow`;
- `TestMaterializedViewRefreshCompleteOutOfPlaceCutoverCASMismatch`;
- `TestMaterializedViewRefreshCompleteOutOfPlaceCutoverRevisionCASMismatch`;
- `TestMaterializedViewRefreshCompleteOutOfPlaceCutoverWithUnsignedBuildReadTSO`;
- `TestCompareMaterializedViewUsesSnapshotMetadataAfterOutOfPlaceCutover`.

### Scope and non-gaps

- `pkg/mvservice/service_helper.go` and the rest of `pkg/mvservice` are absent from
  this branch by design; the service subsystem belongs to the later PR6/PR7 scope.
- `pkg/ddl/job_submitter.go` from the final branch was moved to
  `pkg/ddl/jobsubmit/submit.go`; shadow ID allocation and cutover job table-ID
  tracking are present there.
- No `MPPShadowTable`, `MppShadow`, or equivalent MPP-specific metadata type was
  found in the final diff. The relevant mechanism is `MaterializedViewShadow`.
- The `ActionModifyTableComment` reload path in `pkg/infoschema/builder.go` exists in
  both branches. It is the previously noted affected-options compatibility concern,
  not a newly discovered port omission.

## Outcomes & Retrospective

The branch now contains the PR5 refresh runtime and observability slices on top of PR4:
manual COMPLETE/FAST/bounded refresh, COMPLETE DELTA APPLY, COMPLETE OUT OF PLACE,
refresh cancellation and history, schedule Unix-second/duration metadata, refresh step
profiles, slow-log fallback text, metrics, and the fast-refresh planner/operator stack.
The background `pkg/mvservice` framework, SHOW/COMPARE features, and their tests remain
outside this branch as planned for PR6/PR7. Focused planner, delta-merge, executor
regression, multi-package compile, `go vet`, and Bazel preparation checks passed. Full
integration/failpoint suites were not run; the targeted failpoint-wrapped tests passed.
The subsequent final-diff shadow audit found and resolved one blocking
statistics-subscriber gap and one defensive planner-preprocessor gap. The focused-test
gaps listed above remain, so the branch should not yet be described as fully equivalent
to the final diff.

## Context and Orientation

The authoritative split is `docs/note/materialized_view/mv_master_port_tracking.md` on `cp_mv_for_master_base`. PR5 owns manual refresh, complete/fast/bounded refresh, cancel refresh, refresh history/metrics, refresh schedule Unix-second fields, schedule timezone/duration, and refresh observability. Current PR4 code is in `pkg/executor/mview_log_purge.go`; current DDL/create/alter code already initializes refresh-info metadata but does not execute refresh.

Source reference is `remotes/xufei/cp_mv_for_master`, compared with `cp_mv_for_master_base`. Relevant source paths are `pkg/executor/materialized_view.go`, `pkg/executor/mv_refresh_observability.go`, `pkg/planner/mview`, `pkg/executor/mview_delta_merge_agg_builder.go`, `pkg/executor/mviewdeltamergeagg`, `pkg/metrics/materialized_view.go`, and refresh tests under `pkg/executor/test/executor` and `pkg/planner/core/casetest/mview`.

## Plan of Work

First adapt the refresh AST/planner/executor dispatch to current master names and session APIs. Then implement the manual and complete refresh lifecycle, including refresh-info locking, advisory-lock ownership, target TSO selection, internal maintenance sessions, result reporting, and history/alert updates. Add complete out-of-place and delta-apply behavior where required by the PR5 source semantics.

Next add the `pkg/planner/mview` fast/bounded refresh derivation and the `mviewdeltamergeagg` executor plus builder integration. Reuse aggregate primitives already present in master and preserve current plan codec/build conventions.

Finally add refresh-specific metrics and step observability, schedule-duration accounting, slow-log fields, tests, generated Bazel metadata, and documentation updates. Do not add SHOW/COMPARE or MV service packages in this branch.

## Concrete Steps

Run commands from `/Users/feixu/dev/pingcap/tidb`.

    git status --short --branch
    git diff --check
    gofmt -w <changed Go files>
    make bazel_prepare
    go test ./pkg/executor -run '<targeted refresh tests>' -tags=intest,deadlock -count=1
    go test ./pkg/planner/mview -run '<targeted planner tests>' -tags=intest,deadlock -count=1
    make lint

Packages using failpoints must be run through `./tools/check/failpoint-go-test.sh` and cleaned up by that script.

## Validation and Acceptance

Manual `REFRESH MATERIALIZED VIEW` succeeds for a valid MV and updates its rows and refresh-info success metadata. Complete, fast, and bounded paths choose the correct source data and preserve MLog safety. Cancellation records a canceled refresh history row and releases the advisory lock. Refresh history, metrics, step timing/row fields, slow-log output, and schedule duration are observable through their existing interfaces. Existing PR4 purge tests continue to pass.

## Idempotence and Recovery

Use `git diff` and targeted compilation after each slice. Keep `mv_metrcis.md` untracked and out of commits. If source code requires service-only symbols, replace them with a PR5-local helper or defer that behavior to PR7; do not broaden this branch silently.

## Interfaces and Dependencies

The refresh AST is dispatched through the existing utility-plan and executor-builder paths. Refresh execution uses `mysql.tidb_mview_refresh_info`, `mysql.tidb_mview_refresh_hist`, and existing `TableInfo.MaterializedView` metadata. Fast refresh uses planner output from `pkg/planner/mview` and the `MViewDeltaMergeAgg` executor. Refresh maintenance sessions must use the current `SessionVars` APIs and restore caller/session state before release.
