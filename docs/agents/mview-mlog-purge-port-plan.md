# Port MLog Purge To Master

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date while implementing the MLog purge port.

Reference: `PLANS.md` at the repository root.

## Purpose / Big Picture

After this change, a user can execute `PURGE MATERIALIZED VIEW LOG ON <base_table>` on the master-port branch. The command will find the MLog owned by the base table, serialize concurrent purges with the MLog purge-info row, compute a safe commit-timestamp boundary for all dependent MViews, delete old MLog rows in batches, persist the checkpoint, and record the purge result in the purge-history system table. Internal scheduled purge SQL will also reschedule `NEXT_PURGE_UNIX_SECONDS` using the MLog definition SQL mode and schedule timezone.

The behavior is intentionally scoped to MLog purge. MView refresh, the MView service, and unrelated observability are not ported in this step unless a small shared helper is required by purge correctness.

## Progress

- [x] (2026-09-09) Confirmed the working branch is based on `mv_pr3_mlog_dml_for_master` and compared the final source range `xufei/cp_mv_for_master_base...xufei/cp_mv_for_master`.
- [x] (2026-09-09) Added the purge parser/AST/planner/builder skeleton and runtime schedule-evaluation helper.
- [x] (2026-09-09) Implemented the standalone purge executor and its required SQL helpers.
- [x] (2026-09-09) Added MLog purge session variables and verified the purge-info, purge-history, and refresh-info system-table contracts.
- [x] (2026-09-09) Ported focused purge tests that do not depend on refresh/service code.
- [x] (2026-09-09) Ran formatting, `make bazel_prepare`, targeted compilation, and failpoint-enabled purge tests.
- [x] (2026-09-09) Ran the final `make lint` check and reviewed the final diff against the source range; no refresh/service implementation was included.
- [x] (2026-09-09) Rebased the PR4 branch onto `origin/master` at `91e2f28ac7b` (`#70941`), which contains the PR3 MLog DML port, then restored the shelved purge changes.
- [x] (2026-09-09) Revalidated the rebased baseline: parser inventory tests, executor compilation, and failpoint-enabled purge tests pass.
- [x] (2026-09-09) Ran the final `make lint` and diff self-check after recording the validation results.
- [x] (2026-09-12) Reworked the temporary `_tidb_commit_ts` access guard to use the caller's `tidb_mview_enable` session setting, and made every purge-owned pooled session enable and restore that setting while executing MLog maintenance SQL.
- [x] (2026-09-12) Added regression coverage for disabled/enabled `_tidb_commit_ts` access and the pooled-session purge path. The full `TestPurgeMaterializedViewLog*` suite and the new policy test pass with failpoints enabled.
- [x] (2026-09-12) Added independent source-derived regressions for MLog purge slow logging, the statement DDL flag, truncate/vector-index protections, and purge sysvar defaults and validation. The source MV privilege-model test was excluded because its production implementation is a separate later commit (`ee3965416a`) absent from this purge branch; SHOW CREATE MLog, refresh executor, and MV-service-dependent coverage remain excluded.

## Surprises & Discoveries

- The final source implementation places purge code in `pkg/executor/materialized_view.go`, which also contains refresh, compare, cancellation, and service-support code. The target PR3 branch has no such file, so copying that file would include unrelated later-port functionality.
- The source branch is based on an older release line and uses names that were subsequently refined on the master-port branches. New code must use the current `mview` naming and current session-variable APIs.
- The current branch already contains the three purge-related bootstrap tables and the MLog purge metadata initialized by CREATE/ALTER MLog. The executor can therefore use those tables directly without adding another bootstrap change in PR4.
- The source purge implementation depends on MView refresh-info rows to calculate the safe purge boundary. That is a metadata contract already established by the preceding MView bootstrap/create/alter ports; it is not a reason to port the refresh executor here.
- The isolated executor initially referenced `allocJobID`, which is a private helper in the source aggregate file. The helper only reads `store.CurrentVersion(kv.GlobalTxnScope)`, so it was reproduced locally to keep the isolated port buildable without importing refresh/service code.
- The adaptive TiFlash pending-row path is intentionally best effort. On the mock TiKV store, the TiFlash-only probe has no valid access path and falls back to an unscoped purge; the focused tests still verify deletion and history behavior through the TiKV path.
- The purge DELETE predicate needs `_tidb_commit_ts`. The master preprocessor rejects that hidden column by default; for this temporary port, `tidb_mview_enable=on` is the session-scoped opt-in for both user SQL and purge-owned maintenance sessions.
- PR3 was squash-merged as `91e2f28ac7b`, so the former `mv_pr3_mlog_dml_for_master` tip is not an ancestor of the new master. PR4 had no committed changes at the time; its branch was therefore moved directly to `origin/master` and its uncommitted purge work was restored from a temporary stash.
- Adding `PurgeMaterializedViewLogStmt` also changes the parser AST visitor inventory. The corresponding generated receiver and writeback candidate counts are now updated from `221/148/290` to `222/149/291`; the focused AST inventory tests cover these baselines.

## Decision Log

- Decision: Port behavior from the final source diff rather than cherry-picking the historical purge commits.
  Rationale: The master-port process is defined by the final diff between `cp_mv_for_master_base` and `cp_mv_for_master`, and the source history contains intermediate implementations that no longer describe the final behavior.
  Date/Author: 2026-09-09 / Codex.
- Decision: Keep purge in a new executor file instead of copying the source `materialized_view.go` aggregate file.
  Rationale: The target branch does not yet contain refresh or MV service code; a separate file keeps PR4 independently buildable and avoids silently porting later PRs.
  Date/Author: 2026-09-09 / Codex.
- Decision: Preserve the source purge semantics for missing MLog purge-info rows and metadata inconsistencies: return an error rather than silently creating or skipping required state.
  Rationale: The purge-info row is both the cross-node mutex carrier and the durable checkpoint; silently proceeding would remove the concurrency and recovery guarantees.
  Date/Author: 2026-09-09 / Codex.
- Decision: Keep the purge history fallback job-ID allocation local to the purge executor.
  Rationale: The source helper is private to `pkg/executor/materialized_view.go`; duplicating its small store-version implementation avoids coupling PR4 to the source file's refresh, compare, cancellation, and service code.
  Date/Author: 2026-09-09 / Codex.
- Decision: Do not port MV service or refresh executor behavior in PR4, but retain the purge-owned cancellation monitor.
  Rationale: The monitor polls and heartbeats the purge-history row, so it is required for the lifecycle, cancellation, and recovery semantics of `PURGE MATERIALIZED VIEW LOG`; it does not require MV service or refresh execution.
  Date/Author: 2026-09-09 / Codex.
- Decision: Use the post-merge `origin/master` as the PR4 baseline, rather than retaining the old PR3 branch tip.
  Rationale: PR3 has been squash-merged as `#70941`, so rebasing onto its historical tip would duplicate its MLog DML implementation. The current PR4 diff contains only the purge port on top of master.
  Date/Author: 2026-09-09 / Codex.
- Decision: Gate `_tidb_commit_ts` access with `SessionVars.EnableMView`, which is the session state backed by `tidb_mview_enable`, rather than using restricted-SQL maintenance flags.
  Rationale: This implements the agreed temporary policy without adding another feature switch. Purge-owned internal sessions must set the same variable during their lease because they do not inherit the invoking session's session-scoped value.
  Date/Author: 2026-09-12 / Codex.

## Outcomes & Retrospective

The port now provides an independently buildable `PURGE MATERIALIZED VIEW LOG ON ...` executor. It locks and reads purge-info state, calculates the dependent-MView safe TSO, deletes MLog rows in bounded batches, persists the monotonic checkpoint, records purge history, monitors cancellation and heartbeats, and reschedules internal purges using the persisted schedule timezone and SQL mode. `tidb_mview_enable` also acts as the temporary opt-in for `_tidb_commit_ts`; purge-owned sessions apply and restore it around their maintenance SQL. Focused tests cover manual deletion, explicit-transaction rejection, delete failure preservation, cancellation, heartbeat, result messages, and internal Unix-second rescheduling.

MV refresh execution, MV service scheduling, and service-only observability remain intentionally deferred. The adaptive TiFlash probe is only exercised as a fallback in the mock-store tests because no TiFlash replica exists there; a TiFlash-backed integration test is still a residual validation gap for the performance path.

Validation on the rebased baseline: `make bazel_prepare` completed before the source-only inventory assertion changes; the executor package compiled, both focused parser AST inventory tests passed, the full `TestPurgeMaterializedViewLog*` suite and the `_tidb_commit_ts` policy regression passed with failpoints enabled, and `git diff --check` reported no whitespace errors. The aggregate `make parser_unit_test` was not used as completion evidence because this local Go toolchain reports `go: no such tool "covdata"` for several parser subpackages. The worktree remains intentionally uncommitted and unpushed for review.

## Context and Orientation

TiDB utility statements are represented by an AST node, a planner utility plan, and an executor under `pkg/executor`. MLog rows are written by the PR3 DML path into the physical table named `$mlog$<base_table>`. The base table metadata points to its MLog and dependent MView IDs. The bootstrap work creates `mysql.tidb_mlog_purge_info`, `mysql.tidb_mlog_purge_hist`, and `mysql.tidb_mview_refresh_info`.

`LAST_PURGED_TSO` is a checkpoint: once all MLog rows with `_tidb_commit_ts <= safe_purge_tso` have been removed, the boundary can be persisted. `safe_purge_tso` is the minimum of the current purge transaction start TSO and the last successful read TSO of every dependent public or building MView. This prevents purging rows that a refresh may still need.

## Plan of Work

1. Format and inspect the existing parser/planner/helper skeleton. Fix imports and any generated visitor/parser artifacts without restoring the old generated parser.
2. Add `pkg/executor/mview_log_purge.go`. Implement metadata resolution, `OPERATE VIEW` authorization, explicit-transaction rejection, internal session acquisition/release, pessimistic purge-info row locking with `NOWAIT`, safe-TSO calculation, batched MLog deletion, checkpoint update, and purge-history lifecycle writes.
3. Add only purge-specific helper code required by the executor: SQL escaping, timestamp conversion, failure/cancel status handling, and session-variable restoration. Reuse existing repository helpers when available.
4. Add the MLog purge configuration variables to the existing variable definitions using the branch's current `mview` naming. Keep defaults and bounds aligned with the final source behavior.
5. Add focused unit/integration coverage for the SQL path, lock/checkpoint behavior, dependent-MView safe TSO, and internal schedule rescheduling. Do not add refresh/service tests to this PR.
6. Update Bazel metadata with `make bazel_prepare`, then run the smallest relevant build and test commands. Review the final diff against the source final range and confirm no refresh/service implementation was included accidentally.

## Concrete Steps

Run commands from `/Users/feixu/dev/pingcap/tidb`.

    git diff --check
    gofmt -w <changed Go files>
    make bazel_prepare
    go test ./pkg/executor -run '^$' -count=1
    ./tools/check/failpoint-go-test.sh pkg/executor/test/mview -run '^TestPurgeMaterializedViewLog' -count=1
    make lint
    git diff --stat
    git diff --check

If a package contains failpoint references, enable failpoints before its tests using `./tools/check/failpoint-go-test.sh` and rely on that script for cleanup, as required by `docs/agents/testing-flow.md`.

## Validation and Acceptance

Acceptance requires all of the following:

- `PURGE MATERIALIZED VIEW LOG ON t` parses, plans, and executes against a base table with an MLog.
- Rows at or below the safe TSO are deleted in batches, and rows above it remain.
- A second concurrent purge of the same MLog fails fast on the purge-info row lock.
- The checkpoint never moves backwards and a completed purge records a final history status.
- A dependent MView's refresh-info state limits the purge boundary; a missing required public refresh-info row returns a metadata error.
- Internal scheduled purge updates the persisted next Unix-second value using the captured schedule timezone; user-issued purge does not reschedule it.
- The changed packages compile and targeted tests pass; generated Bazel metadata is included when required.

Evidence collected on 2026-09-09:

- `go test ./pkg/executor -run '^$' -count=1` passed after adding the local `allocJobID` helper.
- `./tools/check/failpoint-go-test.sh pkg/executor/test/mview -run '^TestPurgeMaterializedViewLog' -count=1` passed the full purge suite. The mock TiKV run logged the expected best-effort TiFlash probe fallback.
- `./tools/check/failpoint-go-test.sh pkg/executor/test/mview -run '^TestMViewEnableControlsMLogCommitTSAccessAndPurge$' -count=1` passed the `_tidb_commit_ts` gate and pooled-session regression.
- `make bazel_prepare` completed before the final helper-only change and generated the required executor/expression BUILD metadata. A later source-only helper edit did not change imports or test targets.

## Idempotence and Recovery

Formatting, code generation, and targeted tests are safe to rerun. If a test fails after a partial purge, use the test's isolated mock store or clean test database; do not reset the worktree. If `make bazel_prepare` changes generated BUILD metadata, retain those changes and inspect them before committing. If an implementation attempt starts importing refresh/service-only symbols, stop and replace those calls with a purge-local helper or defer that feature to its planned PR.

## Artifacts and Notes

The source reference is `xufei/cp_mv_for_master:pkg/executor/materialized_view.go`, especially the final purge executor and helper sections around the source functions `executePurgeMaterializedViewLog`, `calcMaterializedViewLogSafePurgeTSO`, `resolvePurgeMaterializedViewLogMeta`, `acquireMaterializedViewLogPurgeLock`, `purgeMaterializedViewLogData`, and the purge-history helpers.

## Interfaces and Dependencies

The target implementation must connect:

- `pkg/parser/ast.PurgeMaterializedViewLogStmt` to `pkg/planner/core.PurgeMaterializedViewLog` and `pkg/executor.PurgeMaterializedViewLogExec`.
- `pkg/executor.PurgeMaterializedViewLogExec.Next` to a standalone execution flow tagged with `kv.InternalTxnMVMaintenance`.
- `pkg/meta/model.MaterializedViewLogInfo` and the base table's MView metadata to the purge dependency calculation.
- `pkg/util/sqlescape` for all generated SQL identifiers and values.
- `pkg/util/sqlexec` and pooled internal sessions for system-table reads/writes and MLog deletion.
- `pkg/expression.DeriveMaterializedScheduleNextTime` for runtime internal schedule evaluation.

## Change Note: 2026-09-12

The master baseline keeps `_tidb_commit_ts` disabled by default. For this temporary MLog purge port, `tidb_mview_enable=on` is also the opt-in for that hidden column. The preprocessor must reject it while the setting is off and permit it while the setting is on. Every pooled session used by purge must enable the setting for the duration of its maintenance SQL, then restore the prior value before release.

The executor should expose no new public API beyond the planner/executor types needed by the existing utility-statement dispatch.
