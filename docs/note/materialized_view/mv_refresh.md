# Materialized View Refresh (Implementation and Design Notes)

This document describes the current implementation and the next evolution steps for TiDB `REFRESH MATERIALIZED VIEW` (`COMPLETE` / `FAST`).

At the moment:

- `COMPLETE` refresh is implemented with transactional semantics.
- `FAST` refresh is implemented and uses the same transactional framework as `COMPLETE`.

## Runtime `NEXT_TIME` Update (Internal SQL Success Path)

- For **internal SQL** triggered refresh (identified by `SessionVars.InRestrictedSQL`), after a successful refresh commit, `mysql.tidb_mview_refresh_info.NEXT_TIME` should be updated together with success metadata.
- Runtime `NEXT_TIME` derivation in this path is intentionally different from create-time derivation:
  - evaluate and use only `RefreshNext` expression;
  - do not apply create-time `START WITH` priority / near-now rules;
  - if `RefreshStartWith` is non-empty and `RefreshNext` is empty, explicitly set `NEXT_TIME = NULL`;
  - if both are empty, keep `NEXT_TIME` unchanged.
- For non-internal (user) SQL refresh, keep existing behavior (do not update `NEXT_TIME` on success path).

> Note: refresh metadata and refresh history are now split:
> - `mysql.tidb_mview_refresh_info` stores per-MV metadata for next refresh (for example `LAST_SUCCESS_READ_TSO`).
> - `mysql.tidb_mview_refresh_hist` stores per-refresh lifecycle and results (`running/success/failed`).
> See `pkg/session/bootstrap.go` for system table definitions.

## Goals (scope of the current implementation)

1. **Transactional all-or-nothing for MV data**: one refresh must commit or roll back data replacement atomically.
2. **Concurrency mutex**: for one MV, when multiple sessions refresh concurrently, only one can enter the execution path; others fail immediately with a locking error.
3. **Success-only refresh metadata update with CAS + double check**: only when refresh succeeds, update the MV row in `mysql.tidb_mview_refresh_info`, especially:
   - read `refresh_read_tso` from `TxnCtx.GetForUpdateTS()`
   - update `LAST_SUCCESS_READ_TSO` by CAS-style condition
     (`WHERE MVIEW_ID = <mview_id> AND LAST_SUCCESS_READ_TSO <=> <locked_tso>`)
   - re-read `LAST_SUCCESS_READ_TSO` and require it equals `refresh_read_tso`, otherwise fail refresh as inconsistent
4. **Refresh lifecycle history**: after lock acquisition, insert a `running` row into `mysql.tidb_mview_refresh_hist` using an independent session.
   - `REFRESH_JOB_ID` uses this refresh's `start_tso`.
5. **Finalize history after refresh commit**: after refresh transaction commit outcome is known, update the same history row to `success` or `failed`.
6. **Usable COMPLETE refresh**: do full data replacement with transactional `DELETE + INSERT`.
7. **Usable FAST refresh**: run fast refresh through an internal statement path and incremental merge execution.
8. **Privilege semantics scoped to MVP**: for outer SQL semantics, only check `ALTER` on MV; run refresh with internal sessions so system-table privileges on `mysql.tidb_mview_refresh_info` / `mysql.tidb_mview_refresh_hist` do not leak to business users.

## Non-goals (not included yet)

- Async execution semantics for `WITH ASYNC MODE`.
  The syntax is reserved by spec, but async refresh is not implemented yet.
  `REFRESH MATERIALIZED VIEW ... WITH ASYNC MODE ...` is parsed and rejected by executor.
- Performance optimization for large MVs (for example large-transaction mitigation, delete cost reduction, swap table strategies).
- Long-term retention/cleanup strategy for `mysql.tidb_mview_refresh_hist` (TTL/archival policy).

## Data and metadata sources

- MV physical storage is a normal table marked by `TableInfo.MaterializedView != nil`.
- MV definition SQL is stored in `TableInfo.MaterializedView.SQLContent`, canonical `SELECT ...`.
  See `pkg/meta/model/table.go` and `pkg/ddl/materialized_view.go`.
- Refresh metadata table:
  - `mysql.tidb_mview_refresh_info` (PK `MVIEW_ID`, fields include success metadata used by next refresh, for example `LAST_SUCCESS_READ_TSO`).
- Refresh history table:
  - `mysql.tidb_mview_refresh_hist` (per-job lifecycle/status, primary key is `REFRESH_JOB_ID`; each row also stores `MVIEW_ID`).

`MVIEW_ID` directly uses MV physical table `TableInfo.ID`.

## Create-time `NEXT_TIME` Initialization (`CREATE MATERIALIZED VIEW`)

`REFRESH MATERIALIZED VIEW` relies on an existing row in `mysql.tidb_mview_refresh_info`.

When `CREATE MATERIALIZED VIEW` succeeds, DDL worker initializes (or upserts) that row with:

- `MVIEW_ID`
- initial `LAST_SUCCESS_READ_TSO` (from create-time initial build read tso)
- `NEXT_TIME` (derived from create-time schedule expressions)

Create-time `NEXT_TIME` derivation rules (for `RefreshStartWith` / `RefreshNext`) are:

1. If both are empty, do not update `NEXT_TIME` (row keeps default `NULL`).
2. Evaluate expressions in prepared eval session (`UTC` timezone + DDL job SQL mode).
3. `START WITH` has higher priority, unless it is near-now (`START WITH < now + 10s`) and `NEXT` exists; in that case use `NEXT`.
4. If the chosen expression evaluates to `NULL`, explicitly write `NEXT_TIME = NULL`.

This create-time rule set is intentionally different from runtime internal-refresh reschedule rule:

- runtime internal refresh uses `RefreshNext` only;
- runtime internal refresh does not apply create-time `START WITH`/near-now priority.

## SQL behavior (user view)

Supported syntax (all use one common transactional framework today):

```sql
REFRESH MATERIALIZED VIEW db.mv COMPLETE;
REFRESH MATERIALIZED VIEW mv COMPLETE; -- uses current DB
REFRESH MATERIALIZED VIEW mv WITH ASYNC MODE COMPLETE; -- parsed, but rejected: async refresh is not supported yet

REFRESH MATERIALIZED VIEW mv FAST;
REFRESH MATERIALIZED VIEW mv WITH ASYNC MODE FAST; -- parsed, but rejected: async refresh is not supported yet
```

Current note: `FAST` requires `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO` to be non-`NULL`; otherwise refresh fails.

Current privilege semantics (MVP):

- `REFRESH MATERIALIZED VIEW` requires `ALTER` privilege on target MV (outer semantic privilege).
- Internal `DELETE/INSERT`, `mysql.tidb_mview_refresh_info` updates, and `mysql.tidb_mview_refresh_hist` writes run on internal sessions, so caller does not need direct DML privilege on those system tables.
- If finer-grained privilege semantics are introduced later (for example base-table `SELECT` checks), extend from this MVP baseline.

## Core execution flow (transactional refresh framework)

The most direct implementation is: transaction + row-lock mutex + history lifecycle + data refresh + success-metadata update.

`COMPLETE` and `FAST` share the same outer framework; only the "refresh implementation" step differs.

1. Get an internal session from session pool and start a transaction on it (recommended **pessimistic**, so `FOR UPDATE NOWAIT` works immediately).
2. In transaction, lock refresh-info row by `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh_info` (used as refresh mutex), and remember the locked row's `LAST_SUCCESS_READ_TSO` value (nullable).
3. Record refresh `start_tso` as `REFRESH_JOB_ID`.
4. Use an independent session to insert one `mysql.tidb_mview_refresh_hist` row with `REFRESH_STATUS='running'` and `REFRESH_JOB_ID=<start_tso>`.
5. Run refresh implementation by refresh type:
   - `COMPLETE`: `DELETE FROM <mv_table>` + `INSERT INTO <mv_table> <mv_select_sql>`.
   - `FAST`: construct internal statement and run via `ExecuteInternalStmt` to apply incremental changes.
6. Success path: read `refresh_read_tso` from transaction context (`TxnCtx.GetForUpdateTS()`).
7. Before commit, persist success metadata with CAS-style SQL:
   - `UPDATE ... SET LAST_SUCCESS_READ_TSO = <refresh_read_tso> WHERE MVIEW_ID = <mview_id> AND LAST_SUCCESS_READ_TSO <=> <locked_tso>`.
   - runtime internal-SQL rule: update `NEXT_TIME` by evaluating only `RefreshNext`; if `RefreshStartWith != ''` and `RefreshNext == ''`, set `NEXT_TIME = NULL`.
8. Do double check by reading back `LAST_SUCCESS_READ_TSO` from `mysql.tidb_mview_refresh_info`:
   - if value is `NULL` or not equal to `<refresh_read_tso>`, treat as unknown inconsistency and fail refresh.
9. Commit refresh transaction.
10. After commit returns success, use independent session to update `mysql.tidb_mview_refresh_hist` for this `REFRESH_JOB_ID` to `REFRESH_STATUS='success'` and fill completion fields.

Failure path (for example `INSERT INTO ... SELECT ...` fails):

1. `ROLLBACK` the refresh transaction to roll back MV data changes (no partial MV data update).
2. Do **not** update `mysql.tidb_mview_refresh_info` (failure does not change success watermark).
3. After refresh transaction finishes and failure is known, use independent session to update `mysql.tidb_mview_refresh_hist` for this `REFRESH_JOB_ID`:
   - `REFRESH_STATUS='failed'`
   - failure reason / error message
   - completion timestamp.
4. Return original error to user.

Pseudo SQL (key points only):

```sql
-- refresh transaction SQL runs on one internal session
BEGIN PESSIMISTIC;

-- (A) mutex: lock row; if NOWAIT fails, fail immediately
SELECT MVIEW_ID, LAST_SUCCESS_READ_TSO
  FROM mysql.tidb_mview_refresh_info
 WHERE MVIEW_ID = <mview_id>
 FOR UPDATE NOWAIT;
-- locked_last_success_read_tso := row.LAST_SUCCESS_READ_TSO (nullable)

-- (A2) use transaction start_tso as refresh job id
-- refresh_job_id := <start_tso>;

-- (A3) independent internal session (not this transaction) inserts running history
INSERT INTO mysql.tidb_mview_refresh_hist (
    MVIEW_ID, REFRESH_JOB_ID, REFRESH_METHOD, REFRESH_TIME, REFRESH_STATUS
) VALUES (
    <mview_id>, <refresh_job_id>, <refresh_method>, NOW(6), 'running'
);

-- (B) full replacement
DELETE FROM <db>.<mv>;
-- note: in strict mode TiDB normally blocks TiFlash/MPP on the SELECT part of a write statement.
-- for internal MV maintenance, internal session can set a dedicated flag
-- (for example `SessionVars.InMaterializedViewMaintenance`) so optimizer bypasses that strict-mode guard,
-- allowing the SELECT side of INSERT ... SELECT to use TiFlash/MPP.
INSERT INTO <db>.<mv> <SQLContent>;
  -- SQLContent is MV definition SELECT (rollback whole refresh txn on failure)
  -- so COMPLETE refresh can leverage TiFlash for heavy scans.

-- (C1) read refresh tso from transaction context
-- refresh_read_tso := <TxnCtx.GetForUpdateTS()>;

-- (C2) success-only metadata update in the same refresh transaction (CAS style)
UPDATE mysql.tidb_mview_refresh_info
   SET LAST_SUCCESS_READ_TSO = <refresh_read_tso>
   -- internal SQL path only:
   --   1) if RefreshNext is non-empty: NEXT_TIME = eval(RefreshNext)
   --   2) else if RefreshStartWith is non-empty: NEXT_TIME = NULL
   --   3) else: NEXT_TIME unchanged
   NEXT_TIME = <runtime_derived_or_unchanged>
 WHERE MVIEW_ID = <mview_id>
   AND LAST_SUCCESS_READ_TSO <=> <locked_last_success_read_tso>;

-- (C3) double check after UPDATE
SELECT LAST_SUCCESS_READ_TSO
  FROM mysql.tidb_mview_refresh_info
 WHERE MVIEW_ID = <mview_id>;
-- if result is NULL or != <refresh_read_tso>, fail refresh as inconsistent

COMMIT;

-- (D) independent internal session finalizes history AFTER refresh commit
UPDATE mysql.tidb_mview_refresh_hist
   SET REFRESH_STATUS = 'success',
       REFRESH_ENDTIME = NOW(6),
       REFRESH_READ_TSO = <refresh_read_tso>,
       REFRESH_FAILED_REASON = NULL
 WHERE MVIEW_ID = <mview_id>
   AND REFRESH_JOB_ID = <refresh_job_id>;

-- (D-failed) if refresh transaction ends as failure, finalize the same row as failed
UPDATE mysql.tidb_mview_refresh_hist
   SET REFRESH_STATUS = 'failed',
       REFRESH_ENDTIME = NOW(6),
       REFRESH_READ_TSO = NULL,
       REFRESH_FAILED_REASON = <refresh_error>
 WHERE MVIEW_ID = <mview_id>
   AND REFRESH_JOB_ID = <refresh_job_id>;
```

### Lock behavior and error semantics

For `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh_info`, there are 3 outcomes:

1. **Returns 1 row**: lock acquired, refresh can continue.
2. **Returns lock-conflict error**: another session is refreshing (or at least holding this row lock).
   - Typical TiDB/MySQL error code is `3572` (`ErrLockAcquireFailAndNoWaitSet`).
   - MVP can pass through this error directly; a friendlier wrapper is also acceptable
     (for example "another session is refreshing this materialized view").
3. **No error, 0 rows**: missing `MVIEW_ID` row in system table.
   - This is metadata inconsistency and should fail the refresh.

### Why pessimistic transaction

`FOR UPDATE NOWAIT` is meaningful only inside a transaction and should fail immediately on conflict.
Explicit `BEGIN PESSIMISTIC` ensures lock acquisition and conflict behavior match mutex semantics.

### Refresh read tso (`for_update_ts`)

Requirement: on successful COMPLETE refresh, `LAST_SUCCESS_READ_TSO` must store the transaction `for_update_ts` used for refresh read.

Reason: in `BEGIN PESSIMISTIC`, DML reads (such as `INSERT INTO ... SELECT ...`) use `for_update_ts`.
So MV data snapshot corresponds to `for_update_ts`.
If only `start_ts` is stored, users may observe that MV data includes rows newer than `LAST_SUCCESS_READ_TSO`,
which also misleads later incremental-refresh/check logic.

The same success-path read-tso persistence rule is used for both `COMPLETE` and `FAST`.

Current code path (in-place COMPLETE / FAST) reads refresh success tso from transaction context:

1. after refresh data changes, call `sctx.GetSessionVars().TxnCtx.GetForUpdateTS()`
2. if the value is `0`, fail refresh
3. persist it to `LAST_SUCCESS_READ_TSO` through CAS update + post-update readback check.

## Code placement (current implementation)

`REFRESH MATERIALIZED VIEW` is a utility/maintenance statement and does not enter DDL job queue.
Execution path:

1. Parser/AST:
   - `RefreshMaterializedViewStmt` and `RefreshMaterializedViewImplementStmt` are defined in `pkg/parser/ast/misc.go`.
   - parser grammar parses `REFRESH MATERIALIZED VIEW` under generic `Statement` branch in `pkg/parser/parser.y`.
2. Planner:
   - `PlanBuilder.buildRefreshMaterializedView` builds plan and enforces outer privilege check (MVP: `ALTER` on MV).
3. Executor:
   - executor builder maps plan to `RefreshMaterializedViewExec`.
   - `RefreshMaterializedViewExec` runs refresh service directly (`Validate + Lock + HistRunning Persist + DataChanges + SuccessInfo Persist + Commit + HistFinalize`).

Core execution semantics:

- Refresh uses internal session, not caller session transaction/variables.
- Refresh path uses dedicated internal source type (`kv.InternalTxnMVMaintenance`).
- For in-place COMPLETE / FAST, uses `BEGIN PESSIMISTIC` + `SELECT ... FOR UPDATE NOWAIT` on `mysql.tidb_mview_refresh_info` for mutex.
- For in-place COMPLETE / FAST success path, updates `LAST_SUCCESS_READ_TSO` with CAS condition (`LAST_SUCCESS_READ_TSO <=> <locked_tso>`) and verifies readback equals `TxnCtx.GetForUpdateTS()`.
- For in-place COMPLETE / FAST execution failure, rolls back the whole refresh transaction to guarantee all-or-nothing MV data replacement.
- `COMPLETE` rebuilds data with `DELETE FROM mv` + `INSERT INTO mv SELECT ...`.
- `FAST` uses internal-only statement `RefreshMaterializedViewImplementStmt` and a dedicated incremental merge plan.
- `COMPLETE OUT OF PLACE` uses a dedicated execution path (not the above refresh transaction):
  - build stage runs in independent internal session(s) outside explicit refresh transaction;
  - cutover and `mysql.tidb_mview_refresh_info` migration/update are done atomically in DDL worker transaction.
- For `FAST`, executor constructs `RefreshMaterializedViewImplementStmt` with:
  - original `RefreshMaterializedViewStmt` (must be `Type=FAST`)
  - `LAST_SUCCESS_READ_TSO` value (must be non-`NULL` uint64 / `BIGINT UNSIGNED`)
- `FAST` execution goes through `ExecuteInternalStmt(ctx, stmtNode)`.
- If `ExecuteInternalStmt` returns non-nil `RecordSet`, refresh drains it before `Close()` to guarantee full executor-tree execution.
- `RefreshMaterializedViewStmt` is a normal `StmtNode` with no DDL-statement semantics
  (for example it does not set `LastExecuteDDL` flag).
- Statement is forbidden inside explicit user transactions (`BEGIN` / `START TRANSACTION`),
  and must run as standalone autocommit statement.

## Next phases

### Support out-of-place COMPLETE refresh (decouple build and cutover)

For out-of-place COMPLETE refresh, the recommended model is "utility main flow + DDL sub-steps":

1. Utility stage: build shadow data (new table or temporary physical object).
2. Cutover stage: run dedicated DDL sub-step for atomic switch (metadata/schema-level operation).
3. Utility stage: clean old objects and update refresh metadata.

1. Parse and validate refresh mode:
   - accept `REFRESH MATERIALIZED VIEW ... COMPLETE OUT OF PLACE`;
   - reject `FAST OUT OF PLACE`;
   - keep existing `COMPLETE` (without `OUT OF PLACE`) and `FAST` paths unchanged.
2. Entry lock and history initialization:
   - reuse unified advisory lock in `executeRefreshMaterializedView` as outer mutex for the whole out-of-place flow;
   - insert `running` row into `mysql.tidb_mview_refresh_hist` before heavy work.
   - do not reuse the current in-place refresh transaction (`BEGIN PESSIMISTIC` + row-lock + CAS) for build stage.
3. Build shadow table from current MV physical table:
   - out-of-place build runs in dedicated internal session with autocommit semantics;
   - create shadow by `CREATE TABLE <shadow> LIKE <mv>`;
   - shadow table must remain a normal table during build (`MaterializedView == nil`);
   - rely on existing `CREATE TABLE ... LIKE` behavior (copy physical schema/index/table options; follow TiFlash replica-state handling of LIKE path).
   - do not run this step inside refresh transaction:
     - DDL (`CREATE TABLE`) has implicit transaction-commit semantics;
     - `IMPORT INTO` is rejected in explicit transaction.
4. Populate shadow table and capture build tso:
   - load data into shadow (prefer `IMPORT INTO ... FROM (<mv_select_sql>)`, fallback path only when required by environment limits);
   - capture the build read tso from build session `@@tidb_last_query_info.start_ts`.
5. Submit dedicated cutover DDL job/action:
   - add a new DDL action dedicated to out-of-place COMPLETE cutover;
   - job args should include at least `old_mv_id`, `shadow_table_id`, `build_read_tso` (and required schema/name context).
6. Execute cutover atomically in DDL worker:
   - keep MV logical name unchanged for users: cutover action is responsible for table-name handover
     (shadow takes original MV name; old physical table is renamed/drop-handled internally);
   - move MV definition metadata to shadow target;
   - update base-table reverse references (`MaterializedViewBase.MViewIDs`) from old ID to shadow ID;
   - lock and update `mysql.tidb_mview_refresh_info` in the same DDL transaction:
     - migrate ownership from old `MVIEW_ID` to shadow ID;
     - set `LAST_SUCCESS_READ_TSO = build_read_tso`;
     - preserve `NEXT_TIME` semantics;
   - convert old MV table metadata to normal table form before final drop if required by current metadata constraints.
7. Finalization and cleanup:
   - on successful cutover, finalize history row to `success`;
   - on failure, finalize history row to `failed`, keep old MV serving path unchanged, and do best-effort shadow cleanup;
   - release advisory lock in deferred cleanup before returning pooled internal session.

Detailed development plan (recommended implementation order):

1. Freeze behavior contract before code changes:
   - keep semantic matrix stable: `COMPLETE` (in-place), `COMPLETE OUT OF PLACE`, `FAST` (existing), and reject `FAST OUT OF PLACE`;
   - align user-visible error messages for unsupported mode combinations and cutover failures.
2. Extend parser and AST for mode expression:
   - add optional `OUT OF PLACE` clause to `REFRESH MATERIALIZED VIEW`;
   - carry mode in `RefreshMaterializedViewStmt` (for example `OutOfPlace` flag);
   - update parser tokens/keywords (`PLACE` token is required);
   - update parser/AST restore tests.
3. Add early mode validation in executor entry:
   - in `validateRefreshMaterializedViewStmt`, reject invalid combinations (for example `FAST + OUT OF PLACE`);
   - keep privilege and transaction-boundary checks unchanged.
4. Introduce out-of-place COMPLETE executor path:
   - branch from existing refresh execution flow using `(Type == COMPLETE && OutOfPlace)`;
   - reuse existing outer advisory-lock lifecycle and refresh-history lifecycle;
   - bypass current in-place/fast refresh transaction flow (`BEGIN PESSIMISTIC` + row-lock + CAS).
5. Implement shadow-table build stage:
   - use dedicated internal build session with autocommit semantics;
   - create shadow table via `CREATE TABLE <shadow> LIKE <mv>`;
   - ensure shadow remains a normal table during build (`MaterializedView == nil`);
   - use deterministic unique shadow naming and keep cleanup hooks for failures.
6. Implement shadow data load and build tso capture:
   - load `mv_select_sql` results into shadow (prefer `IMPORT INTO ... FROM (<mv_select_sql>)`);
   - capture `build_read_tso` from build session `@@tidb_last_query_info.start_ts` for cutover metadata update.
7. Add dedicated DDL action/job for cutover:
   - create new DDL action type and job args (at least `old_mv_id`, `shadow_table_id`, `build_read_tso`);
   - add DDL API submission/wait helper used by refresh executor.
8. Implement DDL worker cutover logic (single atomic action):
   - keep MV logical name unchanged for users (shadow table takes original MV name);
   - move MV definition metadata to shadow target;
   - rewrite base-table reverse references (`MaterializedViewBase.MViewIDs`) from old ID to shadow ID;
   - lock/migrate `mysql.tidb_mview_refresh_info` row ownership (`MVIEW_ID` old to new), set `LAST_SUCCESS_READ_TSO = build_read_tso`, and preserve `NEXT_TIME` in the same DDL transaction;
   - drop/cleanup old physical table after metadata handover.
9. Integrate finalization and failure semantics:
   - finalize refresh history as `success` only after cutover success;
   - on build/cutover failure, finalize as `failed`, keep old MV serving path unchanged, and do best-effort shadow cleanup;
   - ensure advisory lock is released before internal session returns to pool.
10. Add targeted tests for each layer:
   - parser tests for `COMPLETE OUT OF PLACE` and reject cases;
   - executor tests for success/failure/concurrency behavior;
   - DDL tests for cutover metadata correctness, logical name continuity, old-table cleanup, and refresh-info migration.
11. Run required validation and repo checks:
   - run failpoint-aware targeted tests in affected packages;
   - if Go files are added/moved/renamed, run `make bazel_prepare`;
   - run `make bazel_lint_changed` before final submission.
12. Submit in small reviewable commits:
   - parser/AST and tests;
   - executor build path and tests;
   - DDL cutover action and tests;
   - final doc adjustment if behavior changed during implementation.

### Support FAST refresh upper bound with `AS OF TIMESTAMP`

Another planned evolution for `FAST` refresh is to allow users to specify a refresh upper bound
with `AS OF TIMESTAMP`, so one large backlog can be applied in smaller windows instead of forcing
one refresh to catch up all the way to "now".

This section records the intended semantics and implementation constraints before code changes.

#### Scope

1. Only `FAST` refresh should support this feature.
2. `COMPLETE` / `COMPLETE OUT OF PLACE` / `COMPLETE DELTA APPLY` do not need this option.
3. `AS OF TIMESTAMP` here should be treated as the refresh apply upper bound, not as generic
   statement stale-read semantics for the whole refresh statement.

#### Problem statement

When `FAST` refresh falls behind for a long time, applying all changes in one refresh may create
an overly large transaction and fail.

Desired user-facing behavior:

1. lower bound remains the previous successful watermark:
   `FROM_TS = LAST_SUCCESS_READ_TSO`
2. user specifies one target upper bound:
   `TARGET_TS = tso(parsed from AS OF TIMESTAMP)`
3. this refresh applies only changes in `(FROM_TS, TARGET_TS]`
4. after success, the persisted refresh watermark advances to `TARGET_TS`

This allows users to manually move the watermark forward in multiple smaller steps.

#### Timestamp model

With this feature, one `FAST` refresh must distinguish three timestamps:

1. `fromTS`:
   - previous successful refresh watermark
   - loaded from `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO`
2. `targetTSO`:
   - user-specified apply upper bound from `AS OF TIMESTAMP`
   - this is the logical "refresh up to here" watermark
3. `writeTxnTSO`:
   - the refresh transaction's current `for_update_tso`
   - used by the outer refresh statement and MV/MV-log readers in the existing refresh txn

Required invariants:

1. `targetTSO >= fromTS`
   - if `targetTSO < fromTS`, refresh should fail
   - if `targetTSO == fromTS`, implementation may treat refresh as a no-op success
2. `targetTSO <= writeTxnTSO`
   - refresh must not claim to apply beyond the current transaction read horizon
3. `targetTSO` must pass normal snapshot/gc-safe-point validation before execution starts

Most importantly, after success the persisted watermark must become `targetTSO`, not `writeTxnTSO`.

Reason:

- `writeTxnTSO` and `targetTSO` diverge with this feature;
- if success still persists `writeTxnTSO`, then `(targetTSO, writeTxnTSO]` would be skipped forever
  by the next `FAST` refresh.

#### Read snapshot split inside one refresh

The intended read-snapshot split is:

1. MV table:
   - read at current refresh transaction `writeTxnTSO`
2. MV log table:
   - read at current refresh transaction `writeTxnTSO`
   - but delta extraction must explicitly filter `_tidb_commit_ts > fromTS AND _tidb_commit_ts <= targetTSO`
3. base table:
   - full-update / min-max recompute paths must read at `targetTSO`

Why this split is correct:

1. MV table:
   - MV does not contain rows beyond the previous successful watermark
   - refresh execution is serialized by existing refresh mutex semantics
   - so reading MV at current `writeTxnTSO` is acceptable
2. MV log:
   - log visibility can use current transaction snapshot, as long as logical delta window is
     restricted by explicit commit-ts predicates
3. base table:
   - recomputation logic must see the base-table state exactly at `targetTSO`
   - otherwise rows committed in `(targetTSO, writeTxnTSO]` may leak into group recomputation and
     make refresh results "too new"

This mixed-snapshot model is intentional and specific to MV refresh maintenance.

#### Why not use generic statement/table stale read directly

Current TiDB stale-read processing rejects `AS OF TIMESTAMP` inside an already opened transaction.

That means this feature should not be implemented by turning the entire refresh statement into one
generic stale-read statement, nor by directly attaching SQL-layer `AS OF TIMESTAMP` to base-table
references inside the existing refresh write transaction.

Instead, the design should keep:

1. the outer refresh statement and transaction model unchanged
2. `targetTSO` as a dedicated MV-refresh internal concept
3. special handling only for the base-table reader(s) inside `mvmerge`

In other words, this feature is "bounded fast refresh", not "refresh statement stale read".

#### Base-table read path

The cleanest execution model is:

1. `mvmerge` as a whole still runs under the current refresh transaction and uses `writeTxnTSO`
   for its normal child plan
2. the base-table full-update / min-max recompute sub-plan uses a dedicated inner reader that reads
   at `targetTSO`
3. this snapshot override should happen below SQL syntax level, inside planner/executor wiring,
   rather than by injecting generic SQL `AS OF TIMESTAMP`

Implementation notes:

1. planner/executor contract should carry `targetTSO` explicitly for `FAST` refresh
2. `mvmerge.BuildOptions` should grow from only `FromTS` to `FromTS + ToTS`
3. mlog delta SQL generation must really emit both bounds:
   `_tidb_commit_ts > FromTS AND _tidb_commit_ts <= ToTS`
4. full-update/min-max recompute readers should get a dedicated read-ts override equal to `targetTSO`
5. that inner reader should also be treated as stale read at request level, even if the outer refresh
   statement itself is not a generic stale-read statement

#### Metadata and schema assumptions

This design relies on two existing MV constraints:

1. refresh execution for one MV is serialized by the existing mutex path
2. base tables with MV dependencies already block relevant DDL operations

Because of these constraints, planning base-table recompute with the current InfoSchema and reading
data at `targetTSO` is acceptable for this feature.

#### GC-safe-point protection

If users want to refresh an old backlog in many small windows, ordinary snapshot validation alone is
not sufficient. GC may already have advanced beyond the desired historical target, or may advance
during a long-running refresh.

The recommended protection model has two layers.

##### 1. Persistent backlog protection (opt-in per MV)

Introduce one MV-level option, for example a `block gc` style flag.

Semantics:

1. default should be disabled
2. only MVs with this option enabled participate in GC blocking
3. the owner/service side periodically computes:
   `MIN(LAST_SUCCESS_READ_TSO)` across opted-in MVs
4. TiDB then publishes that value through PD service safe point

Rationale:

1. if an MV falls far behind and the user still wants to recover by repeated bounded fast refresh,
   GC must not pass below that MV's current watermark
2. making this behavior opt-in avoids pinning cluster GC unexpectedly for all MVs

Operational notes:

1. if no MV opts in, the MV service safe point should be removed
2. documentation should make the trade-off explicit: a stalled opted-in MV can hold back cluster GC

##### 2. Per-refresh execution protection

A second, temporary protection is still needed for each bounded fast refresh execution.

Semantics:

1. before running one `FAST ... AS OF TIMESTAMP ...` refresh, publish a temporary service safe point
   at `targetTSO`
2. keep it for the whole refresh execution
3. remove it after the refresh finishes (success or failure)

Rationale:

1. even if `targetTSO` is valid when refresh starts, GC may advance during a long refresh
2. the outer refresh transaction's own `startTS` / `for_update_tso` does not protect this older
   historical snapshot automatically

#### Recommended implementation shape

1. Extend parser/AST for `FAST` refresh to carry an optional `AS OF TIMESTAMP` expression.
2. Parse/evaluate that expression into `targetTSO` during refresh preparation.
3. Keep current outer refresh transaction framework unchanged.
4. Carry both `fromTS` and `targetTSO` into `mvmerge` build/execution.
5. Read MV at `writeTxnTSO`.
6. Read MV log at `writeTxnTSO`, but explicitly filter `_tidb_commit_ts` into `(fromTS, targetTSO]`.
7. Read base-table recompute paths at `targetTSO` through dedicated inner reader wiring.
8. Persist `LAST_SUCCESS_READ_TSO = targetTSO` on success.
9. If enabled for the MV, maintain persistent GC protection from the minimum opted-in watermark.
10. For every bounded fast refresh execution, hold one temporary service safe point at `targetTSO`.

This feature should be implemented as an MV-refresh-specific extension of `FAST` refresh,
not as generic in-transaction stale-read SQL.

### Support COMPLETE DELTA APPLY (full compute, delta apply)

After `COMPLETE OUT OF PLACE`, the next refresh mode is:

- `COMPLETE DELTA APPLY`: still compute full MV definition result, but apply only changed rows to MV.

#### Syntax contract (V1)

Refresh mode matrix should be:

- `REFRESH MATERIALIZED VIEW ... COMPLETE`
- `REFRESH MATERIALIZED VIEW ... COMPLETE OUT OF PLACE`
- `REFRESH MATERIALIZED VIEW ... COMPLETE DELTA APPLY`
- `REFRESH MATERIALIZED VIEW ... FAST`

and reject these combinations:

- `FAST OUT OF PLACE`
- `FAST DELTA APPLY`
- `COMPLETE OUT OF PLACE DELTA APPLY` (not in V1)

`OUT OF PLACE` and `DELTA APPLY` should be treated as `COMPLETE`-only options.
Parser should enforce that they can only appear after `COMPLETE`.

#### Scope and assumptions (V1)

V1 is correctness-first and keeps implementation scope tight:

1. V1 targets grouped MVs; for diff computation, the logical row identity is the `GROUP BY` key.
2. All `GROUP BY` key columns used by the diff join must map to MV columns that are `NOT NULL`.
3. Physical row locators used later by `UPDATE` / `DELETE` are a separate concern from diff-join identity:
   - preferred locators are table handles (`PRIMARY KEY` / common handle);
   - `_tidb_rowid` may still be carried from the current MV side as a physical locator,
     but it is not used as the diff-join key.
4. If these requirements are not met, reject `COMPLETE DELTA APPLY` directly
   (do not silently fallback to `COMPLETE` replace mode).
5. Keep existing outer advisory lock for refresh mutex semantics.
6. Keep existing in-place refresh transaction framework (`BEGIN PESSIMISTIC`, history lifecycle, success-only refresh-info persistence).

#### Why not split into three independent re-compute SQLs in one txn

A naive split (`INSERT diff`, `DELETE diff`, `UPDATE diff`) where each statement re-reads MV/query data has two issues:

1. Later statements can read earlier uncommitted writes in the same transaction.
2. Statement-level read ts can drift across statements, so all diffs may not be computed from one stable snapshot.

Also, stale-read SQL (`... AS OF TIMESTAMP ...`) is not a practical fix here because:

- it is rejected when used inside an explicit transaction;
- `tidb_snapshot` mode blocks write statements.

So V1 should avoid "recompute-per-DML-step" design.

#### Diff computation approach (V1)

Use one `FULL OUTER JOIN`-based diff source query, then let one dedicated sink operator apply row changes.

High-level algorithm:

1. Build query-side full result (`Q`) from MV definition SQL.
2. Full-outer-join `Q` with current MV table (`M`) by the `GROUP BY` key
   (which is the logical row identity in this refresh mode).
3. Keep only changed rows:
   - `Q-only` => `INSERT`
   - `M-only` => `DELETE`
   - both exist but payload differs => `UPDATE`
4. Output one diff stream (`FOJ + Selection`) and feed it directly into a dedicated MV-apply sink operator.
   - this operator executes per-row `INSERT` / `UPDATE` / `DELETE` on target MV table in the same transaction.
   - avoid splitting into three standalone write SQL statements.
5. On success, persist refresh watermark (`LAST_SUCCESS_READ_TSO`) with existing CAS + readback validation.

Example diff-shaping SQL (simplified):

```sql
WITH q AS (
    -- Full MV definition result; map selected marker column to q_marker
    SELECT k1, k2, <mv_marker_col> AS q_marker, v1, v2
    FROM (<mv_definition_sql>) q0
),
m AS (
    -- Current MV data; map same marker column to m_marker
    SELECT k1, k2, <mv_marker_col> AS m_marker, v1, v2
    FROM <mv_table>
)
SELECT
    CASE
        WHEN m.m_marker IS NULL THEN 'I'
        WHEN q.q_marker IS NULL THEN 'D'
        ELSE 'U'
    END AS diff_op,
    COALESCE(q.k1, m.k1) AS k1,
    COALESCE(q.k2, m.k2) AS k2,
    q.v1 AS new_v1, q.v2 AS new_v2,
    m.v1 AS old_v1, m.v2 AS old_v2
FROM q
FULL OUTER JOIN m
  ON q.k1 = m.k1
 AND q.k2 = m.k2
WHERE
      q.q_marker IS NULL
   OR m.m_marker IS NULL
   OR NOT (q.v1 <=> m.v1 AND q.v2 <=> m.v2);
```

In the SQL sketch above, `q_marker` / `m_marker` are logical aliases used to express
side-missing detection and `diff_op` derivation. They do not have to remain as standalone
output columns in the final planner-executor layout; the chosen marker can be read from
the `Q` / `M` row image via explicit metadata.

#### Join and diff rules

1. Join predicate:
   - use the `GROUP BY` key columns as the diff-join key;
   - in V1 these key columns are required to be `NOT NULL`, so `=` can be used;
   - physical locators such as `PRIMARY KEY` handle columns or `_tidb_rowid` are not suitable
     diff-join keys; they are carried only for later `UPDATE` / `DELETE` locate.
2. Payload equality check should use null-safe comparison (`<=>`) per column.
3. Side-missing detection should use one deterministic marker column from MV schema:
   - pick the first visible `NOT NULL` column from MV `TableInfo.Columns` (stable column order);
   - map this column as logical aliases `q_marker` / `m_marker` in diff SQL;
   - `q_marker IS NULL` => row missing on query side (`DELETE`);
   - `m_marker IS NULL` => row missing on MV side (`INSERT`).

This avoids relying on key-column `IS NULL` checks and does not bind design
to any specific aggregate output column.

#### Write-path architecture (align with FAST refresh)

`COMPLETE DELTA APPLY` write stage should follow `FAST` refresh architecture:

1. Use an internal implementation statement path, not ad-hoc SQL text concatenation for write phase.
2. Let optimizer build one physical diff-source plan (`FOJ + Selection`) first.
3. Add one dedicated sink physical operator on top (similar role to `MVDeltaMerge` in FAST path).
4. Executor reads diff rows chunk-by-chunk and applies row operations to MV table directly.

Expected end-to-end shape:

```text
RefreshMaterializedViewExec
  -> executeRefreshMaterializedViewDataChanges(...)
    -> ExecuteInternalStmt(RefreshMaterializedViewImplementStmt for COMPLETE DELTA APPLY)
      -> PlanBuilder.buildRefreshMaterializedViewImplement(...)
        -> optimize diff-source SELECT (FOJ + Selection)
        -> wrap by new sink plan node (for example MVCompleteDiffApply)
      -> executorBuilder.build<NewSink>(...)
        -> new sink exec consumes child rows and writes target table (insert/update/delete)
```

This preserves the same key properties as FAST path:

- one statement-level read snapshot for diff computation;
- write/apply is in the same refresh transaction;
- no "statement A writes, statement B reads uncommitted write" drift from split DMLs.

For operator input layout, keep it explicit and stable (planner-executor contract):

1. row-op column (`diff_op`);
2. optional extra handle column (`_tidb_rowid`) only when MV uses extra row-id handle;
3. old row image (`M`) columns for delete/update old values;
4. new row image (`Q`) columns for insert/update new values.

Additional layout metadata stays explicit even when columns are reused:

1. marker selection is tracked by MV-column offset, so side-missing diagnostics can read the chosen
   marker from the `M` / `Q` row image instead of projecting `q_marker` / `m_marker` twice;
2. `MHandleCols` may either point to old-row-image columns (for PK/common handle) or to the optional
   extra `_tidb_rowid` column.

`diff_op` should be generated in diff-source projection (instead of re-evaluating marker logic in sink):

```sql
CASE
  WHEN m_marker IS NULL THEN 1  -- INSERT
  WHEN q_marker IS NULL THEN 2  -- DELETE
  ELSE 3                        -- UPDATE
END AS diff_op
```

Recommended encoding:

- `1` = `INSERT`
- `2` = `DELETE`
- `3` = `UPDATE`

Use integer op code (for example `TINYINT`) instead of string op code to keep executor branch cost low.

Note on diff filtering:

1. Keep existing diff filter (`q_marker IS NULL OR m_marker IS NULL OR payload_changed`) in `WHERE`.
2. Do not rely on select-field alias visibility in the same query block `WHERE`.
3. If filtering by `diff_op` is needed, wrap one extra projection/query layer.

Write mapping contract for sink executor should be explicit:

Recommended root sink-plan contract (`MVCompleteDeltaApply` style):

1. `OpColID`: child column index of `diff_op`.
2. `MarkerMVOffset`: which MV column is used as the side-missing marker.
3. `GroupKeyMVOffsets`: GROUP BY key offsets in MV column order; sink uses them to skip
   redundant update comparisons on join-equal key columns.
4. `MHandleCols`: physical locator columns built from `M` side (used by `DELETE` and `UPDATE`,
   and intentionally separate from the diff-join key).
5. `MRowInputColIDs` / `QRowInputColIDs`: full old/new row-image mappings in MV column order.

Writable input mappings should be derived in executor from `TargetTable.WritableCols()` plus
`MRowInputColIDs` / `QRowInputColIDs`, instead of being persisted in planner contract. This keeps
complete delta apply aligned with fast-refresh writer ownership.

Per-row operation behavior in sink executor:

1. `diff_op = 1` (`INSERT`): write `Q` row image via `AddRecord`.
2. `diff_op = 2` (`DELETE`): build handle from `MHandleCols`, remove `M` old row via `RemoveRecord`.
3. `diff_op = 3` (`UPDATE`): build handle from `MHandleCols`, update from `M` old row to `Q` new row via `UpdateRecord`.

V1 write strategy:

1. Prioritize correctness first: keep sink writer simple and deterministic.
2. For `UPDATE`, V1 may set touched columns conservatively (all aggregate/writable payload columns).
3. Column-level touched optimization (bitmap/minimal-set update) can be added later as a performance phase.

#### Milestones (recommended implementation order)

M1. Syntax/AST contract milestone

1. Extend grammar to support `COMPLETE DELTA APPLY`.
2. Enforce mode matrix in parser/validator (`OUT OF PLACE` and `DELTA APPLY` are `COMPLETE`-only options).
3. Keep restore output stable for all accepted/rejected combinations.

Done criteria:

1. Parser accepts supported syntax and rejects unsupported combinations.
2. AST can round-trip restore for new syntax.

M2. Planner diff-source milestone

1. Build FOJ-based diff-source AST (`Q` vs `M`) in planner mview builder.
2. Produce stable output layout including `diff_op`, optional extra handle, old/new row images,
   with explicit metadata for marker selection and `M`-side physical locators.
3. Keep `WHERE` diff-filter semantics stable (`side-missing OR payload-changed`).

Done criteria:

1. Planner case tests show expected `FULL OUTER JOIN + Selection + projection(diff_op)` shape.
2. Output layout metadata is explicit and validated in planner.

M3. Planner sink-contract milestone

1. Add new root sink plan node for complete delta apply.
2. Finalize planner-side sink mapping contract (`OpColID`, `MarkerMVOffset`, `GroupKeyMVOffsets`,
   `MHandleCols`, `MRowInputColIDs`, `QRowInputColIDs`).
3. Cover explain/contract tests for the new root plan shape and diff-source layout expectations.

Done criteria:

1. Planner builds `MVCompleteDeltaApply` with explicit sink metadata.
2. Planner-side invalid mapping/layout fails early with clear errors.
3. Executor integration is intentionally deferred to M4.

M4. Executor hookup and correctness-first write milestone

1. Add executor builder/runtime for `MVCompleteDeltaApply`.
2. Derive writable-column mappings from `TargetTable.WritableCols()` and row-image mappings.
3. For `UPDATE`, compare old/new non-group-key writable columns in chunk batches and derive precise touched sets.
4. Implement row writes in sink runtime:
   - `diff_op=1` -> `AddRecord`
   - `diff_op=2` -> `RemoveRecord`
   - `diff_op=3` -> `UpdateRecord`
5. Keep all writes inside existing refresh transaction framework.

Done criteria:

1. `MVCompleteDeltaApply` can be built into an executor and consume diff rows end-to-end.
2. Correctness tests pass for insert-only/delete-only/update-only/mixed/no-op cases.
3. Failure path rolls back MV data and keeps refresh-info watermark unchanged.

M5. Refresh framework integration milestone

1. Route `COMPLETE DELTA APPLY` through data-change dispatch path.
2. Keep existing advisory lock / history lifecycle / CAS watermark semantics unchanged.
3. Add observability step for delta apply.

Done criteria:

1. `WITH PROFILE`/`DRY RUN` can distinguish delta-apply step.
2. Concurrency behavior remains compatible with current refresh mutex semantics.

M6. Hardening/performance milestone (post-V1)

1. Add touched-column minimization for `UPDATE`.
2. Evaluate/optimize large-diff memory behavior (projection trimming, spill behavior checks).
3. Add TiFlash/FOJ path validation when feature switches permit.

Done criteria:

1. No correctness regression versus M4/M5.
2. Performance improvements are measurable and guarded by tests.

#### Performance notes

- `FULL OUTER JOIN` is chosen for V1 because it keeps one-pass diff semantics and simple correctness model.
- Filtering unchanged rows reduces output/write volume, but does not remove full-join compute cost itself.
- For large tables, memory/spill pressure is expected; keep projection minimal in diff query and rely on spill path correctness.
- If future TiFlash full-join pushdown/MPP is available, this diff-query shape can reuse that capability without changing SQL semantics.

## Test suggestions (for future implementation)

Add executor UT coverage in `pkg/executor/test/executor/` (refresh-focused) and `pkg/executor/test/ddl/` (MV DDL-related):

1. **Basic correctness**:
   - create base table + mlog + mv
   - insert base data
   - execute `REFRESH MATERIALIZED VIEW mv COMPLETE`
   - verify MV content equals `SELECT ... GROUP BY ...`
   - verify `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO > 0`
   - verify one row in `mysql.tidb_mview_refresh_hist` has `REFRESH_STATUS='success'` and `REFRESH_JOB_ID=<start_tso>`
2. **Concurrency mutex**:
   - session A starts refresh and pauses after lock acquisition (`FOR UPDATE`) via failpoint or manual lock hold
   - session B executes refresh and should get NOWAIT lock conflict
3. **Missing metadata row**:
   - delete row from `mysql.tidb_mview_refresh_info`
   - execute refresh and expect "refresh info row missing" error
4. **Failure semantics**:
   - force COMPLETE refresh failure (for example injected `INSERT ... SELECT` error)
   - verify `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO` is unchanged
   - verify corresponding `mysql.tidb_mview_refresh_hist` row is finalized to `REFRESH_STATUS='failed'` with error reason

## Known limitations and future direction

- `DELETE FROM mv` + `INSERT INTO mv SELECT ...` in one transaction can create very large transactions for big MVs
  (txn size limits, write amplification, GC pressure).
  A future "build new object + atomic cutover" strategy is possible but needs careful atomicity-boundary design,
  because it introduces DDL semantics.
- History finalization is intentionally after refresh transaction commit, because only then final status is definitive.
  If process crash happens between refresh commit and history finalize update, recovery/reconciliation for
  stale `running` rows is still a future enhancement.
