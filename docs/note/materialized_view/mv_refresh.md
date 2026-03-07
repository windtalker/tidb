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

- Separate semantics for `WITH SYNC MODE`.
  Refresh is synchronous today, so `WITH SYNC MODE` is parsed/executed but behaves the same as without it.
  If async refresh is introduced later, semantics can be redefined.
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

Current implemented syntax (all use one common transactional framework today):

```sql
REFRESH MATERIALIZED VIEW db.mv COMPLETE;
REFRESH MATERIALIZED VIEW mv COMPLETE; -- uses current DB
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE COMPLETE; -- same behavior today (refresh is already synchronous)

REFRESH MATERIALIZED VIEW mv FAST;
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE FAST; -- same behavior today (refresh is already synchronous)
```

Current note: `FAST` requires `mysql.tidb_mview_refresh_info.LAST_SUCCESS_READ_TSO` to be non-`NULL`; otherwise refresh fails.

Planned syntax extension for out-of-place COMPLETE (Oracle-aligned semantics):

```sql
REFRESH MATERIALIZED VIEW mv COMPLETE OUT OF PLACE;
REFRESH MATERIALIZED VIEW mv WITH SYNC MODE COMPLETE OUT OF PLACE;
```

Planned syntax/semantic rules:

1. `OUT OF PLACE` is allowed only with `COMPLETE`.
2. `REFRESH ... COMPLETE` without `OUT OF PLACE` keeps current in-place behavior.
3. `REFRESH ... FAST` keeps current behavior; `FAST OUT OF PLACE` should fail with a clear syntax/semantic error.
4. `WITH SYNC MODE` remains syntax-compatible and has the same runtime behavior as today (refresh is already synchronous).

Oracle mapping note:

- Oracle exposes out-of-place refresh through `DBMS_MVIEW.REFRESH(..., method => 'C', atomic_refresh => FALSE, out_of_place => TRUE)`.
- TiDB can provide equivalent semantics through SQL surface `REFRESH MATERIALIZED VIEW ... COMPLETE OUT OF PLACE`.
- Oracle's `out_of_place` is API parameter-based, while TiDB chooses SQL clause-based exposure.

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

### Advisory lock design refinement (planned, applies to all refresh types)

To avoid path inconsistency between refresh modes, advisory lock should be used as an
outer mutex for **all** `REFRESH MATERIALIZED VIEW` execution paths (`FAST` and `COMPLETE`,
including out-of-place COMPLETE build/cutover flow).

Recommended placement and ownership:

1. Acquire advisory lock in `executeRefreshMaterializedView` after target MV metadata is resolved
   (MV ID + schema ID available), and before entering heavy work.
2. Hold lock on the refresh internal session (`refreshSctx`) used by the refresh framework.
   - Do not hold it on caller/user session.
   - Do not rely on outer mv-service scheduling session to own this lock.
3. Keep lock name stable by identity (for example `mv_refresh_<schemaID>_<mviewID>`).
4. Preserve existing row-lock + CAS checks on `mysql.tidb_mview_refresh_info`.
   - advisory lock is outer mutual exclusion;
   - row-lock + CAS remains metadata consistency guard.

Release and cleanup rules:

1. Always release by deferred cleanup in function scope, and ensure release runs before
   putting the internal session back to session pool.
2. For pooled internal sessions, release by lock name with a helper that drains reference count:
   - repeat `ReleaseAdvisoryLock(lockName)` until it returns `false`;
   - this guarantees the session does not retain that lock name when returning to pool.
3. Do **not** silently pre-clean this lock name before acquire.
   - by design, borrowed internal session should not already hold the same refresh lock;
   - silent pre-clean may hide lock-leak/session-reuse bugs.
4. Enforce a strict invariant for observability:
   - expected drained release count is exactly `1` for one refresh execution;
   - if drained release count is not `1` (for example `0` or `>1`), treat as internal
     invariant violation and report/log explicitly.

Error mapping and operational notes:

1. Advisory-lock conflict should be mapped to user-visible "another refresh is running"
   style error, instead of exposing low-level lock-wait details.
2. `defer` protects normal returns and panic-unwind paths, but cannot guarantee cleanup for
   process-abort/fatal-exit scenarios.
3. If process exits unexpectedly, lock owner session is gone and lock should be released by
   transactional lock lifecycle eventually; however, history reconciliation for stale `running`
   rows is still needed as a separate concern.

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

Motivation: in-place `DELETE FROM mv + INSERT INTO mv SELECT ...` can produce very large transactions on big MVs.
Out-of-place COMPLETE is the fallback path for this scenario.

Detailed planned execution steps:

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
5. **Out-of-place COMPLETE metadata cutover**:
   - build shadow table and run cutover
   - verify base table `MaterializedViewBase.MViewIDs` old ID -> new ID replacement
   - verify new table has inherited `MaterializedView` metadata
   - verify old physical table is dropped after cutover
   - verify `mysql.tidb_mview_refresh_info` row is migrated to new `MVIEW_ID` with refreshed `LAST_SUCCESS_READ_TSO`
6. **Advisory-lock concurrency**:
   - session A holds out-of-place refresh lock during build
   - session B runs refresh on same MV and gets lock-conflict error before entering heavy build work
   - after session A finishes, session B can refresh normally
7. **Cross-type mutex with unified advisory lock**:
   - session A runs `REFRESH MATERIALIZED VIEW ... FAST` and holds advisory lock
   - session B runs `REFRESH MATERIALIZED VIEW ... COMPLETE` on the same MV and gets lock-conflict error
   - after session A finishes, session B can execute normally
8. **Leak prevention on pooled internal session**:
   - inject refresh failure/panic after advisory lock acquired
   - ensure deferred unlock runs before returning system session to pool
   - verify next refresh on same MV can acquire lock immediately (no stale lock retained)

## Known limitations and future direction

- `DELETE FROM mv` + `INSERT INTO mv SELECT ...` in one transaction can create very large transactions for big MVs
  (txn size limits, write amplification, GC pressure).
  A future "build new object + atomic cutover" strategy is possible but needs careful atomicity-boundary design,
  because it introduces DDL semantics.
- History finalization is intentionally after refresh transaction commit, because only then final status is definitive.
  If process crash happens between refresh commit and history finalize update, recovery/reconciliation for
  stale `running` rows is still a future enhancement.
