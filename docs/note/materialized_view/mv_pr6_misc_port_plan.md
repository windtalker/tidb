# Port materialized view inspection and comparison


This completed ExecPlan follows `PLANS.md` and records the scope, implementation decisions, and verification evidence.

## Purpose / Big Picture


Users can inspect materialized views (MVs), their materialized view logs (MLogs), dependencies, refresh/purge status, and reproducible CREATE statements. They can compare an MV with its defining query at the last successful refresh snapshot and optionally write differences to a table. This is the complete PR6 scope of the MV master port plan. Background scheduling and maintenance services belong to PR7.

## Progress


- [x] Fetch remote refs, verify a clean workspace, and create `mv_pr6_misc` from latest `origin/master` in the existing directory.
- [x] Pin source `xufei/cp_mv_for_master` at `fd4636bb442de52a1a5c6321f3f29092472aa6f4` and master at `63ec01b2440281b0debbfb05c5dfd378e94474d1`.
- [x] Read repository instructions, verification skills, and the global PR6/PR7 ownership plan.
- [x] Inventory final source behavior and current master interfaces.
- [x] Port parser/AST, SHOW planning/execution, Information Schema metadata, and predicate extraction.
- [x] Port COMPARE planning/execution, preserving snapshot, privilege, join, and output cleanup behavior.
- [x] Port source tests and integration scenarios; regenerate parser and Bazel artifacts.
- [x] Pass scoped feature, parser race, MV race, privilege/refresh regression, and integration recording/replay checks.
- [x] Pass Ready lint, review source coverage and diff, and describe future PR boundaries.
- [x] Record the agreed four-PR split: one parser prerequisite followed by SHOW, Information Schema, and COMPARE, including ownership of shared privileges and mixed test fixtures.
- [x] Rename the complete reference branch to `mv_pr6_misc_for_master` for the requested commit and push to `xufei`.

## Surprises & Discoveries


The historical tracking table described parser support as fully ported, but the pinned master still lacked SHOW and COMPARE syntax. These AST/grammar changes accompany PR6, including master's SEM command classification and generated in-place visitors.

Source virtual-table IDs 96 through 98 were already allocated in master. The new tables use offsets 103 through 105 after master's existing allocation 102.

Current master requires explicit registration of virtual-table readers in the executor builder. Its internal MLog status reads also require the MV feature flag. SHOW borrows an internal session, enables MV reads and maintenance for that operation, and restores both flags before releasing the session.

Master's shared column-comparison helper rejects NULL for non-null inputs. A full outer join adds NULLs to a missing side even for NOT NULL MV columns; COMPARE treats these join inputs as nullable. Source tests reproduced the failure before this adaptation.

Master folds contradictory metadata predicates into an empty logical table plan. Extractor tests accept that plan when the expected result is empty. COMPARE test snapshots wait before capturing a datetime because datetime snapshots omit the TSO logical component and can otherwise miss a commit in the same millisecond.

One race-enabled integration replay reported a server-startup race between gRPC logger initialization and the metrics channelz collector. Its stack was in unchanged `cmd/tidb-server/main.go`, `pkg/util/logutil/log.go`, and `pkg/metrics/metrics.go`; Go dependencies were also unchanged. All four integration suites passed on rerun, and SHOW/COMPARE race unit tests passed. This port does not change the startup race.

## Decision Log


Decision: Port final behavior from the pinned latest source instead of cherry-picking mixed historical commits. Rationale: master already contains the preceding MV slices with newer framework APIs. Date: 2026-10-26.

Decision: Keep PR6 together on `mv_pr6_misc` for review. Rationale: the user requested the complete result before deciding whether to split it. Date: 2026-10-26.

Decision: Add `pkg/executor/mview_compare.go` and reuse current master refresh/session/hash-join infrastructure. Rationale: the source combines multiple runtimes in one large file, while master has focused executor files. Date: 2026-10-26.

Decision: Include MV/MLog object-level grant restrictions and the SHOW visibility privilege list. Rationale: they complete the source privilege behavior exercised by the PR6 SHOW and COMPARE scenarios. The already supported global `SHOW PRIVILEGES` listing is omitted from the imported integration input; MV-specific cases remain. Date: 2026-10-26.

Decision: Deliver PR6 as one parser prerequisite followed by three feature PRs: SHOW, Information Schema, and COMPARE. Concentrate all changes under `pkg/parser` in `pr6a_misc_parser`, including AST/grammar inputs, generated outputs, and parser tests. The three feature PRs carry their own planner/executor work and runtime tests. Put common MV/MLog GRANT restrictions in the SHOW PR, and keep statement-specific authorization and metadata visibility with their owning feature. Rationale: syntax and generated parser changes receive one focused review, while runtime reviews retain complete feature boundaries. Date: 2026-10-26.

Decision: Preserve the combined port on `mv_pr6_misc_for_master` and push it to `xufei` before extracting the four PRs. Rationale: the user requested this branch name and a committed remote reference containing all current changes. Date: 2026-10-26.

## Outcomes & Retrospective


The complete PR6 port is available on `mv_pr6_misc_for_master`, renamed from `mv_pr6_misc` at the user's request for commit and push to `xufei`. The port has 44 files and approximately 7,200 added lines excluding generated `pkg/parser/parser.go`. Most non-generated additions are tests and integration fixtures. No DDL/bootstrap schema change is needed for these virtual Information Schema tables.

The agreed delivery consists of `pr6a_misc_parser` (all parser changes), `pr6b_misc_show` (SHOW and common object-level GRANT rules), `pr6c_misc_mem_table` (Information Schema metadata and extractors), and `pr6d_misc_compare` (COMPARE). The full port remains together on `mv_pr6_misc_for_master` as the reference implementation. This document defines the extraction plan; separate branches and PRs have not been created.

## Context and Orientation


The repository is `/Users/feixu/dev/pingcap/tidb`. The base is master commit `63ec01b2440281b0debbfb05c5dfd378e94474d1`. Source is `xufei/cp_mv_for_master` at `fd4636bb442de52a1a5c6321f3f29092472aa6f4`. Inspect source with `git show <source>:<path>` without changing branches. Historical source changes can be located with `git diff f08c648a20380ea723449c6c3eb5b171d96fd567..<source> -- <path>`; feature ownership is based on final behavior.

`pkg/parser` translates SQL into AST nodes. `pkg/planner/core` validates statements, creates plans, and extracts virtual-table predicates. `pkg/executor/show.go` renders SHOW output; `pkg/executor/infoschema_reader.go` reads metadata; `pkg/infoschema/tables.go` defines virtual-table columns and IDs. SQL behavior tests use the existing `pkg/executor/test/materializedview` suite.

A snapshot timestamp identifies a consistent historical read. COMPARE resolves MV metadata at snapshot S, reads the refresh watermark R from `mysql.tidb_mview_refresh_info` at S, and compares MV data at S against its defining query at R. An output table contains differing rows plus a classification column. Failed writing triggers output-table cleanup.

## Plan of Work


The implementation adds six SHOW variants, their schemas and privilege checks, and the virtual tables `TIDB_MVIEWS`, `TIDB_MLOGS`, and `TIDB_TABLE_MVIEW_DEPENDENCIES`. Metadata predicates support names, IDs, schemas, and dependency endpoints. Parser outputs and AST visitors are regenerated from their inputs.

COMPARE is implemented in `pkg/executor/mview_compare.go`, with plan/executor builder wiring. It preserves nullable group keys, empty inputs, definition-time SQL context, historical metadata after out-of-place refresh, and the base-query side as the hash-join build side. Output writing batches transactions, avoids colliding with an existing classification-column name, and cleans up failed output.

Source tests are grouped as 20 SHOW/COMPARE subtests in the existing MV suite. Reader tests, metadata column contracts, and three extractor tests stay in their owning packages. Integration inputs and expected results cover metadata, privileges, and COMPARE. Existing refresh/grant regression and metadata integration tests were also checked.

## PR Split Plan


### Boundaries and order


`pr6a_misc_parser` contains all parser changes for SHOW and COMPARE as a shared prerequisite. Each subsequent feature PR delivers its planner, executor, authorization, and runtime test paths using the already available syntax. Shared runtime source files are split by behavior: for example, `pkg/executor/builder.go` registers Information Schema readers in `pr6c_misc_mem_table` and the COMPARE executor in `pr6d_misc_compare`.

Use the order `pr6a_misc_parser`, `pr6b_misc_show`, `pr6c_misc_mem_table`, then `pr6d_misc_compare` for review and merging. All four reuse the MV DDL and refresh runtime already present in the pinned master. SHOW and COMPARE require the syntax from `pr6a_misc_parser`. Information Schema adds no syntax and has no parser dependency of its own; it follows the same merge sequence for delivery. `pr6b_misc_show` establishes the common MV/MLog GRANT rules. `pr6c_misc_mem_table` and `pr6d_misc_compare` can each be validated on top of that baseline; COMPARE does not query the three new Information Schema tables, and Information Schema does not execute the new SHOW commands. The order between `pr6c_misc_mem_table` and `pr6d_misc_compare` is organizational rather than a runtime dependency.

| Slice / planned branch | Suggested PR title | User-visible result |
| --- | --- | --- |
| `pr6a_misc_parser` | `parser: support materialized view SHOW and COMPARE syntax` | Parse and restore all new statements and expose their AST and SQL command classifications. |
| `pr6b_misc_show` | `executor, planner: support materialized view SHOW statements` | List MV/MLog objects, inspect CREATE statements and log counts, and apply common object-level GRANT rules. |
| `pr6c_misc_mem_table` | `infoschema, executor, planner: expose materialized view metadata tables` | Query the three virtual metadata tables with their visibility rules and predicate extraction. |
| `pr6d_misc_compare` | `executor, planner: support materialized view comparison` | Compare an MV with its defining query at the refresh snapshot and optionally write differences to a table. |

These names identify the planned slices and their branches; this documentation update does not extract code or create branches. Background scheduling and maintenance services remain in PR7.

### `pr6a_misc_parser`: shared syntax prerequisite


Include the entire PR6 diff under `pkg/parser`: all six SHOW forms, `COMPARE MATERIALIZED VIEW ... AS OF TIMESTAMP ... [OUTPUT INTO TABLE ...]`, keyword/token handling, SHOW AST types, `CompareMaterializedViewStmt`, statement interfaces, SQL command classifications, AST restore/visitor support, and parser tests. This includes both generation inputs and their generated outputs.

The owning files include `pkg/parser/parser.y`, `pkg/parser/{keywords,misc}.go`, `pkg/parser/ast/{ast,dml,misc,sem}.go`, the in-place visitor generator and inventory expectations, and all corresponding test changes. Regenerate `pkg/parser/parser.go` and `pkg/parser/ast/visitor_inplace_generated.go` from these inputs. The later SHOW and COMPARE PRs consume these definitions without carrying additional parser diffs.

Acceptance requires all new forms to parse into the expected AST, restore correctly, traverse visitors correctly, and return the intended SQL command classifications. Preserve negative syntax and keyword compatibility coverage. Run the parser suite and verify that the server still compiles with this parser-only change against the chosen base. This PR provides syntax support; executable SHOW and COMPARE behavior is delivered by `pr6b_misc_show` and `pr6d_misc_compare` respectively. Runtime preprocessing, privilege checks, plan construction, and execution remain with those feature PRs.

### `pr6b_misc_show`: SHOW and common object privileges


Include all six SHOW forms:

    SHOW MATERIALIZED VIEWS;
    SHOW MATERIALIZED VIEW LOGS;
    SHOW CREATE MATERIALIZED VIEW <mv>;
    SHOW CREATE MATERIALIZED VIEW LOG ON <base_table>;
    SHOW MATERIALIZED VIEW <mv> REMAIN_LOGS;
    SHOW MATERIALIZED VIEW LOG ON <base_table> WAIT_PURGE;

Using the syntax and AST from `pr6a_misc_parser`, carry the corresponding preprocessing, result schemas, LIKE/WHERE handling, privilege checks, and `ShowExec` implementations. The two count commands are the status portion of SHOW: `REMAIN_LOGS` counts logs newer than the MV's successful read watermark, while `WAIT_PURGE` counts logs already safe to purge but still awaiting cleanup. Their internal sessions must retain the consistent transaction snapshot and restore the borrowed session's MV settings.

Place `materializedViewTablePrivs`, `validateGrantOnSpecialMViewObject`, and `tablePrivsForGrantTarget` from `pkg/executor/grant.go` here. MV/MLog object-level grants allow SELECT, SHOW VIEW, ALTER, DROP, and OPERATE VIEW, with USAGE and grant-option handling; GRANT ALL expands to the applicable set. Column grants are limited to SELECT/USAGE, and shadow tables reject grants. Ordinary table grants retain their current behavior. SHOW visibility and SHOW CREATE/status authorization are also part of this PR.

The owning files are the SHOW-related portions of `pkg/planner/core/{planbuilder,preprocess,show_predicate_extractor}.go`, `pkg/executor/show.go`, and `pkg/executor/grant.go`, together with their runtime tests and Bazel metadata. The three virtual-table readers belong to `pr6c_misc_mem_table`.

Keep the eight SHOW/GRANT subtests in `materialized_view_show_test.go` after moving its two Information Schema subtests to `pr6c_misc_mem_table`. Start `executor/mview_privilege` with the SHOW, common privilege, and existing refresh/purge/DML regression cases; move COMPARE-specific setup, assertions, and cleanup to `pr6d_misc_compare`. Acceptance requires correct listings and filters, valid CREATE output, counts that change correctly after writes/refresh/purge, permission denial for unauthorized users, and unchanged ordinary-table grant behavior.

### `pr6c_misc_mem_table`: Information Schema metadata


Include `information_schema.TIDB_MVIEWS`, `information_schema.TIDB_MLOGS`, and `information_schema.TIDB_TABLE_MVIEW_DEPENDENCIES` as virtual tables. Carry column definitions and table IDs, metadata readers, table-specific row visibility, planner predicate extractors, and executor reader registration together. These tables read existing metadata; no bootstrap table creation or persisted-system-table migration is needed.

The owning files are `pkg/infoschema/tables.go`, `pkg/executor/infoschema_reader.go`, the reader-registration portion of `pkg/executor/builder.go`, and the Information Schema portions of `pkg/planner/core/{logical_plan_builder,memtable_infoschema_extractor}.go`. Preserve the destination's available virtual-table IDs and recheck allocations if the base advances; the original source offsets are already occupied in the pinned master.

Move `testInformationSchemaTiDBMViews` and `testInformationSchemaTiDBMLogs` from the SHOW test file into a metadata-focused test file with a `TestInformationSchemaMaterializedView` entry point. Include `TestSetDataFromTiDBMViews`, `TestSetDataFromTiDBMLogs`, `TestSetDataFromTiDBTableMViewDependencies`, the column contracts in `pkg/infoschema/infoschema_test.go`, the three extractor tests, and the complete `executor/infoschema_mv` integration pair. The new entry point is planned work for extraction and is not present in the reference diff yet.

Acceptance requires correct MV/MLog/dependency rows and column types, object visibility under restricted users, and schema/name/ID filters that agree with unfiltered results. Preserve coverage for absent or non-public objects and contradictory predicates. The two relocated metadata tests should run without invoking any new SHOW command.

### `pr6d_misc_compare`: COMPARE


Using `CompareMaterializedViewStmt` and its syntax from `pr6a_misc_parser`, include the complete COMPARE runtime path: preprocessing, plan/executor builders, and `pkg/executor/mview_compare.go`. Carry `docs/note/materialized_view/mv_compare.md` with this PR.

Keep snapshot semantics intact: resolve MV metadata and MV data at snapshot S, read the last successful refresh watermark R at S, and compare with the defining query at R. Preserve the definition-time SQL context, historical metadata after out-of-place cutover, nullable outer-join comparison inputs, all three difference classifications, batched output writes, and cleanup after output failure. COMPARE requires OPERATE VIEW on the MV and SELECT on its sources; OUTPUT additionally requires CREATE and INSERT for the destination.

The owning files are the COMPARE-related portions of `pkg/planner/core/{common_plans,planbuilder,preprocess}.go`, `pkg/executor/builder.go`, `pkg/executor/mview_compare.go`, and their Bazel metadata. Add `materialized_view_compare_test.go` and the `executor/mview_compare` integration pair. Move the COMPARE privilege cases from `executor/mview_privilege` into `executor/mview_compare`, including their own users, grants, setup, and cleanup, so neither suite depends on the other suite's execution.

Acceptance requires matching results to report equality and controlled differences to produce the expected classifications/output rows. Historical cutover, nullable/empty input, definition-time time zone, invalid snapshot/watermark, permission-denial-before-snapshot-resolution, multi-transaction output, and failed-output cleanup cases must remain covered.

### Extraction and validation


Retain `mv_pr6_misc_for_master` as the complete reference while extracting the four diffs. Extract all changes under `pkg/parser` into `pr6a_misc_parser` first. Shared runtime test files and fixtures must then be split by the cases above. Review cumulative feature coverage against the reference after all four extractions; file names and test layout may differ. Keep all setup and cleanup necessary for each test in its owning slice.

Regenerate the parser and AST visitors together in `pr6a_misc_parser` from the complete SHOW/COMPARE inputs. Subsequent runtime PRs reuse those generated artifacts. Regenerate Bazel metadata for Go-file/test-target changes in each applicable slice. Reuse the generation commands and bounded Bazel invocation in Concrete Steps if needed.

The following commands are the planned scoped checks after extraction, run from repository root. The failpoint wrapper performs the package-test instrumentation lifecycle. `pr6a_misc_parser` checks all parser packages, then compiles the server to verify compatibility with the base:

    GOMAXPROCS=4 GOFLAGS=-p=4 ./tools/check/failpoint-go-test.sh pkg/parser -race -count=1 ./...
    GOMAXPROCS=8 GOFLAGS=-p=8 go build -o /tmp/pr6a_misc_parser-tidb-server ./cmd/tidb-server

`pr6b_misc_show` checks SHOW and ordinary GRANT regressions:

    GOMAXPROCS=8 GOFLAGS=-p=8 ./tools/check/failpoint-go-test.sh . ./pkg/executor/test/materializedview ./pkg/executor -run '^Test(ShowMaterializedView|Grant(DBScope|TableScope|ColumnScope))$' -count=1

`pr6c_misc_mem_table` checks the relocated metadata tests, readers, column contracts, and extractors:

    GOMAXPROCS=8 GOFLAGS=-p=8 ./tools/check/failpoint-go-test.sh . ./pkg/executor/test/materializedview ./pkg/executor ./pkg/infoschema ./pkg/planner/core/operator/logicalop/logicalop_test -run '^Test(InformationSchemaMaterializedView|SetDataFromTiDB(MViews|MLogs|TableMViewDependencies)|TiDB(MViews|MLogs|TableMViewDependencies)|Basic)$' -count=1

`pr6d_misc_compare` checks the complete comparison runtime, including race detection:

    GOMAXPROCS=8 GOFLAGS=-p=8 ./tools/check/failpoint-go-test.sh pkg/executor/test/materializedview -race -run '^TestCompareMaterializedView$' -count=1

For every code slice run `make lint` and `git diff --check`; run `make bazel_prepare` before build/test whenever triggered by that slice's changes. Parser regeneration and parser-suite checks belong to `pr6a_misc_parser`; subsequent runtime-only changes do not require repeating them unless new changes or failures justify it.

    GOMAXPROCS=8 GOFLAGS=-p=8 make lint
    git diff --check

Record and replay each slice's integration pair with its own build. These are the recording commands; repeat each owning command with `-t` replacing `-r` to replay it. `pr6c_misc_mem_table` also replays `infoschema/tables` to check general metadata compatibility.

    pushd tests/integrationtest && ./run-tests.sh -r executor/mview_privilege && popd
    pushd tests/integrationtest && ./run-tests.sh -r executor/infoschema_mv && popd
    pushd tests/integrationtest && ./run-tests.sh -t infoschema/tables && popd
    pushd tests/integrationtest && ./run-tests.sh -r executor/mview_compare && popd

The passing checks recorded below cover the combined reference port. After extraction, validate each new PR at its own base because moving tests and regenerating shared files can expose missing dependencies. The document update itself changes no runtime behavior and does not rerun those code tests.

## Concrete Steps


Commands below run from repository root. Bazel preparation passed with remote cache disabled after initial remote-cache timeouts:

    make bazel_prepare BAZEL_CMD_CONFIG='--jobs=8 --remote_cache= --http_timeout_scaling=0.1 --remote_timeout=5s --remote_retries=0 --remote_upload_local_results=false'
    make parser_ast_visitor parser
    make -C pkg/parser generate
    make parser_fmt
    make parser_yacc

The failpoint wrapper enables instrumentation before tests and restores it afterward. Parser and MV race checks passed:

    GOMAXPROCS=4 GOFLAGS=-p=4 ./tools/check/failpoint-go-test.sh pkg/parser -race -count=1 ./...
    GOMAXPROCS=8 GOFLAGS=-p=8 ./tools/check/failpoint-go-test.sh pkg/executor/test/materializedview -race -run '^Test(Compare|Show)MaterializedView$' -count=1

The scoped feature, reader, schema, and extractor check is:

    GOMAXPROCS=8 GOFLAGS=-p=8 ./tools/check/failpoint-go-test.sh . ./pkg/executor/test/materializedview ./pkg/executor ./pkg/infoschema ./pkg/planner/core/operator/logicalop/logicalop_test -run '^Test((Compare|Show)MaterializedView|SetDataFromTiDB(MViews|MLogs|TableMViewDependencies)|TiDB(MViews|MLogs|TableMViewDependencies)|Basic)$' -count=1

Shared privilege and refresh regression checks passed:

    GOMAXPROCS=8 GOFLAGS=-p=8 ./tools/check/failpoint-go-test.sh . ./pkg/executor ./pkg/executor/test/materializedview -run '^Test(Grant(DBScope|TableScope|ColumnScope)|MaterializedViewRefresh(CompleteBasic|CompleteDeltaApply|CompleteOutOfPlaceCutoverBasic|RequiresOperateViewPrivilege))$' -count=1

Integration recording built a race-enabled server for the first suite, then reused that binary:

    pushd tests/integrationtest && ./run-tests.sh -r executor/infoschema_mv && popd
    pushd tests/integrationtest && ./run-tests.sh -b n -s ./integrationtest_tidb-server -r executor/mview_privilege && popd
    pushd tests/integrationtest && ./run-tests.sh -b n -s ./integrationtest_tidb-server -r executor/mview_compare && popd
    pushd tests/integrationtest && ./run-tests.sh -b n -s ./integrationtest_tidb-server -r infoschema/tables && popd

The recording/replay processes used `GOMAXPROCS=8 GOFLAGS=-p=8`. Expected results were reviewed. The existing `infoschema/tables` result remained unchanged. Replay passed all four suites, with 145, 174, 26, and 130 cases respectively:

    GOMAXPROCS=8 GOFLAGS=-p=8 bash -e -c 'for mv_case in executor/infoschema_mv executor/mview_privilege executor/mview_compare infoschema/tables; do
      pushd tests/integrationtest
      ./run-tests.sh -b n -s ./integrationtest_tidb-server -t "$mv_case"
      popd
    done'

Ready lint and diff checks passed:

    GOMAXPROCS=8 GOFLAGS=-p=8 make lint
    git diff --check

`build/get_changed_bazel_pkgs.sh origin/master` selects `//:all` alongside owning packages because notes and integration paths map to the root. The user-supplied build flow permits skipping `make bazel_lint_changed` for `//:all`; this broad sweep is skipped. Generated Bazel metadata, scoped tests, and Ready lint were completed.

## Validation and Acceptance


SHOW exposes permitted objects and round-trips accepted CREATE syntax. Metadata queries agree with source definitions and honor schema/name/ID filters. Remaining-log and waiting-purge status use a consistent internal transaction. COMPARE reports equality and all three difference classifications, supports historical MV metadata, handles nullable/empty inputs, batches output writes, and cleans up failed output. Source scenarios passed after adapting master interfaces and the shared column-comparison contract.

Compatibility changes are the new SQL syntax, virtual metadata tables, and intended MV/MLog grant restrictions. SHOW log counts and COMPARE read data, so their cost grows with the amount scanned. Snapshot, permission, and cleanup cases are covered locally. RealTiKV and full CI were not run. The unrelated server-startup race remains a validation caveat despite a clean replay afterward.

## Idempotence and Recovery


Inspection and scoped tests are repeatable. Parser/Bazel generation should be rerun after corresponding input changes. Failpoint state was restored to zero, and integration server processes were cleaned up. Rerun a failed scope after diagnosing its cause; do not replace generated expected output to conceal a failure. Review the complete diff and staged changes before committing. Keep the combined reference branch intact while extracting the smaller feature branches.

## Artifacts and Notes


Primary files are `pkg/executor/mview_compare.go`, `pkg/executor/show.go`, `pkg/executor/infoschema_reader.go`, `pkg/infoschema/tables.go`, parser/AST/visitor inputs and outputs, and planner builder/extractor changes. Tests are in `pkg/executor/test/materializedview/materialized_view_{compare,show}_test.go`, `pkg/executor/infoschema_reader_internal_test.go`, `pkg/infoschema/infoschema_test.go`, planner extractor tests, and the three new integration input/result pairs. `docs/note/materialized_view/mv_compare.md` contains the source comparison design with current executor paths.

Local logs under `/tmp/mv-pr6-*` contain generation, test, lint, integration, and race-triage output. The final diff and this record provide the durable review context.

## Interfaces and Dependencies


The new AST connects `pkg/planner/core.(*PlanBuilder).Build`, `CompareMaterializedView`, and `pkg/executor.(*executorBuilder).build`. SHOW uses `ShowExec`. Virtual tables use `memtableRetriever` and the Information Schema extractor interfaces. COMPARE uses current session pooling, restricted SQL, historical schema loading, snapshot reads, and hash joins, with no background-service dependency.

Final revision: implementation and combined-port verification completed; source/destination refs, master adaptations, startup-race triage, and exact commands are recorded above. The extraction plan now places all parser changes in a shared prerequisite before the three runtime feature PRs, concentrating syntax and generated artifacts in one review. All four slice/branch names follow the user's requested naming, and the combined reference branch is `mv_pr6_misc_for_master`. PR extraction remains future work.
