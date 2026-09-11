# MV Master Port Commit Audit

本文档审计 `cp_mv_for_master_base` 到 `cp_mv_for_master` 之间的全部 source commit，
并记录每个 commit 的最终语义、port 归属和 `origin/master` 覆盖状态。

本文档是 commit 级审计，不是按 commit 机械 cherry-pick 的 port 计划。实际 port
必须以 source 分支的最终 diff 为准：一个早期 commit 的内容可能已经被后续 commit
重写、合并或删除；一个 commit 也可能同时包含 MV 代码、通用 prerequisite、测试
变更和 branch drift。因此，下面的“已覆盖”表示语义已经在 master 的最终实现中
得到覆盖，不表示 source commit 的 patch 可以直接 cherry-pick。

## 审计范围

远端分支边界：

```text
base: xufei/cp_mv_for_master_base
      2c66f412d3c333fca544b8fffc90d150a8b12dd6

head: xufei/cp_mv_for_master
      8d2633e8e55a6e7d09649e650df39f1c9f64a7f2

master: origin/master
        c5c97d97a64597686d602d9742910dd0a6009902
```

本范围包含 **113 个 commit**。最终 source diff 的规模为：

```text
500 files changed
97356 insertions(+)
25104 deletions(-)
```

固定审计边界的命令：

```bash
git log --format='%H%x09%ad%x09%s' --date=short --reverse \
  xufei/cp_mv_for_master_base..xufei/cp_mv_for_master

git diff --stat \
  xufei/cp_mv_for_master_base...xufei/cp_mv_for_master
```

## Master 对应实现

截至本次审计，`origin/master` 中与 MV port 主线直接对应的实现为：

| Master commit | 内容 | 对应 port slice |
| --- | --- | --- |
| `d6afc7d9912` | session/meta: add materialized view bootstrap system tables (#70599) | PR1 bootstrap / MV 系统表 |
| `5e8a1a229a7` | parser: port materialized view DDL syntax (#70744) | PR2a 基础 DDL parser / AST / 语法（不含 REFRESH） |
| `8cde78af3c5` | session/parser/privilege/executor: add OPERATE VIEW privilege (#70694) | 独立权限闭环 |
| `94a9cbedabb` | ddl: support create materialized view and log (#70789) | PR2b-create |
| `f3f7b3bb4dd` | ddl: support drop materialized view and log (#70874) | PR2b-drop |
| `10f06594e7e` | ddl: port materialized view alter support (#70927) | PR2b-alter |
| `91e2f28ac7b` | executor/ddl: support base table DML with materialized view logs (#70941) | PR3 MLog DML 写入 |

这些 master commit 是最终实现的参考点。source commit 与上述 commit 不需要保持
patch-id 一致；port 时应以当前 master 的 API、DDL worker 状态机、系统表 schema、
测试组织方式和命名为准。

## Port slice 定义

| Slice | 范围 | 当前结论 |
| --- | --- | --- |
| PR1 | bootstrap、MV 专用系统表及最终 schema | 已进入 master |
| PR2a | MV/MLog 基础 DDL parser、AST、Restore、Digest 和语法测试 | 已进入 master（REFRESH parser 随 PR5） |
| PR2b-create | CREATE MV/MLog 的 DDL 实现、validation、schema tracker 和测试 | 已进入 master |
| PR2b-drop | DROP MV/MLog、依赖清理、普通 DDL 约束和测试 | 已进入 master |
| PR2b-alter | ALTER MV/MLog、base-table DDL 约束和测试 | 已进入 master |
| PR3 | base-table DML 写 MLog、显式写入拦截和相关测试 | 已进入 master |
| PR4 | MLog purge、cancel purge、purge history、hazard guard、purge observability | 待后续 port |
| PR5 | MV refresh、fast/bounded/complete refresh、cancel refresh、refresh history/metrics | 待后续 port |
| PR6 | SHOW、SHOW CREATE、COMPARE、infoschema MV metadata | 待后续 port |
| PR7 | MV service、scheduler、backoff、alert、history cleanup、service observability | 待后续 port |

以下逐项审计中的状态含义：

- `已覆盖`：最终语义已经在 master 对应实现中覆盖。
- `重构后覆盖`：master 已有相同能力，但实现经过 PR 拆分、API 适配或代码重组。
- `部分覆盖`：同一 source commit 中的一部分已进入 master，剩余部分属于后续 slice。
- `待后续 MV slice`：能力仍属于 PR4/PR5/PR6/PR7，当前 master 不应标记为已完成。
- `master 已有等价 prerequisite`：不是 MV 专属能力，master 已有等价底层实现，
  后续 MV port 只需要适配，不重复 port source 的旧实现。
- `非 MV，不纳入`：branch drift 或独立通用改动，不进入 MV 主线 port。
- `混合 commit`：必须按 hunk/语义拆分，不能整体判断。

## 前 10 个 Commit 的测试复核

下面单独核对前 10 个 source commit 中新增或修改的测试 hunk。这里的“覆盖”按测试意图和断言语义判断，不要求测试文件路径、测试函数名或固定 ID 与 source 一致；branch drift 带来的大规模通用 testdata 结果变化不计作 MV 测试 port。

| # | Source 测试内容 | Master 对照和结论 |
| ---: | --- | --- |
| 1 | 未修改或新增测试文件。 | **不适用**。该 commit 只有 `TableInfo` 元数据实现，测试由后续 DDL/metadata 测试覆盖。 |
| 2 | 新增 bootstrap/upgrade MV 系统表测试，及 restore 的 bootstrap version 断言。 | **MV 测试意图覆盖（100%）**。测试已迁移到 `pkg/session/test/bootstraptest/boot_test.go` 和 `bootstrap_upgrade_test.go`，并扩展为最终 5 张系统表及完整列定义；restore 测试仍校验当前 bootstrap version（现为 287）。 |
| 3 | parser/AST visitor、MV DDL restore/parse、错误和关键字测试。 | **部分覆盖**。CREATE/DROP/ALTER MV/MLog 的 parser 测试已在 master；source 的 `REFRESH MATERIALIZED VIEW` parse cases、`TestMaterializedViewCreateRefreshOnClauseSyntax` 以及 AST visitor 覆盖项在 master 中找不到，随 PR5 处理。 |
| 4 | 5 个 CREATE MLog executor 测试，以及 `TestCheckHistoryJobStmtType`。 | **主体测试覆盖，非 100%**。5 个 CREATE MLog 测试已拆到 `materialized_view_basic_test.go`/`materialized_view_create_test.go` 并保留；`TestCheckHistoryJobStmtType` 及其旧 helper 在 DDL framework 重构后不再存在。 |
| 5 | `_tidb_commit_ts` 相关通用测试结果、planner/testdata 同步。 | **不适用（非 MV 测试 port）**。这是混合 branch-sync commit，没有新增 MV 专属测试；通用 golden/testdata 的逐项结果变化不应作为 MV port 完整性指标。 |
| 6 | 36 个 MLog DML writetest，以及 `mview_log_dml` integration test。 | **语义覆盖，非逐字 100%**。master 保留其中 34 个测试并拆分文件；两个旧的 tracked-column “CurrentBehavior” case 已被后续“Rejected”行为测试替代，integration case 也已保留并扩展。 |
| 7 | `TestReservedRowIDAlloc` 的 `Current()` 断言，以及 MLog row-ID integration 回归。 | **部分覆盖**。master 的 MLog integration 已包含 row-ID 回归场景，`ReservedRowIDAlloc.Current()` 实现也存在，但 source 新增的两个 unit-test `Current()` 断言未保留。 |
| 8 | IndexJoin/IndexHashJoin 的 4 个 NULL-safe equality 测试及 benchmark 初始化。 | **覆盖（100%）**。测试文件迁移到 `pkg/executor/join/test/indexjoin/index_lookup_join_test.go`，4 个测试函数和 `HashIsNullEQ` benchmark 初始化均存在。 |
| 9 | `SUM_INT` 的 aggfunc、executor、expression、tipb/checker 测试。 | **覆盖（100%）**。source 新增的 SUM_INT 断言在 master 对应测试中均存在，并有后续 distinct/pushdown 扩展。 |
| 10 | CREATE MLog `table_ids` 测试，以及 2 个 metadata-lock blocking 测试。 | **部分覆盖**。`TestCreateMaterializedViewLogJobTableIDs` 已在 master；两个 `TestMDLCreateMaterializedViewLog...` 并发 MDL 测试在当前 master 中找不到。 |

## 第 11 到第 20 个 Commit 的测试复核

下面继续按 source commit 的测试 hunk 逐项核对。每项同时给出代码语义和测试语义的结论：
代码已经进入 master，不代表 source 中的每个测试函数都仍以同名形式存在；测试只有在
相同场景的断言已迁移、重构后等价覆盖，或明确由更强的回归测试替代时，才记为覆盖。

| # | Source 测试内容 | Master 对照和结论 |
| ---: | --- | --- |
| 11 | CREATE/DROP MV/MLog 的成功、失败回滚、重试、取消、依赖约束、GC、DDL sanity，以及 MLog 基础和元列冲突测试。 | **代码重构后覆盖 100%；测试主体覆盖但不是逐项 100%**。executor 测试已拆分到 `materialized_view_basic_test.go`、`materialized_view_create_test.go`、`materialized_view_drop_test.go` 和 `materialized_view_alter_test.go`，重点 rollback、重试、依赖和约束场景均有对应测试；`TestMaterializedViewDDLBasic` 也已拆散。source 的 `TestCreateMaterializedViewBuildSessionSQLMode`、`TestCheckHistoryJobStmtType` 及其旧 helper 在 DDL framework 重构后未保留，不能宣称 source 测试逐项 100% port。 |
| 12 | 仅同步 `AGENTS.md`、`.gitignore`、Makefile/build helper 和开发流程文件，没有 MV 测试。 | **不适用**。这是 branch drift，代码和测试均不属于 MV port 审计范围。 |
| 13 | CREATE MV validation 回归：`CAST(0 AS UNSIGNED)` 的 build-read-TS 查询类型、`COUNT(column)`、大小写不敏感的 `SUM`/`COUNT`/`MIN`，以及缺少列时的错误信息。 | **代码覆盖 100%；测试部分覆盖**。`TestCreateMaterializedViewBuildReadTSQueryTypeAlignment` 已在 master；validation 代码也已支持 `COUNT(column)`、aggregate 名称大小写不敏感和正确错误信息。但 source 中 COUNT(column) 成功、大写 aggregate 成功/失败及 MLog 缺列的具体回归场景未在 master 中找到同等逐项测试。 |
| 14 | unistore/cop/rowcodec 的 `_tidb_commit_ts` 传递测试，包括 `active_active/commit_ts.test` integration case 和 decoder 行为。 | **代码覆盖 100%；测试部分覆盖**。master 已有 unistore commit-ts 传递和 rowcodec `TestDecodeWithCommitTS` 等 unit coverage；source 的 `tests/integrationtest/t/active_active/commit_ts.test` 及其 result 在 master 中未找到，因此完整 active-active integration 测试不能标记为已 port。 |
| 15 | MV 依赖 base table 执行 `SET TIFLASH REPLICA` 应放行，同时其他受 MV 约束的 DDL 仍应拒绝。 | **代码覆盖 100%；MV 专项测试未明确逐项覆盖**。master 的 `isAlterTiFlashReplica` 特例和 MV-related DDL constraint 已实现该行为；但 master 中未找到明确对应 source 场景的 MV-dependent `SET TIFLASH REPLICA` 回归测试，普通 TiFlash replica 测试不能替代该结论。 |
| 16 | CREATE MV initial build 前预写 refresh-info、运行中可见、成功后更新以及事务提交可见性。 | **代码和测试语义均覆盖 100%（重构后）**。master 的 `prewriteCreateMaterializedViewRefreshInfo`/warmup/upsert 流程完整存在；`TestCreateMaterializedViewRefreshInfoRunningAndSuccess` 和 `TestCreateMaterializedViewSuccessRefreshInfoVisibilityBeforeCommit` 已迁移，并按最终 `tidb_mview_refresh_info` schema 适配。 |
| 17 | MV/MLog current/history 系统表 bootstrap、upgrade、字段/索引/类型变更，以及相关 bootstrap 和 information_schema 测试。 | **代码和测试语义均覆盖 100%（重构后）**。最终定义集中在 `pkg/meta/metadef/system_tables_def.go`，注册和升级测试已迁移到 `pkg/session/test/bootstraptest/boot_test.go` 与 `bootstrap_upgrade_test.go`；source 的固定 information_schema 行数断言属于 branch-dependent 全局计数，不作为 MV 测试缺失。 |
| 18 | `TestNewDistAggFuncSumInt`，验证 `NewDistAggFunc` 对 `tipb.ExprType_SumInt` 的构造和 aggregation mode。 | **代码覆盖 100%；直接构造测试未逐项保留**。master 已有完全对应的 `ExprType_SumInt` dist-agg 分支，并有 SUM_INT executor、spill、pushdown 等核心语义测试；但未找到 source 同名的 `TestNewDistAggFuncSumInt`，因此测试不能写成逐项 100%。 |
| 19 | 新增 `LAST_PURGED_TSO`，并将 refresh/purge history 时间列改为 `datetime(6)` 的 schema/bootstrap 测试。 | **代码和测试语义均覆盖 100%（重构后）**。master 最终 schema 已包含 `LAST_PURGED_TSO` 和四个 `datetime(6)` 时间列，bootstrap 测试明确检查类型、默认值和精度；全局列数变化不单独视为缺失。 |
| 20 | `MIN_COUNT`/`MAX_COUNT` 的 parser、expression、executor、spill、planner、tipb/checker、SQL 和窗口测试。 | **代码和测试覆盖 100%，且 master 有额外测试**。master 的等价实现已进入最终分支，source 的主要测试函数（类型、特殊值、重复语义、partial merge、内存、SQL、滑动窗口、返回类型和 final-mode schema）均可找到对应测试，并额外覆盖 row-based 限制和 pushdown。 |

## 第 21 到第 30 个 Commit 的测试复核

本节继续区分 source commit 的最终代码语义和测试语义。特别是 #25 和 #30 同时包含已
进入 DDL 创建路径的 prerequisite 与未进入 master 的 refresh/purge runtime，不能以部分
metadata 已存在就将整个 commit 或整组测试标记为已覆盖。#29 则是一个已确认的 importer
prerequisite 缺口：它会直接影响配置了 MV initial-build disk quota 的 TiKV 路径。

| # | Source 测试内容 | Master 对照和结论 |
| ---: | --- | --- |
| 21 | CREATE/ALTER MV 和 MLog 的 `START WITH`/`NEXT` 表达式类型校验、默认 schedule 去除及 schema tracker 校验。新增 `TestCreateMaterializedViewLogScheduleExprTypeCheck`、`TestCreateMaterializedViewRefreshExprTypeValidation`、`TestCreateMaterializedViewLogPurgeExprTypeValidation`。 | **代码和测试语义均覆盖 100%（重构后）**。master 保留 `BuildAndValidateMViewScheduleExpr` 和无默认 schedule 的最终行为；三项 source 测试均以同名方式迁移到 schema tracker/basic DDL 测试，并增加 ALTER schedule 的对应回归。source 后续删除 `NEVER` refresh method 后的最终语法状态也与 master 一致。 |
| 22 | 仅调整 `AGENTS.md` 文档，没有 MV 或代码测试。 | **不适用**。这是非 MV 的开发流程文档同步，不进入 MV port 或测试完整性结论。 |
| 23 | CREATE MV 后在隔离 eval session 中派生下一次 refresh 时间，覆盖 SQL upsert 的“更新/不更新 next time”、START/NEXT 优先级和时区。最终 source 测试为 `TestBuildCreateMaterializedViewRefreshInfoUpsertSQL`、`TestCreateMaterializedViewRefreshInfoNextUnixSecondsDerivation`、`TestCreateMaterializedViewRefreshInfoNextUnixSecondsUsesScheduleTimeZone`。 | **代码和测试语义均覆盖 100%（经后续时区/Unix-seconds 重构）**。master 将 helper 拆到 `mview_schedule_expr.go`、将保存值演进为 `NEXT_REFRESH_UNIX_SECONDS` 并保存 schedule timezone；上述三个最终测试均保留，只是文件迁移到 `materialized_view_create_test.go`。原 commit 的 UTC `NEXT_TIME` 断言已被 source #74 的最终 timezone 语义取代，不能按早期字段名机械比较。 |
| 24 | CREATE MV/MLog 透传 `SHARD_ROW_ID_BITS`、`PRE_SPLIT_REGIONS`，验证 parser restore/重复 option 错误、MLog physical metadata/region 和 MV physical metadata/region。 | **代码覆盖 100%；测试部分覆盖**。master 的 MV/MLog AST option、executor/schema-tracker `Options` 透传、parser 重复 option/restore 测试和 `TestCreateMaterializedViewLogPreSplitOptions` 均存在。source 最终 `TestMaterializedViewDDLBasic` 中 `mv_presplit` 的 MV physical table/region 断言仍在 source，但 master 未找到等价 MV 专项 E2E 测试，因此不能把测试写成逐项 100%。 |
| 25 | 初版 REFRESH MV 的 parser/AST、planner、utility executor、refresh-info/history、事务约束、权限和 complete/fast refresh 测试；同一 commit 还整理了 CREATE initial-build 的 metadata/session/read-TS 测试。 | **代码和测试均为部分覆盖**。master 已覆盖 CREATE initial-build 所需的 build session、read-TS、refresh-info prewrite/upsert、rollback 等 DDL 语义和相关测试；但没有 `REFRESH MATERIALIZED VIEW` AST、parser、plan、executor 或 integration test。source 的 `TestMaterializedViewRefreshComplete*`、`TestMaterializedViewRefreshFastNotSupported`、refresh AST/session tests 与 `mview_refresh` integration case 都尚未进入 master，随 PR5 处理。 |
| 26 | 禁止用户对 MV/MLog 执行 INSERT/REPLACE/UPDATE/DELETE、prepared DML、LOAD DATA、IMPORT INTO；同时为内部维护 SQL 保留 restricted-session bypass。新增 `TestCheckMViewUpdatable`，并更新 writetest/integration cleanup 路径。 | **代码覆盖 100%；测试主体覆盖但直接 unit test 未保留**。master 的 `CheckMViewUpdatable`、planner/executor guard 和 restricted maintenance helper 均存在，`mview_log_dml` integration 覆盖所有 source 外部 DML 场景，并已扩展 point-get/batch point-get。source 的 `TestCheckMViewUpdatable`（包含“不 restricted 的 maintenance 是 internal error”断言）在 master 中未找到同名或直接 unit coverage，故按 source 测试逐项计为部分覆盖。 |
| 27 | TopSQL client network in/out bytes 的统计、Top-N 选择与 reporter/stmtstats 测试；不含 MV 内容。 | **非 MV，不纳入 MV port；通用代码和测试已在 master 覆盖**。master 已有等价的 network-byte pipeline，并保留 source 新增的 `TestProcessStmtStatsData` 和 `TestNetworkBytesAccumulation`；这是既有 upstream prerequisite，不需要随 MV port 重复处理。 |
| 28 | 修复 point-get/batch-point-get 的 UPDATE/DELETE fast path 绕过 MV/MLog 不可写检查，针对 MV 和 MLog 的四类 SQL 均新增 integration 断言。 | **代码和测试覆盖 100%**。master `point_get_plan.go` 在进入 point-get/batch-point-get 前调用 `CheckMViewUpdatable`；`mview_log_dml.test` 保留 MLog/MV 的单点与 `IN (...)` UPDATE/DELETE 全部 8 个拒绝断言。 |
| 29 | 允许 `IMPORT INTO ... FROM SELECT` 使用 `disk_quota`，在 query import 生命周期启动/停止 quota checker，并新增 query-option、checker 和 real-TiKV `TestDiskQuotaFromSelect` 测试。 | **未覆盖，且是 MV initial-build 的实际 prerequisite 缺口**。master 的 `allowedOptionsOfImportFromQuery` 仍没有 `diskQuotaOption`，`TableImporter` 也没有 source 的 `StartDiskQuotaCheck` 调用；但 master `BuildMViewImportIntoOptions` 会在 `tidb_mview_maintain_import_disk_quota` 非空时为 CREATE MV 的 `IMPORT INTO ... FROM SELECT` 生成该 option，当前会被 importer 拒绝。source 的 `TestInitOptionsFromQueryPositiveCase`、`TestInitOptionsFromQueryNegativeCase`、`TestStartDiskQuotaCheck`、`TestDiskQuotaFromSelect` 也均未在 master 找到。需要在后续 CREATE/importer 修复中补齐代码和测试。 |
| 30 | MLog purge 的 utility SQL/parser/planner/executor、mutex、batch delete、checkpoint/history、失败重试、schedule 更新、session variable 及大量 purge runtime 测试；同时新增 CREATE/DROP MLog 的 purge-info row 初始化和清理。 | **代码和测试均部分覆盖**。master 已覆盖 prerequisite：`tidb_mlog_purge_info`/history system table、CREATE MLog 的 purge-info row、DROP 清理、schedule metadata 和对应的 upsert/job-ID/schedule/drop-cleanup 测试，且多个 source 测试仍同名存在。`PURGE MATERIALIZED VIEW LOG` 的 AST/parser/plan/executor、批量删除、mutex/checkpoint/history runtime 与其 `TestPurgeMaterializedViewLog*` 测试均不存在，仍属于 PR4。 |

## 第 31 到第 40 个 Commit 的测试复核

本节把 source commit 的实现和测试分开判断，并以 `origin/master` 的最终文件为准。对于
后续被 Unix-seconds、权限模型或 DDL 拆分重构的测试，只有能找到相同场景和断言语义时才算
覆盖；通用 prerequisite 不等于 MV 功能本身已经 port。

| # | Source 代码核对 | Source 测试核对 |
| ---: | --- | --- |
| 31 | **未覆盖**。`pkg/planner/mview/mvmerge.go`、`MVDeltaMerge` 计划节点、COUNT/SUM fast-refresh plan builder 和 plan codec ID 在 master 均不存在；master 只有通用聚合实现。该能力仍属于 PR5/S9。 | **未覆盖**。source 新增的 `TestBuildRefreshMVFastPlan`、`TestExplainRefreshMVFastPlanTree`、`TestBuildRefreshMVFastSumNotNullNoCountExpr`、`TestMVMergeBuildResultHandleCols`、`TestBuildCountSum`、`TestBuildCountExprSumExpr`、`TestBuildMinMaxHasRemovedGate`、`TestBuildSumWithoutCountExpr`、`TestBuildMissingCountStar`、`TestBuildMissingOldNew` 及其 casetest package 在 master 均找不到。 |
| 32 | **覆盖 100%（重构后）**。master `pkg/table/tables/mview_log.go` 的 `writeMLogRow` 保留独立 row slice、使用浅拷贝并同步消费 datum，符合 source 对 `AddRecord` 生命周期和 tracked datum 的优化语义。 | **覆盖 100%（语义等价）**。`origin/master:pkg/executor/test/writetest/mview_log_write_test.go` 的 `TestMLogTrackedReferenceTypes` 用 text/varbinary/decimal fixture 覆盖 source JSON/blob 等引用型 datum 的 INSERT/UPDATE 日志内容，并验证后续 tracked 列类型修改后的可读性。 |
| 33 | **未覆盖**。master 没有 source 的 `FullUpdateInnerSource`、MIN/MAX full-update lookup、相关 plan builder 分支或 `pkg/planner/mview` 实现；这是 PR5/S9 的 refresh planner 能力。 | **未覆盖**。`TestBuildRefreshMVFastPlanWithMinMaxHasFullUpdate`、`TestExplainRefreshMVFastPlanTreeMinMax`、`TestBuildMLogDeltaSelectTiFlashHint`、`TestBuildMLogDeltaSelectCommitTSWindow`、`TestBuildMergeSourceSelectJoinOperatorByMVNullability`、`TestBuildMinMaxNullableDependencyOrder` 均未在 master 找到。 |
| 34 | **未覆盖（MVS framework）**。master 没有 `pkg/mvservice`，也没有 domain/server/session 的 MV service 注册、任务执行、backpressure 或 service 配置变量。Grafana 的 Materialized View 面板由独立 commit `ea0454def0` 带入，不能作为 framework 代码覆盖。 | **未覆盖（MVS tests）**。source `pkg/mvservice` 测试文件中的 59 个 task executor、consistent hash、service helper、alert、history cleanup、time proxy 和 lifecycle 测试在 master 没有对应 package。source 的 BR 脚本修改属于非 MV branch drift。 |
| 35 | **未覆盖**。master 没有 `pkg/executor/mviewdeltamergeagg`、`MViewDeltaMergeAgg` executor、count/sum/min/max merge 实现、spill writer 或 runtime stats；source 同 commit 的 `Column.AppendCellRange`、`Column.HasNull` 和 delta-merge runtime stat 类型也未找到。 | **未覆盖**。source `exec_test.go` 的 33 个 merge、nullable、type-check、batch recompute、spill 和 runtime-stat 测试，以及 `chunk/column_test.go` 的新增断言，在 master 均不存在。 |
| 36 | **未覆盖**。master 的 executor builder、plan builder 没有 delta-merge aggregate builder 接入；source 对 `mview_delta_merge_agg_builder.go` 的扩展依赖 #35/#31/#33 的 refresh 链路。 | **未覆盖**。source 新增的 `TestMaterializedViewRefreshFastMinMax`（以及对应 planner casetest 调整）在 master 中没有等价测试；当前 master 仍无 refresh executor。 |
| 37 | **未覆盖；无独立功能新增**。这是 source refresh planner/operator 从 `mvmerge` 到 `mview` 的内部 rename，目标实现尚未进入 master，不能把文件 rename 视为 port。 | **无独立新增测试语义，仍待后续 slice**。source 只是同步修改 `mvmerge_test.go` 和 casetest 的名称/引用；由于整组 refresh tests 尚未进入 master，rename 也没有可核对的 master 测试落点。 |
| 38 | **覆盖 100%（重构后）**。source 的 DDL constraint、MIN/MAX supporting-index 检查、multi-schema effective table、ALTER MV/MLog refresh/purge/attributes action、job args 和 schema tracker 均可在 master #70789/#70874/#70927 的最终实现中找到；`PURGE IMMEDIATE` 的 CREATE/ALTER 拒绝也已进入 master。 | **部分覆盖**。两个 MIN/MAX index constraint 测试、`TestAlterMaterializedViewRefreshExprTypeValidation`、`TestAlterMaterializedViewLogPurgeExprTypeValidation`、`TestMLogDropTrackedColumnRejected` 和两个 job-args 测试已迁移或等价覆盖；refresh/purge metadata 更新测试已按 Unix-seconds、权限和 timezone 重构（旧函数名不存在），vector-index-on-MLog 专项测试未找到，`TestPurgeMaterializedViewLogDisallowExplicitTransaction` 和 source 中大量 purge runtime 测试仍待 PR4。 |
| 39 | **不适用**。只同步 `OWNERS`/`OWNERS_ALIASES`，没有 MV 代码。 | **不适用**。没有 MV 测试；该 commit 不纳入 port 完整性统计。 |
| 40 | **SHOW CREATE 未覆盖**。master 没有 `ShowCreateMaterializedView`/`ShowCreateMaterializedViewLog` AST、parser production、`ShowExec` 分支或 SHOW CREATE 格式化 helper；source 同 commit 中单独的 ALTER MLog `PURGE IMMEDIATE` 错误 hunk 已被 #70927 的 DDL 实现覆盖，属于混合 commit 的已覆盖部分。 | **SHOW CREATE 测试未覆盖**。source 的 `TestShowCreateMaterializedView`、`TestShowCreateMaterializedViewLog` 以及 parser restore cases 在 master 均不存在；master 的普通 `SHOW CREATE TABLE` 测试不能替代这些用户可见语义，仍属于 PR6。 |

## 第 41 到第 50 个 Commit 的测试复核

本节继续将 source commit 的代码语义和测试语义分开核对。对于混合 commit，只将
已经在 master 最终实现中找到等价落点的 hunk 记为覆盖；系统表字段或通用 helper
存在，但对应 refresh/purge/service/show runtime 不存在时，仍明确记录为部分覆盖。

| # | Source 代码核对 | Source 测试核对 |
| ---: | --- | --- |
| 41 | **未覆盖（仅有 schema prerequisite）**。source 增加 slow-log fallback、refresh 慢执行 timing、fast refresh 扫描行数收集和 `REFRESH_ROWS` 写入；master 只有 `REFRESH_ROWS` 字段及 bootstrap 断言，没有 refresh executor、`MVDeltaMerge` 或这些 runtime helper。 | **未覆盖**。`TestRestoreStmtTextForSlowLogWhenEmptySQL`、refresh history/`REFRESH_ROWS` 断言、重新启用的 fast-refresh 测试和 integration 调整都依赖 master 尚不存在的 refresh runtime。 |
| 42 | **未覆盖（非 MV prerequisite）**。source 对 signed int/decimal（包括常量）的 unary-minus flen 不加宽；master 后续 `eae6ab99f9` 只对 Column/CorrelatedColumn 保留 flen，常量仍加 1，语义并不等价。 | **未覆盖（非 MV 测试）**。source 的两个 flen 测试要求常量 signed int/decimal 保持原 flen；master 同名测试仍要求常量分别为 `+1`，不能视为 source 测试已 port。 |
| 43 | **部分覆盖**。`NewExtraCommitTSColInfo` 的 unsigned 类型、最终 MV 系统表 TSO schema、planner/rowcodec 的 unsigned 读写已在 master；source refresh/purge executor 和 `pkg/mvservice` 对 unsigned TSO 的使用仍不存在。 | **部分覆盖**。master 的 bootstrap 列定义和 `TestDecodeWithCommitTS` 覆盖最终 unsigned schema/decoder 语义；source `TestNewExtraCommitTSSchemaColType` 未按原名保留，refresh/service 相关测试仍缺失。 |
| 44 | **部分覆盖**。master 已有 `tidb_mview_maintain_mem_quota`、`SessionVars`、DDL job 变量快照和通用 apply/restore helper；source 将 quota 应用于 refresh/purge executor 的逻辑因对应 runtime 不在 master，尚未覆盖。 | **未覆盖**。`TestPurgeMaterializedViewLogUsesMVMaintainMemQuota`、`TestMaterializedViewRefreshUsesMVMaintainMemQuota` 和 `TestMVMaintainMemQuota` 在 master 均未找到；变量定义存在不能替代这些 quota 行为回归。 |
| 45 | **覆盖 100%（重构后）**。master 的 CREATE MV/MLog dispatcher、schema tracker 和 validation 已包含清除复制列 key/auto-increment 等 flags，以及禁止对 DATE/DATETIME/TIMESTAMP/TIME 使用 SUM 的最终逻辑。 | **覆盖 100%（语义等价）**。`TestCreateMaterializedViewColumnFlags` 与 `TestCreateMaterializedViewLogColumnKeyFlag` 已迁移到 `materialized_view_basic_test.go`，并保留 source 的列元数据断言；SUM 时间类型错误场景也在同一 basic DDL 测试中。 |
| 46 | **部分覆盖**。ALTER MV refresh、ALTER MLog purge 的 notifier event constructor/producer 和 tracked-column DDL 保护已在 master；`pkg/mvservice` 消费这些事件的 handler 仍不存在。 | **部分覆盖**。`TestMVAlterEventConstructors`（master 还扩展了 attributes event）、`TestMLogOnlineDDLDropTrackedColumnRejected`、`TestMLogDropTrackedColumnRejected` 及 integration 行为已覆盖；source `pkg/mvservice/task_handler_test.go` 的事件触发验证因 service package 缺失未覆盖。 |
| 47 | **未覆盖**。`AttachStatsCollectorForInternalSession` 及 refresh internal-session usage/index collector 的接入依赖 master 尚不存在的 refresh executor。 | **未覆盖**。`TestMaterializedViewRefreshFastUpdatesStatsModifyCount`、`TestMaterializedViewRefreshFastMinMaxWhereSeparateIndexes` 和相关 refresh regression 在 master 均不存在。 |
| 48 | **未覆盖**。master 没有 `SHOW MATERIALIZED VIEWS`/`SHOW MATERIALIZED VIEW LOGS` 的 AST、parser、planner predicate 或 executor 分支；普通 SHOW TABLES/SHOW CREATE TABLE 不等价。 | **未覆盖**。`TestShowMaterializedViews`、`TestShowMaterializedViewLogs` 及 source parser restore cases 在 master 均不存在，仍属于 PR6。 |
| 49 | **部分覆盖**。master 已有 alert attributes 的 parser、CREATE/ALTER DDL metadata、专用 action/job args、notifier 和 statistics no-op 分支；source 的 MV service alert handling 和 SHOW CREATE MV 属性格式化因 `pkg/mvservice`/SHOW CREATE MV 缺失仍未覆盖。 | **部分覆盖**。CREATE/ALTER attributes、非法值、job-args 和 notifier 测试已迁移或由更强断言覆盖；source `SHOW CREATE` attributes 测试及 service handler 测试在 master 不存在。 |
| 50 | **部分覆盖（混合 commit）**。多 MLog `table_ids`、DROP DATABASE 的 refresh/purge info 清理、CREATE MV import option/变量和最终 system-table indexes 已在 master；但 `disk_quota` 生成后仍受 #29 importer query-option/checker 缺口影响，端到端功能不能算已覆盖。通用 CBO/testdata 和 branch-sync hunk 不纳入。 | **部分覆盖**。`TestBuildCreateMaterializedViewImportSQLDiskQuota` 已并入并扩展为 `TestBuildCreateMaterializedViewImportSQL`，多 MLog job-id 和 DROP DATABASE cleanup 也有等价/更强测试；source `TestMViewMaintainImportDiskQuota` 及 importer `disk_quota` positive/negative/real-TiKV 回归未在 master 找到。 |

## 第 51 到第 60 个 Commit 的测试复核

本节继续将 source 代码和测试分别与 `origin/master` 的最终状态核对。特别注意：
一个 source commit 内已经进入 master 的通用 prerequisite、DDL metadata 或 initial-build
实现，不能替代尚未进入的 refresh/purge/MV service runtime；测试结论也只在同一场景的
断言实际存在时才写为覆盖。

| # | Source 代码核对 | Source 测试核对 |
| ---: | --- | --- |
| 51 | **部分覆盖（通用 prerequisite 已覆盖）**。`MAX_COUNT`/`MIN_COUNT` 的 TiFlash pushdown、PB codec、MPP one-stage planning 和 checker 已在 master；`TRUNCATE TABLE` 拒绝 MLog 也已由 basic DDL 实现覆盖。`mv_refresh_observability.go`、refresh step timing/plan rows、`DRY RUN`/`WITH PROFILE` plan/executor、`WITH ASYNC MODE` refresh syntax 和实际 refresh runtime 均不存在。source 同 commit 的 TiFlash unary-minus pushdown hunk 在 master 未找到，不能随 aggregate prerequisite 计为覆盖。 | **部分覆盖**。`TestAggFuncMaxMinCountToPb`、`TestCheckAggPushDownMaxMinCount`、`TestTryToGetMppHashAggsForMaxMinCount` 在 master 同名保留；`TestProfileMaterializedViewRefreshStepRuntime`、dry-run/profile plan、current DB、async refresh 和 refresh SQL syntax 测试没有 master 对照，仍属于 PR5。 |
| 52 | **部分覆盖（抽象已覆盖，owner runtime 未覆盖）**。master 已有 `MViewExecutionSessionVars`、capture/apply/restore helper 及 CREATE MV DDL job 的 execution-vars snapshot，覆盖 source 抽象的最终形态；source 将这些变量应用到 refresh、purge 和 `pkg/mvservice` global-session 工作流的代码无落点，因为 master 没有 refresh/purge executor 或 MVS。 | **未覆盖**。6 个 source 测试分别验证 refresh/purge 的 caller-session quota、manual/internal refresh 的 TiFlash vars 和 MVS 读取 global vars；master 没有这些运行时 owner，也没有等价行为测试。 |
| 53 | **部分覆盖**。CREATE MV validation 已保留 nullable `SUM(expr)` 必须有 `COUNT(expr)` 的最终语义（包括重复 `COUNT(expr)` 可接受）；但 `ArgNotNull`、nullable MIN/MAX 的 optional count dependency、duplicate-count delta-merge planner/executor 逻辑都依赖未 port 的 `pkg/planner/mview`/`mviewdeltamergeagg`，不在 master。 | **未覆盖**。source 的 5 个测试覆盖 nullable MIN/MAX fallback、duplicate `COUNT(expr)` 和 fast refresh 聚合结果；master 没有 delta-merge/runtime 或对应 planner tests，亦未找到 source 在 basic DDL 测试中新增的 nullable aggregate CREATE case。 |
| 54 | **部分覆盖**。`normalizeMVDefinitionHintDBNames` 已在 master 的 CREATE MV 路径中保留；其余 shadow table、`ActionCreateMaterializedViewShadow`/cutover action、out-of-place refresh worker、schema diff、notifier、rollback/CAS/cleanup、statistics 和 session 处理均不存在。source 同时修复的 advisory-lock owner/cleanup 也未在当前 master 找到等价实现，不能因其非 MV 主线而计作覆盖。 | **部分覆盖**。`TestNormalizeMVDefinitionHintDBNames` 同名存在；其余 20 个 MV out-of-place/cutover 测试以及 3 个 advisory-lock regression test 在 master 均无对照。out-of-place refresh 仍属于 PR5，不能由 CREATE MV initial-build 测试替代。 |
| 55 | **部分覆盖，不能整体标记为 DDL 已覆盖**。CREATE MLog duplicate-column validation 及 schema-tracker 对应检查已在 master；但 source 对 `EXCHANGE PARTITION`、`ALTER TABLE ... PARTITION BY`、`REMOVE PARTITIONING` 的 MV/MLog 依赖拦截，在 master 的对应入口没有调用或等价 helper。`latestServerInfosByInstance` 的 restart de-dup 也因 MVS 不存在而未覆盖。 | **部分覆盖**。`TestCreateMaterializedViewLogRejectsDuplicateColumns` 已迁移到 `materialized_view_basic_test.go`；两个 partition-DDL MV guard 测试和 server restart/DDL-ID 去重测试没有 master 对照。 |
| 56 | **覆盖 100%（通用 prerequisite）**。FULL OUTER JOIN 的 parser/AST、feature switch、logical/physical planning、hash join V1、MPP serialization、cost/reorder 适配均已在 master；此能力不是 MV runtime，本轮及后续 MV complete-delta 只应复用它。 | **覆盖 100%**。source 的 3 个 hash-join tests、10 个 full-join planner/cost tests、`TestTiDBEnableFullOuterJoin` 与 integration case 都在 master 保留（部分文件路径已随测试拆分调整）。 |
| 57 | **部分覆盖（仅 FULL OUTER JOIN follow-up 已覆盖）**。source 中为 FULL OUTER JOIN MPP 加入的 plan-to-PB/task 适配已在 master；但 complete-delta AST/mode、touched-row marker、`MViewCompleteDeltaApply` operator/builder/runtime stats、diff-source planner、NULL-safe join pushdown、parser 和 refresh integration 都不存在。 | **部分覆盖且 MV 主体未覆盖**。`TestMPPFullOuterJoinToPB` 和 `TestMPPFullOuterJoinWithoutShuffleHint` 在 master；其余 28 个 complete-delta/touched-row/NULL-EQ/refresh parser-planner-executor tests 均没有对照，因此不能以 FULL OUTER JOIN prerequisite 宣称该 commit 的 MV 测试已 port。 |
| 58 | **部分覆盖**。CREATE/ALTER schedule 的“无 schedule 写 NULL”、best-effort metadata update/warning 及最终 history cancel-request schema 字段已被 master 的 Unix-seconds/timezone 版本重构覆盖；`CANCEL MATERIALIZED VIEW ... JOB` parser/AST/executor、watcher/controller、refresh/purge cancellation state、MVS backoff 和 pooled-session lifecycle 未进入 master。 | **部分覆盖**。`TestAlterMaterializedViewRefreshBestEffortInfoUpdateWarning` 与 `TestAlterMaterializedViewLogPurgeBestEffortInfoUpdateWarning` 同名迁移；bootstrap tests 检查最终 cancel-request columns。其余 13 个 purge/refresh cancel watcher、manual-cancel/backoff、no-schedule runtime 和 source 旧 upgrade-version tests 没有等价 master runtime coverage。 |
| 59 | **部分覆盖（仅共享 DDL helper）**。master 已有 `mviewutil.HasIndexWithPrefixCoveringColumns`，CREATE/ALTER MV 的 MIN/MAX supporting-index validation 复用该 helper；但 FAST `AS OF TIMESTAMP` AST/parser、refresh TSO window/read snapshot、bounded plan/executor、GC safe-point handling、history/finalize 语义和 `getHashOrKeyPartitionColumnName` 改为直接使用 provided table 的修复均未进入 master。 | **未覆盖**。21 个 source tests 都是 bounded fast-refresh、history/GC/snapshot或 provided-table regression；master 没有 REFRESH MV runtime，也未保留 `TestGetHashOrKeyPartitionColumnNameUsesProvidedTable`。 |
| 60 | **部分覆盖（DDL initial-build/权限已覆盖）**。master 保留 `MViewExecutionSessionVars` abstraction、CREATE MV job snapshot/apply、import option builder、ALTER-only privilege 下的 internal schedule update，以及 best-effort metadata session；这些是 source DDL 面向的最终重构。source 对 refresh/purge/MVS 执行变量 fallback、out-of-place build 和 service global-vars 的运行时接入仍不存在。 | **部分覆盖**。`TestBuildCreateMaterializedViewImportSQL` 已覆盖 thread/disk-quota SQL，`TestAlterMaterializedViewRefreshUpdatesNextUnixSecondsWithAlterPrivilegeOnly` 和 MLog counterpart 覆盖仅 ALTER 权限更新 schedule；其余 execution-vars apply-failure、manual/internal refresh、out-of-place/dry-run 和 MVS best-effort tests 均未迁移，因为 owner runtime 尚未进入 master。 |

## 第 61 到第 70 个 Commit 的测试复核

本节继续把 source 的代码语义与测试语义分开核对。特别是 refresh/purge/MV
service 的最终系统表字段已经出现在 master，不表示其执行器、调度器或对应的运行时
回归测试也已经完成 port；只有能在 `origin/master` 找到相同场景的实际实现或测试，才
会计为覆盖。

| # | Source 代码核对 | Source 测试核对 |
| ---: | --- | --- |
| 61 | **部分覆盖**。master 已有 `MViewInitBuildState`、CREATE MV 的 `Building -> Ready` 元数据更新和 schema diff reload；`CheckMViewReadable` 已接入普通 datasource、point-get/batch-point-get，IMPORT 子 session 也继承 maintenance flag。`REFRESH MATERIALIZED VIEW COMPLETE` 必须显式指定 subtype 的 parser/AST/runtime、refresh-ready gate，以及 auto-analyze 的 not-ready MV skip/retry 仍不存在；因此不能把 initial-build state 的已覆盖部分扩大为完整 refresh/auto-analyze 行为。 | **部分覆盖**。`TestCreateMaterializedViewHistoryJobSchemaVersion` 与 `TestImportIntoChildSessionInheritsMaintenanceFlag` 同名保留；`TestCreateMaterializedViewPauseAndResume` 等价覆盖 building MV 的普通/point-get/batch-point-get 读拦截。source 的“读取并拒绝 REFRESH”、未显式 COMPLETE subtype Restore、auto-analyze skip 和 priority-queue retry 四类测试没有 master 对照，故不是 100%。 |
| 62 | **部分覆盖**。最终 history schema 已在 `pkg/meta/metadef/system_tables_def.go`：MV/MLog 名称快照、duration 字段和时间维度 indexes 都以其最终命名和布局存在。source 的 refresh/purge statement result、affected-row metrics、history 实际写入、时区转换和 complete-refresh result 路径依赖尚未进入的 refresh/purge executor，master 没有等价 runtime。 | **部分覆盖**。`TestBootstrapMaterializedViewSystemTables`/bootstrap upgrade tests 断言最终 history 表的字段和 indexes，覆盖最终 schema 结果；但没有 `TestUpgradeToVer223MaterializedViewHistoryColumnsAndIndexes` 的逐版本升级回归，也没有 source 的 6 个 refresh statement-result 测试或 `TestHistTimeUsesLocation`，不能将 schema assertion 视为 runtime 测试覆盖。 |
| 63 | **混合 commit；MV 相关代码覆盖**。该 commit 的唯一 MV hunk 是 BR 将 5 张 MV maintenance system table 加入 `unRecoverableTable`；master `br/pkg/restore/snap_client/systable_restore.go` 已包含相同表集合。prepare dedup cache、plan cache、expression/vectorization 和会话优化是非 MV branch drift，不作为 MV port 漏项。 | **MV 测试未覆盖；其余不纳入**。source `TestMVSystemTablesAreUnrecoverable` 在 master 没有同名或直接等价断言，虽然其被测 table list 已存在；其余 `TestDisableReuseChunk` 和 prepare-dedup tests 是非 MV 测试，不参与本审计。 |
| 64 | **部分覆盖**。master 已有 refresh-alert system table、CREATE rollback、ALTER refresh schedule disable、DROP MV 和 DROP DATABASE 的 alert 清理及 best-effort failure handling；最终 schema 也保留 history duration/index/heartbeat 字段。source 的 running-history heartbeat、MV service alert reconciliation、stale-alert/history cleanup 和 owner-only cleanup 仍没有 `pkg/mvservice` 或等价实现。 | **部分覆盖**。source 的 DROP alert cleanup、DROP DATABASE/delete-failure 和 ALTER delete-failure 测试在 master 同名保留；`TestAlterMaterializedViewRefreshDisableScheduleUpdatesAlert` 以两个 subtest 覆盖清理普通 alert 与保留 refresh-failed alert。最终 bootstrap/upgrade tests 覆盖字段布局，但 source 的 heartbeat、history cleanup 和 service reconciliation 测试均无对照。 |
| 65 | **部分覆盖**。`tidb_mview_maintain_isolation_read_engines`、`MViewExecutionSessionVars` capture/apply/restore，以及 CREATE MV job snapshot 和 initial-build application 已在 master 的后续重构中存在。source 让 refresh、purge 和 MV service maintenance session 独立使用该变量的 owner runtime 尚未进入 master，不能由 CREATE initial-build 路径代替。 | **未覆盖**。7 个 source 测试均验证 purge/refresh/service 的 isolation-read-engine 行为或 apply-failure fallback；master 没有 refresh/purge/MV service owner，也没有这些场景的等价测试。 |
| 66 | **部分覆盖**。最终 refresh history schema 已有 `REFRESH_COMMIT_TSO` 及 `idx_mview_name_commit_tso`。master 没有 refresh executor 取得事务 commit TSO 并写入 refresh info/history 的实现，out-of-place preserve 行为也没有落点。 | **部分覆盖**。bootstrap schema tests 覆盖最终列和 index；source 的 5 个 fast/complete refresh snapshot-match 测试以及 `TestUpgradeToVer226MaterializedViewRefreshCommitTSO` 都没有 master 对照。 |
| 67 | **未覆盖**。将 MLog purge delete 移出 pessimistic transaction、部分删除后的 history/checkpoint 处理和手工取消/begin-failure 行为全都属于尚未进入 master 的 purge executor。 | **未覆盖**。`TestPurgeMaterializedViewLogDeleteErrorAfterPartialSuccess`、`TestPurgeMaterializedViewLogManualCancelAfterPartialSuccess` 和 `TestPurgeMaterializedViewLogBeginFailure` 没有 master 对照。 |
| 68 | **未覆盖**。MLog purge adaptive batch size、deadline/sleep fallback、manual-cancel controller、相关 sysvar 的运行时使用及 MV service 调度均依赖尚未进入 master 的 purge/service runtime。 | **未覆盖**。3 个 throttle fallback、2 个 adaptive batch-size、cancel-controller 和 deadline derivation 测试在 master 均无对应实现或测试。 |
| 69 | **部分覆盖**。master 已解析、校验并持久化 `mview_alert_refresh_failed`，包含 `MaterializedViewInfo`、ALTER job args、CREATE/ALTER metadata 与 disable-schedule 时对 failed alert 的保留语义。refresh 失败写 alert、fallback/finalize 顺序以及 MV service alert 处理仍依赖未 port 的 refresh/service runtime。 | **部分覆盖**。basic DDL 测试覆盖 CREATE/ALTER 的 yes/no、非法值和 metadata 更新，`TestAlterMaterializedViewRefreshDisableScheduleUpdatesAlert` 覆盖保留 failed alert；job-args tests 也覆盖新字段。source 的 refresh failure/fallback alert 写入和 `buildResolvedMVRefreshAlertSQL` 测试没有 master 对照。 |
| 70 | **部分覆盖**。最终 MLog purge history schema 有 `PURGE_CUTOFF_TSO`，并由 bootstrap tests 检查；但 purge cutoff fence 单调性和 fast refresh 读取 purge history 后的 hazardous-state 拒绝逻辑都需要尚未进入 master 的 purge/refresh runtime。 | **部分覆盖**。bootstrap schema tests 覆盖最终 `PURGE_CUTOFF_TSO` 列；`TestPurgeMaterializedViewLogSkipsWhenCutoffFenceWouldGoBackward`、`TestMaterializedViewFastRefreshRejectsHazardousPurgeHist` 及 source 的版本 228 upgrade test 在 master 均无等价测试。 |

## 第 71 到第 80 个 Commit 的测试复核

本节按最终用户可见语义核对。尤其注意：master 的 MV DDL 已经复用并重构了部分
source 的 metadata/validation，但这不能覆盖尚未进入的 refresh、purge、COMPARE 或
`pkg/mvservice` owner runtime；反过来，系统表字段、parser 或静态权限枚举存在，也不能
单独证明运行时的权限检查和状态查询已经完成。

| # | Source 代码核对 | Source 测试核对 |
| ---: | --- | --- |
| 71 | **代码覆盖 100%（重构后）**。master 在 DROP/TRUNCATE 入口统一通过 `checkTableMaterializedViewConstraints` 拒绝截断 MV、MLog 及带 MLog/MV 依赖的 base table；`IMPORT INTO` builder 也拒绝带 MLog 的 base table。原 commit 在 `TruncateTable` 中直接检查 `MLogID` 的写法已被这一统一约束替代。 | **部分覆盖**。`TestDropTableMaterializedViewConstraints` 覆盖 TRUNCATE MV、MLog、带依赖 MV 的 base table 和仅有 MLog 的 base table。source 新增的 `TestImportIntoRejectsMaterializedViewLogBaseTable` 在 master 没有同场景测试；不能因 builder 中已有 guard 而把测试记为 100%。 |
| 72 | **部分覆盖**。`OPERATE VIEW` 已完整进入 parser、privilege cache、bootstrap/upgrade 和 GRANT/REVOKE 静态权限集合；CREATE/DROP MLog 的权限路径也已在 PR2b DDL 中实现。source 的 `SHOW MATERIALIZED VIEW ... REMAIN_LOGS`、`SHOW MATERIALIZED VIEW LOG ... WAIT_PURGE`、refresh/purge/cancel/COMPARE 的实际权限检查依赖尚未进入的 PR4/PR5/PR6/PR7 runtime，不能由静态 privilege 定义替代。 | **部分覆盖**。`TestCreateMaterializedViewLogPrivilege`、`TestDropMaterializedViewLogPrivilege` 在 master 有直接对照，`OPERATE VIEW` 的 parser/cache/bootstrap 也有测试。source 的 MV/MLog 特殊 `GRANT ALL` 限制、status/SHOW CREATE 授权细节、ALTER purge 权限、refresh/cancel/COMPARE 权限及其 integration 测试没有完整等价的 master runtime 覆盖。 |
| 73 | **部分覆盖**。master 已有 MLog `ADD COLUMN`、base-column 到 MLog/MV 的类型和默认值同步、tracked NULL 到 NOT NULL 拒绝、online DDL 安全性、schema tracker 和 involving-schema 处理。source 同时包含的 `MViewShadow` 可读性语义依赖 out-of-place refresh/cutover，master 没有对应 owner runtime，因此该混合 commit 不能整体记为 DDL 已覆盖。 | **主体覆盖，但非 100%**。`TestMaterializedViewBaseModifyColumnMultiSchemaInvolvingSchemaInfo`、MLog add-column/default/invalid/privilege/new-MV/online-DDL 等主要回归场景均已迁移到 master 的 MV DDL/writetest 文件。`TestCheckMViewShadowReadable` 以及 `TestAnalyzeMVColumnUsageGroupByAlias`、`TestAnalyzeMVColumnUsageWhereReferenceUnsupported`、`TestFieldTypeForMVRelatedColumnClearsBaseOnlyFlags`、`TestAnalyzeMVColumnUsageGroupByOrdinal` 这些 source 内部单测没有保留。 |
| 74 | **未覆盖**。手工 cancel 后持久化 backoff、fallback reschedule 和 task handler 行为属于 `pkg/mvservice`；当前 master 没有该 package 或同等 service/controller。 | **未覆盖**。source `task_handler_test.go` 的 manual-cancel/backoff 回归在 master 没有 owner 实现和对照测试。 |
| 75 | **未覆盖**。master 只有 mysql MV maintenance system tables，并未暴露 `information_schema.tidb_mviews`、`tidb_mlogs`、`tidb_table_mview_dependencies`，也没有对应 reader、extractor 或 predicate pushdown。 | **未覆盖**。source 的 internal reader、infoschema、schema-tracker、extractor 和 SQL integration tests 都没有 master 对照。 |
| 76 | **部分覆盖**。`ALERT ROWS` 的 grammar/AST、DDL validation、`MaterializedViewLogInfo.LogAccumulationAlertRows` 及 metadata 持久化已在 master；`BuildMLogAccumulationAlertRows` 和最终 default/zero/custom 语义均存在。按 owner/阈值扫描 MLog、异步去重、metrics 和 alert 触发依赖缺失的 MVS，仍属于 PR7。 | **部分覆盖**。`TestCreateMaterializedViewLogAccumulationAlert` 在 master 直接覆盖负值、默认、0、非零阈值和 metadata 语义。source 的 MVS scan/filter/error/strict-threshold/non-overlap 和 service-helper row-count 测试无法在 master 找到等价实现。 |
| 77 | **未覆盖**。master 没有 `COMPARE MATERIALIZED VIEW` 的 AST/planner/executor、snapshot/privilege/output-table runtime 或比较结果语义。FULL OUTER JOIN 只是独立 prerequisite，不能视为 COMPARE 功能已覆盖。 | **未覆盖**。source 的 privilege、summary/output、out-of-place cutover、batch write、cleanup、timezone、NULL group key、empty input 和 read-TSO guard 测试均没有 master 对照。 |
| 78 | **代码语义覆盖 100%（最终存储表示已演进）**。master parser 接受空的 `ALTER MATERIALIZED VIEW LOG ON t PURGE`；ALTER DDL 清空 `PurgeStartWith`/`PurgeNext` 后，将最终的 `NEXT_PURGE_UNIX_SECONDS` 设为 `NULL`。这等价于 source 的清空 `NEXT_TIME`，并与当前 master 的 Unix-seconds schema 一致。 | **测试语义覆盖 100%**。master 的 `TestAlterMaterializedViewLogDDL` 显式执行空 `PURGE`，断言两个 schedule expression 为空，并断言 `NEXT_PURGE_UNIX_SECONDS is null`；parser test 也覆盖该语句可解析和 Restore。 |
| 79 | **未覆盖**。该 commit 只调整 COMPARE 执行器以 `baseQuery` 为 build side；master 不存在 COMPARE executor，因此没有可比的 build/probe owner。 | **未覆盖**。没有 COMPARE runtime，source 的相关执行语义也没有测试落点。 |
| 80 | **未覆盖**。complete-delta refresh planner/executor 及 `pkg/planner/mview` 在 master 均不存在；`M`/`Q` 到 current/recomputed 的命名重构必须随 PR5 complete-delta owner 一并 port，不能映射到普通 DDL。 | **未覆盖**。source 的 complete-delta layout、nullable group key、handle projection 和 mapping-validation tests 都依赖缺失的 refresh planner/executor。 |

## 第 81 到第 90 个 Commit 的测试复核

本节继续把生产代码语义和 source 测试语义分开核对。测试文件在 master 中已经经过
`materialized_view_basic_test.go`、`materialized_view_create_test.go`、
`materialized_view_alter_test.go` 和 `mview_log_write_test.go` 的拆分；只有场景和断言
语义等价时才算覆盖。对于 source 中依赖 `MaterializedViewShadow` 的分支，当前 master
没有 shadow owner/runtime，不能用普通 MV/MLog 校验替代。

| # | Source 代码核对 | Source 测试核对 |
| ---: | --- | --- |
| 81 | **代码覆盖 100%（重构后）**。master 的 `pkg/ddl/executor.go` 提供 `CheckMaterializedViewLogColumnSupported` 和按操作区分错误信息的 helper，CREATE/ALTER MLog executor 与 schema tracker 均调用它，worker 侧消费已校验的 job metadata；JSON 以及 binary charset 的 BLOB/TINYBLOB/MEDIUMBLOB/LONGBLOB 会被拒绝，TEXT/VARBINARY 等支持类型仍可复制。 | **测试覆盖 100%（重构后）**。`TestCreateMaterializedViewLogRejectUnsupportedColumns`、`TestAlterMaterializedViewLogAddColumnRejectsInvalidColumns` 和 `TestMLogTrackedReferenceTypes` 均在 master；后者把原先会被新规则拒绝的 JSON/BLOB fixture 改为 TEXT/VARBINARY，保留了支持类型的 MLog 写入回归。 |
| 82 | **代码覆盖 100%**。master 的 `pkg/ddl/mview_worker.go`、`pkg/ddl/materialized_view.go` 和 `pkg/ddl/schematracker/dm_tracker.go` 都拒绝 partitioned base table 创建 MLog；CREATE MV 的 executor/worker 检查也返回同一 `Unsupported ... on partition table` 错误，`isCreateMaterializedViewBaseCheckCancelledErr` 同时覆盖该错误。 | **部分覆盖**。`TestMLogPartitionedTableNotSupported` 覆盖 CREATE MLog 的拒绝，`TestIsCreateMaterializedViewBaseCheckCancelledErr` 覆盖取消错误分类；但 source 的 `TestCreateMaterializedViewOnPartitionTable`（同时断言 CREATE MLog 和 CREATE MV）在当前 master 测试树中没有等价用例，CREATE MV 分区表拒绝缺少直接回归测试。 |
| 83 | **代码覆盖 100%（重构后）**。master 的 `hasAlterTableAddUniqueIndexOperation` 和 `CheckIndexOperationMaterializedViewConstraints` 已接入 CREATE UNIQUE INDEX、ALTER TABLE ADD UNIQUE/PRIMARY KEY 以及 schema tracker 路径；MV 表仍允许普通非 unique index，MLog 表的既有 index 保护不受影响。 | **部分覆盖，未找到专项回归**。当前 master 没有 `TestCreateUniqueIndexOnMaterializedView` 或等价的 MV unique-index/primary-key 拒绝测试；`TestMaterializedViewDDLProtectsMinMaxSupportingBaseTableIndexes*` 只验证 base table 的 MIN/MAX supporting index 保护，不能替代该 commit 的 MV 表操作测试。 |
| 84 | **部分覆盖**。master 的 `isValidMaterializedViewLogBaseTable` 及 executor/worker/schema tracker 已拒绝把 MV 或 MLog 作为 CREATE MLog target；但 master 没有 `MaterializedViewShadow` 字段、owner 或 refresh cutover runtime，因此 source 对 shadow table target 的保护分支没有实现落点。 | **部分覆盖**。`TestCreateMaterializedViewLogRejectNonBaseObject` 和 `TestCreateMaterializedViewLogRejectMaterializedObjects` 覆盖 view/sequence/temporary/system/MV/MLog target；source 的 `TestCreateMaterializedViewLogRejectShadowTable` 依赖 shadow refresh failpoint，当前 master 没有对应测试或运行时。 |
| 85 | **不适用（仅测试 commit）**。source 没有生产代码变更，相关 generated-column MLog runtime 由前后 DDL/DML commit 提供。 | **测试覆盖 100%（重构后）**。`TestCreateMaterializedViewLogAllowsGeneratedColumns`、`TestMLogInsertGeneratedColumn`、`TestMLogUpdateTrackedGeneratedColumnOnly`、`TestMLogAlterAddGeneratedColumn`、`TestMLogAlterDropTrackedGeneratedColumnCurrentBehavior`、`TestMLogAlterGeneratedColumnConstraints`、`TestMLogAlterModifyTrackedVirtualGeneratedColumn` 和 `TestMLogAlterRenameTrackedGeneratedColumnCurrentBehavior` 均在 master，只是迁移到了 create/writetest 文件。 |
| 86 | **代码覆盖 100%（本 commit 新增的 system/non-base 语义）**。master 的 `isValidMaterializedViewLogBaseTable` 统一排除 memory/system DB、view、sequence、temporary、MV 和 MLog，并由 executor、worker、schema tracker 复用；错误统一为 `is not BASE TABLE`。shadow guard 属于 #84 的未覆盖 runtime，不是本 commit 新增的 system-table 语义。 | **测试覆盖 100%（本 commit 新增场景）**。master 的 `TestCreateMaterializedViewLogRejectNonBaseObject` 已覆盖 global temporary、mysql.user、information_schema、temporary table、MV 和 MLog，并检查统一错误；shadow case 仍随 #84 的缺失 runtime 单独计为未覆盖。 |
| 87 | **代码覆盖 100%（重构后）**。master parser/AST 保留两个 DROP 语句的 `IfExists`，DDL executor 对缺失 MV/schema 和缺失 derived MLog 追加 note，同时不抑制缺失 base table/schema 或错误对象类型，并将标志传入底层 DropTable。 | **测试覆盖 100%（重构后）**。`TestDropMaterializedViewIfExists` 覆盖缺失对象、缺失 schema、重复 drop、错误对象和 MLog no-op/note；`pkg/parser/parser_test.go` 覆盖两个 IF EXISTS 语句的 parse/restore。 |
| 88 | **代码覆盖 100%**。master 的 `FieldTypeForMaterializedViewLogColumn` clone base field type、清除 key/auto-increment/on-update flags，并归一普通 BLOB 的 unspecified flen；CREATE MLog、ALTER MLog ADD COLUMN 和 schema tracker 共用该 helper，避免 TEXT 被升级成 MEDIUMTEXT。 | **测试覆盖 100%（重构后）**。`TestCreateMaterializedViewLogPreservesTextColumnTypes` 断言 tinytext/text/mediumtext/longtext 的 SHOW CREATE 结果，`TestAlterMaterializedViewLogAddColumnBasic` 断言新增 text 列最终为 `mysql.TypeBlob`，均在 master。 |
| 89 | **代码覆盖 100%**。master `pkg/ddl/modify_column.go` 的 `isColumnCommentOnlyChange` 会在 clone 后仅忽略 comment 字段比较；base table 存在 MV/MLog 依赖时，comment-only MODIFY 跳过类型/依赖约束和相关表同步，实际类型变化仍被拒绝。 | **未覆盖 source 的 MV 专项回归**。当前 master 测试树中没有 source 的 comment-only SQL、`column_comment` 断言或等价 MV/MLog 依赖测试；`pkg/ddl/tests/partition/modify_column_test.go` 的普通 partition comment-only case 不涉及 MV 依赖，不能算作该测试的覆盖。 |
| 90 | **代码覆盖 100%（且 master 扩展到 ALTER）**。master CREATE MV 在构造前调用 `validateCommentLength`，ALTER MV comment 也复用相同校验；strict mode 报错，非 strict mode 截断并产生 warning，最大长度仍为 `ddl.MaxCommentLength * 2`。 | **测试覆盖 100%（重构后）**。`TestMaterializedViewCommentLength` 覆盖 CREATE/ALTER 的最大长度、超长 strict error、非 strict warning 及最终截断长度，已在 master `materialized_view_basic_test.go`。 |

## 第 91 到第 100 个 Commit 的测试复核

这一组 commit 主要进入尚未 port 的 purge、refresh、MV service 和 planner runtime。这里把
master 已有的通用 prerequisite 与 MV 专属接入分开记录；只有生产代码和测试场景都能在
当前 `origin/master` 找到等价 owner，才记为完整覆盖。

| # | Source 代码核对 | Source 测试核对 |
| ---: | --- | --- |
| 91 | **未覆盖**。source 在 `ExecStmt.LogSlowQuery` 增加 `tidb_mlog_log_slow_purge` 开关和 `shouldSkipSlowLogForInternalMVMaintenance`，默认不记录 MLog purge slow query。master 的 slow-query pipeline 没有该 sysvar、判断函数或 MLog purge statement type；通用 internal slow-query 处理不能替代这个按开关控制的行为。 | **未覆盖**。source 修改 `TestSlowQueryMisc`，验证外部和 restricted internal MLog purge 都不进入 `information_schema.slow_query`。master 的同名测试没有 MLog purge 场景，且没有可执行的 MLog purge runtime。 |
| 92 | **未覆盖**。source `pkg/mvservice/service_helper.go` 将 refresh/purge history orphan cleanup SQL 抽成 helper，并加入 `FORCE INDEX (idx_refresh_status)` 与 `FORCE INDEX (idx_purge_status)`。master 没有 `pkg/mvservice`、history cleanup helper 或这些索引 hint。 | **未覆盖**。`TestServerHelperPurgeMVHistoryBeforeTSOUsesStatusIndexHints` 及其 service-helper fixture 在 master 没有对应 package 或测试 owner。 |
| 93 | **部分覆盖**。master `pkg/util/mviewutil/util.go` 已有 `FindVisibleIndexesWithPrefixCoveringColumns`，属于早先 DDL prerequisite；但 source 新增的 `SetFullUpdateLookupIndexHint`、refresh lookup template 的 `USE INDEX (...)` 绑定以及 `pkg/planner/mview` refresh planner 在 master 均不存在。 | **未覆盖**。`TestFullUpdateLookupIndexHintUsesAllSupportingIndexes` 和 `TestMaterializedViewRefreshFastMinMaxUsesSupportingIndex` 依赖缺失的 refresh planner/executor；master 的普通 index-hint 测试不覆盖 MV full-update lookup。 |
| 94 | **未覆盖**。source 在 MLog purge 的删除 internal session 上调用 `AttachStatsCollectorForInternalSession`，使删除后的 `mysql.stats_meta.count/modify_count` 正确落盘。master 没有 MLog purge executor 或该接入点；通用 `AttachStatsCollector`（如 importer）不是等价实现。 | **未覆盖**。`TestPurgeMaterializedViewLogUpdatesStatsMetaRowCount` 依赖 MLog purge runtime，master 没有该测试或可执行路径。 |
| 95 | **未覆盖**。source 的 auto-analyze priority queue 新增 `isTableHandledByPriorityQueue`，跳过 `MaterializedViewLog`，并在 DDL event 中删除已有 MLog analyze job。master priority queue 仍只跳过 `tblInfo.IsView()`，没有 MLog 类型判断、helper 或清理分支。 | **未覆盖**。`TestAnalysisPriorityQueueSkipsMaterializedViewLog` 在 master 不存在；现有 priority-queue 测试不创建或识别 MLog。 |
| 96 | **未覆盖**。source 增加 MLog `_tidb_commit_ts` 保留窗口的 executor/planner 传递和 selectivity 估算，包括 `MLogRetainedLowerTSO`、`MLogCommitTSEstimationContext`、`SplitMLogCommitTSFilterSelectivity` 及 refresh dry-run 计划接入。master 没有这些符号、MLog refresh plan 或对应 stats path。 | **未覆盖**。`TestExtractMLogCommitTSFilterBound`、三组 `TestExplainRefreshMVFastPlanUses...` 以及 hazardous purge-info dry-run 回归均依赖缺失的 refresh/purge runtime，master 没有等价测试。 |
| 97 | **未覆盖**。source 在 `pkg/mvservice` 增加 MLog analyze 调度器、`tidb_mlog_auto_analyze_ratio`、任务扫描/去重/并发控制、`AnalyzeMVLog` 和 statistics session-var restore。master 没有 `pkg/mvservice`、该 sysvar、MLog analyze task 或 service metrics。 | **未覆盖**。`TestMVServiceMLogAnalyze*`、`TestNeedAnalyzeMLog`、`TestServerHelperAnalyzeMVLogTemporarilyUpdatesStatsSessionVars` 和 `TestTiDBMLogAutoAnalyzeRatio` 均没有 master 对照。 |
| 98 | **未覆盖**。source 统一 cancel MV/MLog job 的用户错误，避免无权限用户通过“job not running/not found”探测状态，并修改 `CancelMaterializedViewJobExec` 的 privilege/precheck 路径。master 没有 refresh/purge cancel AST、executor、controller 或 `requestPurgeHistCancel` owner，不能用普通 `ADMIN CANCEL DDL JOBS` 替代。 | **未覆盖**。source 的 `TestCancelMaterializedViewJobNotRunning` 及 `TestCancelMaterializedViewLogPurgeJob` 权限断言在 master 没有对应 MV cancel 命令或测试。 |
| 99 | **代码覆盖 100%（重构后）**。master `PlanBuilder` 已将 CREATE MLog 权限错误改为 `CREATE MATERIALIZED VIEW LOG`，并以 base table 名称生成错误；不再暴露内部 `$mlog$` 名称，和 source 最终语义一致。 | **部分覆盖**：unit test 语义覆盖 100%，master 已将 `TestCreateMaterializedViewLogPrivilege` 迁移到 `materialized_view_create_test.go`，并保留 command、base-table name、无 `$mlog$` 三项断言；但 source 同步修改的 `mview_privilege.result` integration golden 在 master 没有对应 `mview_privilege` integration test/file，不能宣称 integration 测试已 port。 |
| 100 | **部分覆盖**。master 已有通用 `SessionVars.InternalSQLScanUserTable`，optimizer 也允许 restricted SQL 在该标志为 true 时收集 predicate columns（TTL prerequisite）；但 source 为 MV purge/fast refresh/dry-run 设置并恢复该标志、扩展 refresh plan 渲染和断言的 MV-specific hunk 在 master 没有 owner。 | **未覆盖**。source 的 `TestInternalSQLScanUserTableCollectsPredicateColumns` 以及 MLog commit-ts EXPLAIN 结果更新在 master 没有等价测试；现有 TTL/internal SQL 测试不能证明 MV maintenance 接入。 |

## 第 101 到第 110 个 Commit 的测试复核

这一组包含 MV service observability、refresh/purge runtime 以及 bootstrap/schema/naming
演进。对于同时修改通用依赖或历史 migration 的 commit，下面按 hunk 语义拆分；最终字段
名称或 bootstrap registry 已在 master 出现，不代表缺失的 runtime 行为也已经 port。

| # | Source 代码核对 | Source 测试核对 |
| ---: | --- | --- |
| 101 | **未覆盖（MV 部分）**。source 将 MV service metrics 从单一 `type` label 拆为 `component/type`，并同步 reporter、service helper、Grafana 查询和文档。master 没有 MV metrics/service 实现；Grafana 仍按旧的 `type` 聚合，不能视为 component 维度已覆盖。 | **不适用/未覆盖**。该 commit 没有新增 Go 测试；其 metrics reporter 行为依赖 master 尚不存在的 `pkg/mvservice`，没有可对照测试。 |
| 102 | **部分覆盖，混合 commit**。master 已有最终系统表中的 `REFRESH_SCHEDULE_DURATION_SEC`、`LAST_SUCCESS_REFRESH_END_UNIX_SECONDS`、相关 duration index，以及 CREATE MV 初始化这些字段的 DDL 路径；但 source 新增的 MLog purge row-ID range delete、delete-session TiFlash thread 设置、purge/refresh runtime observability、service metrics 和 history runtime 均不在 master。source 中的 `LAST_SUCCESS_ENDTIME` 中间形态应按后续 #108 的 Unix-seconds 最终命名处理，不能单独回灌。 | **部分覆盖**。master 的 bootstrap schema、`TestBuildCreateMaterializedViewRefreshInfoUpsertSQL` 以及 CREATE schedule duration/Unix-seconds 断言覆盖了 schema/DDL 部分；`TestPurgeMaterializedViewLogUsesDeleteTiFlashThreadsOnlyOnDeleteSession`、`TestPurgeMaterializedViewLogUsesRowIDRangeDeleteSQL`、`TestBuildMLogPurgeDeleteRowIDRanges`、`TestBuildPurgeMaterializedViewLogDeleteSQL`、`TestApplyMLogPurgeDeleteTiFlashThreads` 和 refresh/purge runtime 测试均依赖缺失的 executor/service。`TestUpgradeToVer229MaterializedViewRefreshScheduleDuration` 也没有同名升级路径，只有最终 bootstrap schema 检查。 |
| 103 | **未覆盖**。source 将 refresh-alert checker 改为从全局任务快照构造状态、按 owner 清理/更新 metrics，并增加双 hash-ring owner；master 没有 `pkg/mvservice` 或 refresh-alert checker runtime。 | **未覆盖**。`TestMVServiceRefreshAlertCheckerKeysScanGlobalTasks` 以及相关 global-task/alert helper tests 在 master 没有对应 owner。 |
| 104 | **混合 commit；MV 部分未覆盖**。source 的 MV 依赖是 purge service 使用 chunk-RPC/MPP 编码路径；但 commit 同时包含 `DEPS.bzl`、Makefile、go.mod/go.sum、cmd/mirror 和 local MPP timeout/encoding 等通用 branch drift。master 虽有通用 `distsql.SetEncodeType`，local MPP coordinator 仍未接入 source 的 `setMPPEncodeType`，且 MLog purge service 本身不存在；通用依赖同步不应作为 MV port 处理。 | **未覆盖 source 场景**。`TestSetMPPEncodeTypeRespectsChunkRPCSetting` 和对 MV purge helper 的 `EnableChunkRPC` 断言在 master 均无等价测试；现有 distsql `SetEncodeType` unit tests 不覆盖 local MPP coordinator 或 MV maintenance。 |
| 105 | **未覆盖**。source 修正 `ServerConsistentHash` 的有序 server ID 缓存、refresh-alert checker owner 选择和 owner 生命周期，全部属于 `pkg/mvservice`；master 没有该 service/controller。 | **未覆盖**。`TestMVServiceRefreshAlertCheckerOwnersScanGlobalTasks`、`TestMVServiceRefreshAlertCheckerOwnerSingleServer` 和 `TestServerConsistentHashRebuildMaintainsSortedServerIDsAndRing` 在 master 没有对应实现或测试。 |
| 106 | **代码和测试语义覆盖 100%（重构后）**。source 删除 classic bootstrap 中 version 223–229 的逐版本 MV migration，改为 final system-table bootstrap；master 采用 NextGen `MaterializedViewNextGenBootTableVersion` registry 和最终 schema，`TestBootstrapMaterializedViewSystemTables`/`TestUpgradeVersion285MaterializedViewBootstrap` 覆盖五张系统表及最终列/索引。旧版本函数删除不应机械 port。 | **覆盖 100%（重构后）**。source 的 `TestUpgradeToVer221MaterializedViewSystemTables` 及被删除的逐版本 schema 测试，已由 master 的最终 bootstrap schema 和 version-285 upgrade 测试按当前 bootstrap 架构等价覆盖；固定旧版本号和中间字段断言不再适用。 |
| 107 | **部分覆盖**。master 已在 DDL create/alter 路径保存 schedule timezone，并将 refresh/purge schedule 写入 Unix seconds；`TimeZoneLocation.Clone`、schedule eval helper、CREATE/ALTER metadata 和初始化 upsert 均存在。source 的 refresh executor、purge executor、out-of-place cutover、service schedule 读取和 internal-SQL schedule 更新仍没有 master owner。 | **部分覆盖**。CREATE/ALTER refresh 和 purge 的 Unix-seconds derivation、schedule-timezone 及权限测试已迁移到 master 的 create/basic/alter 测试；`TestPurgeMaterializedViewLogNextUnixSecondsOnlyUpdatesForInternalSQL`、`TestMaterializedViewRefreshNextUnixSecondsOnlyUpdatesForInternalSQL`、internal no-schedule/start-with cases、out-of-place case 和 `TestMaterializedScheduleRuntimeEvalUsesScheduleSQLMode` 均未找到等价测试。 |
| 108 | **部分覆盖**。master 的 refresh-info schema、CREATE MV 初始化 upsert 和 DDL SQL builder 已使用最终 `LAST_SUCCESS_REFRESH_END_UNIX_SECONDS`；但 source 在 refresh executor、`refreshInfoSnapshot`、锁定/读取/持久化成功路径和 out-of-place cutover 中的 Unix-seconds runtime 改造，因 refresh executor 尚未进入 master，仍未覆盖。 | **部分覆盖**。master `pkg/ddl/ddl_test.go` 和 bootstrap tests 已覆盖 SQL builder 与最终列类型/名称；source refresh runtime 测试文件及其读取、previous-success-time、persist-success 断言在 master 没有对应测试。 |
| 109 | **部分覆盖，重构后**。master bootstrap/schema、已合入 DDL 引用和错误/SQL helper 已采用 `REFRESH_START_TIME`/`REFRESH_END_TIME`、`PURGE_START_TIME`/`PURGE_END_TIME`、`LAST_HEARTBEAT_TIME`、`LAST_SUCCESS_SNAPSHOT_TIME`、`UPDATE_TIME` 等最终名称；但 source 对 purge/refresh history executor、alert/service SQL 和 runtime 的全量改名没有 master owner。 | **部分覆盖**。master bootstrap schema test 已断言最终列名和索引名；source 对 `mview_log_ddl`、refresh runtime、alert、heartbeat、history cleanup 的改名测试因缺失 purge/refresh/service runtime 未迁移，不能记为测试 100%。 |
| 110 | **部分覆盖，重构后**。master 已在 metadata、CREATE/DROP/ALTER/DML 和 DDL worker 中采用 `MView`/`MLog` 的最终命名（包括 `MViewInitBuildState`、`mviewTableInfo` 等）；但 source 同时改名的 refresh/purge executor、MV service、SHOW、COMPARE、complete-delta planner/executor 仍不存在，通用变量重命名不能替代这些缺失 owner。 | **部分覆盖**。master 的 bootstrap、DDL、DML 测试已使用最终 MV/MLog 命名；source 对 refresh/service/show/complete-delta 测试的大量重命名没有 master 对照。该 commit 没有新增独立测试函数，测试缺口来自相关 runtime 测试树尚未 port。 |

## 全部 Commit 审计

| # | Source commit | 日期 | Subject | 主要最终语义 | Port 归属 | Master 对照和结论 |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | `f3af7d5a83fd` | 2026-02-05 | `pkg/meta: store mview/mlog dependency metadata in TableInfo (#66023)` | 在 `TableInfo` 中加入 base table、MV、MLog 之间的反向依赖元数据。 | PR2b-create/drop/alter；元数据基础 | **重构后覆盖（100%）**。源 commit 新增的三个 `TableInfo` 指针、`MViewIDs`/`BaseTableIDs`/`BaseTableID` 字段及三类 `Clone` 深拷贝，均可在 master `pkg/meta/model/table.go` 找到；master #70789 及后续 DDL commit 还扩展了 `DependentMViewIDs`、schedule/alert/time-zone 字段。字段布局和 clone 实现已演进，但该 commit 的元数据语义完整保留。 |
| 2 | `e0d58ec5ca8c` | 2026-02-06 | `pkg/session: bootstrap mview refresh/purge system tables (#66024)` | 首次 bootstrap MV refresh/purge 信息、历史和 alert 相关系统表，并补充 bootstrap 测试及 restore 行为。 | PR1 | **重构后覆盖（100%）**。源 commit 的 4 张 current/history 表和 version-221 upgrade 在 master #70599 中改为 `pkg/meta/metadef/system_tables_def.go` 定义、`systemTablesOfMaterializedViewNextGenVersion` 注册及 `TestBootstrapMaterializedViewSystemTables`/`TestUpgradeVersion285MaterializedViewBootstrap` 测试；current 表最终命名为 `tidb_mview_refresh_info`/`tidb_mlog_purge_info`，并由后续演进增加 `tidb_mview_refresh_alert`。列、索引和 history schema 经过后续 refine，但源 commit 的 refresh/purge current-state、history bootstrap/restore 语义均已覆盖。 |
| 3 | `96a16b284bc8` | 2026-02-06 | `*: add materialized view DDL syntax (#66022)` | 增加 CREATE/DROP/ALTER MV/MLog 语法、AST、Restore、关键字和 parser 测试，同时引入早期 `REFRESH MATERIALIZED VIEW` 语法。 | PR2a；REFRESH 部分属于 PR5 | **部分覆盖**。master #70744 已覆盖 CREATE/DROP/ALTER MV/MLog 的 grammar、AST、Restore、visitor、关键字和 parser 测试；源 commit 中的 `REFRESH MATERIALIZED VIEW` AST/grammar（以及其 planner 入口）在当前 `origin/master` 不存在，仍属于后续 PR5。源后续 #79 删除的 `NEVER REFRESH` 分支不计为待 port 内容。 |
| 4 | `d56cc857183c` | 2026-02-07 | `executor: support CREATE MATERIALIZED VIEW LOG (#66080)` | 实现 CREATE MLog 的 DDL dispatcher、job args、schema tracker、内部表构造和测试。 | PR2b-create | **重构后覆盖（100%）**。master #70789 的 `CreateMaterializedViewLog` executor/worker、专用 `ActionCreateMaterializedViewLog`、job args、schema tracker、infoschema placement 更新和 DDL 测试完整覆盖源 commit；action 数值因 master 中已有 action 扩展而重新编号，但 action 语义和持久化路径一致。 |
| 5 | `6fb36360b1cd` | 2026-02-09 | `*: Cherry-pick pr-66089 into materialized_view branch (#66163)` | 混合 branch 同步内容，包含 `_tidb_commit_ts` extra column、planner/sample/point-get 适配、notifier 行为调整以及大量通用测试结果变化。 | 混合 commit；commit-ts prerequisite；非 MV 测试 drift | **混合 commit，未完全覆盖**。源 commit 的 `_tidb_commit_ts` 元数据、planner/schema/sample/row-size 适配在 master #65620（`0dd5a8be21`）及后续 rowcodec 实现中已有等价代码，notifier hunk 也已被 master 的 `EnableInternalCheck` 逻辑重构；但源 `pkg/executor/point_get.go` 对旧 row format 缺少列信息时调用 `NewExtraCommitTSColInfo` 的 hunk 在当前 master 中找不到。因此不能把整个 commit 标为已覆盖；其余大规模 planner/testdata 和通用同步内容也不应整体 port。 |
| 6 | `1587a903f980` | 2026-02-12 | `table,executor: sync base-table DML to materialized-view log tables (#66204)` | base-table INSERT/UPDATE/DELETE/REPLACE/LOAD DATA 等 DML 生成并写入 MLog，新增 MLog table 实现和 writetest/integration test。 | PR3 | **已覆盖（100%）**。master #70941 保留 `WrapTableWithMaterializedViewLog`、各 DML builder 的包装、tracked-column 判断、I/U/D 与 OLD/NEW 行写入，以及 `mview_log_dml` integration 和 writetest 覆盖；实现已增加校验和资源处理，但源 commit 的 DML 语义完整闭环。 |
| 7 | `b476f8f72604` | 2026-02-13 | `table, sessionctx: fix bug where mlog consumes reserved row IDs from base table (#66246)` | 修正 MLog 写入对 base table 保留 row ID/handle 的影响。 | PR3 | **已覆盖（100%）**。master `pkg/table/tables/mview_log.go` 在写 MLog 前暂时清空 `ReservedRowIDAlloc`、写入后恢复 base-table allocation；`TestReservedRowIDAlloc` 和 `mview_log_dml.test` 的 issue-66245 场景均存在，回归语义可直接对应源 commit。 |
| 8 | `581718f52f96` | 2026-02-14 | `planner, executor: support nulleq for IndexJoin and IndexHashJoin (#66017) (#66199)` | 为通用 IndexJoin/IndexHashJoin 增加 NULL-safe equality 支持。 | 通用 prerequisite | **master 已有等价 prerequisite（100%）**。master 已包含原始 #66017 的 `c6d1fffb98` 实现及 index-join NULL-EQ 测试；`HashIsNullEQ`、物理计划 `IsNullEQ` 传播和运行时 NULL lookup 逻辑均在当前 master，MV 不需要再 port 该源分支同步版本。 |
| 9 | `6ac0a2fb86f5` | 2026-02-14 | `*: add aggregate function sum_int (#66085)` | 增加 `SUM_INT` 的 parser、expression、executor、tipb pushdown 和测试。 | 通用 prerequisite；PR5/快速刷新只适配 | **master 已有等价 prerequisite（100%）**。master #69457（`b212b93152`）及其后续 parallel-distinct/agg-elimination 修订包含 `SUM_INT` 的 parser、expression、executor、spill、tipb/checker 和测试闭环；源 commit 的旧实现和依赖变更不应重复移植。 |
| 10 | `f7f7e7f85cda` | 2026-02-15 | `pkg/ddl: include base table id for create mlog MDL table ids (#66266)` | CREATE MLog 的 metadata lock involving tables 加入 base table，保证相关 DDL 串行化。 | PR2b-create | **已覆盖（100%）**。master `pkg/ddl/jobsubmit/submit.go` 的 `job2TableIDs` 对 `ActionCreateMaterializedViewLog` 将 job table ID 与 `MaterializedViewLog.BaseTableID` 一并写入 `table_ids`，`TestCreateMaterializedViewLogJobTableIDs` 验证两者；同时 CREATE MLog job 的 involving schema 也包含 base table。 |
| 11 | `a76fec7f4d03` | 2026-02-15 | `executor: support CREATE/DROP MATERIALIZED VIEW (#66083)` | 初始 CREATE/DROP MV/MLog DDL worker、依赖元数据更新、清理和测试。 | 拆分到 PR2b-create、PR2b-drop | **代码重构后覆盖 100%；测试主体覆盖但非逐项 100%**。原 commit 已被拆成 master #70789、#70874；executor 测试已迁移到 basic/create/drop/alter 文件，但 `TestCreateMaterializedViewBuildSessionSQLMode` 和 `TestCheckHistoryJobStmtType` 未保留。 |
| 12 | `800fad39c987` | 2026-02-19 | `*: sync AGENTS/.gitignore and Bazel helper updates from master (#66308)` | 同步 AGENTS、`.gitignore`、Makefile/build helper 和开发流程内容。 | 非 MV | **非 MV，不纳入**。这些是 branch drift，不属于 MV 功能。 |
| 13 | `76ef4adbc110` | 2026-02-21 | `ddl, executor: fix CREATE MATERIALIZED VIEW validation (#66316)` | 修正 CREATE MV 的对象类型、查询和依赖 validation。 | PR2b-create | **代码覆盖 100%；测试部分覆盖**。master #70789 的 validation 已覆盖该语义，build-read-TS 类型测试已迁移，但 source 的 COUNT(column) 和 aggregate 大小写回归场景未逐项保留。 |
| 14 | `4bd68061a54b` | 2026-02-22 | `*: backport _tidb_commit_ts support for unistore cop request (#66325)` | 为 unistore/cop/rowcodec 等路径传递 commit timestamp，并加入独立测试。 | commit-ts prerequisite；PR4/PR5 使用时适配 | **代码覆盖 100%；测试部分覆盖**。master 已有等价底层传递和 decoder unit test，但 source 的 active-active `commit_ts.test` integration case 未找到。 |
| 15 | `0f22124d57d0` | 2026-02-24 | `ddl: allow SET TIFLASH REPLICA on MV-dependent base tables (#66328)` | 放宽 MV 相关表的 TiFlash replica placement DDL，同时保留其他 base-table DDL 约束。 | PR2b-create/drop/alter | **代码覆盖 100%；MV 专项测试未明确逐项覆盖**。master 的 MV-related DDL constraint 已实现该特例，但未找到明确对应的 MV-dependent `SET TIFLASH REPLICA` 回归测试。 |
| 16 | `7fe4ee1528fa` | 2026-02-24 | `ddl: prewrite mv refresh info before init build (#66330)` | 在 MV initial build 前预写 refresh-info 行，避免 build 阶段缺少系统表状态。 | PR2b-create；部分涉及 PR5 initial build | **代码和测试语义均覆盖 100%（重构后）**。CREATE MV 的 prewrite/warmup/upsert 流程和 running/success、commit visibility 测试均已在 master；完整 refresh executor 仍属于 PR5，但不影响本 commit 的 prewrite 语义。 |
| 17 | `a2036935163f` | 2026-02-25 | `pkg/session, pkg/ddl, pkg/executor: update mview/mlog system table schemas (#66341)` | 更新 MV/MLog 系统表字段、类型及 DDL/executor 引用。 | PR1；PR2b 对最终 schema 的引用 | **代码和测试语义均覆盖 100%（重构后）**。最终 schema、bootstrap/upgrade 注册及字段断言已迁移到 master；后续新增的 alert 表和字段属于 schema 演进，不构成 source 内容缺失。 |
| 18 | `049564311187` | 2026-02-25 | `expression/aggregation: support sum_int in NewDistAggFunc (#66360)` | 为 `SUM_INT` 增加分布式聚合构造。 | 通用 prerequisite；PR5 适配 | **代码覆盖 100%；直接构造测试未逐项保留**。master 已包含 `ExprType_SumInt` 的 dist-agg 分支及完整 SUM_INT 语义测试，但未找到 source 同名 `TestNewDistAggFuncSumInt`。 |
| 19 | `d5ca6127eea7` | 2026-02-25 | `session, executor: update mview system table schema (#66382)` | 继续调整 MV 系统表字段和初始化逻辑。 | PR1；PR2b/后续 runtime 引用 | **代码和测试语义均覆盖 100%（重构后）**。master bootstrap 已包含 `LAST_PURGED_TSO` 和 `datetime(6)` 的最终 schema 及对应断言；refresh/service runtime 使用属于后续 PR5/PR7，不是该 schema commit 的测试缺口。 |
| 20 | `78096e182edc` | 2026-02-25 | `*: add aggregate function min_count/max_count (#66250)` | 增加 `MIN_COUNT`/`MAX_COUNT` 及 expression、executor、tipb 和测试。 | 通用 prerequisite；PR5 快速刷新适配 | **代码和测试覆盖 100%，且 master 有额外测试**。master #69642 等价实现已覆盖 source 的 parser、expression、executor、spill、planner、tipb/checker、SQL 和窗口测试，并增加 row-based 限制及 pushdown 测试。 |
| 21 | `e8829b004dcc` | 2026-02-26 | `pkg/ddl, pkg/executor: validate MV START WITH/NEXT expr type and remove defaults (#66383)` | 校验 schedule expression 类型，移除不明确的默认行为。 | PR2b-create/alter；PR5/PR7 schedule runtime | **代码和测试语义均覆盖 100%（重构后）**。master 保留 schedule 类型校验和无默认 schedule 的最终行为，三项 source 类型校验测试均同名迁移，并补充 ALTER 场景；运行时 service 调度虽仍待后续 slice，但不属于本 commit 的 validation 语义。 |
| 22 | `a36d83807816` | 2026-02-27 | `*: refine AGENTS.md style and wording guidance (#66556) (#66558)` | 调整 agent 文档风格和措辞。 | 非 MV | **非 MV，不纳入**。 |
| 23 | `b06308c62738` | 2026-02-27 | `ddl, executor: derive MV NEXT_TIME in create flow with safe eval session (#66555)` | CREATE MV 时使用安全 eval session 求值 `NEXT_TIME`/schedule。 | PR2b-create；PR5/PR7 运行时 schedule | **代码和测试语义均覆盖 100%（经后续时区/Unix-seconds 重构）**。master 的 create schedule helper、`NEXT_REFRESH_UNIX_SECONDS` 和 saved timezone 是 source 最终形态；三个 source 最终测试均已迁移。 |
| 24 | `58bba0d7a9cd` | 2026-02-27 | `pkg/ddl, pkg/parser, pkg/executor: support pre-split options for create materialized view (#66286)` | CREATE MV 支持 pre-split/scatter 等建表选项，并传入 physical table build。 | PR2b-create | **代码覆盖 100%；测试部分覆盖**。master 已透传 MV/MLog options，保留 parser 和 MLog pre-split E2E 测试；source 的 MV physical pre-split/region 测试仍未找到等价 master coverage。 |
| 25 | `c0cdf05369c5` | 2026-02-28 | `*: basic mv refresh support (#66385)` | 新增基本 refresh executor、refresh SQL、initial build 相关逻辑和测试。 | PR5；部分 CREATE initial-build prerequisite 已随 PR2b-create | **代码和测试均部分覆盖**。CREATE initial-build prerequisite 和相关测试已进入 master；REFRESH 的 AST/parser/planner/executor/history/transaction 语义及 `TestMaterializedViewRefreshComplete*`、integration tests 均仍待 PR5。 |
| 26 | `3b8b6e4a7230` | 2026-02-28 | `planner, executor, sessionctx: reject explicit dml on mview / mlog tables (#66396)` | 拦截用户直接对 MV/MLog 的 DML，并覆盖 planner fast path 和 integration test。 | PR3 | **代码覆盖 100%；测试主体覆盖但非逐项 100%**。master #70941 的 planner/executor guard 和 integration 覆盖全部用户可见 DML；source `TestCheckMViewUpdatable` 的 direct unit coverage 未保留。 |
| 27 | `e5fdd577b2a7` | 2026-03-02 | `*: Cherry pick topsql adding network field to feature branch (#66290)` | TopSQL 增加 network in/out bytes 统计及相关字段。 | 非 MV | **非 MV，不纳入 MV port；通用代码和测试已覆盖**。master 已有 client network TopSQL pipeline，保留 `TestProcessStmtStatsData` 与 `TestNetworkBytesAccumulation`。 |
| 28 | `85f88a3ff5b3` | 2026-03-10 | `planner: block UPDATE/DELETE on mview/mlog via point-get fast path (#66805)` | 修正 point-get fast path 绕过 MV/MLog DML 禁止检查的问题。 | PR3 | **代码和测试覆盖 100%**。master point-get/batch-point-get 入口均检查 `CheckMViewUpdatable`，integration 保留 MV/MLog 的全部 8 个单点/批量 UPDATE/DELETE 拒绝场景。 |
| 29 | `7185487da7dd` | 2026-03-12 | `importer: add disk quota support for IMPORT INTO SELECT FROM (#66902)` | 通用 IMPORT INTO SELECT 的 disk quota 支持，同时被 MV initial build 的 import option 使用。 | importer prerequisite；PR2b-create 的实际依赖 | **未覆盖，需补 port**。master 的 query-import option 白名单仍拒绝 `disk_quota`，且没有 quota checker lifecycle；但 CREATE MV 已会在该变量非空时生成 `disk_quota` option，因而会失败。source 的 query-option/checker/real-TiKV 回归测试也均未迁移。 |
| 30 | `6f309157d091` | 2026-03-17 | `*: support mv log purge (#66660)` | 实现 MLog purge executor、purge schedule、purge info、DDL dispatcher 和基础测试。 | PR4；部分 DDL prerequisite 已随 PR2b-create/drop/alter | **代码和测试均部分覆盖**。purge-info system table、CREATE/DROP row lifecycle、schedule metadata 和相应测试已在 master；`PURGE MATERIALIZED VIEW LOG` runtime（AST/parser/plan/executor、batch delete、mutex/checkpoint/history）及其 purge 测试尚未进入，仍属于 PR4。 |
| 31 | `c447836a4989` | 2026-03-19 | `planner: support fast refresh mview for count/sum (#66595)` | 增加 count/sum fast refresh planner、MV merge plan 和 planner casetest。 | PR5；S9 fast refresh planner | **代码 0%，测试 0%（待后续 MV slice）**。`origin/master` 没有 `pkg/planner/mview`、`MVDeltaMerge` 或对应 fast-refresh planner；通用 COUNT/SUM 只能算 prerequisite，不能替代 MV merge 语义和 source casetest。 |
| 32 | `ce48aec35f24` | 2026-03-25 | `table, executor: avoid deep-copying tracked MLog datums (#67160)` | 优化 MLog tracked-column datum 复制，降低 base-table DML capture 的开销。 | PR3 | **代码和测试均 100% 覆盖（重构后）**。master #70941 的 `writeMLogRow` 使用独立 row slice 加浅拷贝，且 `TestMLogTrackedReferenceTypes` 已迁移；其 text/varbinary/decimal fixture 覆盖 source JSON/blob 等引用型 datum 的 INSERT/UPDATE 和后续列类型变更语义。 |
| 33 | `bb1f16b69cb5` | 2026-04-10 | `planner: add min/max full-update lookup for mv refresh (#67146)` | 为 MIN/MAX fast refresh 增加 full-update lookup 和 planner 支持。 | PR5；S9 | **代码 0%，测试 0%（待后续 MV slice）**。`origin/master` 没有 source 的 full-update lookup 字段、MIN/MAX refresh planner 或 `TestBuildRefreshMVFastPlanWithMinMaxHasFullUpdate` 等 6 个 source 测试。 |
| 34 | `2e944507d529` | 2026-04-18 | `mvservice,domain,server,session,metrics: add materialized view service (MVS) framework (#66242)` | 新增 MV service、任务分配、backpressure、metrics、domain/server/session 集成和大量 service 测试。 | PR7 | **MVS 代码 0%，MVS 测试 0%（待后续 MV slice）**。master 没有 `pkg/mvservice` 及其 59 个 service/time-proxy 测试；Grafana 中的 Materialized View 面板来自独立 upstream commit `ea0454def0`，不能视为 MVS framework 已覆盖。 |
| 35 | `17ad85353d3e` | 2026-05-21 | `executor: implement MViewDeltaMergeAgg operator (#66326)` | 新增 MViewDeltaMergeAgg executor、merge count/sum/min/max、spill 和测试。 | PR5；S10 | **代码 0%，测试 0%（待后续 MV slice）**。master 没有 `pkg/executor/mviewdeltamergeagg`、`MViewDeltaMergeAgg` 或其 33 个 merge/spill/nullability 测试；source 同时新增的 `Column.AppendCellRange`/`HasNull` 也未在 master 找到。 |
| 36 | `e3d29c0b773d` | 2026-05-26 | `executor: add mview delta merge agg builder (#68636)` | 将 delta merge aggregate 接入 executor builder、planner 和 refresh 测试。 | PR5；S10 | **代码 0%，测试 0%（待后续 MV slice）**。master 没有 delta-merge builder 接入或 `TestMaterializedViewRefreshFastMinMax`；source 对 planner/builder 的适配依赖尚未 port 的 refresh operator。 |
| 37 | `72d1670b394f` | 2026-05-27 | `planner/mview: clean up mvmerge naming (#68655)` | 将 `mvmerge` 等内部 planner/operator 名称统一为 mview 命名。 | PR5；S9/S10 命名 refine | **代码 0%，测试无独立新增语义（待后续 MV slice）**。这是对 source refresh planner/operator 及测试文件的 rename；这些目标文件整体不在 master，因此不能把 rename 当作已覆盖。 |
| 38 | `adc727df5a8b` | 2026-05-28 | `*: several ddl enhancement for mv (#68649)` | 大量 DDL worker、job args、schema tracker、multi-schema-change、依赖约束和测试增强。 | PR2b-create/drop/alter | **代码 100%（重构后），测试部分覆盖**。DDL 约束、MIN/MAX index recheck、ALTER MV/MLog refresh/purge/attributes action/job args 和 schema-tracker 语义已拆入 #70789、#70874、#70927；但 source 的 vector-index 专项测试未保留，旧的 refresh/purge metadata 测试被 Unix-seconds/权限测试重构，`TestPurgeMaterializedViewLogDisallowExplicitTransaction` 及其 purge runtime 测试仍待 PR4。 |
| 39 | `1cd7b66e493d` | 2026-05-28 | `OWNERS: Auto Sync OWNERS files from community membership (#67717) (#68706)` | 同步 OWNERS 和 OWNERS_ALIASES。 | 非 MV | **非 MV，不纳入**。 |
| 40 | `b21036c39400` | 2026-05-28 | `executor, parser, planner: support SHOW CREATE for materialized views (#68698)` | 增加 SHOW CREATE MV/MLog 的 parser、executor、planner 和测试。 | PR6 | **SHOW CREATE 代码 0%，测试 0%；混合 hunk 部分覆盖**。master 没有 `ShowCreateMaterializedView`/`ShowCreateMaterializedViewLog` AST、SHOW executor 或对应 parser/DDL 测试；该 commit 中 ALTER MLog 对 `PURGE IMMEDIATE` 的错误 hunk 已由当前 DDL 实现覆盖，但不等于 SHOW CREATE 已 port。 |
| 41 | `d8a6eac392cc` | 2026-05-28 | `executor: several refinements for MV refresh (#68704)` | refresh executor/session 使用、结果处理和测试修正。 | PR5 | **未覆盖（仅有 schema prerequisite）**。master 只有 `REFRESH_ROWS` schema 字段，source 的 slow-log、refresh timing、fast-refresh rows 和 executor runtime 仍待 PR5。 |
| 42 | `25e037cb02e4` | 2026-05-29 | `expression: keep unary minus flen for signed int/decimal (#68723)` | 修正通用 unary-minus 的 field length 推导。 | 非 MV prerequisite candidate | **未覆盖（非 MV prerequisite）**。master 后续只修正 Column/CorrelatedColumn 的 flen；source 对 signed int/decimal 常量的行为和测试仍未覆盖。 |
| 43 | `edbfd5121f7f` | 2026-05-29 | `executor, mvservice: use unsigned TSO for materialized view metadata (#68719)` | 将 refresh/maintenance metadata 中的 TSO 表示统一为 unsigned 语义。 | PR1 schema；PR5 refresh；PR7 service | **部分覆盖**。unsigned schema/planner/rowcodec 已在 master；refresh executor 和 service runtime 仍待 PR5/PR7。 |
| 44 | `74b10f298fa2` | 2026-05-29 | `executor, sessionctx/variable: add MV maintenance memory quota (#68736)` | 增加 MV maintenance memory quota 变量和 executor/service 使用。 | PR2b-create 的 build option；PR5/PR7 runtime | **部分覆盖**。变量、job 快照和通用 apply helper 已在 master；refresh/purge quota 使用及 source 行为测试仍待后续 slice。 |
| 45 | `f432536c5f34` | 2026-05-29 | `ddl: fix some bug in create mv/ create mv log (#68766)` | 修正 CREATE MV/MLog dispatcher、schema tracker 和 DDL validation。 | PR2b-create | **已覆盖（代码和测试 100%）**。master #70789 的最终实现和 basic DDL 测试覆盖 source hunk。 |
| 46 | `5f3246d2f28a` | 2026-05-30 | `ddl, mvservice: notify alter MV and MV log events (#68776)` | 增加 ALTER MV/MLog notifier event，并让 service 感知相关变化。 | PR2b-alter；PR7 | **部分覆盖**。DDL notifier、ALTER metadata 和 MLog DDL regression 已覆盖；MV service consumer/test 仍待 PR7。 |
| 47 | `ffbb0cef41b6` | 2026-05-30 | `executor: fix MV refresh internal-session usage collection (#68778)` | 修正 refresh 使用 pooled/internal session 时的资源统计。 | PR5；PR7 | **未覆盖**。refresh internal-session collector 接入和 fast-refresh stats tests 均依赖尚未进入 master 的 refresh runtime。 |
| 48 | `c0f0d1b212b4` | 2026-06-01 | `executor, parser: add show materialized views and show materialized view logs (#68808)` | 增加 SHOW MATERIALIZED VIEWS/MATERIALIZED VIEW LOGS。 | PR6 | **未覆盖**。parser、planner 和 SHOW executor 用户可见行为及测试均待 PR6。 |
| 49 | `3cfcfc49d821` | 2026-06-02 | `ddl, parser: support materialized view alert attributes (#68810)` | 支持 MV alert attributes 的 parser、DDL metadata 和 notifier/service 引用。 | PR2b-create/alter；PR7 | **部分覆盖**。attributes parser/DDL/job/notifier/statistics 已覆盖；service alert handling 和 SHOW CREATE MV 属性输出仍待 PR7/PR6。 |
| 50 | `725e4f64ad7d` | 2026-06-02 | `*: backport mv metadata, import options, and refresh support fixes (#68849)` | 混合回灌 MV metadata、IMPORT options、DDL schema/job args 和 refresh 修复。 | 混合 commit；按 hunk 拆到 PR2b-create、PR5 | **部分覆盖（混合 commit）**。多 MLog IDs、DROP DATABASE cleanup、import option/变量和 schema indexes 已覆盖；disk-quota importer prerequisite #29、其余 refresh/runtime 仍缺失。 |
| 51 | `c99ca71584bd` | 2026-06-02 | `*: add MV refresh observability and backport related fixes (#68774)` | 增加 refresh observability、结果/metrics/history、expression pushdown 和大量 refresh 测试。 | PR5；部分 PR7；通用 aggregation prerequisite | **部分覆盖**。MAX/MIN-count 的 PB/pushdown/MPP/checker、3 项同名单测和 MLog TRUNCATE guard 已在 master；refresh observability、dry-run/profile/async refresh parser-plan-executor 和其测试未进入，source 的 TiFlash unary-minus pushdown hunk 也未覆盖。 |
| 52 | `259810a1abf8` | 2026-06-02 | `executor,mvservice: unify MV maintenance session vars (#68877)` | 统一 refresh/purge/service 的 maintenance session variables 和 restore 逻辑。 | PR4/PR5/PR7；PR2b-create helper | **部分覆盖**。master 的 `MViewExecutionSessionVars` abstraction 和 CREATE DDL snapshot/apply 是 source 的最终抽象落点；refresh/purge/MVS 使用 current/global vars 的 runtime 和 6 项行为测试未覆盖。 |
| 53 | `bdb304c4d378` | 2026-06-03 | `ddl, executor, planner: backport duplicate count expr support for nullable MV aggs (#68893)` | 修正 nullable MV aggregate 的 duplicate count expression，并更新 planner/executor/DDL 测试。 | PR2b-create validation；PR5 S9/S10 | **部分覆盖**。nullable SUM 必须有 matching COUNT、重复 COUNT 可接受的 CREATE validation 已在 master；nullable MIN/MAX fallback、`ArgNotNull`、duplicate-count delta-merge planner/executor 及 5 项测试仍未进入。 |
| 54 | `f30199fba937` | 2026-06-03 | `*: support complete refresh out of place (#68894)` | 实现 out-of-place complete refresh、cutover、临时表、DDL worker、notifier 和测试。 | PR5 | **部分覆盖**。仅 `normalizeMVDefinitionHintDBNames` 及其同名单测在 master；shadow/cutover DDL action、schema diff/notifier、rollback/CAS/cleanup/统计和 23 项 out-of-place/advisory-lock 测试均未覆盖。 |
| 55 | `7a3b5d9443e1` | 2026-06-04 | `ddl, mvservice: backport materialized view maintenance fixes (#68947)` | 混合 DDL worker 和 MV service maintenance 修复。 | PR2b；PR7 | **部分覆盖**。CREATE MLog duplicate-column validation/schema tracker 和其测试已覆盖；source 的 EXCHANGE PARTITION、ALTER/REMOVE PARTITIONING MV/MLog guards 在 master 入口不存在，MVS server restart de-dup 及其测试也未覆盖。 |
| 56 | `6ba9bb6053d6` | 2026-06-05 | `parser, planner, executor, sessionctx: support full outer join (#68919)` | 通用 FULL OUTER JOIN parser/planner/executor/Mpp 支持及测试。 | 通用 prerequisite | **master 已有等价 prerequisite（代码和测试 100%）**。parser/planner/executor/MPP/cost/reorder、feature switch、14 项 unit/casetest 和 integration case 均已覆盖；MV complete-delta 后续直接复用。 |
| 57 | `3892bda264c2` | 2026-06-10 | `planner, executor, parser, expression: support MV complete delta apply (#68994)` | 实现 complete delta apply、touched rows、delta merge、planner/executor/parser 接入。 | PR5；S9/S10；FULL OUTER JOIN follow-up | **部分覆盖，MV 主体未覆盖**。FULL OUTER JOIN MPP follow-up 和两项 PB 测试已在 master；complete-delta AST/mode、touched rows、operator/builder/runtime stats、diff-source/NULL-EQ 和其余 28 项测试均仍待 PR5。 |
| 58 | `a2e127422a1d` | 2026-06-11 | `executor, parser, session, mvservice: support canceling MV jobs (#69102)` | 增加 refresh/purge cancel 语法、状态更新、controller/monitor 和 pooled session 生命周期处理。 | PR5；PR4；PR7；部分 PR2b-alter/schema | **部分覆盖**。schedule NULL/best-effort DDL 更新、两项同名 warning 测试和最终 cancel-request schema 字段已覆盖；cancel parser/executor/watcher/state、refresh/purge/MVS backoff 和其余 13 项运行时测试未覆盖。 |
| 59 | `c7b499b9bc6b` | 2026-06-12 | `executor, planner, parser, ddl: support bounded MV fast refresh (#69117)` | 增加 bounded fast refresh 语法、planner、executor、DDL metadata 和测试。 | PR5；S9/S10；PR2b shared index helper | **部分覆盖**。master 复用 source 的 `mviewutil` supporting-index helper 进行 DDL validation；FAST `AS OF TIMESTAMP`、bounded TSO window/snapshot/GC/history、provided-table partition helper 修复和全部 21 项测试均未进入。 |
| 60 | `8ecd2a11bb59` | 2026-06-12 | `pkg/ddl, pkg/executor, pkg/mvservice: fix MV privilege check and refactor execution vars (#69126)` | 混合修正 MV privilege check、DDL/executor/service execution vars 和测试。 | 独立权限闭环；PR2b-create/alter；PR4/PR5/PR7 | **部分覆盖**。master 已有 execution-vars abstraction、CREATE initial-build snapshot/apply、import SQL test，以及 ALTER-only privilege 下的 refresh/purge schedule update tests；refresh/purge/MVS fallback、out-of-place/dry-run runtime 和其余 source tests 未覆盖。 |
| 61 | `09824b200e7e` | 2026-06-13 | `ddl, executor, parser, planner: refine materialized view initialization and refresh syntax (#69159)` | 混合包含 COMPLETE 语法、schema version、initial-build state gate、admin index 限制和 refresh 初始化修复。 | PR2a；PR2b-create/alter；PR5；PR7 | **部分覆盖**。master 已覆盖 initial-build state、state reload、read gate、DDL history-job schema version 和 IMPORT child-session maintenance flag；COMPLETE subtype 的 refresh parser/runtime 以及 auto-analyze not-ready gate/retry 未覆盖。source 测试中 building-MV read gate 有等价覆盖，refresh/auto-analyze/parser cases 没有。 |
| 62 | `9a30eedc0650` | 2026-06-14 | `*: MV refresh/purge result reporting, metrics, and history enrichment (#69137)` | 增加 refresh/purge 结果、metrics、history 字段和 bootstrap 表初始化。 | PR1；PR4；PR5；PR7 | **部分覆盖**。最终 history schema 的名称、duration 和时间维度 index 语义已在 master；refresh/purge result、metrics、history write 和时区 runtime 仍缺。最终 bootstrap schema 有测试，但 source 的逐版本 upgrade 和全部 statement-result tests 未覆盖。 |
| 63 | `99948f2aac85` | 2026-06-14 | `*: cherry pick some non-mv related optimazations to mv branch (#69165)` | prepare dedup、plan cache、通用 session/expression 优化及大量测试结果同步。 | 非 MV；混合 branch drift | **混合 commit，MV 代码覆盖**。唯一 MV hunk 的 5 张 maintenance system table 已在 master BR `unRecoverableTable` 中；source `TestMVSystemTablesAreUnrecoverable` 未保留，其他代码/测试都是非 MV branch drift，不纳入 MV port。 |
| 64 | `fd53fbc6a7f2` | 2026-06-15 | `pkg/mvservice, pkg/session: add MV alert table and history cleanup (#69166)` | 增加 alert table、history cleanup、service 清理和系统表相关初始化/DDL 清理。 | PR1；PR7；部分 PR2b-drop | **部分覆盖**。alert table、最终 history heartbeat schema、DDL cleanup 和对应 DROP/ALTER failure tests 已覆盖；MVS alert reconciliation、running-history heartbeat、history/stale-alert cleanup 及其测试仍待 PR7。 |
| 65 | `37ff05b94b37` | 2026-06-15 | `mv,mvservice: make maintenance isolation read engines independent (#69172)` | 让 MV maintenance 的 isolation read engines 与普通 session 独立配置。 | PR4/PR5/PR7；PR2b-create initial build | **部分覆盖**。maintenance isolation sysvar、execution-vars abstraction 和 CREATE initial-build snapshot/apply 已重构进入 master；refresh/purge/MVS owner runtime 及 source 的 7 项行为测试仍未覆盖。 |
| 66 | `90447ef0160d` | 2026-06-15 | `executor, session: record MV refresh commit TSO (#69174)` | 记录 refresh commit TSO，用于 refresh info/history 和后续调度。 | PR5；PR1 schema | **部分覆盖**。`REFRESH_COMMIT_TSO` 和 commit-TSO index 已在最终 schema/boot tests；refresh executor 获取、写入和 out-of-place preserve 语义及其 runtime tests 未 port。 |
| 67 | `4a2bc184c38c` | 2026-06-15 | `executor: move MV log purge deletes out of pessimistic txn (#69183)` | 将 MLog purge delete 从 pessimistic transaction 中移出，调整执行和统计语义。 | PR4 | **未覆盖**。purge executor 的事务边界、partial-delete/history、manual-cancel/begin-failure 代码和 3 项回归测试均不在 master。 |
| 68 | `9cef8d892f06` | 2026-06-15 | `executor, mvservice: throttle materialized view log purge (#69192)` | 增加 MLog purge 限流及 service 调度参数。 | PR4；PR7 | **未覆盖**。adaptive throttle、cancel controller、purge/service runtime 和 7 项 fallback/batch/deadline tests 均不在 master。 |
| 69 | `fbbf288e1869` | 2026-06-16 | `ddl, executor, mvservice: impl mv attribute mview_alert_refresh_failed and enhance mysql.tidb_mview_refresh_alert (#69210)` | 增加 refresh-failed alert attribute、alert 表更新和 service 处理。 | PR2b-create/alter；PR5；PR7 | **部分覆盖**。`mview_alert_refresh_failed` 的 parser validation、metadata/job args、CREATE/ALTER 和 disable-schedule DDL lifecycle 及等价测试已在 master；refresh failure/fallback 写 alert 与 MVS service 处理及其测试仍待 PR5/PR7。 |
| 70 | `a534759aca3e` | 2026-06-16 | `executor, session: guard fast refresh against hazardous mlog purge (#69227)` | 在 fast refresh 前检测危险的 MLog purge 状态，避免增量数据已被清理。 | PR4；PR5 | **部分覆盖**。`PURGE_CUTOFF_TSO` 最终 schema 和 bootstrap assertion 已有；purge cutoff 单调 fence、fast-refresh hazardous-history guard 和 source 的两个 runtime regression/逐版本 upgrade test 均未覆盖。 |
| 71 | `78e1033f4aa7` | 2026-06-16 | `ddl, executor: reject truncating mview-related tables with mlog guards (#69218)` | 拒绝会破坏 MV/MLog 依赖的 TRUNCATE/相关 DDL，并增加 worker 侧约束。 | PR2b-drop/alter | **代码覆盖 100%，测试部分覆盖**。master 的统一 `checkTableMaterializedViewConstraints` 和 IMPORT base-table guard 已覆盖最终代码语义；source 的 `TestImportIntoRejectsMaterializedViewLogBaseTable` 没有等价 master 测试。 |
| 72 | `ee3965416a3f` | 2026-06-16 | `planner, executor, ddl, session: support MV privilege model and show status commands (#69233)` | 混合增加权限模型、SHOW STATUS/状态输出、planner/executor/DDL 检查。 | 独立权限闭环；PR6；PR7 | **部分覆盖，混合 commit**。`OPERATE VIEW` 静态权限和 CREATE/DROP MLog 权限已覆盖；SHOW remain/wait-purge、refresh/purge/cancel/COMPARE 权限运行时仍待 PR4/PR5/PR6/PR7，相关 source 测试也未完整保留。 |
| 73 | `9dfaa1bc4cd6` | 2026-06-17 | `ddl, executor: cherry-pick MV log DDL enhancements (#69247)` | MLog DDL 增强，包括 create/drop/alter 约束、job args、schema tracker 和测试。 | PR2b-create/drop/alter | **部分覆盖，重构后**。主要 MLog DDL 代码和回归测试已分散进入 #70789/#70874/#70927；`MViewShadow` 可读性和若干 `analyzeMVColumnUsage` 内部测试随未 port 的 refresh/cutover runtime 仍缺失。 |
| 74 | `ff53f54bb001` | 2026-06-17 | `mvservice: improve manual cancel backoff handling (#69265)` | 改进手工 cancel 后的 service backoff 和任务重新调度。 | PR7；PR4/PR5 cancel | **代码和测试均未覆盖**。依赖当前 master 尚不存在的 MV service/controller。 |
| 75 | `79156a515145` | 2026-06-17 | `infoschema, ddl: support materialized view metadata tables (#69271)` | 在 infoschema/DDL 中暴露 MV metadata tables，并增加读取/测试。 | PR6；部分 PR1/PR2b | **代码和测试均未覆盖**。master 的 bootstrap maintenance tables 不等于 source 的 infoschema virtual tables、reader/extractor 和 SQL integration。 |
| 76 | `6d14c9de5201` | 2026-06-17 | `ddl, parser, mvservice: support ALERT ROWS for materialized view logs (#69278)` | CREATE MLog 的 alert rows 语法和 metadata，以及 service alert 触发。 | PR2b-create；PR7 | **部分覆盖**。ALERT ROWS parser、DDL validation、metadata 持久化和基础测试已在 master；MVS owner 扫描、metrics/alert 触发及对应测试待 PR7。 |
| 77 | `5018ef08bddd` | 2026-06-18 | `executor: support compare materialized view (#69297)` | 增加 COMPARE MATERIALIZED VIEW executor、SQL 和测试。 | PR6；FULL OUTER JOIN prerequisite | **代码和测试均未覆盖**。FULL OUTER JOIN 只是通用 prerequisite，COMPARE parser/planner/executor/runtime 尚未 port。 |
| 78 | `d2dc23f5c71a` | 2026-06-18 | `parser, ddl: allow clearing materialized view log purge schedule (#69280)` | 支持清除 MLog purge schedule 的 parser 和 ALTER DDL 行为。 | PR2a；PR2b-alter；PR4 metadata/runtime | **代码和测试语义覆盖 100%**。空 PURGE 的 parser、schema metadata 清空以及最终 `NEXT_PURGE_UNIX_SECONDS = NULL` 均在 master 有等价实现和断言；purge service 对后续调度变化的处理不属于该 commit 的清空语义。 |
| 79 | `2e489b053bc6` | 2026-06-18 | `executor: use baseQuery as build side for compare materialized view (#69312)` | 调整 COMPARE MV 的 join build side，降低/修正 compare 执行开销。 | PR6 | **代码和测试均未覆盖**。这是缺失的 COMPARE executor 内部实现，master 没有对应 owner。 |
| 80 | `c4207360618b` | 2026-06-18 | `planner, executor: rename MV delta sides (#69307)` | 统一 MV delta apply 中 build/probe/old/new side 的命名。 | PR5；S9/S10 命名 refine | **代码和测试均未覆盖**。complete-delta refresh planner/executor 及 `pkg/planner/mview` 尚未进入 master，命名变更必须随 PR5 一并 port。 |
| 81 | `f145795d551e` | 2026-06-19 | `ddl: forbid the creation of mlog for unsupported column type (#68997)` | 拒绝 MLog 复制 JSON、binary BLOB 等不支持的列类型。 | PR2b-create | **代码和测试覆盖 100%（重构后）**。master 的 CREATE/ALTER/schema-tracker 校验及三组列类型、写入回归测试均已保留，详见本节 #81。 |
| 82 | `e7a30c7889ba` | 2026-06-19 | `ddl: forbid the creating of mlog on partition base table (#68903)` | 拒绝对 partitioned base table 创建 MLog，并让 CREATE MV 分区表错误正确取消。 | PR2b-create | **代码覆盖 100%，测试部分覆盖**。MLog/CREATE MV 的 executor、worker、schema tracker 检查已在 master；但缺少 source 同时覆盖 CREATE MLog 与 CREATE MV 的直接回归，详见本节 #82。 |
| 83 | `4769ef44fab9` | 2026-06-20 | `ddl: forbid unique indexes on materialized view (#68911)` | 拒绝会破坏 MV 维护语义的 unique index 操作。 | PR2b-alter | **代码覆盖 100%，测试部分覆盖**。unique/primary key 的 MV 约束已在 master，未找到 source 专项 MV unique-index 测试的等价用例，详见本节 #83。 |
| 84 | `552472a43d33` | 2026-06-22 | `ddl: throw error when setting mlog and mv as target to create materialized view log (#68948)` | 拒绝将 MV/MLog/shadow table 作为 CREATE MLog 的 target。 | PR2b-create | **部分覆盖**。MV/MLog target 校验及测试已在 master，但 shadow-table owner/runtime 和 failpoint 测试没有 port，详见本节 #84。 |
| 85 | `b441fe3584eb` | 2026-06-22 | `test: cover materialized view log on generated column (#68082)` | 增加 generated column 与 MLog column capture 的测试覆盖。 | PR2b-create；PR3 | **测试覆盖 100%；代码不适用**。source 的 8 个 generated-column 测试均已在 master 的 create/writetest 文件中，详见本节 #85。 |
| 86 | `a8bc88b4d857` | 2026-06-22 | `ddl: forbid to set system table as mlog target and unify the error info for unsupported object types (#68969)` | 拒绝 system table 作为 MLog target，并统一 unsupported object 错误。 | PR2b-create | **本 commit 新增的 system/non-base 代码和测试覆盖 100%**。shadow guard 属于 #84 的未覆盖运行时，不重复计入本 commit，详见本节 #86。 |
| 87 | `7c01d98e8ad6` | 2026-06-23 | `ddl, parser: support IF EXISTS for dropping materialized views (#69351)` | 增加 DROP MV/MLog 的 IF EXISTS parser 和 DDL 行为。 | PR2a；PR2b-drop | **代码和测试覆盖 100%（重构后）**。parser restore、缺失对象 note、错误对象保护和 MLog no-op 均已在 master，详见本节 #87。 |
| 88 | `d96dfd09a259` | 2026-06-23 | `ddl: avoid the upgrade of column type from TEXT to MEDIUMTEXT in MLog internal table (#69356)` | 防止 MLog 内部复制列在 DDL 过程中错误升级 TEXT 类型。 | PR2b-alter；PR3 相关 schema 同步 | **代码和测试覆盖 100%（重构后）**。CREATE/ALTER/schema tracker 共用 field-type helper，TEXT family 的 CREATE/ADD COLUMN 断言均已保留，详见本节 #88。 |
| 89 | `0fd4796db0c4` | 2026-06-23 | `ddl: allow comment-only modify on materialized view base columns (#69391)` | 允许不改变数据语义的 comment-only base column 修改，同时保持 MLog 约束。 | PR2b-alter | **代码覆盖 100%，测试未覆盖 source 的 MV 专项回归**。comment-only helper 和约束例外已在 master，但 source 的 MV 依赖 SQL/断言未保留，详见本节 #89。 |
| 90 | `597952c8226e` | 2026-06-23 | `ddl: limit COMMENT strings length when creating materialized view (#68944)` | CREATE/ALTER MV 时限制 COMMENT 长度并产生正确 warning。 | PR2b-create；PR2b-alter | **代码和测试覆盖 100%（master 扩展）**。master 同时覆盖 CREATE/ALTER 的 strict/non-strict、warning 和截断长度，详见本节 #90。 |
| 91 | `42d9a021010e` | 2026-06-25 | `*: slow query skip purge statement (#69462)` | 将 MLog purge 维护 SQL 从 slow query 统计/暴露路径中排除。 | PR4；部分 PR7 observability | **代码和测试均未覆盖**。master 没有 MLog purge slow-log 开关或对应回归；待 PR4/PR7。 |
| 92 | `2ad2da61a2ff` | 2026-06-28 | `mvservice: force status indexes for history cleanup (#69496)` | history cleanup 强制使用 status 相关索引，降低清理成本。 | PR7 | **代码和测试均未覆盖**。master 没有 MV service/history cleanup owner 或 status index hints。 |
| 93 | `14ec59608231` | 2026-06-29 | `planner: bind MV MIN/MAX fast refresh lookup to supporting index (#69490)` | 将 MIN/MAX fast refresh lookup 绑定到支持索引。 | PR5；S9 | **部分覆盖**。仅通用 `FindVisibleIndexesWithPrefixCoveringColumns` utility 已在 master；refresh lookup index hint 和测试待 PR5。 |
| 94 | `8aff9064f372` | 2026-06-29 | `executor: collect stats delta for mlog purge (#69510)` | MLog purge 后收集 statistics delta。 | PR4 | **代码和测试均未覆盖**。master 没有 MLog purge executor 的 stats collector 接入或回归测试。 |
| 95 | `6645850375d3` | 2026-06-29 | `statistics: skip MV log tables in auto analyze priority queue (#69519)` | 将 MLog 从 auto-analyze priority queue 中排除或改由专门调度处理。 | PR4；PR7 | **代码和测试均未覆盖**。master priority queue 仍不识别 MLog。 |
| 96 | `3c11363cc32e` | 2026-06-30 | `planner: estimate mlog commit ts filter selectivity (#69445)` | 为 MLog commit-ts filter 增加 selectivity 估算和 planner stats。 | PR4；PR5；S9 | **代码和测试均未覆盖**。MLog commit-ts refresh estimate 及 EXPLAIN 回归待 PR4/PR5。 |
| 97 | `46f2873a3cc7` | 2026-07-01 | `mvservice, variable: schedule mlog analyze in MV service (#69516)` | 由 MV service 调度 MLog analyze，并增加相关变量和 statistics helper。 | PR4；PR7 | **代码和测试均未覆盖**。master 没有 MLog analyze service、sysvar 或测试。 |
| 98 | `190c9d508e3` | 2026-07-01 | `executor: avoid exposing materialized view cancel job state (#69555)` | 隐藏内部 cancel job 状态，调整用户可见结果和测试。 | PR4/PR5；PR7 | **代码和测试均未覆盖**。master 没有 MV refresh/purge cancel runtime。 |
| 99 | `c9fd6ddba9ea` | 2026-07-02 | `executor: update error messages for materialized view log creation privileges (#69578)` | 修正 CREATE MLog 权限错误信息和对应测试。 | 独立权限闭环；PR2b-create | **代码覆盖 100%；unit test 覆盖 100%，integration test 未覆盖**。master 已保留最终错误路径和 unit assertions，但没有 source 的 `mview_privilege` integration file。 |
| 100 | `19544ab81d34` | 2026-07-03 | `planner: collect predicate columns for MV maintenance SQL (#69596)` | 收集 MV maintenance SQL 的 predicate columns，改善 stats/plan 选择。 | PR5；S9 | **部分覆盖**。restricted internal SQL 的通用 prerequisite 已在 master；MV purge/refresh/dry-run 接入和专门测试待 PR5。 |
| 101 | `b1568c2e4a41` | 2026-07-09 | `metrics, mvservice: refine MV service metrics by component (#69737)` | 按组件细化 MV service metrics、reporter 和 Grafana。 | PR7 | **代码未覆盖；无新增测试**。master 没有 MV service/metrics 实现，Grafana 仍是旧 label 聚合。 |
| 102 | `17575377d73f` | 2026-07-12 | `mview: improve refresh observability and mlog purge delete (#69758)` | 混合修正 refresh observability、MLog purge delete、metrics、bootstrap 字段和测试。 | PR4；PR5；PR7；部分 PR1 | **部分覆盖，混合 commit**。最终 schema/CREATE 初始化字段已覆盖；purge row-ID delete、TiFlash thread、refresh/purge runtime observability 和 service metrics 未覆盖。 |
| 103 | `06030b2809c0` | 2026-07-13 | `mvservice: refine MV refresh alert checking (#69808)` | 改进 refresh alert checker、过期/失败判断和 service 测试。 | PR7；部分 PR5 observability | **代码和测试均未覆盖**。master 没有 refresh-alert checker owner。 |
| 104 | `7d7b687384f8` | 2026-07-13 | `executor,mvservice: avoid memory retention in MV maintenance SQL (#69778)` | 混合修正 MV maintenance SQL memory retention，同时回灌 DEPS、Makefile、cmd/mirror 等 branch drift。 | PR4/PR5/PR7 的 MV hunk；非 MV drift 不纳入 | **混合 commit；MV 部分未覆盖**。local MPP chunk-encoding 接入和 MV purge service 缺失；依赖/Makefile/mirror 等通用 drift 不纳入 MV port。 |
| 105 | `6910cef84061` | 2026-07-17 | `mvservice: fix refresh alert checker ownership (#69849)` | 修正 refresh alert checker 的 owner/server maintainer 生命周期。 | PR7 | **代码和测试均未覆盖**。依赖完整 MV service。 |
| 106 | `f5dfdf58b9c9` | 2026-08-21 | `refine bootstrap (#73)` | 合并/简化 bootstrap version，删除旧 upgrade 路径中的重复 MV system-table registry 和大量历史测试。 | PR1 | **代码和测试覆盖 100%（重构后）**。master NextGen registry、最终 schema 和 version-285 upgrade 测试已替代旧 version-223–229 路径。 |
| 107 | `43c6999be1c9` | 2026-08-22 | `mv: use saved schedule timezone for unix seconds (#74)` | 保存 schedule timezone，按定义/session timezone 求值，再将 schedule 存为与 timezone 无关的 Unix seconds。 | PR1 schema；PR2b-create/alter；PR4/PR5/PR7 runtime | **部分覆盖**。CREATE/ALTER metadata、timezone 和 Unix-seconds 初始化及测试已覆盖；refresh/purge/service runtime 及 internal-SQL schedule 测试未覆盖。 |
| 108 | `c226d733626f` | 2026-08-22 | `executor, ddl, session: store MV refresh end time as Unix seconds (#75)` | 将 refresh end time 从 datetime 改为 Unix seconds，并调整 schema、DDL、refresh 写入和测试。 | PR1；PR2b-create/alter；PR5 | **部分覆盖**。最终 schema/CREATE SQL builder 已覆盖；refresh executor 读写、snapshot 和 cutover runtime 未覆盖。 |
| 109 | `24eaea3deee7` | 2026-08-23 | `mview: unify maintenance timestamp names (#76)` | 统一 refresh/purge info/history 的时间字段命名，例如 snapshot/start/end/heartbeat/update。 | PR1；PR4/PR5/PR7 | **部分覆盖，重构后**。bootstrap/DDL 使用最终命名；purge/refresh/history/service runtime 及测试未覆盖。 |
| 110 | `bf681b1b662a` | 2026-08-25 | `mview: unify materialized view naming (#77)` | 统一 `MV`/`MView`/`MLog` 在系统表字段、Go 类型、task/service 和 planner 中的命名。 | PR1；PR2b；PR4/PR5/PR6/PR7 | **部分覆盖，重构后**。master 的 bootstrap/DDL/DML 已采用最终命名；refresh/purge/service/show/COMPARE/complete-delta 相关代码和测试未覆盖。 |
| 111 | `d05b5da91b50` | 2026-08-26 | `add mv system table rebuild sql` | 提供旧 MV 系统表清理并按最终 schema 重建的 SQL 运维脚本。 | PR1 文档/运维辅助 | **尚未 port 为 master 功能**。它是 system-table maintenance artifact，不是运行时 DDL；是否随 PR1 文档/发布材料提供，应单独决定。 |
| 112 | `9439fdfa65e0` | 2026-08-29 | `parser: align materialized view options with spec (#78)` | 按 spec 重排 CREATE MV options、REFRESH、ATTRIBUTES 以及 AST Restore/parser 测试。 | PR2a；PR2b-create/alter 对 metadata 的适配 | **重构后覆盖**。master #70744 的最终 parser 实现应作为 source of truth；后续 DDL 只适配最终 AST，不直接搬 generated parser diff。 |
| 113 | `8d2633e8e55a` | 2026-08-29 | `mview: remove unused never refresh method (#79)` | 删除未使用的 `NEVER REFRESH` refresh method、AST 分支和 DDL metadata builder 分支。 | PR2a；PR2b-create/alter | **已覆盖**。master parser/DDL 的最终状态已经不保留该未使用 method。 |

## 汇总结论

### 已经进入 master 的部分

以下 source 语义已经在 master 中形成可工作的闭环：

- MV/MLog bootstrap 和最终系统表 schema；
- MV/MLog parser、AST、Restore 和基本 DDL 语法；
- CREATE MV/MLog；
- DROP MV/MLog；
- ALTER MV/MLog；
- CREATE MLog 的对象、列类型、partition、target 和 metadata validation；
- MV/MLog 相关的普通 DROP/TRUNCATE/ALTER DDL 约束；
- base-table DML 写入 MLog；
- 对用户直接写 MV/MLog 的拦截，包括 planner fast path；
- OPERATE VIEW 权限闭环。

这些能力在 master 中是经过 PR 拆分、review 修复和命名/schema refine 后的最终实现，
不应再按 source 的早期 commit 逐个 cherry-pick。

### 仍然属于后续 port 的部分

以下 source 语义在本次审计边界内仍不能标记为 master 已覆盖：

- MLog purge executor、purge schedule runtime、purge history 和 purge cancel；
- MV refresh executor；
- complete/fast/bounded refresh；
- complete delta apply 和 MViewDeltaMergeAgg；
- out-of-place refresh/cutover；
- refresh/purge cancel controller、monitor 和 backoff；
- refresh/purge history、alert checker、metrics 和完整 observability；
- MV service、scheduler、maintenance session vars 和 MLog analyze；
- SHOW MV、SHOW MLog、SHOW CREATE、COMPARE MV；
- infoschema MV metadata 展示；
- fast refresh planner、MIN/MAX lookup、commit-ts filter selectivity；
- refresh/purge 的 Unix-seconds runtime 读写和 schedule timezone loading。

### 仅作为 prerequisite 或 branch drift 的部分

- FULL OUTER JOIN：master 已有 parser/planner/executor/TiFlash MPP 实现，MV 后续直接复用。
- `SUM_INT`、`MIN_COUNT`、`MAX_COUNT`：master 已有通用 aggregate 实现，MV 后续只适配。
- `_tidb_commit_ts`：master 已有底层 commit-ts metadata/rowcodec/cop 传递能力；普通 SQL
  直接引用和 MV 内部维护 SQL 的可引用策略必须在对应 refresh/purge slice 单独确认。
- TopSQL network bytes、prepare dedup、plan cache、unary-minus 等通用改动不纳入 MV 主线。
- AGENTS、`.gitignore`、OWNERS、DEPS、Makefile、cmd/mirror 和大范围 integration result
  churn 不应整体从 source branch 带到 master。

## 使用本审计的注意事项

1. source commit 的状态是“最终语义状态”，不是 patch 是否可以直接 cherry-pick。
2. 混合 commit 必须按 hunk 拆分；同一个 commit 可以同时出现在多个后续 PR slice。
3. 生成的 parser、Bazel metadata、integration result 应在 master 上重新生成或录制，
   不能仅因为 source diff 有对应文件就认为功能已经 port。
4. 系统表字段的最终命名、类型和 Unix-seconds 语义以 master 的 PR1 和已合入 DDL
   实现为准；后续 refresh/purge/service PR 不应重新引入旧字段名。
5. `mv_system_tables_rebuild.sql` 是运维辅助脚本，和 bootstrap runtime/version 逻辑
   分开审计。

## 审计命令

检查 source commit 数量：

```bash
git rev-list --count \
  xufei/cp_mv_for_master_base..xufei/cp_mv_for_master
```

查看单个 source commit：

```bash
git show --stat --summary <source_commit>
git show <source_commit> -- <path>
```

查看最终 source diff：

```bash
git diff --stat \
  xufei/cp_mv_for_master_base...xufei/cp_mv_for_master
```

检查 master 是否包含某个已确认的 port commit：

```bash
git merge-base --is-ancestor <master_commit> origin/master
```
