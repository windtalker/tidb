# MV Master Port Commit Audit

本文档逐 commit 审计 `cp_mv_for_master` 最新代码中最终保留的逻辑和测试，在最新
`origin/master` 中是否有相应的实现和场景断言。早期 commit 已被后续删除、替换或合并的
内容按最终语义判断；不以 patch-id、文件路径、函数名称或代码字面一致作为覆盖标准。

## 审计范围

本轮审计日期：2026-10-17。固定边界如下，后续分支移动不改变本轮结论：

```text
source base (merge-base): f08c648a20380ea723449c6c3eb5b171d96fd567
source head:             fd4636bb442de52a1a5c6321f3f29092472aa6f4
                         xufei/cp_mv_for_master
master:                  f40ad1d845cbb1bce94253b4d8965a7389177dd7
                         origin/master
```

source 范围共 **115 个 commit**，最终 diff 为：

```text
509 files changed, 99759 insertions(+), 25157 deletions(-)
```

本轮只以该 master 快照为目标，**不计入未合入的
`mv_pr5e_refresh_runtime_for_master` 或其他工作分支**。旧审计基准
`c5c97d97a64597686d602d9742910dd0a6009902` 的结论已在下文逐条更新。

本轮是静态逻辑和测试断言审计，没有执行 Go/package/SQL/RealTiKV 测试。
“测试覆盖”表示目标树存在对应场景及断言，不表示本轮已经运行通过。
同名测试只用来定位：fixture、分支和断言仍须核对；不同名但断言等价的测试可以计为覆盖。

## Master 对应实现

下列已合入 commit 用于定位最终实现，source commit 与 port commit 不要求一一对应：

| Master commit | 内容 | Port 归属 |
| --- | --- | --- |
| `d6afc7d9912` | MV system-table bootstrap（#70599） | PR1 |
| `8cde78af3c5` | OPERATE VIEW privilege（#70694） | 权限闭环 |
| `5e8a1a229a7` | MV/MLog DDL syntax（#70744） | PR2a |
| `94a9cbedabb` | CREATE MV/MLog（#70789） | PR2b-create |
| `f3f7b3bb4dd` | DROP MV/MLog（#70874） | PR2b-drop |
| `10f06594e7e` | ALTER MV/MLog（#70927） | PR2b-alter |
| `91e2f28ac7b` | base-table DML 写 MLog（#70941） | PR3 |
| `28a3d227c64` | partition/exchange DDL guards（#71012） | PR2b |
| `bd762047caf` | 补齐并拆分 MV regression tests（#71025） | PR1–PR3 tests |
| `22958d6535` | IMPORT INTO SELECT disk quota（#71048） | importer prerequisite |
| `5dd438d8ee` | MLog purge SQL surface（#71045） | PR4 parser/variables |
| `21f9ba4ee22` | MLog purge executor/planner（#71118） | PR4 runtime |
| `fe7d7d978e` | NextGen MV bootstrap version 顺序（#71552） | PR1 |
| `4f27dad8db` | REFRESH MV syntax（#71566） | PR5 parser |
| `aabfb399a8` | schedule UTC/SQL-mode 求值（#71616） | DDL/purge prerequisite |
| `4b2221cddf` | shadow-table create/drop/cutover DDL（#71645） | PR5 DDL prerequisite |
| `1f819a0b4a6` | delta merge/apply operators（#71812） | PR5 operator |
| `d09ecd3830` | MV cleanup 事务 rollback/retry（#71816） | PR2b/DDL prerequisite |
| `34d5c2b75b4` | REFRESH planner/lookup/stats（#71672） | PR5 planner |

## PR1-PR3 测试补齐跟踪

旧文档所列 #71025 已以 `bd762047caf` 合入，不能继续标记为“PR 待合入”。
这些补齐项的实际测试已按下表及逐 commit 记录核对：

| Source commit # | 目标测试 / 场景 | 本轮结论 |
| --- | --- | --- |
| 7 | `TestReservedRowIDAlloc` 的 `Current()` base/max | 已补齐 |
| 10 | 两个 `TestMDLCreateMaterializedViewLog...` 的阻塞/提交后写日志 | 已补齐 |
| 13 | `TestCreateMaterializedViewValidationCoverage`：COUNT(column)、大小写、缺列 | 已补齐 |
| 15 | `TestMaterializedViewBaseSetTiFlashReplica` | 已补齐 |
| 24 | `TestCreateMaterializedViewLogPreSplitOptions` 中 MV/MLog metadata/regions | 已补齐 |
| 26 | `TestCheckMViewUpdatable` 的 direct/restricted/internal-error 分支 | 已补齐 |
| 38 | `TestCreateVectorIndexOnMaterializedViewLogTableRejected` | 已补齐 |
| 53 | `TestCreateMaterializedViewNullableAggregateValidation` | CREATE validation 已补齐；SQL refresh 待后续 |
| 63 | `TestMVSystemTablesAreUnrecoverable` 五张表 | 已补齐 |
| 71 | `TestImportIntoRejectsMaterializedViewLogBaseTable` | 已补齐 |
| 73 | column-usage 与 related-column field-type 四个内部回归 | 已补齐 |
| 82 | `TestCreateMaterializedViewOnPartitionTable` 的 MV/MLog 双路径 | 已补齐 |
| 83 | `TestCreateUniqueIndexOnMaterializedView` | 已补齐 |
| 89 | `TestMaterializedViewCommentOnlyBaseColumnModify` | 已补齐 |

## Port slice 定义与判定规则

| Slice | master 当前范围 | 剩余范围 |
| --- | --- | --- |
| PR1/权限 | 最终系统表、NextGen bootstrap、OPERATE VIEW 已有 | 旧运维 rebuild SQL artifact 独立审计 |
| PR2a/PR2b | DDL parser、CREATE/DROP/ALTER、依赖保护及补齐测试已合入 | 个别直接回归场景见 T5；混合 refresh SQL 不算 DDL 已覆盖 |
| PR3 | MLog DML 与 direct-write guard 已合入 | 按最终允许类型/DDL 行为核对 |
| PR4 | purge parser/executor、cancel-purge、history/checkpoint、throttle 已合入 | service analyze/调度仍归 PR7 |
| PR5 | REFRESH parser/planner、delta operators、shadow/cutover DDL 已合入 | refresh executor/builder、history/result/observe、refresh cancel 尚未合入 |
| PR6 | 通用 prerequisite 已有 | SHOW/SHOW CREATE/COMPARE、MV infoschema reader 未合入 |
| PR7 | schema/通用 helpers 部分已有 | MV service、scheduler/backoff/alert/history cleanup/analyze 未合入 |

- `已覆盖/等价覆盖`：找到相同最终语义的实际代码或断言；不要求 source 布局不变。
- `部分覆盖`：同一 commit 的不同语义分开记录，已合入 prerequisite 不替代未合入 owner。
- `待 PR5/PR6/PR7`：有明确后续 owning slice，仍未 port；不能写为完成，也不直接当成已合入 slice 的遗漏。
- `候选缺口 Cn/Tn`：最终 source 仍保留，目标中缺少对应逻辑或直接回归证据，详见后文。
- `不纳入/不适用`：非 MV drift、无代码/测试，或明确被最终实现替代的旧过程。

目标测试文件已经拆分：DDL MV 在 `pkg/ddl/tests/materializedview/`，DDL MLog 在
`pkg/ddl/tests/materializedviewlog/`，purge SQL 在 `pkg/executor/test/materializedviewlog/`，
planner SQL 在 `pkg/planner/core/casetest/mview/`，operator 在
`pkg/executor/mviewdeltamergeagg/exec_test.go`。表中测试名应在这些最终位置及所述 helper
package 中查找，不能只搜索 source 的旧 `pkg/executor/test/ddl/` 路径。

## 前 10 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 1 | **已覆盖（重构后）**。`model.TableInfo` 的 MV/MLog/base 依赖、`Clone` 深拷贝和最终字段均在 master；按当前 metadata 布局适配。 | **无独立测试变更**。依赖维护由 CREATE/DROP/ALTER 和 `mview_worker_test.go` 检查；不以结构体字面一致为条件。 |
| 2 | **已覆盖（重构后）**。最终五张 maintenance system table 已通过 NextGen bootstrap registry 注册。 | **等价覆盖**。`TestBootstrapMaterializedViewSystemTables` 和当前 NextGen upgrade 测试检查最终列、类型和索引；旧 version-221 的逐版本实现已被替代。 |
| 3 | **已覆盖 DDL/REFRESH/PURGE 语法主体**。REFRESH AST/grammar 已随 #71566 合入，旧“无 REFRESH parser”结论失效。 | **部分覆盖**。`TestMaterializedViewDDLStatements`、`TestRefreshMaterializedViewStatements`、visitor 测试有实际断言；source 单独拒绝 CREATE 的 `REFRESH FAST START WITH ...` 无 NEXT 场景未找到直接断言，记 T5，不能仅凭改名宣称全覆盖。 |
| 4 | **已覆盖（重构后）**。CREATE MLog executor/worker、job args、tracker、placement/rollback 均已有落点。 | **已覆盖主体及旧缺项**。五个 CREATE MLog 测试已拆入 DDL MLog suites；`TestCheckHistoryJobStmtType` 现在也存在于 `pkg/ddl/ddl_test.go`，不再记为缺失。 |
| 5 | **混合，候选缺口 C2**。commit-TS 底层与通用 planner/notifier 已有等价实现；source 最终 `decodeOldRowValToChunk` 的 synthetic commit-TS column fallback 仍未在 master 找到。其他 branch-sync hunk 不纳入。 | **通用 testdata 不机械迁移**。需针对 C2 的旧 row-format/隐藏 commit-TS 列场景核实并补回归；现有 `TestDecodeWithCommitTS` 不能证明该 point-get fallback 已覆盖。 |
| 6 | **已覆盖（重构后）**。base-table INSERT/UPDATE/DELETE/REPLACE/IODKU/LOAD DATA、rollback 和 tracked-column 写入路径均已有。 | **等价覆盖**。34 个保留的 writetest 和 `mview_log_dml.test` 已在 master；source 后续移除的两个 tracked-column CurrentBehavior 场景按最终 Rejected 测试检查，不要求复活旧行为。 |
| 7 | **已覆盖**。reserved row-ID 的 `Current()` 和 MLog 独立分配语义均存在。 | **已补齐**。#71025 已合入；`TestReservedRowIDAlloc` 明确校验 `Current()` 的 base/max，MLog integration row-ID 回归也保留。 |
| 8 | **已覆盖（通用 prerequisite）**。IndexJoin/IndexHashJoin 的 NULL-safe equality 已有。 | **已覆盖**。四个 `TestIndexJoinNullEQ*` 和 benchmark 的 NULL-EQ 初始化均存在；包拆分不影响断言语义。 |
| 9 | **已覆盖（通用 prerequisite）**。SUM_INT 的 parser/expression/executor/PB/checker 使用 master 现有实现。 | **已覆盖主体**。SUM_INT 类型、溢出/滑窗、partial merge、spill、SQL 和 pushdown 断言存在；不重复 port source 的旧 aggregate 实现。 |
| 10 | **已覆盖**。CREATE MLog 的 base-table ID/job table IDs 和 MDL 关联均已实现。 | **已补齐**。`TestCreateMaterializedViewLogJobTableIDs` 及两个 `TestMDLCreateMaterializedViewLog...` 事务阻塞/新事务写日志回归均已进入 master。 |

## 第 11 到第 20 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 11 | **已覆盖（DDL 重构后）**。CREATE/DROP MV 的 job、initial build、cancel/rollback、依赖/GC 和普通表保护已有。 | **主体等价覆盖**。原单一 `TestMaterializedViewDDLBasic` 已拆成 DDL suites；import SQL 无 AS OF、普通表 TRUNCATE 和重试/取消场景有对应断言。source 最终已删除 `TestCreateMaterializedViewBuildSessionSQLMode`，不再列作待 port 测试。 |
| 12 | **不纳入 MV port**。AGENTS/ignore/Makefile/build helper 属于 branch drift。 | **不适用**。没有 MV 专属测试；仓库工具输出不作为 MV 功能覆盖指标。 |
| 13 | **已覆盖**。CREATE MV aggregate/列依赖 validation 及 read-TS 返回类型已实现。 | **已补齐**。`TestCreateMaterializedViewValidationCoverage` 检查 COUNT(column)、aggregate 大小写、缺 MLog 列；`TestCreateMaterializedViewBuildReadTSQueryTypeAlignment` 保留类型回归。 |
| 14 | **已覆盖底层 commit-TS 传递**。unistore/cop/rowcodec 的 metadata 和解码已有；SQL 引用另受 MView gate 控制。 | **部分覆盖**。rowcodec 的 commit-TS unit test 已有，source `active_active/commit_ts.test` 不在 master；独立 active-active 测试仍属非 MV 清单，不据此推断 MV runtime 缺失。 |
| 15 | **已覆盖**。MV-dependent base table 的 TiFlash replica DDL 例外存在，其他 dependency guard 保留。 | **已补齐**。`TestMaterializedViewBaseSetTiFlashReplica` 直接创建 MV/MLog 依赖后执行 SET TIFLASH REPLICA。 |
| 16 | **已覆盖**。CREATE initial build 前 prewrite refresh-info、成功写入和事务可见性路径已有。 | **已覆盖**。`TestCreateMaterializedViewRefreshInfoRunningAndSuccess` 和 `TestCreateMaterializedViewSuccessRefreshInfoVisibilityBeforeCommit` 检查运行中/提交前后状态。 |
| 17 | **已覆盖（最终 schema）**。current/history 表按后续最终命名和 NextGen bootstrap 实现；不回灌中间 schema。 | **等价覆盖**。bootstrap/upgrade schema 和 CREATE build lifecycle 测试均已有；全局 information_schema 行数不是固定 MV 契约。 |
| 18 | **代码已覆盖**。`NewDistAggFunc` 的 `tipb.ExprType_SumInt` 分支存在。 | **直接回归缺项 T1**。source `TestNewDistAggFuncSumInt` 的 dist-agg 构造/mode 断言未找到等价直接测试；其他 SUM_INT SQL/pushdown 测试不等同于该分支测试。 |
| 19 | **已覆盖（最终 schema）**。LAST_PURGED_TSO 和最终 history 微秒时间类型均存在。 | **等价覆盖**。最终 bootstrap/upgrade tests 校验相关列和精度；被后续替换的历史 version-221 断言不机械迁移。 |
| 20 | **已覆盖（通用 prerequisite）**。MIN_COUNT/MAX_COUNT 的类型、partial/final、spill、SQL/window/PB 支持已有。 | **已覆盖主体**。`TestMaxMinCount*`、partial merge、内存、滑窗和 final schema 等 source 场景有对应断言，并有 master 扩展。 |

## 第 21 到第 30 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 21 | **已覆盖，按最终 UTC/SQL-mode 语义**。schedule 表达式类型校验、无默认 schedule、DDL/tracker 均有；旧 timezone 行为已被 #114 替换。 | **已覆盖最终场景**。三个 CREATE schedule validation 测试和 ALTER counterparts 存在，UTC/SQL-mode 另见 #107/#114；不沿用已被删除的 timezone 断言。 |
| 22 | **不纳入 MV port**。仅 AGENTS 文档改动。 | **不适用**。无代码或 MV 测试。 |
| 23 | **已覆盖（演进后）**。CREATE refresh-info 的 next-time derivation、START/NEXT 优先级和 upsert 已适配 Unix seconds/UTC。 | **等价覆盖**。`TestBuildCreateMaterializedViewRefreshInfoUpsertSQL`、`TestCreateMaterializedViewRefreshInfoNextUnixSecondsDerivation`、`...UsesUTC` 覆盖最终语义；旧 NextTime/保存 timezone 测试已被替换。 |
| 24 | **已覆盖**。MV/MLog pre-split/shard options 从 AST 到 physical metadata/regions 均有。 | **已补齐**。`TestCreateMaterializedViewLogPreSplitOptions` 同时校验 MV 和 MLog 的 physical metadata/regions；parser restore/duplicate options 也有断言。 |
| 25 | **部分覆盖，待 PR5 runtime**。REFRESH parser/AST 和 plan 已合入；CREATE initial-build prerequisite 也有。`RefreshMaterializedViewExec`、事务/history/权限执行尚未进入 master。 | **部分覆盖**。REFRESH parser/visitor/mode 和 planner 测试已有；`TestMaterializedViewRefreshComplete*`、refresh session/DDL 标志及 `mview_refresh.test` 的执行场景仍待 runtime，不能算已 port。 |
| 26 | **已覆盖**。用户 MV/MLog DML 拦截和 restricted maintenance bypass 已接入 planner/executor。 | **已补齐**。`TestCheckMViewUpdatable` 的 direct/restricted 分支和 MLog integration 的 DML 拒绝断言都存在。 |
| 27 | **非 MV，master 已有等价 pipeline**。TopSQL network bytes 不随 MV 重复 port。 | **MV 审计不纳入**。network-byte/stmtstats 测试独立核对；不能把通用测试名变化算 MV 漏项。 |
| 28 | **已覆盖**。point-get/batch-point-get UPDATE/DELETE 在 fast path 前检查 MV/MLog 可写性。 | **已覆盖**。`mview_log_dml.test` 包含 MV/MLog 的单点和 IN-list UPDATE/DELETE 拒绝场景。 |
| 29 | **已覆盖，旧 prerequisite 缺口已关闭**。#71048 已合入；query-import 接受 disk_quota，并在生命周期启动/停止 quota checker。 | **已覆盖**。positive/negative query options、`TestStartDiskQuotaCheck` 和 real-TiKV `TestDiskQuotaFromSelect` 均已有；本次只检查代码/断言，未启动 RealTiKV 执行。 |
| 30 | **purge 主体已覆盖，refresh hunk 待 PR5**。#71118 已提供 purge parser/plan/executor、lock/checkpoint/history、batch delete 和 schedule 更新；共享 CREATE/DROP prerequisite 已有。 | **purge 场景已覆盖**。专用 MLog purge suite 覆盖锁、分批删除、失败/finalize、状态等；旧 NextTime 测试按 Unix-seconds/UTC 最终场景核对。混在该 commit 的 refresh SQL 测试仍待 PR5。 |

## 第 31 到第 40 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 31 | **已覆盖 planner，执行接入待 PR5 runtime**。`pkg/planner/mview` COUNT/SUM derivation、MViewDeltaMerge plan 和 plan codec 已合入 #71672。 | **planner 测试已覆盖**。`TestBuildRefreshMVFastPlan`、`TestExplainRefreshMVFastPlanTree`、COUNT/SUM/dependency 校验在 master；旧 MVMerge handle 测试按最终 `TestMViewBuildResultHandleCols` 核对。 |
| 32 | **已覆盖**。MLog write 用独立 row slice/shallow datum，并在同步消费期间保持引用有效。 | **等价覆盖**。`TestMLogTrackedReferenceTypes` 使用最终允许的 TEXT/VARBINARY/decimal 场景验证 INSERT/UPDATE/DDL 后读取；已禁止的 JSON/BLOB fixture 不复活。 |
| 33 | **已覆盖 planner，执行接入待 PR5 runtime**。MIN/MAX full-update lookup、TiFlash hint、NULL-key join 和 dependency layout 已有。 | **planner 场景已覆盖**。`TestBuildRefreshMVFastPlanWithMinMaxHasFullUpdate`、plan tree、commit-TS window、nullability join 和 nullable dependency tests 均在 master。 |
| 34 | **待 PR7**。master 没有 `pkg/mvservice` 或等价后台 scheduler/executor/backpressure owner；Grafana 面板不等于 service。 | **待 PR7**。task executor/hash ring/service helper/alert/history/lifecycle 等 source MVS 测试没有 master owner；不把框架缺失误记为已 port。 |
| 35 | **operator 主体已覆盖，存在 C1**。#71812 已合入 count/sum/min/max merge 和 writer；batch group-key encoding 尚未适配新 codec 重置输出 slice 的契约。运行时统计仍待后续 PR5。 | **部分覆盖，T2**。29 个保留 operator 测试和最终 nullable/enum-set 场景已有；chunk 的 `TestColumnAppendCellRangeFixed`、`...VarLen`、`TestHasNull` 未迁移。现有 batch tests 只有单列键，未检测 C1；runtime stats 测试随统计 owner 待 port。 |
| 36 | **部分覆盖**。planner 和 delta-merge operator 已有，但 `mview_delta_merge_agg_builder.go` 的执行器组装/重算 builder 尚未进入 master。 | **部分覆盖**。planner tree/window 测试有；`TestMaterializedViewRefreshFastMinMax` 需要缺失的 SQL refresh runtime，待后续 PR5。 |
| 37 | **已覆盖命名后的 planner/operator**。以最终 `pkg/planner/mview`、MView plan/operator 为准，不要求恢复旧 mvmerge 路径。 | **已覆盖对应 planner 测试**。`TestMViewBuildResultHandleCols` 和 COUNT/SUM/MIN/MAX derivation suites 已在 master；rename 本身无额外行为。 |
| 38 | **已覆盖 DDL 和 purge 语义**。supporting-index guard、effective table、ALTER job args/tracker、PURGE IMMEDIATE 拒绝及显式事务拦截已有。 | **已覆盖该部分测试**。supporting-index 并发约束、ALTER validation、MLog vector-index、tracked-column 和 purge explicit-transaction 测试均在 master；混合 refresh runtime tests 仍待 PR5。 |
| 39 | **不纳入 MV port**。OWNERS 同步。 | **不适用**。没有 MV 测试。 |
| 40 | **待 PR6；混合 DDL hunk 已覆盖**。SHOW CREATE MV/MLog 的 executor/格式化尚无 owner，ALTER MLog PURGE IMMEDIATE 的拒绝已实现。 | **部分覆盖**。DDL 拒绝断言已有；`TestShowCreateMaterializedView`/`...Log` 和 source SHOW restore 场景仍待 PR6。 |

## 第 41 到第 50 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 41 | **待 PR5 runtime**。refresh slow-log SQL restore、step timing 和 mlog scan-row 统计未接入；已有 purge slow-log 不能替代。 | **待 PR5 runtime**。`TestRestoreStmtTextForSlowLogWhenEmptySQL`、FAST method/manual-auto 和 COMPLETE 执行断言未在 master 找到。 |
| 42 | **master 已有等价通用实现**。signed integer/decimal unary-minus flen 语义存在，不是 MV runtime。 | **已覆盖**。`TestUnaryMinusDecimalRetTypeFlen` 和 `TestUnaryMinusIntRetTypeFlen` 已有；旧“均未覆盖”结论取消。 |
| 43 | **部分覆盖**。unsigned commit-TS、job ID 和系统表类型已有；refresh 和 MVS 的 unsigned runtime 流程仍待 PR5/PR7。 | **部分覆盖，T5**。默认值/bootstrap 类型断言已有；`TestNewExtraCommitTSSchemaColType` 对 datasource 隐藏列 unsigned 标志的直接断言未找到等价测试，refresh/MVS 场景亦未 port。 |
| 44 | **部分覆盖**。maintenance memory-quota sysvar、CREATE 与 purge 的 apply/restore 已有；refresh/MVS 应用待后续 owner。 | **部分覆盖，T5**。purge caller quota 和 apply-failure 场景已有；source session/global 默认值与新 session 继承场景未找到直接等价断言，refresh quota 测试待 PR5。 |
| 45 | **已覆盖**。CREATE MV/MLog 的 flags 清理、列校验、TS/ID 类型和 DDL 修复均可映射到当前 worker/helper。 | **等价覆盖**。`TestCreateMaterializedViewLogColumnKeyFlag`、`TestCreateMaterializedViewColumnFlags` 及拆分后的 CREATE validation 覆盖最终断言。 |
| 46 | **部分覆盖**。ALTER MV/MLog notifier constructors、事件和 DDL 发布路径已有；MV service 注册/消费仍待 PR7。 | **部分覆盖**。`TestMVAlterEventConstructors` 和 tracked-column DDL tests 已有；`TestRegisterMVServiceBootstrapAndDDLHandler` 没有 service owner。 |
| 47 | **待 PR5 runtime**。refresh session 接 stats/index-usage collector 的执行入口未合入。 | **待 PR5 runtime**。`TestMaterializedViewRefreshFastUpdatesStatsModifyCount` 和 separate-index MIN/MAX SQL 场景尚未迁移；普通 internal collector 不是等价覆盖。 |
| 48 | **待 PR6**。SHOW MV/MLog 的 AST/plan/reader/executor 尚未形成闭环。 | **待 PR6**。`TestShowMaterializedViews`、`TestShowMaterializedViewLogs` 的输出/权限断言无 master 对照；通用 `TestShow` 名称存在不算覆盖。 |
| 49 | **部分覆盖**。alert attributes 的 parser、validation、metadata/job args、CREATE/ALTER 已有；SHOW 格式化和 MVS alert 处理待 PR6/PR7。 | **部分覆盖**。CREATE/ALTER attributes、非法值、job args/notifier 场景已有；SHOW CREATE attributes 和 service handler 测试仍无对应 owner。 |
| 50 | **部分覆盖；disk-quota 缺口已关闭**。多 MLog job IDs、DROP DATABASE info 清理、CREATE import options/vars 已有；混合 refresh runtime 修复仍待 PR5。 | **部分覆盖，T5**。`TestBuildCreateMaterializedViewImportSQL`、多 MLog IDs、DROP cleanup 和 importer quota tests 已覆盖；source import-quota sysvar 的 session scope/非法值 SQL 专项断言尚未找到等价测试，refresh session 场景待 PR5。 |

## 第 51 到第 60 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 51 | **部分覆盖**。MAX_COUNT/MIN_COUNT pushdown、REFRESH DRY RUN/PROFILE parser/plan 已有；observability executor/step runtime 未合入。source unary-minus TiFlash pushdown hunk 在目标 allowlist 中仍缺，作为通用 prerequisite 差异 C4 单列，不能以 agg 已有代替。 | **部分覆盖**。agg PB/checker/MPP 与 dry-run/profile planner tests 已有；step runtime、current DB、ASYNC 执行拒绝和 SQL profile tests 待 PR5；`TestExprPushDownToFlash` 的 UnaryMinus 分支也未迁移（C4）。parser 场景按最终 Restore tests 核对。 |
| 52 | **部分覆盖**。`MViewExecutionSessionVars`、CREATE snapshot 及 purge apply/restore 已有；refresh/MVS global-session 接入待 PR5/PR7。 | **部分覆盖**。`TestPurgeMaterializedViewLogUsesCurrentSessionMVMaintainMemQuota` 有实际 caller/session 断言；manual/internal refresh TiFlash vars 和 MVS 读取 global vars 测试未 port。 |
| 53 | **部分覆盖**。nullable aggregate validation、duplicate COUNT 依赖和 MIN/MAX optional-count planner/operator 已有；SQL refresh builder/执行仍待 PR5。 | **部分覆盖**。nullable CREATE suite、`TestBuildSumNullableWithDuplicateCountExpr`、`TestBuildMinMaxNullableWithDuplicateCountExpr` 和 fallback operator tests 已有；FAST nullable aggregate SQL 结果测试待 runtime。 |
| 54 | **部分覆盖，候选缺口 C3**。shadow create/drop/cutover DDL、CAS/dependency/schema diff/notifier/GC prerequisite 已合入 #71645；完整 out-of-place refresh orchestration 未合入。无 ProcessInfo 的内部 advisory-lock owner 仍缺 C3；INSERT 冲突后的 Close/归还 pool 已有等价路径，但 SET/BEGIN 早期错误没有 source 的 Close；两种失败阶段必须分开。 | **部分等价覆盖**。direct DDL cutover 的 dependency/失败回滚、schema diff、shadow cleanup 和 stats event tests 已有；完整 refresh/cutover SQL 测试待 PR5。无 ProcessInfo/stable owner 的两个回归未找到对应测试，见 C3；source conflict-cleanup 专项测试也未保留，但 master GetLock 的 INSERT 错误会 Close/clean，不能据测试名缺失判清理逻辑漏 port。 |
| 55 | **部分覆盖**。duplicate MLog columns、partition/exchange/remove-partition dependency guards 已有；server-instance restart de-dup 尚待 PR7。 | **部分等价覆盖**。duplicate-column 与 `TestMaterializedViewPartitionDDLConstraints`/dependency/statistics event tests 已有；MVS newest-DDL-ID 去重测试未 port。 |
| 56 | **master 已有等价 prerequisite**。FULL OUTER JOIN parser、planner、hash join 和 MPP/PB 能力已有。 | **主体等价覆盖**。hash-join oracle/spill、planner/cost、feature sysvar 和 integration 场景在 master；source 单独 ConvertOuterToInner rule test 在 master 不存在，master 用 predicate-pushdown 的同四类最终 join-type 断言覆盖，不能要求移植已重构掉的规则 flag。 |
| 57 | **部分覆盖**。complete-delta parser/plan、NULL-safe diff source、touched-row helper 和 sink operator 已有；executor builder、runtime stats 与 refresh SQL 执行尚未合入。 | **部分等价覆盖**。complete-delta layout/nullable/common-handle planner、sink Open/type validation、NULL-EQ MPP 和共享 touched helper 的字符串/enum/set 断言已有；sink/runtime-stats 和 SQL refresh 测试仍待后续 PR5，不以单个同名缺失直接判逻辑遗漏。 |
| 58 | **部分覆盖**。purge cancel AST/executor/controller/watcher、cancel-request schema 和 schedule NULL 行为已有；refresh cancel 只有语法，执行分派/monitor 未合入，MVS backoff 待 PR7。 | **部分覆盖**。purge cancel request/权限/运行中 heartbeat、controller 和 DDL warning 测试已有；refresh cancel/watcher/task-finish、MVS cancel/backoff 和 refresh no-schedule SQL 场景未 port。 |
| 59 | **部分覆盖**。FAST AS OF AST/parser、bounded plan、target-snapshot 解析 prerequisite、lookup helper 已有；window/GC/history 的实际 refresh 执行未合入。provided-table partition helper 的修复已随 #71672 合入。 | **部分覆盖**。bounded parser/planner 断言和 `TestGetHashOrKeyPartitionColumnNameUsesProvidedTable` 已在 master；snapshot data/schema、GC safe point、history/finalize SQL 回归待 PR5 runtime。 |
| 60 | **部分覆盖**。CREATE initial-build/ALTER privilege、execution-vars snapshot 和 purge session fallback 已有；refresh/out-of-place/dry-run 与 MVS runtime 接入仍待后续。 | **部分等价覆盖**。import options、definition precision helper、仅 ALTER 权限更新 schedule 和 purge fallback tests 已有；refresh per-var apply-failure、out-of-place/dry-run 和 service best-effort tests 尚未迁移。 |

## 第 61 到第 70 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 61 | **部分覆盖**。Building/Ready state、读拦截、child maintenance flag、显式 COMPLETE subtype parser 已有；REFRESH ready-check 的执行入口和 auto-analyze not-ready skip/retry 待 PR5/PR7。 | **部分覆盖**。history schema version、pause/resume 读拦截、child-session、REFRESH Mode/非法 COMPLETE syntax 已有；read-and-refresh-before-ready 中 refresh 半边、auto-analyze skip/retry 测试未 port。 |
| 62 | **部分覆盖**。最终 history schema、purge result/history/affected-row 统计和时间转换已有；refresh statement result/history runtime 未合入。 | **部分覆盖，T4**。bootstrap 和 purge affected rows/early failure/history 场景已有；`TestHistTimeUsesLocation` 的 UTC/+08/微秒截断直接断言没有对应 `historyTime` 测试。六类 refresh statement-result SQL 场景仍待 PR5。 |
| 63 | **MV hunk 已覆盖，其余不纳入**。BR 将五张 MV maintenance system table 标为 unrecoverable 已有；通用缓存/表达式优化不随 MV 重复 port。 | **MV 测试已补齐**。#71025 已合入，`TestMVSystemTablesAreUnrecoverable` 枚举相同五张表；非 MV optimization tests 不计为 MV 缺口。 |
| 64 | **部分覆盖**。DDL alert cleanup 和 purge running-history heartbeat 已有；refresh heartbeat 与 MVS alert/history reconciliation/GC 仍待后续。 | **部分覆盖**。DROP/ALTER alert cleanup、failure handling、`TestPurgeMaterializedViewLogRunningHistHeartbeat` 已有；refresh heartbeat、service stale-row/owner/history GC 测试未 port。 |
| 65 | **部分覆盖**。independent isolation-read-engine sysvar、capture/apply/restore、CREATE 与 purge session 使用已有；refresh/MVS owner 接入待后续。 | **部分覆盖，T5**。purge caller/default isolation engines 和失败 fallback 已有；source maintenance sysvar 的 session/global 独立性专项断言未找到等价测试，refresh/MVS tests 未 port。 |
| 66 | **部分覆盖**。REFRESH_COMMIT_TSO 最终 schema/index 已有；取得实际 refresh 事务 commit TSO 并写 info/history 的 runtime 未合入。 | **schema 等价覆盖，runtime 未覆盖**。最终 bootstrap schema 检查已有；五个 fast/complete snapshot-match SQL 测试待 PR5。旧逐版本 upgrade 已被当前 bootstrap 流程替代。 |
| 67 | **已覆盖 purge 行为**。delete 在独立事务执行，部分成功后的 history/checkpoint 与 cancellation/failure 处理已有。 | **已覆盖**。`TestPurgeMaterializedViewLogDeleteErrorAfterPartialSuccess`、`...ManualCancelAfterPartialSuccess`、`...BeginFailure` 均在专用 purge suite，包含 checkpoint/history 断言。 |
| 68 | **purge 已覆盖，service 待 PR7**。adaptive batch/deadline/sleep fallback、取消控制与相关 sysvars 已有；后台 scheduler 的 runtime 使用仍无 owner。 | **purge 已覆盖**。三个 throttle fallback、两个 adaptive-size、controller、deadline tests 已有；MVS 配置/global vars/backoff 测试未 port。 |
| 69 | **部分覆盖**。refresh-failed attribute metadata 和 DDL alert 清理/保留已有；refresh failure/fallback 写 alert 与 MVS reconcile 未合入。 | **部分覆盖**。CREATE/ALTER/job-args/disable-schedule failed-alert 保留测试已有；refresh failure-report 顺序和 `TestBuildResolvedMVRefreshAlertSQL` 等 service 场景待后续。 |
| 70 | **部分覆盖**。purge cutoff fence 单调性已有；FAST refresh 读取危险 purge history 并拒绝执行的入口仍待 PR5。 | **部分覆盖**。`TestPurgeMaterializedViewLogSkipsWhenCutoffFenceWouldGoBackward` 和最终 schema 断言已有；`TestMaterializedViewFastRefreshRejectsHazardousPurgeHist` 尚无 runtime 对照。 |

## 第 71 到第 80 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 71 | **已覆盖**。TRUNCATE/worker dependency guard 和 IMPORT base table with MLog 拒绝已有。 | **已补齐**。`TestImportIntoRejectsMaterializedViewLogBaseTable`、TRUNCATE dependency 和 worker 重查回归已在 master，不再标 #71025 待合入。 |
| 72 | **部分覆盖**。OPERATE VIEW、CREATE/DROP MLog 与 purge/cancel-purge 权限已形成闭环；SHOW/COMPARE、refresh/cancel-refresh runtime 和 MVS 尚未合入。 | **部分覆盖**。purge 和 cancel-purge 权限/precheck 不泄漏错误场景已有；SHOW remain/wait-purge、refresh/COMPARE 权限、MV-object GRANT 组合和 `mview_privilege.test` 仍缺，需按 PR5/PR6 及 T5 继续核对。 |
| 73 | **已覆盖 DDL/DML 主体**。MV-related field type、MLog ADD COLUMN、job args/tracker 和 shadow 读保护 prerequisite 都有；混合 FAST no-op SQL 仍待 runtime。 | **内部缺项已补齐**。四个 column-usage/field-type 回归、ADD COLUMN/default/privilege/tracked writes 已有；单一 DDLBasic 的断言按拆分 suites 核对，FAST AS OF no-op SQL 测试仍待 PR5。 |
| 74 | **待 PR7**。manual cancel 后 service task-result/backoff 逻辑尚无 owner。 | **待 PR7**。`TestMVServiceTaskResult` 的手工取消/重排语义无 master 对照；purge request 成功不等于 service backoff 已覆盖。 |
| 75 | **MV metadata 展示待 PR6；DDL 名称约束已有**。TIDB_MVIEWS/TIDB_MLOGS/依赖虚表的 schema、reader/extractor 尚未合入，不能以 mysql maintenance tables 代替。 | **部分覆盖**。MLog physical-name length/truncation 的 DDL tests 已有；虚表 unit/query/integration 场景仍待 PR6。 |
| 76 | **部分覆盖**。ALERT ROWS parser、metadata、validation/DDL 已有；SHOW 输出和 service accumulation owner 扫描/metrics 尚无。 | **部分覆盖**。CREATE MLog accumulation-alert/default/invalid values 测试已有；SHOW CREATE 和 owner filtering/strict-greater-than/error-preserve/async service tests 待 PR6/PR7。 |
| 77 | **待 PR6**。COMPARE parser/planner/executor/snapshot/output writer 尚未合入；FULL OUTER JOIN prerequisite 已有。 | **待 PR6**。十个 COMPARE 权限、snapshot、output batching/failure cleanup、NULL-key、empty-input 场景无 master owner，不能以普通 join 测试代替。 |
| 78 | **已覆盖**。清除 MLog purge schedule 的 parser、metadata 和 NEXT_PURGE_UNIX_SECONDS=NULL 已实现，purge runtime 已适配最终 schema。 | **等价覆盖**。CREATE/ALTER purge clause parser 和 MLog ALTER schedule 测试检查清空；旧 UpdatesMetaAndNextTime 测试按最终 Unix-seconds/UTC assertions 核对。 |
| 79 | **待 PR6**。COMPARE baseQuery build-side 优化属于尚未合入的 COMPARE 实现。 | **无独立测试变更**。后续应通过 #77 的 COMPARE 场景核对 build-side/结果语义，普通 hash-join tests 不构成该入口覆盖。 |
| 80 | **planner/operator 命名已覆盖，执行组装待 PR5**。current/recomputed 命名和 complete-delta layout 已有；完整 executor builder 未合入。 | **已覆盖 planner/operator 部分**。complete-delta nullable/layout/handle projection 和 sink mapping/Open validation 测试均已有；SQL 执行部分仍待后续 runtime。 |

## 第 81 到第 90 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 81 | **已覆盖（重构后）**。CREATE/ALTER/tracker 统一检查 MLog supported column types；最终禁止 JSON/binary BLOB，允许 TEXT/VARBINARY 等。 | **等价覆盖**。unsupported-column、ALTER invalid columns 和 tracked reference-type tests 都已有；旧 JSON/BLOB 写入 fixture 按 source 最终限制替换，不算遗漏。 |
| 82 | **已覆盖**。CREATE MV/MLog 的 partitioned-base 拒绝、worker/tracker 及 cancel-error 分类都有；partition/exchange guard 也已有。 | **已补齐**。`TestCreateMaterializedViewOnPartitionTable` 同时验证 MV/MLog 拒绝，另有 partition dependency/worker tests；不再标 #71025 待合入。 |
| 83 | **已覆盖 DDL 语义**。MV unique/primary index 拒绝和普通 index 允许均有。 | **DDL 回归已补齐**。`TestCreateUniqueIndexOnMaterializedView` 覆盖三种拒绝及普通 index；同 commit 修改的 refresh rollback SQL 场景仍待 PR5，不能因此把整条测试记为完成。 |
| 84 | **代码已覆盖**。CREATE MLog 的 executor/worker/tracker 检查 MV/MLog/shadow target，shadow metadata 已随 #71645 合入。 | **部分覆盖**。non-base/materialized target 场景已有；source 由 REFRESH 建 shadow 后尝试 CREATE MLog 的 `TestCreateMaterializedViewLogRejectShadowTable` 未找到等价直接断言。master 其他 shadow 读写/drop 保护测试不能替代该 target 场景，记 T5。 |
| 85 | **仅测试 commit，相关 runtime 已有**。generated-column 写入和 DDL 约束取最终行为。 | **已覆盖**。八个 generated-column CREATE/INSERT/UPDATE/ALTER/rename 场景都在拆分的 DDL/writetest suites 中保留。 |
| 86 | **已覆盖**。system/memory DB、view/sequence/temporary/MV/MLog 的统一 BASE TABLE 检查和错误已有；shadow 另见 #84。 | **已覆盖本 commit 新增场景**。`TestCreateMaterializedViewLogRejectNonBaseObject` 对系统/非 base 对象和统一错误有实际断言；不把 #84 的 shadow 测试缺口重复当作本条 system-table 逻辑遗漏。 |
| 87 | **已覆盖**。DROP MV/MLog IF EXISTS 的 missing-object note、base-table/error-type 区分及底层 job 标志已有。 | **等价覆盖**。`TestDropMaterializedViewIfExists` 和 parser 的 MV DDL Restore cases 保留 note/no-op/错误对象断言。 |
| 88 | **已覆盖**。`FieldTypeForMaterializedViewLogColumn` 被 CREATE/ALTER/tracker 共用，避免 TEXT family 错误升级。 | **已覆盖**。`TestCreateMaterializedViewLogPreservesTextColumnTypes` 和 `TestAlterMaterializedViewLogAddColumnBasic` 保留 TEXT family/TypeBlob 断言。 |
| 89 | **已覆盖**。comment-only MODIFY 在 MV/MLog dependency 下放行，实际类型变化仍受约束。 | **已补齐**。`TestMaterializedViewCommentOnlyBaseColumnModify` 在真实 MV/MLog 依赖下检查 SQL 成功和 column_comment；旧单一 DDLBasic 测试名无需保留。 |
| 90 | **已覆盖，master 扩展到 ALTER**。strict/non-strict COMMENT 长度、warning 和截断语义都有。 | **已覆盖**。`TestMaterializedViewCommentLength` 对 CREATE/ALTER、最大长度、strict error、warning 和截断结果有断言。 |

## 第 91 到第 100 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 91 | **已覆盖**。`shouldSkipSlowLogForInternalMVMaintenance` 与 tidb_mlog_log_slow_purge 已接入实际 slow-log pipeline。 | **已覆盖且有扩展**。`TestSlowQueryMisc` 检查手工/restricted purge 在 OFF 时不记录、ON 时记录；不再标 purge runtime 不存在。 |
| 92 | **待 PR7**。MV refresh/purge history orphan cleanup 的 FORCE INDEX SQL 属于缺失的 service helper。 | **待 PR7**。`TestServerHelperPurgeMVHistoryBeforeTSOUsesStatusIndexHints` 无 master owner；系统表索引存在不代表 cleanup 查询已使用。 |
| 93 | **lookup planner 已覆盖，运行结果待 PR5**。`SetFullUpdateLookupIndexHint`、supporting-index 集合和 USE INDEX template 已随 #71672 合入。 | **部分覆盖**。`TestFullUpdateLookupIndexHintUsesAllSupportingIndexes` 已有；`TestMaterializedViewRefreshFastMinMaxUsesSupportingIndex` 的 SQL 执行断言仍待 runtime。 |
| 94 | **已覆盖（API 适配后）**。purge delete executor 通过 `statshandle.AttachStatsCollector` 收集 stats delta，而非复制旧 internal-session helper。 | **已覆盖**。`TestPurgeMaterializedViewLogUpdatesStatsMetaRowCount` 对 count/modify_count 实际结果有断言。 |
| 95 | **待后续 statistics/PR7**。master priority queue 仍仅按普通 view 等条件过滤，没有 source 的 MLog skip/移除已有任务逻辑。 | **未覆盖该场景**。`TestAnalysisPriorityQueueSkipsMaterializedViewLog` 不在 master，普通 queue 测试不覆盖 MLog。此项有明确后续 owner，需保留在 port 清单。 |
| 96 | **部分覆盖**。MLog retained window、inclusive/disjoint bound 归一、selectivity 和 refresh planner context 已有；dry-run executor 的 purge-integrity 获取仍待 PR5。 | **部分覆盖**。`TestExtractMLogCommitTSFilterBound`、四组 window/ForUpdateTS EXPLAIN、normalize/disjoint tests 已有；两个 dry-run hazardous-info SQL 测试未合入。 |
| 97 | **待 PR7**。MLog analyze scheduler/sysvar/task dedup、statistics session-var restore 的专门 service 接入尚无。 | **待 PR7**。`TestNeedAnalyzeMLog`、service scan/dedup/error/no-immediate-retry、global ratio 和 stats-session restore 场景未找到等价测试；通用 `TestStatsVariables` 不算这些行为已覆盖。 |
| 98 | **purge 已覆盖，refresh 待 PR5**。cancel-purge 使用统一用户错误、先做权限/precheck，避免暴露 job 状态；refresh 分派/target 尚未合入。 | **部分覆盖**。`TestCancelMaterializedViewLogPurgeJob` 和 `TestCancelMaterializedViewJobPrecheckErrorNotRewritten` 已覆盖 purge 权限/无效状态；refresh/not-running 的 SQL 测试仍待 runtime。 |
| 99 | **代码已覆盖**。CREATE MLog privilege error 使用 base-table 名和 CREATE MATERIALIZED VIEW LOG，不暴露内部 $mlog$ 名。 | **unit 覆盖，integration 未覆盖**。`TestCreateMaterializedViewLogPrivilege` 保留 command/base-name/no-$mlog$ 断言；source `mview_privilege.test/.result` 尚未 port，不能以 unit tests 代替整份 integration 覆盖。 |
| 100 | **部分覆盖**。purge count/delete session 的 InternalSQLScanUserTable 设置/恢复及 planner predicate-column 收集已有；refresh/dry-run runtime 接入待 PR5。 | **部分覆盖**。`TestInternalSQLScanUserTableCollectsPredicateColumns` 和 MLog window EXPLAIN tests 已有；CREATE maintenance session 的专项拦截/应用测试及 refresh/dry-run SQL 场景仍需核对，不能凭通用 flag 存在标整条完成。 |

## 第 101 到第 110 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 101 | **待 PR7，部分 purge metrics 已有**。purge affected-row metrics 已合入；service component/type labels、reporter 和完整 Grafana 适配尚未形成 owner。 | **无独立 Go 测试新增**。不能从 Grafana artifact 或通用 metric helper 推导 service reporter 已覆盖；待 service port 时验证 labels/queries 一致。 |
| 102 | **purge 部分已覆盖，refresh/service 待后续**。row-ID range delete、count/delete TiFlash threads、adaptive batches 和最终 duration schema/CREATE 初始化已有；refresh observability/service metrics 未合入。 | **purge 等价/更强覆盖**。row-ID range/helper tests 及 `TestPurgeMaterializedViewLogUsesConfiguredTiFlashThreadsForCountAndDelete` 覆盖 source delete-session 参数且扩展 count-session 断言；refresh schedule-duration SQL 和 service tests 仍待后续。 |
| 103 | **待 PR7**。global task snapshot、refresh-alert checker reconciliation/owner/metrics 逻辑尚无。 | **待 PR7**。按 source 最终保留的 collect/dedup/sync/stale-metadata tests 核对；早期已删除的 KeysScanGlobalTasks 名称不再作为额外缺项。 |
| 104 | **混合，MV service 部分待 PR7**。purge service 的 chunk-RPC/MPP memory-retention 接入尚无；DEPS/Makefile/mirror 等通用 drift 不整体纳入。local MPP encode-type hunk 未找到对应接入，见通用 prerequisite 差异 C5，需随 service 继续确认。 | **未覆盖 source 的两个专门场景**。`TestSetMPPEncodeTypeRespectsChunkRPCSetting` 和 purge service EnableChunkRPC 断言无对照；通用 distsql SetEncodeType 测试不能替代，非 MV build drift 不计缺口。 |
| 105 | **待 PR7**。sorted server-ID cache、双 hash-ring alert owner 和 lifecycle 均在缺失的 mvservice 内。 | **待 PR7**。owner/global-task/sorted-ring 三类测试无 master owner；不能用普通 hash-ring unit tests 宣称已覆盖。 |
| 106 | **已覆盖（bootstrap 架构替代）**。source 合并旧逐版本 MV migration，master 使用最终 NextGen registry 并在 #71552 调整 bootstrap 版本顺序。 | **等价覆盖**。最终五表 schema 与当前 NextGen upgrade tests 已有；source 仍保留的 classic version-221 fixture 应映射到当前架构，不逐字搬旧版本号。 |
| 107 | **部分覆盖，旧 timezone 语义已被 #114 取代**。source 最新 DDL/purge/runtime 统一 UTC 求值并保存 SQL mode；master DDL/purge 对应逻辑已有，refresh/service runtime 仍待后续。 | **最终 DDL/purge 场景已覆盖**。CREATE/ALTER UsesUTC、Unix-seconds derivation、purge internal-only/START-only/no-schedule tests 都有；refresh/internal/out-of-place 和 MVS schedule tests 未 port；SQL-mode runtime helper 专项断言另记 T4。 |
| 108 | **部分覆盖**。LAST_SUCCESS_REFRESH_END_UNIX_SECONDS 的 schema、CREATE 初始化和 cutover DDL 迁移已有；refresh snapshot/read/persist runtime 未合入。 | **部分等价覆盖**。bootstrap、CREATE SQL builder 和 direct cutover metadata tests 已有；previous-success-time/schedule-duration/refresh 成功读写 SQL 断言仍待 PR5。 |
| 109 | **部分覆盖**。DDL、purge history/heartbeat、cutover prerequisite 使用最终时间列/索引名；refresh/service SQL owner 尚未合入。 | **部分覆盖**。最终 bootstrap、purge history/finalize/heartbeat 和 DDL cleanup assertions 已有；refresh history/alert-service rename 对应测试待后续，不能据字段名相同判全覆盖。 |
| 110 | **部分覆盖（最终命名）**。metadata、DDL/DML、purge、refresh planner、delta operator 和 shadow DDL 均采用最终 MView/MLog 命名；refresh runtime/service/SHOW/COMPARE 未合入。 | **部分覆盖**。相应 DDL/purge/planner/operator suites 已按最终命名落地；runtime/service/SHOW 测试无对应 owner。source rename 不作为独立行为回归。 |

## 第 111 到第 120 个 Commit 的测试复核

按最新 source 最终语义分别核对代码和测试；以下结论仅对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 111 | **运维 artifact 未 port，不是 runtime 缺口**。source rebuild SQL 文件在 master 不存在；bootstrap schema 已有等价实现，但不等于该脚本已迁移。 | **不适用**。该 commit 没有测试；运行时 bootstrap tests 不替代手工运维脚本的 artifact review。 |
| 112 | **parser/DDL 已覆盖，SHOW 待 PR6**。CREATE options/REFRESH/ATTRIBUTES 的最终顺序和 AST Restore 已有；SHOW CREATE 输出格式化尚无 owner。 | **部分覆盖**。`TestMaterializedViewCreateOptionOrder`、DDL parser Restore/非法顺序断言已在 master；SHOW CREATE 的输出顺序测试未 port。 |
| 113 | **已覆盖最终状态**。source/master 均没有 NEVER REFRESH method/AST/DDL builder 分支。 | **无新增测试变更**。按最终 parser/DDL 的不含 NEVER 语法检查，不应恢复已删除分支。 |
| 114 | **部分覆盖，按 hunk 拆分**。parser cancel-type/attributes、DDL/drop/rollback、schedule SQL-mode/UTC、definition division precision、shadow cleanup/cutover dependency/atomicity、infoschema v2 和 planner bound 修复已有；refresh orchestration/observe、service 接入仍未合入。 | **多数 DDL/helper 场景已覆盖**。division precision 分别由 `TestCreateMaterializedViewAndLog` 的 metadata=9 和 `TestInitCreateMaterializedViewBuildSessionAppliesDefinitionDivPrecisionIncrement` 校验；cutover dependency/rollback/publish-event 原子性有 direct DDL 等价测试。source runtime/GC placement、schema-diff 专项与 SQL-mode runtime tests 仍需按 T4/T5 和后续 PR5 处理，不能整体标完成。 |
| 115 | **已覆盖（重构后）**。#71816 已带入 mustRollbackTxnOnError、rollback schema-version retry、重开事务 job meta recheck 和 infoschema v2 affected-table 保留。 | **核心新增回归已覆盖**。八个 CREATE/DROP/rollback/notify 并发失败回归均在 master 的 MV/MLog DDL suites；BR delete-range fixture 的 action-ID 更新按当前 model 编号适配，不能当作功能漏 port。 |
| 116–120 | 当前固定 source 范围不存在对应 commit。 | 没有可核对的 source 测试。 |

## 全部 Commit 审计

每条代码/测试的具体证据见上方相同编号；下表保留完整 source commit 清单与 port 归属。

| # | Source commit | 日期 | Subject | 主要最终语义 | Port 归属 | Master 对照和结论 |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | `f3af7d5a83fd` | 2026-02-05 | `pkg/meta: store mview/mlog dependency metadata in TableInfo (#66023)` | 在 `TableInfo` 中加入 base table、MV、MLog 之间的反向依赖元数据。 | PR2b-create/drop/alter；元数据基础 | 代码：已覆盖（重构后）；测试：无独立测试变更。具体逻辑和断言见上文 #1。 |
| 2 | `e0d58ec5ca8c` | 2026-02-06 | `pkg/session: bootstrap mview refresh/purge system tables (#66024)` | 首次 bootstrap MV refresh/purge 信息、历史和 alert 相关系统表，并补充 bootstrap 测试及 restore 行为。 | PR1 | 代码：已覆盖（重构后）；测试：等价覆盖。具体逻辑和断言见上文 #2。 |
| 3 | `96a16b284bc8` | 2026-02-06 | `*: add materialized view DDL syntax (#66022)` | 增加 CREATE/DROP/ALTER MV/MLog 语法、AST、Restore、关键字和 parser 测试，同时引入早期 `REFRESH MATERIALIZED VIEW` 语法。 | PR2a；REFRESH 部分属于 PR5 | 代码：已覆盖 DDL/REFRESH/PURGE 语法主体；测试：部分覆盖。具体逻辑和断言见上文 #3。 |
| 4 | `d56cc857183c` | 2026-02-07 | `executor: support CREATE MATERIALIZED VIEW LOG (#66080)` | 实现 CREATE MLog 的 DDL dispatcher、job args、schema tracker、内部表构造和测试。 | PR2b-create | 代码：已覆盖（重构后）；测试：已覆盖主体及旧缺项。具体逻辑和断言见上文 #4。 |
| 5 | `6fb36360b1cd` | 2026-02-09 | `*: Cherry-pick pr-66089 into materialized_view branch (#66163)` | 混合 branch 同步内容，包含 `_tidb_commit_ts` extra column、planner/sample/point-get 适配、notifier 行为调整以及大量通用测试结果变化。 | 混合 commit；commit-ts prerequisite；非 MV 测试 drift | 代码：混合，候选缺口 C2；测试：通用 testdata 不机械迁移。具体逻辑和断言见上文 #5。 |
| 6 | `1587a903f980` | 2026-02-12 | `table,executor: sync base-table DML to materialized-view log tables (#66204)` | base-table INSERT/UPDATE/DELETE/REPLACE/LOAD DATA 等 DML 生成并写入 MLog，新增 MLog table 实现和 writetest/integration test。 | PR3 | 代码：已覆盖（重构后）；测试：等价覆盖。具体逻辑和断言见上文 #6。 |
| 7 | `b476f8f72604` | 2026-02-13 | `table, sessionctx: fix bug where mlog consumes reserved row IDs from base table (#66246)` | 修正 MLog 写入对 base table 保留 row ID/handle 的影响。 | PR3 | 代码：已覆盖；测试：已补齐。具体逻辑和断言见上文 #7。 |
| 8 | `581718f52f96` | 2026-02-14 | `planner, executor: support nulleq for IndexJoin and IndexHashJoin (#66017) (#66199)` | 为通用 IndexJoin/IndexHashJoin 增加 NULL-safe equality 支持。 | 通用 prerequisite | 代码：已覆盖（通用 prerequisite）；测试：已覆盖。具体逻辑和断言见上文 #8。 |
| 9 | `6ac0a2fb86f5` | 2026-02-14 | `*: add aggregate function sum_int (#66085)` | 增加 `SUM_INT` 的 parser、expression、executor、tipb pushdown 和测试。 | 通用 prerequisite；PR5/快速刷新只适配 | 代码：已覆盖（通用 prerequisite）；测试：已覆盖主体。具体逻辑和断言见上文 #9。 |
| 10 | `f7f7e7f85cda` | 2026-02-15 | `pkg/ddl: include base table id for create mlog MDL table ids (#66266)` | CREATE MLog 的 metadata lock involving tables 加入 base table，保证相关 DDL 串行化。 | PR2b-create | 代码：已覆盖；测试：已补齐。具体逻辑和断言见上文 #10。 |
| 11 | `a76fec7f4d03` | 2026-02-15 | `executor: support CREATE/DROP MATERIALIZED VIEW (#66083)` | 初始 CREATE/DROP MV/MLog DDL worker、依赖元数据更新、清理和测试。 | 拆分到 PR2b-create、PR2b-drop | 代码：已覆盖（DDL 重构后）；测试：主体等价覆盖。具体逻辑和断言见上文 #11。 |
| 12 | `800fad39c987` | 2026-02-19 | `*: sync AGENTS/.gitignore and Bazel helper updates from master (#66308)` | 同步 AGENTS、`.gitignore`、Makefile/build helper 和开发流程内容。 | 非 MV | 代码：不纳入 MV port；测试：不适用。具体逻辑和断言见上文 #12。 |
| 13 | `76ef4adbc110` | 2026-02-21 | `ddl, executor: fix CREATE MATERIALIZED VIEW validation (#66316)` | 修正 CREATE MV 的对象类型、查询和依赖 validation。 | PR2b-create | 代码：已覆盖；测试：已补齐。具体逻辑和断言见上文 #13。 |
| 14 | `4bd68061a54b` | 2026-02-22 | `*: backport _tidb_commit_ts support for unistore cop request (#66325)` | 为 unistore/cop/rowcodec 等路径传递 commit timestamp，并加入独立测试。 | commit-ts prerequisite；PR4/PR5 使用时适配 | 代码：已覆盖底层 commit-TS 传递；测试：部分覆盖。具体逻辑和断言见上文 #14。 |
| 15 | `0f22124d57d0` | 2026-02-24 | `ddl: allow SET TIFLASH REPLICA on MV-dependent base tables (#66328)` | 放宽 MV 相关表的 TiFlash replica placement DDL，同时保留其他 base-table DDL 约束。 | PR2b-create/drop/alter | 代码：已覆盖；测试：已补齐。具体逻辑和断言见上文 #15。 |
| 16 | `7fe4ee1528fa` | 2026-02-24 | `ddl: prewrite mv refresh info before init build (#66330)` | 在 MV initial build 前预写 refresh-info 行，避免 build 阶段缺少系统表状态。 | PR2b-create；部分涉及 PR5 initial build | 代码：已覆盖；测试：已覆盖。具体逻辑和断言见上文 #16。 |
| 17 | `a2036935163f` | 2026-02-25 | `pkg/session, pkg/ddl, pkg/executor: update mview/mlog system table schemas (#66341)` | 更新 MV/MLog 系统表字段、类型及 DDL/executor 引用。 | PR1；PR2b 对最终 schema 的引用 | 代码：已覆盖（最终 schema）；测试：等价覆盖。具体逻辑和断言见上文 #17。 |
| 18 | `049564311187` | 2026-02-25 | `expression/aggregation: support sum_int in NewDistAggFunc (#66360)` | 为 `SUM_INT` 增加分布式聚合构造。 | 通用 prerequisite；PR5 适配 | 代码：代码已覆盖；测试：直接回归缺项 T1。具体逻辑和断言见上文 #18。 |
| 19 | `d5ca6127eea7` | 2026-02-25 | `session, executor: update mview system table schema (#66382)` | 继续调整 MV 系统表字段和初始化逻辑。 | PR1；PR2b/后续 runtime 引用 | 代码：已覆盖（最终 schema）；测试：等价覆盖。具体逻辑和断言见上文 #19。 |
| 20 | `78096e182edc` | 2026-02-25 | `*: add aggregate function min_count/max_count (#66250)` | 增加 `MIN_COUNT`/`MAX_COUNT` 及 expression、executor、tipb 和测试。 | 通用 prerequisite；PR5 快速刷新适配 | 代码：已覆盖（通用 prerequisite）；测试：已覆盖主体。具体逻辑和断言见上文 #20。 |
| 21 | `e8829b004dcc` | 2026-02-26 | `pkg/ddl, pkg/executor: validate MV START WITH/NEXT expr type and remove defaults (#66383)` | 校验 schedule expression 类型，移除不明确的默认行为。 | PR2b-create/alter；PR5/PR7 schedule runtime | 代码：已覆盖，按最终 UTC/SQL-mode 语义；测试：已覆盖最终场景。具体逻辑和断言见上文 #21。 |
| 22 | `a36d83807816` | 2026-02-27 | `*: refine AGENTS.md style and wording guidance (#66556) (#66558)` | 调整 agent 文档风格和措辞。 | 非 MV | 代码：不纳入 MV port；测试：不适用。具体逻辑和断言见上文 #22。 |
| 23 | `b06308c62738` | 2026-02-27 | `ddl, executor: derive MV NEXT_TIME in create flow with safe eval session (#66555)` | CREATE MV 时使用安全 eval session 求值 `NEXT_TIME`/schedule。 | PR2b-create；PR5/PR7 运行时 schedule | 代码：已覆盖（演进后）；测试：等价覆盖。具体逻辑和断言见上文 #23。 |
| 24 | `58bba0d7a9cd` | 2026-02-27 | `pkg/ddl, pkg/parser, pkg/executor: support pre-split options for create materialized view (#66286)` | CREATE MV 支持 pre-split/scatter 等建表选项，并传入 physical table build。 | PR2b-create | 代码：已覆盖；测试：已补齐。具体逻辑和断言见上文 #24。 |
| 25 | `c0cdf05369c5` | 2026-02-28 | `*: basic mv refresh support (#66385)` | 新增基本 refresh executor、refresh SQL、initial build 相关逻辑和测试。 | PR5；部分 CREATE initial-build prerequisite 已随 PR2b-create | 代码：部分覆盖，待 PR5 runtime；测试：部分覆盖。具体逻辑和断言见上文 #25。 |
| 26 | `3b8b6e4a7230` | 2026-02-28 | `planner, executor, sessionctx: reject explicit dml on mview / mlog tables (#66396)` | 拦截用户直接对 MV/MLog 的 DML，并覆盖 planner fast path 和 integration test。 | PR3 | 代码：已覆盖；测试：已补齐。具体逻辑和断言见上文 #26。 |
| 27 | `e5fdd577b2a7` | 2026-03-02 | `*: Cherry pick topsql adding network field to feature branch (#66290)` | TopSQL 增加 network in/out bytes 统计及相关字段。 | 非 MV | 代码：非 MV，master 已有等价 pipeline；测试：MV 审计不纳入。具体逻辑和断言见上文 #27。 |
| 28 | `85f88a3ff5b3` | 2026-03-10 | `planner: block UPDATE/DELETE on mview/mlog via point-get fast path (#66805)` | 修正 point-get fast path 绕过 MV/MLog DML 禁止检查的问题。 | PR3 | 代码：已覆盖；测试：已覆盖。具体逻辑和断言见上文 #28。 |
| 29 | `7185487da7dd` | 2026-03-12 | `importer: add disk quota support for IMPORT INTO SELECT FROM (#66902)` | 通用 IMPORT INTO SELECT 的 disk quota 支持，同时被 MV initial build 的 import option 使用。 | importer prerequisite；PR2b-create 的实际依赖 | 代码：已覆盖，旧 prerequisite 缺口已关闭；测试：已覆盖。具体逻辑和断言见上文 #29。 |
| 30 | `6f309157d091` | 2026-03-17 | `*: support mv log purge (#66660)` | 实现 MLog purge executor、purge schedule、purge info、DDL dispatcher 和基础测试。 | PR4；部分 DDL prerequisite 已随 PR2b-create/drop/alter | 代码：purge 主体已覆盖，refresh hunk 待 PR5；测试：purge 场景已覆盖。具体逻辑和断言见上文 #30。 |
| 31 | `c447836a4989` | 2026-03-19 | `planner: support fast refresh mview for count/sum (#66595)` | 增加 count/sum fast refresh planner、MV merge plan 和 planner casetest。 | PR5；S9 fast refresh planner | 代码：已覆盖 planner，执行接入待 PR5 runtime；测试：planner 测试已覆盖。具体逻辑和断言见上文 #31。 |
| 32 | `ce48aec35f24` | 2026-03-25 | `table, executor: avoid deep-copying tracked MLog datums (#67160)` | 优化 MLog tracked-column datum 复制，降低 base-table DML capture 的开销。 | PR3 | 代码：已覆盖；测试：等价覆盖。具体逻辑和断言见上文 #32。 |
| 33 | `bb1f16b69cb5` | 2026-04-10 | `planner: add min/max full-update lookup for mv refresh (#67146)` | 为 MIN/MAX fast refresh 增加 full-update lookup 和 planner 支持。 | PR5；S9 | 代码：已覆盖 planner，执行接入待 PR5 runtime；测试：planner 场景已覆盖。具体逻辑和断言见上文 #33。 |
| 34 | `2e944507d529` | 2026-04-18 | `mvservice,domain,server,session,metrics: add materialized view service (MVS) framework (#66242)` | 新增 MV service、任务分配、backpressure、metrics、domain/server/session 集成和大量 service 测试。 | PR7 | 代码：待 PR7；测试：待 PR7。具体逻辑和断言见上文 #34。 |
| 35 | `17ad85353d3e` | 2026-05-21 | `executor: implement MViewDeltaMergeAgg operator (#66326)` | 新增 MViewDeltaMergeAgg executor、merge count/sum/min/max、spill 和测试。 | PR5；S10 | 代码：operator 主体已覆盖，存在 C1；测试：部分覆盖，T2。具体逻辑和断言见上文 #35。 |
| 36 | `e3d29c0b773d` | 2026-05-26 | `executor: add mview delta merge agg builder (#68636)` | 将 delta merge aggregate 接入 executor builder、planner 和 refresh 测试。 | PR5；S10 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #36。 |
| 37 | `72d1670b394f` | 2026-05-27 | `planner/mview: clean up mvmerge naming (#68655)` | 将 `mvmerge` 等内部 planner/operator 名称统一为 mview 命名。 | PR5；S9/S10 命名 refine | 代码：已覆盖命名后的 planner/operator；测试：已覆盖对应 planner 测试。具体逻辑和断言见上文 #37。 |
| 38 | `adc727df5a8b` | 2026-05-28 | `*: several ddl enhancement for mv (#68649)` | 大量 DDL worker、job args、schema tracker、multi-schema-change、依赖约束和测试增强。 | PR2b-create/drop/alter | 代码：已覆盖 DDL 和 purge 语义；测试：已覆盖该部分测试。具体逻辑和断言见上文 #38。 |
| 39 | `1cd7b66e493d` | 2026-05-28 | `OWNERS: Auto Sync OWNERS files from community membership (#67717) (#68706)` | 同步 OWNERS 和 OWNERS_ALIASES。 | 非 MV | 代码：不纳入 MV port；测试：不适用。具体逻辑和断言见上文 #39。 |
| 40 | `b21036c39400` | 2026-05-28 | `executor, parser, planner: support SHOW CREATE for materialized views (#68698)` | 增加 SHOW CREATE MV/MLog 的 parser、executor、planner 和测试。 | PR6 | 代码：待 PR6；混合 DDL hunk 已覆盖；测试：部分覆盖。具体逻辑和断言见上文 #40。 |
| 41 | `d8a6eac392cc` | 2026-05-28 | `executor: several refinements for MV refresh (#68704)` | refresh executor/session 使用、结果处理和测试修正。 | PR5 | 代码：待 PR5 runtime；测试：待 PR5 runtime。具体逻辑和断言见上文 #41。 |
| 42 | `25e037cb02e4` | 2026-05-29 | `expression: keep unary minus flen for signed int/decimal (#68723)` | 修正通用 unary-minus 的 field length 推导。 | 非 MV prerequisite candidate | 代码：master 已有等价通用实现；测试：已覆盖。具体逻辑和断言见上文 #42。 |
| 43 | `edbfd5121f7f` | 2026-05-29 | `executor, mvservice: use unsigned TSO for materialized view metadata (#68719)` | 将 refresh/maintenance metadata 中的 TSO 表示统一为 unsigned 语义。 | PR1 schema；PR5 refresh；PR7 service | 代码：部分覆盖；测试：部分覆盖，T5。具体逻辑和断言见上文 #43。 |
| 44 | `74b10f298fa2` | 2026-05-29 | `executor, sessionctx/variable: add MV maintenance memory quota (#68736)` | 增加 MV maintenance memory quota 变量和 executor/service 使用。 | PR2b-create 的 build option；PR5/PR7 runtime | 代码：部分覆盖；测试：部分覆盖，T5。具体逻辑和断言见上文 #44。 |
| 45 | `f432536c5f34` | 2026-05-29 | `ddl: fix some bug in create mv/ create mv log (#68766)` | 修正 CREATE MV/MLog dispatcher、schema tracker 和 DDL validation。 | PR2b-create | 代码：已覆盖；测试：等价覆盖。具体逻辑和断言见上文 #45。 |
| 46 | `5f3246d2f28a` | 2026-05-30 | `ddl, mvservice: notify alter MV and MV log events (#68776)` | 增加 ALTER MV/MLog notifier event，并让 service 感知相关变化。 | PR2b-alter；PR7 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #46。 |
| 47 | `ffbb0cef41b6` | 2026-05-30 | `executor: fix MV refresh internal-session usage collection (#68778)` | 修正 refresh 使用 pooled/internal session 时的资源统计。 | PR5；PR7 | 代码：待 PR5 runtime；测试：待 PR5 runtime。具体逻辑和断言见上文 #47。 |
| 48 | `c0f0d1b212b4` | 2026-06-01 | `executor, parser: add show materialized views and show materialized view logs (#68808)` | 增加 SHOW MATERIALIZED VIEWS/MATERIALIZED VIEW LOGS。 | PR6 | 代码：待 PR6；测试：待 PR6。具体逻辑和断言见上文 #48。 |
| 49 | `3cfcfc49d821` | 2026-06-02 | `ddl, parser: support materialized view alert attributes (#68810)` | 支持 MV alert attributes 的 parser、DDL metadata 和 notifier/service 引用。 | PR2b-create/alter；PR7 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #49。 |
| 50 | `725e4f64ad7d` | 2026-06-02 | `*: backport mv metadata, import options, and refresh support fixes (#68849)` | 混合回灌 MV metadata、IMPORT options、DDL schema/job args 和 refresh 修复。 | 混合 commit；按 hunk 拆到 PR2b-create、PR5 | 代码：部分覆盖；disk-quota 缺口已关闭；测试：部分覆盖，T5。具体逻辑和断言见上文 #50。 |
| 51 | `c99ca71584bd` | 2026-06-02 | `*: add MV refresh observability and backport related fixes (#68774)` | 增加 refresh observability、结果/metrics/history、expression pushdown 和大量 refresh 测试。 | PR5；部分 PR7；通用 aggregation prerequisite | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #51。 |
| 52 | `259810a1abf8` | 2026-06-02 | `executor,mvservice: unify MV maintenance session vars (#68877)` | 统一 refresh/purge/service 的 maintenance session variables 和 restore 逻辑。 | PR4/PR5/PR7；PR2b-create helper | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #52。 |
| 53 | `bdb304c4d378` | 2026-06-03 | `ddl, executor, planner: backport duplicate count expr support for nullable MV aggs (#68893)` | 修正 nullable MV aggregate 的 duplicate count expression，并更新 planner/executor/DDL 测试。 | PR2b-create validation；PR5 S9/S10 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #53。 |
| 54 | `f30199fba937` | 2026-06-03 | `*: support complete refresh out of place (#68894)` | 实现 out-of-place complete refresh、cutover、临时表、DDL worker、notifier 和测试。 | PR5 | 代码：部分覆盖，候选缺口 C3；测试：部分等价覆盖。具体逻辑和断言见上文 #54。 |
| 55 | `7a3b5d9443e1` | 2026-06-04 | `ddl, mvservice: backport materialized view maintenance fixes (#68947)` | 混合 DDL worker 和 MV service maintenance 修复。 | PR2b；PR7 | 代码：部分覆盖；测试：部分等价覆盖。具体逻辑和断言见上文 #55。 |
| 56 | `6ba9bb6053d6` | 2026-06-05 | `parser, planner, executor, sessionctx: support full outer join (#68919)` | 通用 FULL OUTER JOIN parser/planner/executor/Mpp 支持及测试。 | 通用 prerequisite | 代码：master 已有等价 prerequisite；测试：主体等价覆盖。具体逻辑和断言见上文 #56。 |
| 57 | `3892bda264c2` | 2026-06-10 | `planner, executor, parser, expression: support MV complete delta apply (#68994)` | 实现 complete delta apply、touched rows、delta merge、planner/executor/parser 接入。 | PR5；S9/S10；FULL OUTER JOIN follow-up | 代码：部分覆盖；测试：部分等价覆盖。具体逻辑和断言见上文 #57。 |
| 58 | `a2e127422a1d` | 2026-06-11 | `executor, parser, session, mvservice: support canceling MV jobs (#69102)` | 增加 refresh/purge cancel 语法、状态更新、controller/monitor 和 pooled session 生命周期处理。 | PR5；PR4；PR7；部分 PR2b-alter/schema | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #58。 |
| 59 | `c7b499b9bc6b` | 2026-06-12 | `executor, planner, parser, ddl: support bounded MV fast refresh (#69117)` | 增加 bounded fast refresh 语法、planner、executor、DDL metadata 和测试。 | PR5；S9/S10；PR2b shared index helper | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #59。 |
| 60 | `8ecd2a11bb59` | 2026-06-12 | `pkg/ddl, pkg/executor, pkg/mvservice: fix MV privilege check and refactor execution vars (#69126)` | 混合修正 MV privilege check、DDL/executor/service execution vars 和测试。 | 独立权限闭环；PR2b-create/alter；PR4/PR5/PR7 | 代码：部分覆盖；测试：部分等价覆盖。具体逻辑和断言见上文 #60。 |
| 61 | `09824b200e7e` | 2026-06-13 | `ddl, executor, parser, planner: refine materialized view initialization and refresh syntax (#69159)` | 混合包含 COMPLETE 语法、schema version、initial-build state gate、admin index 限制和 refresh 初始化修复。 | PR2a；PR2b-create/alter；PR5；PR7 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #61。 |
| 62 | `9a30eedc0650` | 2026-06-14 | `*: MV refresh/purge result reporting, metrics, and history enrichment (#69137)` | 增加 refresh/purge 结果、metrics、history 字段和 bootstrap 表初始化。 | PR1；PR4；PR5；PR7 | 代码：部分覆盖；测试：部分覆盖，T4。具体逻辑和断言见上文 #62。 |
| 63 | `99948f2aac85` | 2026-06-14 | `*: cherry pick some non-mv related optimazations to mv branch (#69165)` | prepare dedup、plan cache、通用 session/expression 优化及大量测试结果同步。 | 非 MV；混合 branch drift | 代码：MV hunk 已覆盖，其余不纳入；测试：MV 测试已补齐。具体逻辑和断言见上文 #63。 |
| 64 | `fd53fbc6a7f2` | 2026-06-15 | `pkg/mvservice, pkg/session: add MV alert table and history cleanup (#69166)` | 增加 alert table、history cleanup、service 清理和系统表相关初始化/DDL 清理。 | PR1；PR7；部分 PR2b-drop | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #64。 |
| 65 | `37ff05b94b37` | 2026-06-15 | `mv,mvservice: make maintenance isolation read engines independent (#69172)` | 让 MV maintenance 的 isolation read engines 与普通 session 独立配置。 | PR4/PR5/PR7；PR2b-create initial build | 代码：部分覆盖；测试：部分覆盖，T5。具体逻辑和断言见上文 #65。 |
| 66 | `90447ef0160d` | 2026-06-15 | `executor, session: record MV refresh commit TSO (#69174)` | 记录 refresh commit TSO，用于 refresh info/history 和后续调度。 | PR5；PR1 schema | 代码：部分覆盖；测试：schema 等价覆盖，runtime 未覆盖。具体逻辑和断言见上文 #66。 |
| 67 | `4a2bc184c38c` | 2026-06-15 | `executor: move MV log purge deletes out of pessimistic txn (#69183)` | 将 MLog purge delete 从 pessimistic transaction 中移出，调整执行和统计语义。 | PR4 | 代码：已覆盖 purge 行为；测试：已覆盖。具体逻辑和断言见上文 #67。 |
| 68 | `9cef8d892f06` | 2026-06-15 | `executor, mvservice: throttle materialized view log purge (#69192)` | 增加 MLog purge 限流及 service 调度参数。 | PR4；PR7 | 代码：purge 已覆盖，service 待 PR7；测试：purge 已覆盖。具体逻辑和断言见上文 #68。 |
| 69 | `fbbf288e1869` | 2026-06-16 | `ddl, executor, mvservice: impl mv attribute mview_alert_refresh_failed and enhance mysql.tidb_mview_refresh_alert (#69210)` | 增加 refresh-failed alert attribute、alert 表更新和 service 处理。 | PR2b-create/alter；PR5；PR7 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #69。 |
| 70 | `a534759aca3e` | 2026-06-16 | `executor, session: guard fast refresh against hazardous mlog purge (#69227)` | 在 fast refresh 前检测危险的 MLog purge 状态，避免增量数据已被清理。 | PR4；PR5 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #70。 |
| 71 | `78e1033f4aa7` | 2026-06-16 | `ddl, executor: reject truncating mview-related tables with mlog guards (#69218)` | 拒绝会破坏 MV/MLog 依赖的 TRUNCATE/相关 DDL，并增加 worker 侧约束。 | PR2b-drop/alter | 代码：已覆盖；测试：已补齐。具体逻辑和断言见上文 #71。 |
| 72 | `ee3965416a3f` | 2026-06-16 | `planner, executor, ddl, session: support MV privilege model and show status commands (#69233)` | 混合增加权限模型、SHOW STATUS/状态输出、planner/executor/DDL 检查。 | 独立权限闭环；PR6；PR7 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #72。 |
| 73 | `9dfaa1bc4cd6` | 2026-06-17 | `ddl, executor: cherry-pick MV log DDL enhancements (#69247)` | MLog DDL 增强，包括 create/drop/alter 约束、job args、schema tracker 和测试。 | PR2b-create/drop/alter | 代码：已覆盖 DDL/DML 主体；测试：内部缺项已补齐。具体逻辑和断言见上文 #73。 |
| 74 | `ff53f54bb001` | 2026-06-17 | `mvservice: improve manual cancel backoff handling (#69265)` | 改进手工 cancel 后的 service backoff 和任务重新调度。 | PR7；PR4/PR5 cancel | 代码：待 PR7；测试：待 PR7。具体逻辑和断言见上文 #74。 |
| 75 | `79156a515145` | 2026-06-17 | `infoschema, ddl: support materialized view metadata tables (#69271)` | 在 infoschema/DDL 中暴露 MV metadata tables，并增加读取/测试。 | PR6；部分 PR1/PR2b | 代码：MV metadata 展示待 PR6；DDL 名称约束已有；测试：部分覆盖。具体逻辑和断言见上文 #75。 |
| 76 | `6d14c9de5201` | 2026-06-17 | `ddl, parser, mvservice: support ALERT ROWS for materialized view logs (#69278)` | CREATE MLog 的 alert rows 语法和 metadata，以及 service alert 触发。 | PR2b-create；PR7 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #76。 |
| 77 | `5018ef08bddd` | 2026-06-18 | `executor: support compare materialized view (#69297)` | 增加 COMPARE MATERIALIZED VIEW executor、SQL 和测试。 | PR6；FULL OUTER JOIN prerequisite | 代码：待 PR6；测试：待 PR6。具体逻辑和断言见上文 #77。 |
| 78 | `d2dc23f5c71a` | 2026-06-18 | `parser, ddl: allow clearing materialized view log purge schedule (#69280)` | 支持清除 MLog purge schedule 的 parser 和 ALTER DDL 行为。 | PR2a；PR2b-alter；PR4 metadata/runtime | 代码：已覆盖；测试：等价覆盖。具体逻辑和断言见上文 #78。 |
| 79 | `2e489b053bc6` | 2026-06-18 | `executor: use baseQuery as build side for compare materialized view (#69312)` | 调整 COMPARE MV 的 join build side，降低/修正 compare 执行开销。 | PR6 | 代码：待 PR6；测试：无独立测试变更。具体逻辑和断言见上文 #79。 |
| 80 | `c4207360618b` | 2026-06-18 | `planner, executor: rename MV delta sides (#69307)` | 统一 MV delta apply 中 build/probe/old/new side 的命名。 | PR5；S9/S10 命名 refine | 代码：planner/operator 命名已覆盖，执行组装待 PR5；测试：已覆盖 planner/operator 部分。具体逻辑和断言见上文 #80。 |
| 81 | `f145795d551e` | 2026-06-19 | `ddl: forbid the creation of mlog for unsupported column type (#68997)` | 拒绝 MLog 复制 JSON、binary BLOB 等不支持的列类型。 | PR2b-create | 代码：已覆盖（重构后）；测试：等价覆盖。具体逻辑和断言见上文 #81。 |
| 82 | `e7a30c7889ba` | 2026-06-19 | `ddl: forbid the creating of mlog on partition base table (#68903)` | 拒绝对 partitioned base table 创建 MLog，并让 CREATE MV 分区表错误正确取消。 | PR2b-create | 代码：已覆盖；测试：已补齐。具体逻辑和断言见上文 #82。 |
| 83 | `4769ef44fab9` | 2026-06-20 | `ddl: forbid unique indexes on materialized view (#68911)` | 拒绝会破坏 MV 维护语义的 unique index 操作。 | PR2b-alter | 代码：已覆盖 DDL 语义；测试：DDL 回归已补齐。具体逻辑和断言见上文 #83。 |
| 84 | `552472a43d33` | 2026-06-22 | `ddl: throw error when setting mlog and mv as target to create materialized view log (#68948)` | 拒绝将 MV/MLog/shadow table 作为 CREATE MLog 的 target。 | PR2b-create | 代码：代码已覆盖；测试：部分覆盖。具体逻辑和断言见上文 #84。 |
| 85 | `b441fe3584eb` | 2026-06-22 | `test: cover materialized view log on generated column (#68082)` | 增加 generated column 与 MLog column capture 的测试覆盖。 | PR2b-create；PR3 | 代码：仅测试 commit，相关 runtime 已有；测试：已覆盖。具体逻辑和断言见上文 #85。 |
| 86 | `a8bc88b4d857` | 2026-06-22 | `ddl: forbid to set system table as mlog target and unify the error info for unsupported object types (#68969)` | 拒绝 system table 作为 MLog target，并统一 unsupported object 错误。 | PR2b-create | 代码：已覆盖；测试：已覆盖本 commit 新增场景。具体逻辑和断言见上文 #86。 |
| 87 | `7c01d98e8ad6` | 2026-06-23 | `ddl, parser: support IF EXISTS for dropping materialized views (#69351)` | 增加 DROP MV/MLog 的 IF EXISTS parser 和 DDL 行为。 | PR2a；PR2b-drop | 代码：已覆盖；测试：等价覆盖。具体逻辑和断言见上文 #87。 |
| 88 | `d96dfd09a259` | 2026-06-23 | `ddl: avoid the upgrade of column type from TEXT to MEDIUMTEXT in MLog internal table (#69356)` | 防止 MLog 内部复制列在 DDL 过程中错误升级 TEXT 类型。 | PR2b-alter；PR3 相关 schema 同步 | 代码：已覆盖；测试：已覆盖。具体逻辑和断言见上文 #88。 |
| 89 | `0fd4796db0c4` | 2026-06-23 | `ddl: allow comment-only modify on materialized view base columns (#69391)` | 允许不改变数据语义的 comment-only base column 修改，同时保持 MLog 约束。 | PR2b-alter | 代码：已覆盖；测试：已补齐。具体逻辑和断言见上文 #89。 |
| 90 | `597952c8226e` | 2026-06-23 | `ddl: limit COMMENT strings length when creating materialized view (#68944)` | CREATE/ALTER MV 时限制 COMMENT 长度并产生正确 warning。 | PR2b-create；PR2b-alter | 代码：已覆盖，master 扩展到 ALTER；测试：已覆盖。具体逻辑和断言见上文 #90。 |
| 91 | `42d9a021010e` | 2026-06-25 | `*: slow query skip purge statement (#69462)` | 将 MLog purge 维护 SQL 从 slow query 统计/暴露路径中排除。 | PR4；部分 PR7 observability | 代码：已覆盖；测试：已覆盖且有扩展。具体逻辑和断言见上文 #91。 |
| 92 | `2ad2da61a2ff` | 2026-06-28 | `mvservice: force status indexes for history cleanup (#69496)` | history cleanup 强制使用 status 相关索引，降低清理成本。 | PR7 | 代码：待 PR7；测试：待 PR7。具体逻辑和断言见上文 #92。 |
| 93 | `14ec59608231` | 2026-06-29 | `planner: bind MV MIN/MAX fast refresh lookup to supporting index (#69490)` | 将 MIN/MAX fast refresh lookup 绑定到支持索引。 | PR5；S9 | 代码：lookup planner 已覆盖，运行结果待 PR5；测试：部分覆盖。具体逻辑和断言见上文 #93。 |
| 94 | `8aff9064f372` | 2026-06-29 | `executor: collect stats delta for mlog purge (#69510)` | MLog purge 后收集 statistics delta。 | PR4 | 代码：已覆盖（API 适配后）；测试：已覆盖。具体逻辑和断言见上文 #94。 |
| 95 | `6645850375d3` | 2026-06-29 | `statistics: skip MV log tables in auto analyze priority queue (#69519)` | 将 MLog 从 auto-analyze priority queue 中排除或改由专门调度处理。 | PR4；PR7 | 代码：待后续 statistics/PR7；测试：未覆盖该场景。具体逻辑和断言见上文 #95。 |
| 96 | `3c11363cc32e` | 2026-06-30 | `planner: estimate mlog commit ts filter selectivity (#69445)` | 为 MLog commit-ts filter 增加 selectivity 估算和 planner stats。 | PR4；PR5；S9 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #96。 |
| 97 | `46f2873a3cc7` | 2026-07-01 | `mvservice, variable: schedule mlog analyze in MV service (#69516)` | 由 MV service 调度 MLog analyze，并增加相关变量和 statistics helper。 | PR4；PR7 | 代码：待 PR7；测试：待 PR7。具体逻辑和断言见上文 #97。 |
| 98 | `190c9d508e3` | 2026-07-01 | `executor: avoid exposing materialized view cancel job state (#69555)` | 隐藏内部 cancel job 状态，调整用户可见结果和测试。 | PR4/PR5；PR7 | 代码：purge 已覆盖，refresh 待 PR5；测试：部分覆盖。具体逻辑和断言见上文 #98。 |
| 99 | `c9fd6ddba9ea` | 2026-07-02 | `executor: update error messages for materialized view log creation privileges (#69578)` | 修正 CREATE MLog 权限错误信息和对应测试。 | 独立权限闭环；PR2b-create | 代码：代码已覆盖；测试：unit 覆盖，integration 未覆盖。具体逻辑和断言见上文 #99。 |
| 100 | `19544ab81d34` | 2026-07-03 | `planner: collect predicate columns for MV maintenance SQL (#69596)` | 收集 MV maintenance SQL 的 predicate columns，改善 stats/plan 选择。 | PR5；S9 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #100。 |
| 101 | `b1568c2e4a41` | 2026-07-09 | `metrics, mvservice: refine MV service metrics by component (#69737)` | 按组件细化 MV service metrics、reporter 和 Grafana。 | PR7 | 代码：待 PR7，部分 purge metrics 已有；测试：无独立 Go 测试新增。具体逻辑和断言见上文 #101。 |
| 102 | `17575377d73f` | 2026-07-12 | `mview: improve refresh observability and mlog purge delete (#69758)` | 混合修正 refresh observability、MLog purge delete、metrics、bootstrap 字段和测试。 | PR4；PR5；PR7；部分 PR1 | 代码：purge 部分已覆盖，refresh/service 待后续；测试：purge 等价/更强覆盖。具体逻辑和断言见上文 #102。 |
| 103 | `06030b2809c0` | 2026-07-13 | `mvservice: refine MV refresh alert checking (#69808)` | 改进 refresh alert checker、过期/失败判断和 service 测试。 | PR7；部分 PR5 observability | 代码：待 PR7；测试：待 PR7。具体逻辑和断言见上文 #103。 |
| 104 | `7d7b687384f8` | 2026-07-13 | `executor,mvservice: avoid memory retention in MV maintenance SQL (#69778)` | 混合修正 MV maintenance SQL memory retention，同时回灌 DEPS、Makefile、cmd/mirror 等 branch drift。 | PR4/PR5/PR7 的 MV hunk；非 MV drift 不纳入 | 代码：混合，MV service 部分待 PR7；测试：未覆盖 source 的两个专门场景。具体逻辑和断言见上文 #104。 |
| 105 | `6910cef84061` | 2026-07-17 | `mvservice: fix refresh alert checker ownership (#69849)` | 修正 refresh alert checker 的 owner/server maintainer 生命周期。 | PR7 | 代码：待 PR7；测试：待 PR7。具体逻辑和断言见上文 #105。 |
| 106 | `f5dfdf58b9c9` | 2026-08-21 | `refine bootstrap (#73)` | 合并/简化 bootstrap version，删除旧 upgrade 路径中的重复 MV system-table registry 和大量历史测试。 | PR1 | 代码：已覆盖（bootstrap 架构替代）；测试：等价覆盖。具体逻辑和断言见上文 #106。 |
| 107 | `43c6999be1c9` | 2026-08-22 | `mv: use saved schedule timezone for unix seconds (#74)` | 原保存 timezone/Unix-seconds 的变更；最终 source 由 #114 改为 UTC 求值并保存 schedule SQL mode，按此最终结果检查。 | PR1 schema；PR2b-create/alter；PR4/PR5/PR7 runtime | 代码：部分覆盖，旧 timezone 语义已被 #114 取代；测试：最终 DDL/purge 场景已覆盖。具体逻辑和断言见上文 #107。 |
| 108 | `c226d733626f` | 2026-08-22 | `executor, ddl, session: store MV refresh end time as Unix seconds (#75)` | 将 refresh end time 从 datetime 改为 Unix seconds，并调整 schema、DDL、refresh 写入和测试。 | PR1；PR2b-create/alter；PR5 | 代码：部分覆盖；测试：部分等价覆盖。具体逻辑和断言见上文 #108。 |
| 109 | `24eaea3deee7` | 2026-08-23 | `mview: unify maintenance timestamp names (#76)` | 统一 refresh/purge info/history 的时间字段命名，例如 snapshot/start/end/heartbeat/update。 | PR1；PR4/PR5/PR7 | 代码：部分覆盖；测试：部分覆盖。具体逻辑和断言见上文 #109。 |
| 110 | `bf681b1b662a` | 2026-08-25 | `mview: unify materialized view naming (#77)` | 统一 `MV`/`MView`/`MLog` 在系统表字段、Go 类型、task/service 和 planner 中的命名。 | PR1；PR2b；PR4/PR5/PR6/PR7 | 代码：部分覆盖（最终命名）；测试：部分覆盖。具体逻辑和断言见上文 #110。 |
| 111 | `d05b5da91b50` | 2026-08-26 | `add mv system table rebuild sql` | 提供旧 MV 系统表清理并按最终 schema 重建的 SQL 运维脚本。 | PR1 文档/运维辅助 | 代码：运维 artifact 未 port，不是 runtime 缺口；测试：不适用。具体逻辑和断言见上文 #111。 |
| 112 | `9439fdfa65e0` | 2026-08-29 | `parser: align materialized view options with spec (#78)` | 按 spec 重排 CREATE MV options、REFRESH、ATTRIBUTES 以及 AST Restore/parser 测试。 | PR2a；PR2b-create/alter 对 metadata 的适配 | 代码：parser/DDL 已覆盖，SHOW 待 PR6；测试：部分覆盖。具体逻辑和断言见上文 #112。 |
| 113 | `8d2633e8e55a` | 2026-08-29 | `mview: remove unused never refresh method (#79)` | 删除未使用的 `NEVER REFRESH` refresh method、AST 分支和 DDL metadata builder 分支。 | PR2a；PR2b-create/alter | 代码：已覆盖最终状态；测试：无新增测试变更。具体逻辑和断言见上文 #113。 |
| 114 | `f6de16d6f2fa` | 2026-10-08 | `port some bug fix back to cp_mv_for_master (#81)` | 回灌 parser/DDL 修复、schedule SQL mode/UTC 求值、refresh 除法精度、shadow cleanup、cutover 依赖/原子性、infoschema v2、GC 和 planner 修复及测试。 | 混合 commit；PR2a/PR2b/PR4/PR5/PR7 及 prerequisite，按 hunk 拆分 | 代码：部分覆盖，按 hunk 拆分；测试：多数 DDL/helper 场景已覆盖。具体逻辑和断言见上文 #114。 |
| 115 | `fd4636bb442d` | 2026-10-09 | `fix some ddl bug` | 修复 MV/MLog DDL 内部 SQL 错误后的整事务回滚、rollback schema-version 失败重试、重开事务后的 job metadata 并发检查和 infoschema v2 affected-table 保留；补充失败回滚回归。 | PR2b-create/drop/alter；共享 DDL/infoschema prerequisite | 代码：已覆盖（重构后）；测试：核心新增回归已覆盖。具体逻辑和断言见上文 #115。 |

## 汇总结论与候选遗漏

### 本轮已关闭的旧缺口

#71025 的 PR1–PR3 测试与 #71048 的 query-import disk quota 已合入；purge runtime、
REFRESH parser/planner、delta operators、shadow/cutover DDL 和 DDL cleanup rollback
也已合入。它们在上面的每条记录中分别更新，不能继续整组记为“代码 0%、测试 0%”。

相反，完整 refresh SQL runtime、refresh cancel、observability、SHOW/COMPARE/infoschema
以及 MV service 仍未合入。它们在对应 commit 下保留为后续 owning slice，不能将未合入
工作分支的逻辑或测试计入 master。

### 代码候选缺口

| ID | Source commit # | 对照证据与语义差异 | 判定 |
| --- | --- | --- | --- |
| C1 | 35；关联 33/53 的 MIN/MAX recompute | source `encodeChunkKeyRows` 依靠旧 codec 的追加语义保存逐列 NULL 标记和复合 key；master 同名调用已换成新签名，但 `codec.preAllocForSerializedKeyBuffer` 每次把输出 row slice 长度重置为 0。逐列调用会覆盖前列，使 `(1,"same")`、`(2,"same")` 编成同一最后列。master 的 `TestMaxBatchRecompute*` 仅用单列整数 key；两侧使用同一 encoder，不能揭示这个冲突。 | **已合入 operator 的 API 语义适配缺口**，不能因调用字面相似或已有单列测试而记为完整覆盖。本轮未运行复合 key 回归，也不计入未合入分支的修复。 |
| C2 | 5 | source `pkg/executor/point_get.go:decodeOldRowValToChunk` 在 `getColInfoByID` 返回 nil 且列 ID 为 ExtraCommitTSID 时构造 synthetic column；master 同一旧 row-format 分支直接将查找结果传给 `GetColOriginDefaultValue`，没有该 fallback。 | **保留的 prerequisite hunk 未找到等价逻辑**。是否由上层路径排除该 schema/row-format 组合仍需场景回归确认；不把底层 rowcodec 已支持 commit TS 当作该分支已覆盖。 |
| C3 | 54 | source 的 internal advisory lock 用 `getAdvisoryLockOwner` 为无 ProcessInfo session 分配稳定非零 ID，并有两项 owner 回归；master `GetAdvisoryLock`/`IsUsedAdvisoryLock` 仍直接取 `s.ShowProcess().ID`，没有相同 nil/内部 ID 分配语义。另有 source `advisoryLock.GetLock` 在 SET/BEGIN 错误时 Close，master 相同早期错误路径直接返回。 | **refresh internal-session prerequisite 候选缺口**。source conflict-cleanup 的实现方式虽不同，master `advisoryLock.GetLock` 在 INSERT 失败时已 Close/clean 归还 pool，该 INSERT 冲突阶段不应误判为遗漏；SET/BEGIN 的前置失败清理与稳定 owner 则仍有差异。 |

### 需单独决定的通用 prerequisite 差异

以下两项 source 最终代码仍保留、master 也确实存在语义差异，但属于通用能力。
是否必须随 MV port 带入，应由实际 MV 依赖决定，不能因为它们出现在 source commit
中就自动把所有通用改动纳入 MV 主线。

| ID | Source commit # | 最终差异与测试 | 处理 |
| --- | --- | --- | --- |
| C4 | 51 | source `scalarExprSupportedByFlash` 的 allowlist 包含 `ast.UnaryMinus`，master 的 TiFlash allowlist 不包含；source `TestExprPushDownToFlash` 对 UnaryMinus 的成功 pushdown 断言在目标同名测试中缺少。master 普通 unary-minus flen/PB 支持不等于 TiFlash 允许下推。 | 保留为通用 pushdown prerequisite 差异；不据此断言整个 MV SQL 不正确，也不记为已覆盖。 |
| C5 | 104 | source root MPP 的 encode type 经 `distsql.SetEncodeType` 尊重 EnableChunkRPC=false，master `local_mpp_coordinator.go` 仍对 root 固定 TypeChunk；`TestSetMPPEncodeTypeRespectsChunkRPCSetting` 也没有对应断言。 | 随后续 purge service 的 chunk-RPC/memory-retention 依赖复核；receive-report timeout 和 build/dependency drift 仍不整体纳入。 |

### 测试直接缺项与待确认的等价性

以下区分已合入 owner 的直接测试缺项、明确待后续 owner 的统计测试，以及尚待确认
是否已有等价断言的场景。“缺项”是静态断言证据结论，不是说现有 CI 一定失败；
代码是否正确和测试是否迁移完整分开判断。

| ID | Source commit # | Source 最终场景 | Master 检查结果 |
| --- | --- | --- | --- |
| T1 | 18 | `TestNewDistAggFuncSumInt` 的构造结果与 mode | 代码分支已有，但未找到等价的直接构造/mode 断言；SUM_INT pushdown/SQL 测试不能直接替代。 |
| T2 | 35 | `TestColumnAppendCellRangeFixed`、`TestColumnAppendCellRangeVarLen`、`TestHasNull`；复合 group-key 不碰撞 | master helper 和 operator 已合入，三个 helper 的直接 NULL/固定长度/变长 range 断言没有迁移。C1 的跨列 key 不碰撞也未被现有单列 batch tests 覆盖。 |
| T3 | 35/51/57 | delta-merge / complete-delta 的 runtime stats String/Merge/Clone 与 step profile | 相关 runtime instrumentation 尚待后续 PR5，source 的 stats/profile tests 也未迁移；属于明确剩余范围，不能拿 operator 算法测试宣称统计行为已覆盖。 |
| T4 | 62/107/114 | `TestHistTimeUsesLocation` 的 UTC/+08/微秒转换；`TestMaterializedScheduleRuntimeEvalUsesScheduleSQLMode` 的 ALLOW_INVALID_DATES/strict/DATE 场景 | purge 的 `historyTime` 与 shared schedule eval helper 已有，未找到这两组直接等价断言。`TestParseExpressionWithSQLMode` 和 UTC schedule tests 已迁移，但只覆盖解析/UTC，不证明这两组运行时转换/错误模式断言已迁移。 |
| T5 | 3/43/44/50/65/72/84/99/114 | CREATE 的 START WITH 无 NEXT 拒绝、commit-TS hidden-column unsigned type、maintenance sysvar 默认/session/global/非法值、shadow 作为 CREATE MLog target、schema-diff rollback 专项、权限 integration | parser/DDL/vars 大部分生产逻辑已有，相关直接 source 测试未逐项找到等价断言；schema-diff rollback 已有更高层失败重试回归，但不能仅据此声称每个直接断言完整。`mview_privilege.test/.result` 仍缺，且混有 PR5/PR6 场景，应按场景拆分核对，而非整体判缺。变量测试按 master 当前名称/约定比较场景，不要求恢复 source 测试中的旧别名。 |

### 已确认的非字面等价 / 被替代场景

- #23/#107 的 saved-timezone tests 已在 source #114 被 UTC/SQL-mode tests 替代；不再要求 port 已删除的 timezone 策略。
- #114 的 definition division precision：master `TestCreateMaterializedViewAndLog` 检查 metadata=9，`TestInitCreateMaterializedViewBuildSessionAppliesDefinitionDivPrecisionIncrement` 检查应用/恢复；不因 source 单独测试名缺失而判漏。
- #54/#114 的 cutover dependency/metadata rollback/publish-event 原子性在 master 由直接 DDL 测试覆盖；完整 REFRESH orchestration/失败清理/读取 snapshot 仍待 runtime，两者不能混为一项。
- #102 的 delete-session TiFlash threads 已由 master count/delete 两个 session 的更强断言覆盖，旧测试名不需要保留。
- #56 的 FULL OUTER JOIN 最终 join-type 转换有相同四类断言；master 没有 source 的独立 ConvertOuterToInner rule flag，不要求复活该内部组织。
- #2/#17/#19/#106 的历史 classic upgrade/version 编号由当前 NextGen 最终 schema/upgrade tests 替代；generated parser/Bazel/test result 均按 master 工具链生成，不机械复制旧 artifact。

## 审计命令与验证边界

本轮更新文档，没有修改生产代码或测试，也没有执行这些测试。以下命令用于固定快照、
定位代码与测试断言，并核对 inventory；函数名匹配或文本 diff 只用于定位，不单独作为语义覆盖结论。

```bash
git fetch origin master
git fetch xufei cp_mv_for_master
git rev-parse origin/master xufei/cp_mv_for_master
git rev-list --count f08c648a20..fd4636bb44
git diff --shortstat f08c648a20..fd4636bb44
git log --reverse --format='%H%x09%ad%x09%s' --date=short f08c648a20..fd4636bb44
git show --format= --unified=0 <source_commit> -- '*_test.go'
git show fd4636bb44:<source_path>
git show f40ad1d845:<master_path>
git merge-base --is-ancestor <port_commit> f40ad1d845
git diff --check
```

本轮仅为静态审计：候选缺口仍需各自最小场景确认；测试存在不等于本轮运行通过。
不基于本轮结果将“未合入的代码”记为 master 已覆盖，也不在本轮直接修复候选缺口。
