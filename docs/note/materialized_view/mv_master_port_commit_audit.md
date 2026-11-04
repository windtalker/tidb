# MV Master Port Commit Audit

本文逐 commit 检查 `cp_mv_for_master` **最终树中仍保留的改动及测试断言**，是否已移植到
master `e51a8506edf7cd4b7970c592fb40d26f55250dd1`。不是只检查功能名称、PR 是否合入、
patch-id 或测试是否同名；source 中被后续删除/替换的中间实现不要求恢复。

**本轮结论：不能标记所有目标已完整 port。** PR1–PR7 的主要入口都已合入，但仍有两处
代码逻辑缺失（C2/C6）、两项 artifact 缺失（C7/C8）、三组较大改写（R1–R3），以及下文
逐项列出的测试/断言缺项。旧 C1/C3/C4/C5 和 T2 已关闭；旧“待 PR5/PR6/PR7”判断已更新。

## 审计范围

本轮日期：2026-11-04。固定边界如下，后续工作分支或 remote ref 移动不改变结论：

```text
source base (merge-base): f08c648a20380ea723449c6c3eb5b171d96fd567
source head:             fd4636bb442de52a1a5c6321f3f29092472aa6f4
                         xufei/cp_mv_for_master
master:                  e51a8506edf7cd4b7970c592fb40d26f55250dd1
```

source 范围共 **115 个 commit**，最终 diff 为 `509 files changed, 99759 insertions(+),
25157 deletions(-)`。使用 remote source 的固定 SHA；本地同名 source 分支较旧，不能作为
对照。master 也始终用上述 SHA，不把其他工作分支中的补丁计入。

范围依据 [port tracking](mv_master_port_tracking.md) 和
[non-MV audit](mv_master_port_non_mv_audit.md)，逐 commit 编号沿用本文原清单。
两份规划文档中的旧进度不是本轮完成证据；本轮只修改本文。

本轮采用比旧审计更严格的实现标准：**功能相近、测试通过或结构被重写，均不能证明
source 最终实现已移植；较大改写单列 R 项，不记完整 port。** 文件拆分、master API/
版本号适配、测试改名不自动算改写，但必须检查实际控制流和断言。明确修复另列 B 项。
[PR #72490](https://github.com/pingcap/tidb/pull/72490) 是本轮判定依据之一：早期
raw-byte touched-row 比较破坏 FLOAT/DOUBLE signed-zero 语义，后续恢复 source typed
comparison/chunk 算法；不能拿“有 touched-row helper”作为已 port 证据。

本轮为静态实现及测试断言审计，**未执行 Go、SQL、RealTiKV、Grafana 或性能测试**。
“测试已保留”表示目标树有相应 fixture、分支及断言，不表示本轮运行通过。缺项也不等于
已复现运行错误。保留 source 测试的同时，允许 master 增加回归；不能以新增用例替代
source 中消失的不同场景。

## Master 对应实现

下列 merge commits 均位于固定目标历史中，用于定位；source 与 port commit 不要求一一对应。

| Master commit | 内容 | Port 归属 |
| --- | --- | --- |
| `d6afc7d9912` | system-table bootstrap（#70599） | PR1 |
| `8cde78af3c5` | OPERATE VIEW privilege（#70694） | 权限 |
| `5e8a1a229a7` | MV/MLog DDL syntax（#70744） | PR2a |
| `94a9cbedabb` | CREATE MV/MLog（#70789） | PR2b-create |
| `f3f7b3bb4dd` | DROP MV/MLog（#70874） | PR2b-drop |
| `10f06594e7e` | ALTER MV/MLog（#70927） | PR2b-alter |
| `91e2f28ac7b` | base-table DML 写 MLog（#70941） | PR3 |
| `28a3d227c64` | partition/exchange guards（#71012） | PR2b |
| `bd762047caf` | MV regression tests（#71025） | PR1–PR3 tests |
| `22958d6535` | IMPORT INTO SELECT disk quota（#71048） | prerequisite |
| `5dd438d8ee` | purge SQL surface（#71045） | PR4 |
| `21f9ba4ee22` | purge runtime（#71118） | PR4 |
| `fe7d7d978e` | NextGen bootstrap version 顺序（#71552） | PR1 |
| `4f27dad8db` | REFRESH syntax（#71566） | PR5 parser |
| `aabfb399a8` | schedule UTC/SQL-mode（#71616） | DDL/runtime |
| `4b2221cddf` | shadow/cutover DDL（#71645） | PR5 prerequisite |
| `1f819a0b4a6` | delta merge/apply operators（#71812） | PR5 |
| `d09ecd3830` | DDL rollback/retry（#71816） | PR2b |
| `34d5c2b75b4` | refresh planner/lookup/stats（#71672） | PR5 |
| `e43c93efd44` | advisory-lock owner/cleanup、MPP encode type（#72099） | prerequisite |
| `0a4db0d86b` | unary-minus TiFlash pushdown（#72107） | prerequisite |
| `f0777c6e46b` | refresh executor builder（#72126） | PR5 |
| `37a4cd4f439` | runtime stats、复合 group-key encoding（#72128） | PR5 |
| `a87bf8db649` | refresh runtime（#72196） | PR5 |
| `05fd982b2d4` | refresh entry/diagnostics（#72211） | PR5 |
| `63ec01b2440` | refresh tests、empty MLog shortcut（#72227） | PR5 |
| `66d0e9e8353` | SHOW MV/MLog（#72430） | PR6 |
| `bb23c552354` | MV infoschema（#72442） | PR6 |
| `59bfe3daabe` | COMPARE（#72472） | PR6 |
| `fba22fa599b` | 恢复 touched-row/chunk source 算法及回归（#72490） | PR5 |
| `7e84fb645ad` | service framework（#72498） | PR7 |
| `1a3583152c6` | service variables（#72497） | PR7 |
| `4f84350e662` | service scheduling（#72502） | PR7 |
| `94c510267e2` | domain/service integration（#72540） | PR7 |
| `2f4ea713bea` | BIT MIN/MAX 修复（#72581） | PR5 |
| `e51a8506edf` | MLog analyze（#72582） | PR7/statistics |

## PR1-PR3 测试补齐跟踪

#71025 已合入；下列是这些专项的结论，不代表整个 DDL/bootstrap suite 已完整保留。

| Source commit # | 目标测试 / 场景 | 本轮结论 |
| --- | --- | --- |
| 7 | ReservedRowIDAlloc Current base/max | 已保留 |
| 10 | 两个 MDLCreateMaterializedViewLog 事务阻塞/提交后写入 | 已保留 |
| 13 | ValidationCoverage COUNT(column)、大小写、缺列 | 已保留；其余 DDLBasic 见 T28 |
| 15 | MV-dependent base SET TIFLASH REPLICA | 已保留 |
| 24 | MV/MLog pre-split metadata/regions | 已保留 |
| 26 | CheckMViewUpdatable direct/restricted/internal-error | 已保留 |
| 38 | MLog vector-index 拒绝 | 已保留；其他 DDL 子场景见 T17/T23 |
| 53 | nullable aggregate CREATE validation | 已保留；FAST SQL 现也已合入 |
| 63 | 五张 MV system tables unrecoverable | 已保留 |
| 71 | IMPORT base table with MLog 拒绝 | 已保留 |
| 73 | column-usage/related-column field-type 四项内部回归 | 已保留；ADD COLUMN 后 SQL 见 T18 |
| 82 | partitioned base 的 MV/MLog 双路径 | 已保留 |
| 83 | unique/primary 拒绝、普通 index 允许 | 已保留 |
| 89 | comment-only MODIFY | 已保留 |

## Port slice 定义与判定规则

| Slice | 目标树已有入口 | 尚不能标完整的范围 |
| --- | --- | --- |
| PR1/权限 | classic/NextGen 五表 bootstrap、OPERATE VIEW | schema 测试 T9、rebuild SQL C8 |
| PR2 | parser、CREATE/DROP/ALTER、shadow/cutover、依赖保护 | R3、T5/T10–T12/T15/T17–T23/T28 等具体断言 |
| PR3 | base DML capture、direct-write guards | 旧 row-format prerequisite C2；ALTER 后 DML 测试 T18 |
| PR4 | purge runtime/history/cancel/throttle | R1、T4/T25/T26；sysvar T6 |
| PR5 | planner、operators、builder、refresh runtime/observe/cancel | R2、T1/T3/T4/T5/T7/T8/T13/T14/T24 等 |
| PR6 | SHOW、COMPARE、MV infoschema | 原 Go 场景已保留；integration 层 T27 |
| PR7 | service、scheduler/backoff/alert/history cleanup/MLog analyze | not-ready MV analyze gates C6/T16、Grafana C7 |

- `已移植`：检查 source 最终代码及目标实际实现，原算法/控制流保留，差异限于注明的适配或修复；不暗示所有测试完整。
- `部分，Rn/Cn`：实际入口已存在，但仍有较大改写或缺失；不能据 PR 合入改记完成。
- `已有通用 prerequisite`：按 non-MV audit 复用 master 原有能力；不把其他实现冒充 source 原实现。
- `测试部分/缺项 Tn`：具体 fixture/分支/断言缺失，含同名测试被缩减；已移动且等价的断言不报缺失。
- `不纳入/最终删除`：已约定的非 MV drift，或 source 最终已删除的内容。

目标测试位置：DDL 在 `pkg/ddl/tests/materializedview/`、`materializedviewlog/`；refresh/SHOW/
COMPARE/infoschema 在 `pkg/executor/test/materializedview/`；purge 在
`pkg/executor/test/materializedviewlog/`；planner 在 `pkg/planner/core/casetest/mview/`；
operator 在 `pkg/executor/mviewdeltamergeagg/`、`pkg/executor/mview_complete_delta_apply.go`；service 在
`pkg/mvservice/`。下表的同名/子测试均指固定目标树，不能只在 source 旧目录找名称。

## 前 10 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 1 | **已移植，metadata API 适配**。`TableInfo`、MV/MLog/base 反向依赖及各 `Clone` 的最终字段和深拷贝均保留。 | **无独立测试变更**。CREATE/DROP/ALTER 的依赖断言存在；本条不以结构体布局相同作为条件。 |
| 2 | **已移植**。五张系统表的最终 DDL 保留；classic `upgradeToVer316` 和 NextGen registry 均注册同一组表，不是只剩 NextGen。 | **部分，T9**。bootstrap/upgrade 测试存在，但 helper 只查列名/索引名，丢失类型、unsigned、精度、索引列顺序及 PK handle 断言。 |
| 3 | **已移植**。CREATE/DROP/ALTER、REFRESH/PURGE AST、grammar、Restore 和 visitor 均有最终实现；parser 生成物按 master 工具链处理。 | **部分，T5**。正常语法/visitor 在 parser suites；CREATE `REFRESH FAST START WITH` 无 NEXT 的拒绝及部分直接 AST Restore/Mode 边界未保留。 |
| 4 | **已移植，DDL 接口适配**。MLog executor/worker、job args、tracker、placement/rollback 保留；table-info 构造提取共享 helper，列校验循环仍在。 | **部分，T21**。CREATE 基础、history statement type 等已保留；长物理名的实际 DML/DROP 与 Unicode 冲突后重试成功断言未完整迁移。 |
| 5 | **部分，C2**。commit-TS/planner/notifier prerequisite 存在；`decodeOldRowValToChunk` 缺 source 的 synthetic `ExtraCommitTSID` ColumnInfo fallback。非 MV branch-sync hunks 排除。 | **C2 无直接目标回归证据**。`TestDecodeWithCommitTS` 测 rowcodec，不是旧 row-format point-get fallback；独立 active-active/result drift 不机械迁移。 |
| 6 | **已移植**。INSERT/UPDATE/DELETE/REPLACE/IODKU/LOAD DATA、rollback、tracked-column MLog capture 的最终路径保留。 | **已保留**。34 个最终 writetest 及 `mview_log_dml.test/.result` 的 MV 场景存在；source 已删除的 CurrentBehavior 场景不复活。 |
| 7 | **已移植**。reserved row-ID `Current()` 与 MLog 独立分配语义保留。 | **已保留**。`TestReservedRowIDAlloc` 检查 base/max，MLog row-ID integration 回归也在。 |
| 8 | **已有通用 prerequisite**。IndexJoin/IndexHashJoin NULL-safe equality 已在 master，按 non-MV audit 复用。 | **已保留相关场景**。四个 `TestIndexJoinNullEQ*` 及 benchmark 的 NULL-EQ 初始化存在。 |
| 9 | **已有通用 prerequisite**。SUM_INT parser/expression/aggregate/PB/checker 已存在；不把 master 原有实现称作 source 原实现的直接 port。 | **主体场景已有**。类型、溢出、滑窗、partial merge、spill、SQL/pushdown 断言存在；#18 的专门 dist-agg 构造另有 T1。 |
| 10 | **已移植**。CREATE MLog 的 base-table ID、job table IDs 与 MDL involving tables 保留。 | **已保留**。job IDs 及两个 `TestMDLCreateMaterializedViewLog...` 的事务阻塞、提交后写日志均有断言。 |

## 第 11 到第 20 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 11 | **已移植 DDL 主体**。CREATE/DROP worker、initial build、cancel/rollback、依赖/GC 和普通表保护在；cutover 后增逻辑另见 #54。 | **部分，T10/T28**。import 无 AS OF、普通表 TRUNCATE、取消重试已在；不能把拆分后的 suites 当作原 `TestMaterializedViewDDLBasic` 全部断言，validation/alias/metadata 场景及 build maintenance flag 有缺项。 |
| 12 | **不纳入**。AGENTS/ignore/Makefile/build helper 属于已约定的 branch drift。 | **不适用**。没有 MV 专属测试变更。 |
| 13 | **已移植**。CREATE aggregate/列依赖 validation 和 build read-TS 类型对齐存在。 | **部分，T28**。`TestCreateMaterializedViewValidationCoverage` 保留 COUNT(column)、大小写、缺 MLog 列；read-TS 类型回归保留，原 DDLBasic 其他 negative cases 未全部保留。 |
| 14 | **已存在底层 prerequisite**。unistore/cop/rowcodec commit-TS metadata、编码与解码支持在。 | **MV 所需底层测试存在**。rowcodec commit-TS 测试保留；独立 `active_active/commit_ts.test` 按 non-MV audit 排除，不能用它替代 C2。 |
| 15 | **已移植**。允许 MV-dependent base table SET TIFLASH REPLICA，其他依赖保护仍在。 | **已保留本条 base-table 回归**。`TestMaterializedViewBaseSetTiFlashReplica` 建立真实 MV/MLog 依赖后执行 DDL；原 DDLBasic 的 MV 自身 TiFlash 子场景另列 T28。 |
| 16 | **已移植**。initial build 前 prewrite refresh-info，成功写入及事务可见性保留。 | **已保留**。RunningAndSuccess、SuccessRefreshInfoVisibilityBeforeCommit 检查运行中、提交前后状态。 |
| 17 | **已移植最终 schema**。采用 source 最终五表/列名及 DDL/runtime 引用，不回灌已废弃的中间 schema。 | **部分，T9**。CREATE lifecycle 已在；bootstrap/upgrade 的最终类型、索引列等断言缩减，不能仅凭列存在记完整。 |
| 18 | **代码已移植**。`NewDistAggFunc` 的 `tipb.ExprType_SumInt` 分支在。 | **缺项 T1**。缺 PB CompleteMode 构造、具体 `sumIntFunction`、Update 1/2/3 后结果 6 的直接回归。 |
| 19 | **已移植最终 schema**。LAST_PURGED_TSO 及 history 微秒时间类型在。 | **部分，T9**。表定义正确不等于测试完整；DATETIME(6)/unsigned 等原断言未保留。 |
| 20 | **已有通用 prerequisite**。MIN_COUNT/MAX_COUNT 类型、partial/final、spill、SQL/window/PB 支持已存在。 | **主体场景已有**。`TestMaxMinCount*`、partial merge、内存、滑窗、final schema 均有断言；master 扩展不回退。 |

## 第 21 到第 30 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 21 | **已移植最终语义，局部 helper 调整 A1**。schedule 类型校验/无默认 schedule 保留，按 #114 最终 UTC/SQL-mode 规则比较。 | **部分，T20**。CREATE/ALTER MV/MLog 校验存在，但 source `START WITH CURRENT_DATE` 分支被 NEXT DATE 场景替换。 |
| 22 | **不纳入**。仅 AGENTS 文档变更。 | **不适用**。无 MV 代码/测试。 |
| 23 | **已移植最终语义，A1**。CREATE refresh-info 的 START/NEXT 优先级、Unix-seconds upsert/UTC 求值在；共享 helper 的求值顺序差异单列。 | **已保留核心场景**。upsert SQL、NextUnixSecondsDerivation、UsesUTC 在；已被 #114 删除的 saved-timezone 策略不要求恢复，SQL-mode 边界另见 T4/T20。 |
| 24 | **已移植**。pre-split/shard options 经 AST 传入 physical metadata/regions。 | **已保留**。MV/MLog metadata、regions、Restore 和 duplicate options 断言存在。 |
| 25 | **已移植 refresh runtime 主体**。`RefreshMaterializedViewExec`、session/事务、history/result/权限入口已由 #72196/#72211 合入；实现 planner 的改写 R2 另列。 | **部分，T5/T14**。COMPLETE/FAST SQL、statement result、`mview_refresh.test` 在；直接 AST 边界及 refresh 不标记 DDL execution 的回归缺失。 |
| 26 | **已移植**。用户 MV/MLog DML 拦截、restricted maintenance bypass 接入 planner/executor。 | **已保留**。`TestCheckMViewUpdatable` direct/restricted/internal-error 与 integration DML 拒绝均在。 |
| 27 | **不纳入 MV port**。TopSQL network bytes 为独立通用能力，按 non-MV audit 处理。 | **不计为 MV 缺项**。network/stmtstats 测试不属于本轮 MV port 目标。 |
| 28 | **已移植**。point/batch-point UPDATE/DELETE fast path 前检查 MV/MLog 可写性。 | **已保留**。`mview_log_dml.test` 的单点、IN-list UPDATE/DELETE 拒绝场景在。 |
| 29 | **已移植 prerequisite**。query-import disk_quota 及 quota checker 生命周期已由 #71048 合入。 | **已保留**。query options 正反例、`TestStartDiskQuotaCheck`、RealTiKV `TestDiskQuotaFromSelect` 存在；本轮未运行。 |
| 30 | **部分，R1**。purge SQL/plan、lock/checkpoint/history/batch/schedule 功能在，但 `executePurgeMaterializedViewLog` session 分工与错误收尾大幅重组，不能记为 source 实现完整移植。 | **purge 主体场景在，T25/T26 有差异**。失败/finalize/checkpoint/取消已在；delete-only TiFlash threads 被改成 count+delete，另缺两个 SQL builder 边界断言。 |

## 第 31 到第 40 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 31 | **部分，R2**。`pkg/planner/mview` COUNT/SUM 推导、plan/codec 和执行器接入在；`buildRefreshMaterializedViewImplement` 的整体重写不能只称文件移动。 | **planner/SQL 主体已保留**。Build/Explain fast plan、COUNT/SUM/dependency 及 handle layout 场景在；通过场景不抵消 R2。 |
| 32 | **已移植**。MLog 独立 row slice/shallow datum 及同步消费引用生命周期保留。 | **已保留**。`TestMLogTrackedReferenceTypes` 的 TEXT/VARBINARY/decimal INSERT/UPDATE/DDL 后读取在；source 最终禁止的 JSON/BLOB fixture 不复活。 |
| 33 | **部分，R2**。MIN/MAX full-update lookup、TiFlash hint、NULL-key join 和 dependency layout 在；入口组装仍有大幅改写。 | **已保留核心场景**。full-update plan/tree、commit-TS window、nullable join/dependency 与 FastMinMax SQL 子测试在。 |
| 34 | **已移植 service 主体**。`pkg/mvservice`、domain/server/session、task executor/hash-ring/backpressure/生命周期已由 #72498/#72502/#72540 合入，不能再写待 PR7。 | **已保留并有扩展**。task/service/owner/history/alert 等最终 suites 在；timer 包含 goroutine 排队时间等 master 修复见 B4，dashboard 缺项另见 #102。 |
| 35 | **已移植；C1 已关闭**。merge/writer、复合 group-key buffer 修复 #72128 已在；#72490 恢复 typed touched-row/chunk helper 原算法，保留明确修复。 | **T2 已关闭，T3 仍部分缺失**。operator/nullable/enum-set、AppendCellRange/HasNull 及复合 key 断言在；stats String/Merge/Clone 的详细断言缩减。 |
| 36 | **执行器 builder 已移植；planner 入口 R2**。`mview_delta_merge_agg_builder.go` 的重算 reader/sink 组装在，不能继续标 runtime 未合入。 | **已保留核心场景**。planner tree/window、FastMinMax SQL 及 builder/session 接入存在。 |
| 37 | **已移植最终命名**。最终 mview planner/operator 路径、plan names 和 handle layout 在。 | **已保留**。`TestMViewBuildResultHandleCols` 及 COUNT/SUM/MIN/MAX 推导 suites 在；rename 本身无额外行为。 |
| 38 | **DDL 主体已移植；purge 总流程 R1**。supporting-index guard、effective table、ALTER/tracker、IMMEDIATE/显式事务限制保留。 | **部分，T17/T23/T28**。并发 guard/vector/tracked-column/purge transaction 场景在，但 supporting-index、关联表 DDL 拒绝及 DDLBasic 多个子场景被删。 |
| 39 | **不纳入**。OWNERS 同步。 | **不适用**。无 MV 测试。 |
| 40 | **已移植**。SHOW CREATE MV/MLog parser/plan/executor、格式化及 IMMEDIATE 拒绝在 #72430。 | **已保留**。source SHOW CREATE helpers 合并进 `TestShowMaterializedView`，最终输出顺序/attributes/错误断言均在。 |

## 第 41 到第 50 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 41 | **已移植；允许明确 master 修复**。slow-log restore、step timing、scan rows 接入；slow-log overhead 排除实际 data execution 的计时调整保留。 | **部分，T24**。method/manual-auto/COMPLETE/step SQL 断言在；slow-log helper 换成 COMPLETE fixture 后丢失 FAST/非零内部 TSO 隐藏及无关 SELECT 场景。 |
| 42 | **已有通用 prerequisite**。signed int/decimal unary-minus flen 规则在。 | **已保留**。`TestUnaryMinusDecimalRetTypeFlen` 与 `TestUnaryMinusIntRetTypeFlen` 存在。 |
| 43 | **已移植**。unsigned commit-TS/job ID/system-table/runtime/service 字段在。 | **部分，T7/T9**。refresh/service SQL 场景在；隐藏 commit-TS schema col 的 unsigned LONG LONG 专项及 bootstrap unsigned 断言缺失。 |
| 44 | **已移植**。maintenance quota sysvar 及 CREATE/purge/refresh/service capture/apply/restore 已完整接入。 | **部分，T6**。purge caller/failure、refresh SessionVars 与 service global apply 测试在；SQL 默认值/session/global/新 session 继承未迁移。 |
| 45 | **已移植**。CREATE MV/MLog key flags、列校验、TS/ID 和 worker/helper 修复在。 | **已保留专项**。ColumnKeyFlag、ColumnFlags、read-TS alignment 等在；原 DDLBasic 未完整迁移部分见 T28，不能借专项测试把整组标全。 |
| 46 | **已移植**。ALTER notifier 构造/发布、domain 注册和 service DDL 消费闭环在。 | **已保留**。`TestMVAlterEventConstructors`、tracked-column 及 RegisterMVServiceBootstrapAndDDLHandler 场景在。 |
| 47 | **已移植**。refresh session 接 stats/index-usage collector；采用 master stats API，collector 生命周期保留。 | **已保留**。FastUpdatesStatsModifyCount 及 FastMinMax 中 separate-index 结果断言在。 |
| 48 | **已移植**。SHOW MV/MLog AST/plan/reader/executor 已合入。 | **已保留**。SHOW 输出、权限、remain logs/wait purge 等 source helpers 在目标 `TestShowMaterializedView` 中；已核对函数体，非只查名称。 |
| 49 | **已移植**。attributes parser/DDL/job args/metadata、SHOW 和 service alert handler 均在。 | **部分，T28**。合法 attributes、ALTER 非法值、job args/notifier、SHOW/service 场景在；原 CREATE 的非法值、zero/default metadata 断言未全保留。 |
| 50 | **已移植**。多 MLog job IDs、DROP DATABASE 清理、CREATE import options、refresh session/failure 修复在。 | **部分，T6/T9**。import SQL/thread/quota、job IDs、DROP cleanup、refresh SessionVars 有断言；import-quota SQL sysvar 与 schema 类型直接断言未全保留。 |

## 第 51 到第 60 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 51 | **部分，R2；C4 已关闭**。MAX/MIN_COUNT、DRY RUN/PROFILE、observe/steps 已合入；unary-minus TiFlash allowlist #72107 已补，planner 入口仍有改写。 | **部分，T3/T5**。pushdown UnaryMinus、dry-run current DB/危险窗口/no-op、profile/ASYNC 场景在；详细 stats 和直接 AST 边界缺项保留。 |
| 52 | **已移植**。execution vars 的 CREATE/purge/refresh/service capture/apply/restore 及 global 获取在。 | **已保留 runtime 场景**。purge caller、refresh manual/internal/逐变量失败恢复、service fake-global 场景在；SQL sysvar 继承仍不能由这些测试替代（T6）。 |
| 53 | **主体已移植；入口 R2**。nullable/duplicate COUNT、MIN/MAX optional-count planner/operator 与 runtime 在；#72581 的 BIT 编码修复可保留。 | **已保留本条场景**。nullable CREATE、duplicate-count planner/operator 与 FastMinMax SQL 结果在；额外 BIT 回归存在。 |
| 54 | **部分，R3；C3 已关闭**。shadow/cutover CAS、依赖、schema diff、GC 和 out-of-place orchestration 在；cutover validation 有实质重写。#72099 补稳定 advisory owner/早期 cleanup。 | **部分，T8/T12/T13**。cutover rollback/publish/CAS、完整 SQL、无 ProcessInfo owner 在；INSERT conflict 泄漏、保留新 placement bundle、两种 storage-name 分支缺失。 |
| 55 | **已移植**。duplicate MLog columns、partition/exchange/remove-partition guards 和 service newest-DDL-ID 去重在。 | **已保留**。duplicate-column、PartitionDDLConstraints、event/worker 与 MVS restart 去重测试在。 |
| 56 | **已有通用 prerequisite，按 non-MV audit 复用**。FULL OUTER parser/planner/hash join/MPP/PB 在；不是新证明 source rule 原实现已 port。 | **相关场景已有**。oracle/spill/cost/sysvar/integration 在；四种 join-type 转换改由 predicate-pushdown 测试覆盖，不恢复已不存在的 rule flag。 |
| 57 | **主体已移植，入口 R2**。complete-delta layout/NULL-safe diff、touched/sink/builder、ForUpdateTS 均在；#72490 恢复 typed touched 比较。 | **部分，T3/T5**。nullable/common-handle/NULL-EQ、sink validation 和 CompleteDeltaApply SQL/rollback 在；stats 及直接 AST Restore 边界缩减。 |
| 58 | **已移植**。purge/refresh cancel 分派、request/controller/watcher/heartbeat、schedule NULL 及 service backoff 在。 | **已保留 runtime 场景**。Cancellation helper 覆盖 refresh watcher、not-running、finish 前后停止；purge 权限/状态和 service cancel/backoff 在，schema 断言另有 T9。 |
| 59 | **主体已移植；planner 入口 R2**。FAST AS OF 的 bounded plan、target snapshot、window/GC/history 与 provided-table partition helper 在。 | **已保留**。FastAsOfTimestamp、GCSafePoint、MinMaxTargetSnapshot 包含 target data/schema、count/sum 免旧 GC、MIN/MAX 拒绝等原场景。 |
| 60 | **已移植**。initial-build/ALTER privilege、session vars snapshot/fallback、out-of-place/dry-run/service best-effort 在。 | **已保留 runtime 场景**。SessionVars 覆盖 manual/internal、逐变量失败、out-of-place、dry-run、initial build；helper 的 maintenance flag 直接断言仍有 T10。 |

## 第 61 到第 70 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 61 | **部分，C6**。Building/Ready 读保护、child maintenance、refresh ready-check 及 COMPLETE subtype 在；auto-analyze scan skip/queued-job retry gates 缺失。 | **部分，T5/T15/T16**。PauseAndResume 保留读取并增加写拒绝；缺 ANALYZE/ADMIN/REFRESH 半组、直接 Ready/legacy/deferred/bypass 断言及两项 auto-analyze 回归。 |
| 62 | **已移植**。最终 history schema、purge/refresh statement result/affected rows 与时间转换在。 | **部分，T4/T9**。StatementResult 合并保留所有 method/result 场景；historyTime UTC/+08/微秒截断及 schema 精度直接断言缺失。 |
| 63 | **MV hunk 已移植；其余排除**。BR 五张 maintenance tables unrecoverable 在；通用缓存/表达式优化按 non-MV 处理。 | **已保留**。`TestMVSystemTablesAreUnrecoverable` 枚举五张表。 |
| 64 | **已移植**。DDL alert cleanup、purge/refresh heartbeat、service stale-running/history reconcile/GC 在。 | **已保留 runtime 场景**。DROP/ALTER/failure、History heartbeat、service stale/owner/history-GC 在；bootstrap 断言缩减见 T9。 |
| 65 | **已移植**。独立 maintenance isolation engines、capture/apply/restore、CREATE/purge/refresh/service 使用在。 | **部分，T6**。runtime caller/default/failure fallback 场景在；SQL session/global 与普通 isolation vars 独立性、新 session 继承缺失。 |
| 66 | **已移植**。REFRESH_COMMIT_TSO schema/index、实际事务 commit TSO 捕获、info/history 写入在。 | **runtime 已保留，schema T9**。StatementResult 合并保留 FAST/AS OF/DELTA APPLY/IN PLACE/OUT OF PLACE snapshot-match；不能再标 runtime 未合入。 |
| 67 | **核心事务语义在；总流程 R1**。每批独立 delete 事务、部分成功后的 checkpoint/history、cancel/failure 路径存在，purge session/收尾结构仍为改写。 | **已保留专项**。DeleteErrorAfterPartialSuccess、ManualCancelAfterPartialSuccess、BeginFailure 均检查 checkpoint/history。 |
| 68 | **throttle/scheduler 已移植；purge 总流程 R1**。adaptive batch、deadline/sleep fallback、controller/sysvars 与 service 接入在。 | **已保留**。fallback/adaptive/controller/deadline 与 service global/backoff 在；四种跨 row-range throttle 断言移到 `utils_test.go`，不算丢失。 |
| 69 | **已移植**。failed-alert attributes、DDL 保留/清理、refresh failure/fallback 报警和 MVS reconcile 在。 | **runtime 已保留，T9/T28**。disable-schedule 保留 failed alert、failure-report 顺序、BuildResolvedMVRefreshAlertSQL/service 场景在；schema 类型和 CREATE attributes 部分断言缺失。 |
| 70 | **核心逻辑已移植；purge 总流程 R1**。cutoff fence 单调性和 FAST 对危险 purge history 的拒绝都在。 | **已保留 runtime 场景**。purge backward fence、FastDryRun hazardous/no-op、FAST refresh hazardous history 断言在；bootstrap 断言见 T9。 |

## 第 71 到第 80 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 71 | **已移植**。TRUNCATE/worker dependency guard 和 IMPORT base table with MLog 拒绝在。 | **已保留**。ImportIntoRejectsMaterializedViewLogBaseTable、TRUNCATE dependency 与 worker 重查在。 |
| 72 | **已移植权限闭环**。OPERATE VIEW、CREATE/DROP MLog、purge/cancel、refresh/cancel、SHOW/COMPARE/GRANT 全部有入口。 | **场景已有，integration 层 T27**。`mview_privilege.test/.result` 已存在，旧“整份缺失”结论撤销；SHOW PRIVILEGES 和五组 COMPARE fixture 从 integration 移除，Go suites 有对应场景。 |
| 73 | **已移植**。related field-type、MLog ADD COLUMN/tracker/job args、shadow 读保护与 FAST no-op 在。 | **部分，T18/T28**。四个内部 column-usage/type 回归及默认值/权限在；ALTER 后 tracked/untracked/generated DML、建新 MV 后 FAST 结果与 alias/多 MV metadata 同步断言缺失。 |
| 74 | **已移植**。manual cancel 后 task result/backoff/requeue 语义在 service。 | **已保留**。`TestMVServiceTaskResult` 的取消/重排断言在。 |
| 75 | **已移植**。TIDB_MVIEWS/TIDB_MLOGS/依赖虚表及 reader/extractor 已在 #72442；MLog 名称约束在。 | **虚表场景保留，名称 T21**。两个 infoschema helpers 在目标 dispatcher，断言保留；长名 SQL 生命周期和 Unicode 冲突后成功重试仍缺。 |
| 76 | **已移植**。ALERT ROWS parser/metadata/DDL、SHOW 与 service accumulation owner/metrics 扫描在。 | **已保留并加强**。owner/strict-greater-than/error-preserve/async 断言在，master 增加 error/panic 后延迟下次扫描，见 B4。 |
| 77 | **已移植，另有明确修复 B3**。COMPARE parser/planner/executor/snapshot/output writer 在 #72472；output timestamp 按 definition timezone 写入并恢复 session。 | **十组 Go 场景保留，integration T27**。权限优先级、snapshot cutover、batch/failure cleanup、NULL/empty helpers 内容保留；新增 output timezone 回归，但原权限 integration fixture 有删减。 |
| 78 | **已移植**。清除 purge schedule 的 parser/metadata、NEXT_PURGE_UNIX_SECONDS=NULL 和 runtime 在。 | **已保留**。CREATE/ALTER purge clause、清空 schedule 和 internal START-only/no-schedule SQL 在；不恢复旧 NextTime 字段。 |
| 79 | **已移植**。COMPARE baseQuery build-side 选择在最终 `buildCompareMaterializedView` 流程。 | **无独立测试新增**。#77 的 COMPARE 结果场景在；不虚构单独的 build-side plan 断言。 |
| 80 | **最终命名/执行器组装已移植；R2**。current/recomputed layout、complete-delta builder 在，planner 入口仍有结构改写。 | **已保留核心场景**。layout/nullable/handle projection、sink mapping/Open 与 CompleteDeltaApply SQL 在。 |

## 第 81 到第 90 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 81 | **已移植**。CREATE/ALTER/tracker 共用 supported-column 判断，禁止 JSON/binary BLOB、允许 TEXT/VARBINARY。 | **主体已保留，ALTER 后 DML T18**。unsupported-column/reference-type 场景在；生成列经 ADD COLUMN 后写日志的后半段未保留。 |
| 82 | **已移植**。partitioned-base 拒绝、worker/tracker/cancel-error 分类及 partition/exchange guards 在。 | **已保留**。CreateMaterializedViewOnPartitionTable 双路径、dependency/worker 回归在。 |
| 83 | **已移植**。MV unique/primary 拒绝、普通 index 允许在。 | **已保留**。CreateUniqueIndexOnMaterializedView 的三类拒绝/普通 index，及 CompleteDeltaApplyRollbackOnError 合并后的 SQL 断言在。 |
| 84 | **已移植**。CREATE MLog executor/worker/tracker 拒绝 MV/MLog/shadow target。 | **旧缺项已关闭**。`TestMaterializedViewRefreshCompleteOutOfPlaceShadowTableProtected` 明确在真实 shadow 上 CREATE MLog 并检查 BASE TABLE 错误，不再记为 T5。 |
| 85 | **仅测试 commit，相关代码在**。generated-column DML/DDL 采用 source 最终规则。 | **主体已保留，T18**。stored/virtual INSERT/UPDATE/only-generated 与 CREATE/rename 约束在；经 ALTER MLog ADD generated column 后 UPDATE 的直接场景仍缺。 |
| 86 | **已移植**。system/memory DB、view/sequence/temporary/MV/MLog 统一 BASE TABLE 检查在，shadow 见 #84。 | **部分，T22**。non-base suite 有系统对象/global temp 等；local temporary 被删，若干完整 db/table 错误断言弱化为 substring。 |
| 87 | **已移植**。DROP IF EXISTS 的 note/no-op、base/object 错误区分及 job flags 在。 | **已保留**。DropMaterializedViewIfExists 与 parser Restore cases 的 note/no-op/错误对象断言在。 |
| 88 | **已移植**。`FieldTypeForMaterializedViewLogColumn` 共用，TEXT family 不错误扩宽。 | **类型断言在，DML T18**。PreservesTextColumnTypes/ADD COLUMN metadata 检查在；原 ADD COLUMN 后 DATE/TEXT 日志值断言未完整保留。 |
| 89 | **已移植**。comment-only MODIFY 放行，实际类型修改仍受依赖约束。 | **已保留专项**。CommentOnlyBaseColumnModify 建真实依赖并查 column_comment；不因原 DDLBasic 名称消失判本条遗漏。 |
| 90 | **代码已移植**。CREATE/ALTER COMMENT 的 strict/non-strict 长度、warning、截断逻辑在。 | **部分，T19**。同名测试只留下 CREATE；ALTER 的 max/strict failure/non-strict warning+截断断言未迁移，撤销旧“完整覆盖”结论。 |

## 第 91 到第 100 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 91 | **已移植**。internal maintenance slow-log skip 与 tidb_mlog_log_slow_purge 接入 pipeline。 | **已保留**。`TestSlowQueryMisc` 检查 manual/restricted purge 的 OFF/ON 行为。 |
| 92 | **已移植**。service orphan history cleanup 的 FORCE INDEX SQL 在。 | **已保留**。`TestServerHelperPurgeMVHistoryBeforeTSOUsesStatusIndexHints` 断言最终索引提示。 |
| 93 | **lookup 算法已移植；入口 R2**。supporting-index 集合、SetFullUpdateLookupIndexHint/USE INDEX template 在。 | **已保留本条场景**。all-supporting-index unit 与 FastMinMax supporting-index SQL 在；DDL supporting-index 删减另见 T17。 |
| 94 | **已移植，stats API 适配**。purge delete 通过 AttachStatsCollector 记录 delta，source collector 生命周期目的保留。 | **已保留**。PurgeMaterializedViewLogUpdatesStatsMetaRowCount 检查 count/modify_count。 |
| 95 | **已移植；不等于 C6 已关闭**。#72582 的 priority queue 跳过 MLog 并移除已排队任务在；Building MV 另缺独立 gate。 | **已保留**。`TestAnalysisPriorityQueueSkipsMaterializedViewLog` 在；它不检查 not-ready MV。 |
| 96 | **window/stats 算法已移植；入口 R2**。retained/inclusive/disjoint bounds、selectivity/context、dry-run integrity 获取在。 | **已保留**。ExtractMLogCommitTSFilterBound、window/ForUpdateTS EXPLAIN、normalize/disjoint 及 FastDryRun hazardous/no-op 场景在。 |
| 97 | **已移植并有明确加强 B4**。#72582 MLog analyze scheduler/sysvar/dedup、stats-session restore 在；run gate/lock/window/shutdown 及 ratio 再检查可保留。 | **已保留并扩展**。NeedAnalyzeMLog、scan/dedup/error/no-immediate-retry、global ratio/stats-vars restore 及取消/锁/窗口场景在。 |
| 98 | **已移植**。purge/refresh cancel 统一用户错误、权限/precheck 优先级与 not-running 分派在。 | **已保留**。purge precheck 测试和 refresh Cancellation helpers 的权限/not-running/stop-after-finish 在。 |
| 99 | **已移植**。CREATE MLog 权限错误使用 base-table 名/CREATE MATERIALIZED VIEW LOG，不暴露内部名。 | **unit 与 integration 本条场景均在**。CreateMaterializedViewLogPrivilege 及 `mview_privilege.test/.result` 的对应错误断言已存在；其他 integration 删减单列 T27。 |
| 100 | **已移植；purge session 分工 R1**。InternalSQLScanUserTable 设置/恢复、predicate-column 收集与 CREATE/refresh/dry-run 应用在。 | **部分，T10**。predicate-column/window EXPLAIN 与 refresh SessionVars 在；CREATE helper 专项缺 maintenance flag 的进入/恢复断言。 |

## 第 101 到第 110 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 101 | **已移植**。service component/type labels、reporter 和相应 Task/Goroutine/Operation Grafana queries 在。 | **无独立 Go 测试新增**。静态核对 metric 注册/调用/queries；不能据此称 dashboard 已运行验证，#102 新面板另见 C7。 |
| 102 | **部分，R1/C7**。row-ID range/adaptive delete、duration schema/runtime/service reporters 在；count 复用 delete session 改变 TiFlash threads，MV Refresh Duration 面板缺失。 | **部分，T9/T25/T26**。refresh schedule-duration 与 service 断言在；purge 原 delete-only 预期被反向替换，SQL builder 两组边界断言及 schema 精度检查缺失。 |
| 103 | **已移植**。global task snapshot、refresh-alert collect/dedup/sync/stale-metadata/owner/metrics 在。 | **已保留**。最终 service checker/reconciliation 场景在；source 已删除的 KeysScanGlobalTasks 名称不算缺项。 |
| 104 | **MV hunks 已移植；C5 已关闭**。purge service 禁用 chunk RPC 与 root MPP 尊重 EnableChunkRPC 的 #72099 适配在；build/mirror/timeout drift 按 non-MV 排除。 | **已保留**。TestSetMPPEncodeTypeRespectsChunkRPCSetting 全部 root/non-root/setting 分支及 service EnableChunkRPC 断言在。 |
| 105 | **已移植**。sorted server-ID cache、双 hash-ring alert owner 和 lifecycle 在。 | **已保留**。owner/global-task/sorted-ring 场景在目标 service suites。 |
| 106 | **已移植，版本号/框架适配**。source 合并后 classic 221/222 对应 master 316/317；classic 五表 migration 和权限 migration 均在，同时支持 NextGen registry。 | **部分，T9**。classic upgrade316 测试先删五表、从315升级再检查；缺的是详细 schema 断言，不能误记成 classic migration 不存在。 |
| 107 | **最终 UTC/SQL-mode 逻辑已移植，A1**。按 #114 最终 source 比较 DDL/purge/refresh/service；不恢复中间 saved-timezone 方案。 | **部分，T4/T20**。runtime internal-only/START-only/no-schedule/UTC 在；invalid-date 保存 SQL-mode 边界及 ALTER 真正使用 PIPES_AS_CONCAT 的表达式断言缺失。 |
| 108 | **已移植**。LAST_SUCCESS_REFRESH_END_UNIX_SECONDS schema、CREATE/cutover/refresh 持久化及 schedule-duration 使用在。 | **runtime 已保留，schema T9**。previous-success-time、schedule-duration、refresh success info/history 与 cutover metadata 在；bootstrap 类型断言缩减。 |
| 109 | **已移植最终命名**。DDL/purge/refresh/service 的时间列、heartbeat/history/index SQL 引用均更新。 | **runtime 已保留，schema T9**。history/finalize/heartbeat/alert-service 最终断言在；bootstrap index 列/类型精确检查未全保留。 |
| 110 | **部分，继承各实际缺项**。最终 MView/MLog 命名已落地所有 slices，但 C6 的 not-ready analyze helper 在 rename 后最终树仍缺；R1/R2/R3 不能靠命名一致关闭。 | **最终 suites 已落地，仍有 T3/T5/T6/T9/T15/T16/T28**。按本表各 owner 的具体缺失断言处理；rename 不作为独立功能测试。 |

## 第 111 到第 120 个 Commit 的测试复核

按 source 最终保留的改动分别核对实现和断言；均对应固定 master 快照。

| # | Source 最终逻辑 / Master 代码核对 | Source 测试 / Master 断言核对 |
| ---: | --- | --- |
| 111 | **artifact 缺失 C8**。`docs/note/materialized_view/mv_system_tables_rebuild.sql` 未在目标树出现；bootstrap 代码存在不等于该运维脚本已 port。 | **无测试变更**。静态 artifact 缺失，不推断 runtime bootstrap 有缺陷，也未执行 destructive rebuild SQL。 |
| 112 | **已移植**。CREATE options/REFRESH/ATTRIBUTES 顺序在 AST/grammar 与 SHOW CREATE formatter 中一致。 | **已保留**。CreateOptionOrder、非法顺序/Restore、SHOW CREATE 最终输出顺序均在。 |
| 113 | **最终删除状态已保留**。source/master 均无 NEVER REFRESH method/AST/DDL 分支。 | **无新增测试变更**。不恢复 source 已删除的语法。 |
| 114 | **多数修复已移植，R3/A1 单列**。parser/DDL rollback、UTC/SQL-mode、definition precision、shadow cleanup、cutover dependency/atomicity、infoschema v2、bound/runtime/service 在；validation 改写不能按修复整体豁免。 | **部分，T4/T5/T10/T11/T12/T20/T28**。precision metadata=9/应用恢复、cutover rollback/publish/CAS 在；schema-diff rollback、placement 保留新 bundle、schedule/AST/DDLBasic 等精确断言仍缺。 |
| 115 | **已移植**。mustRollbackTxnOnError、schema-version retry、重开事务 job metadata 重查、infoschema v2 affected-table 保留在 #71816。 | **已保留核心新增回归**。八个 CREATE/DROP/rollback/notify 并发失败回归在；BR action-ID fixture 按 master model 编号适配。 |
| 116–120 | 固定 source 范围不存在对应 commit。 | 无对应 source 测试。 |

## 全部 Commit 审计

每条具体实现与断言见上方同编号；保留原 source commit 清单与 port 归属。

| # | Source commit | 日期 | Subject | 主要最终语义 | Port 归属 | Master 对照和结论 |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | `f3af7d5a83fd` | 2026-02-05 | `pkg/meta: store mview/mlog dependency metadata in TableInfo (#66023)` | 在 `TableInfo` 中加入 base table、MV、MLog 之间的反向依赖元数据。 | PR2b-create/drop/alter；元数据基础 | 代码：已移植，metadata API 适配；测试：无独立测试变更。详见上文 #1。 |
| 2 | `e0d58ec5ca8c` | 2026-02-06 | `pkg/session: bootstrap mview refresh/purge system tables (#66024)` | 首次 bootstrap MV refresh/purge 信息、历史和 alert 相关系统表，并补充 bootstrap 测试及 restore 行为。 | PR1 | 代码：已移植；测试：部分，T9。关联 T9；详见上文 #2。 |
| 3 | `96a16b284bc8` | 2026-02-06 | `*: add materialized view DDL syntax (#66022)` | 增加 CREATE/DROP/ALTER MV/MLog 语法、AST、Restore、关键字和 parser 测试，同时引入早期 `REFRESH MATERIALIZED VIEW` 语法。 | PR2a；REFRESH 部分属于 PR5 | 代码：已移植；测试：部分，T5。关联 T5；详见上文 #3。 |
| 4 | `d56cc857183c` | 2026-02-07 | `executor: support CREATE MATERIALIZED VIEW LOG (#66080)` | 实现 CREATE MLog 的 DDL dispatcher、job args、schema tracker、内部表构造和测试。 | PR2b-create | 代码：已移植，DDL 接口适配；测试：部分，T21。关联 T21；详见上文 #4。 |
| 5 | `6fb36360b1cd` | 2026-02-09 | `*: Cherry-pick pr-66089 into materialized_view branch (#66163)` | 混合 branch 同步内容，包含 `_tidb_commit_ts` extra column、planner/sample/point-get 适配、notifier 行为调整以及大量通用测试结果变化。 | 混合 commit；commit-ts prerequisite；非 MV 测试 drift | 代码：部分，C2；测试：C2 无直接目标回归证据。关联 C2；详见上文 #5。 |
| 6 | `1587a903f980` | 2026-02-12 | `table,executor: sync base-table DML to materialized-view log tables (#66204)` | base-table INSERT/UPDATE/DELETE/REPLACE/LOAD DATA 等 DML 生成并写入 MLog，新增 MLog table 实现和 writetest/integration test。 | PR3 | 代码：已移植；测试：已保留。详见上文 #6。 |
| 7 | `b476f8f72604` | 2026-02-13 | `table, sessionctx: fix bug where mlog consumes reserved row IDs from base table (#66246)` | 修正 MLog 写入对 base table 保留 row ID/handle 的影响。 | PR3 | 代码：已移植；测试：已保留。详见上文 #7。 |
| 8 | `581718f52f96` | 2026-02-14 | `planner, executor: support nulleq for IndexJoin and IndexHashJoin (#66017) (#66199)` | 为通用 IndexJoin/IndexHashJoin 增加 NULL-safe equality 支持。 | 通用 prerequisite | 代码：已有通用 prerequisite；测试：已保留相关场景。详见上文 #8。 |
| 9 | `6ac0a2fb86f5` | 2026-02-14 | `*: add aggregate function sum_int (#66085)` | 增加 `SUM_INT` 的 parser、expression、executor、tipb pushdown 和测试。 | 通用 prerequisite；PR5/快速刷新只适配 | 代码：已有通用 prerequisite；测试：主体场景已有。关联 T1；详见上文 #9。 |
| 10 | `f7f7e7f85cda` | 2026-02-15 | `pkg/ddl: include base table id for create mlog MDL table ids (#66266)` | CREATE MLog 的 metadata lock involving tables 加入 base table，保证相关 DDL 串行化。 | PR2b-create | 代码：已移植；测试：已保留。详见上文 #10。 |
| 11 | `a76fec7f4d03` | 2026-02-15 | `executor: support CREATE/DROP MATERIALIZED VIEW (#66083)` | 初始 CREATE/DROP MV/MLog DDL worker、依赖元数据更新、清理和测试。 | 拆分到 PR2b-create、PR2b-drop | 代码：已移植 DDL 主体；测试：部分，T10/T28。关联 T10/T28；详见上文 #11。 |
| 12 | `800fad39c987` | 2026-02-19 | `*: sync AGENTS/.gitignore and Bazel helper updates from master (#66308)` | 同步 AGENTS、`.gitignore`、Makefile/build helper 和开发流程内容。 | 非 MV | 代码：不纳入；测试：不适用。详见上文 #12。 |
| 13 | `76ef4adbc110` | 2026-02-21 | `ddl, executor: fix CREATE MATERIALIZED VIEW validation (#66316)` | 修正 CREATE MV 的对象类型、查询和依赖 validation。 | PR2b-create | 代码：已移植；测试：部分，T28。关联 T28；详见上文 #13。 |
| 14 | `4bd68061a54b` | 2026-02-22 | `*: backport _tidb_commit_ts support for unistore cop request (#66325)` | 为 unistore/cop/rowcodec 等路径传递 commit timestamp，并加入独立测试。 | commit-ts prerequisite；PR4/PR5 使用时适配 | 代码：已存在底层 prerequisite；测试：MV 所需底层测试存在。关联 C2；详见上文 #14。 |
| 15 | `0f22124d57d0` | 2026-02-24 | `ddl: allow SET TIFLASH REPLICA on MV-dependent base tables (#66328)` | 放宽 MV 相关表的 TiFlash replica placement DDL，同时保留其他 base-table DDL 约束。 | PR2b-create/drop/alter | 代码：已移植；测试：已保留本条 base-table 回归。关联 T28；详见上文 #15。 |
| 16 | `7fe4ee1528fa` | 2026-02-24 | `ddl: prewrite mv refresh info before init build (#66330)` | 在 MV initial build 前预写 refresh-info 行，避免 build 阶段缺少系统表状态。 | PR2b-create；部分涉及 PR5 initial build | 代码：已移植；测试：已保留。详见上文 #16。 |
| 17 | `a2036935163f` | 2026-02-25 | `pkg/session, pkg/ddl, pkg/executor: update mview/mlog system table schemas (#66341)` | 更新 MV/MLog 系统表字段、类型及 DDL/executor 引用。 | PR1；PR2b 对最终 schema 的引用 | 代码：已移植最终 schema；测试：部分，T9。关联 T9；详见上文 #17。 |
| 18 | `049564311187` | 2026-02-25 | `expression/aggregation: support sum_int in NewDistAggFunc (#66360)` | 为 `SUM_INT` 增加分布式聚合构造。 | 通用 prerequisite；PR5 适配 | 代码：代码已移植；测试：缺项 T1。关联 T1；详见上文 #18。 |
| 19 | `d5ca6127eea7` | 2026-02-25 | `session, executor: update mview system table schema (#66382)` | 继续调整 MV 系统表字段和初始化逻辑。 | PR1；PR2b/后续 runtime 引用 | 代码：已移植最终 schema；测试：部分，T9。关联 T9；详见上文 #19。 |
| 20 | `78096e182edc` | 2026-02-25 | `*: add aggregate function min_count/max_count (#66250)` | 增加 `MIN_COUNT`/`MAX_COUNT` 及 expression、executor、tipb 和测试。 | 通用 prerequisite；PR5 快速刷新适配 | 代码：已有通用 prerequisite；测试：主体场景已有。详见上文 #20。 |
| 21 | `e8829b004dcc` | 2026-02-26 | `pkg/ddl, pkg/executor: validate MV START WITH/NEXT expr type and remove defaults (#66383)` | 校验 schedule expression 类型，移除不明确的默认行为。 | PR2b-create/alter；PR5/PR7 schedule runtime | 代码：已移植最终语义，局部 helper 调整 A1；测试：部分，T20。关联 T20；详见上文 #21。 |
| 22 | `a36d83807816` | 2026-02-27 | `*: refine AGENTS.md style and wording guidance (#66556) (#66558)` | 调整 agent 文档风格和措辞。 | 非 MV | 代码：不纳入；测试：不适用。详见上文 #22。 |
| 23 | `b06308c62738` | 2026-02-27 | `ddl, executor: derive MV NEXT_TIME in create flow with safe eval session (#66555)` | CREATE MV 时使用安全 eval session 求值 `NEXT_TIME`/schedule。 | PR2b-create；PR5/PR7 运行时 schedule | 代码：已移植最终语义，A1；测试：已保留核心场景。关联 T4/T20；详见上文 #23。 |
| 24 | `58bba0d7a9cd` | 2026-02-27 | `pkg/ddl, pkg/parser, pkg/executor: support pre-split options for create materialized view (#66286)` | CREATE MV 支持 pre-split/scatter 等建表选项，并传入 physical table build。 | PR2b-create | 代码：已移植；测试：已保留。详见上文 #24。 |
| 25 | `c0cdf05369c5` | 2026-02-28 | `*: basic mv refresh support (#66385)` | 新增基本 refresh executor、refresh SQL、initial build 相关逻辑和测试。 | PR5；部分 CREATE initial-build prerequisite 已随 PR2b-create | 代码：已移植 refresh runtime 主体；测试：部分，T5/T14。关联 R2/T5/T14；详见上文 #25。 |
| 26 | `3b8b6e4a7230` | 2026-02-28 | `planner, executor, sessionctx: reject explicit dml on mview / mlog tables (#66396)` | 拦截用户直接对 MV/MLog 的 DML，并覆盖 planner fast path 和 integration test。 | PR3 | 代码：已移植；测试：已保留。详见上文 #26。 |
| 27 | `e5fdd577b2a7` | 2026-03-02 | `*: Cherry pick topsql adding network field to feature branch (#66290)` | TopSQL 增加 network in/out bytes 统计及相关字段。 | 非 MV | 代码：不纳入 MV port；测试：不计为 MV 缺项。详见上文 #27。 |
| 28 | `85f88a3ff5b3` | 2026-03-10 | `planner: block UPDATE/DELETE on mview/mlog via point-get fast path (#66805)` | 修正 point-get fast path 绕过 MV/MLog DML 禁止检查的问题。 | PR3 | 代码：已移植；测试：已保留。详见上文 #28。 |
| 29 | `7185487da7dd` | 2026-03-12 | `importer: add disk quota support for IMPORT INTO SELECT FROM (#66902)` | 通用 IMPORT INTO SELECT 的 disk quota 支持，同时被 MV initial build 的 import option 使用。 | importer prerequisite；PR2b-create 的实际依赖 | 代码：已移植 prerequisite；测试：已保留。详见上文 #29。 |
| 30 | `6f309157d091` | 2026-03-17 | `*: support mv log purge (#66660)` | 实现 MLog purge executor、purge schedule、purge info、DDL dispatcher 和基础测试。 | PR4；部分 DDL prerequisite 已随 PR2b-create/drop/alter | 代码：部分，R1；测试：purge 主体场景在，T25/T26 有差异。关联 R1/T25/T26；详见上文 #30。 |
| 31 | `c447836a4989` | 2026-03-19 | `planner: support fast refresh mview for count/sum (#66595)` | 增加 count/sum fast refresh planner、MV merge plan 和 planner casetest。 | PR5；S9 fast refresh planner | 代码：部分，R2；测试：planner/SQL 主体已保留。关联 R2；详见上文 #31。 |
| 32 | `ce48aec35f24` | 2026-03-25 | `table, executor: avoid deep-copying tracked MLog datums (#67160)` | 优化 MLog tracked-column datum 复制，降低 base-table DML capture 的开销。 | PR3 | 代码：已移植；测试：已保留。详见上文 #32。 |
| 33 | `bb1f16b69cb5` | 2026-04-10 | `planner: add min/max full-update lookup for mv refresh (#67146)` | 为 MIN/MAX fast refresh 增加 full-update lookup 和 planner 支持。 | PR5；S9 | 代码：部分，R2；测试：已保留核心场景。关联 R2；详见上文 #33。 |
| 34 | `2e944507d529` | 2026-04-18 | `mvservice,domain,server,session,metrics: add materialized view service (MVS) framework (#66242)` | 新增 MV service、任务分配、backpressure、metrics、domain/server/session 集成和大量 service 测试。 | PR7 | 代码：已移植 service 主体；测试：已保留并有扩展。详见上文 #34。 |
| 35 | `17ad85353d3e` | 2026-05-21 | `executor: implement MViewDeltaMergeAgg operator (#66326)` | 新增 MViewDeltaMergeAgg executor、merge count/sum/min/max、spill 和测试。 | PR5；S10 | 代码：已移植；C1 已关闭；测试：T2 已关闭，T3 仍部分缺失。关联 C1/T2/T3；详见上文 #35。 |
| 36 | `e3d29c0b773d` | 2026-05-26 | `executor: add mview delta merge agg builder (#68636)` | 将 delta merge aggregate 接入 executor builder、planner 和 refresh 测试。 | PR5；S10 | 代码：执行器 builder 已移植；planner 入口 R2；测试：已保留核心场景。关联 R2；详见上文 #36。 |
| 37 | `72d1670b394f` | 2026-05-27 | `planner/mview: clean up mvmerge naming (#68655)` | 将 `mvmerge` 等内部 planner/operator 名称统一为 mview 命名。 | PR5；S9/S10 命名 refine | 代码：已移植最终命名；测试：已保留。详见上文 #37。 |
| 38 | `adc727df5a8b` | 2026-05-28 | `*: several ddl enhancement for mv (#68649)` | 大量 DDL worker、job args、schema tracker、multi-schema-change、依赖约束和测试增强。 | PR2b-create/drop/alter | 代码：DDL 主体已移植；purge 总流程 R1；测试：部分，T17/T23/T28。关联 R1/T17/T23/T28；详见上文 #38。 |
| 39 | `1cd7b66e493d` | 2026-05-28 | `OWNERS: Auto Sync OWNERS files from community membership (#67717) (#68706)` | 同步 OWNERS 和 OWNERS_ALIASES。 | 非 MV | 代码：不纳入；测试：不适用。详见上文 #39。 |
| 40 | `b21036c39400` | 2026-05-28 | `executor, parser, planner: support SHOW CREATE for materialized views (#68698)` | 增加 SHOW CREATE MV/MLog 的 parser、executor、planner 和测试。 | PR6 | 代码：已移植；测试：已保留。详见上文 #40。 |
| 41 | `d8a6eac392cc` | 2026-05-28 | `executor: several refinements for MV refresh (#68704)` | refresh executor/session 使用、结果处理和测试修正。 | PR5 | 代码：已移植；允许明确 master 修复；测试：部分，T24。关联 T24；详见上文 #41。 |
| 42 | `25e037cb02e4` | 2026-05-29 | `expression: keep unary minus flen for signed int/decimal (#68723)` | 修正通用 unary-minus 的 field length 推导。 | 非 MV prerequisite candidate | 代码：已有通用 prerequisite；测试：已保留。详见上文 #42。 |
| 43 | `edbfd5121f7f` | 2026-05-29 | `executor, mvservice: use unsigned TSO for materialized view metadata (#68719)` | 将 refresh/maintenance metadata 中的 TSO 表示统一为 unsigned 语义。 | PR1 schema；PR5 refresh；PR7 service | 代码：已移植；测试：部分，T7/T9。关联 T7/T9；详见上文 #43。 |
| 44 | `74b10f298fa2` | 2026-05-29 | `executor, sessionctx/variable: add MV maintenance memory quota (#68736)` | 增加 MV maintenance memory quota 变量和 executor/service 使用。 | PR2b-create 的 build option；PR5/PR7 runtime | 代码：已移植；测试：部分，T6。关联 T6；详见上文 #44。 |
| 45 | `f432536c5f34` | 2026-05-29 | `ddl: fix some bug in create mv/ create mv log (#68766)` | 修正 CREATE MV/MLog dispatcher、schema tracker 和 DDL validation。 | PR2b-create | 代码：已移植；测试：已保留专项。关联 T28；详见上文 #45。 |
| 46 | `5f3246d2f28a` | 2026-05-30 | `ddl, mvservice: notify alter MV and MV log events (#68776)` | 增加 ALTER MV/MLog notifier event，并让 service 感知相关变化。 | PR2b-alter；PR7 | 代码：已移植；测试：已保留。详见上文 #46。 |
| 47 | `ffbb0cef41b6` | 2026-05-30 | `executor: fix MV refresh internal-session usage collection (#68778)` | 修正 refresh 使用 pooled/internal session 时的资源统计。 | PR5；PR7 | 代码：已移植；测试：已保留。详见上文 #47。 |
| 48 | `c0f0d1b212b4` | 2026-06-01 | `executor, parser: add show materialized views and show materialized view logs (#68808)` | 增加 SHOW MATERIALIZED VIEWS/MATERIALIZED VIEW LOGS。 | PR6 | 代码：已移植；测试：已保留。详见上文 #48。 |
| 49 | `3cfcfc49d821` | 2026-06-02 | `ddl, parser: support materialized view alert attributes (#68810)` | 支持 MV alert attributes 的 parser、DDL metadata 和 notifier/service 引用。 | PR2b-create/alter；PR7 | 代码：已移植；测试：部分，T28。关联 T28；详见上文 #49。 |
| 50 | `725e4f64ad7d` | 2026-06-02 | `*: backport mv metadata, import options, and refresh support fixes (#68849)` | 混合回灌 MV metadata、IMPORT options、DDL schema/job args 和 refresh 修复。 | 混合 commit；按 hunk 拆到 PR2b-create、PR5 | 代码：已移植；测试：部分，T6/T9。关联 T6/T9；详见上文 #50。 |
| 51 | `c99ca71584bd` | 2026-06-02 | `*: add MV refresh observability and backport related fixes (#68774)` | 增加 refresh observability、结果/metrics/history、expression pushdown 和大量 refresh 测试。 | PR5；部分 PR7；通用 aggregation prerequisite | 代码：部分，R2；C4 已关闭；测试：部分，T3/T5。关联 C4/R2/T3/T5；详见上文 #51。 |
| 52 | `259810a1abf8` | 2026-06-02 | `executor,mvservice: unify MV maintenance session vars (#68877)` | 统一 refresh/purge/service 的 maintenance session variables 和 restore 逻辑。 | PR4/PR5/PR7；PR2b-create helper | 代码：已移植；测试：已保留 runtime 场景。关联 T6；详见上文 #52。 |
| 53 | `bdb304c4d378` | 2026-06-03 | `ddl, executor, planner: backport duplicate count expr support for nullable MV aggs (#68893)` | 修正 nullable MV aggregate 的 duplicate count expression，并更新 planner/executor/DDL 测试。 | PR2b-create validation；PR5 S9/S10 | 代码：主体已移植；入口 R2；测试：已保留本条场景。关联 R2；详见上文 #53。 |
| 54 | `f30199fba937` | 2026-06-03 | `*: support complete refresh out of place (#68894)` | 实现 out-of-place complete refresh、cutover、临时表、DDL worker、notifier 和测试。 | PR5 | 代码：部分，R3；C3 已关闭；测试：部分，T8/T12/T13。关联 C3/R3/T8/T12/T13；详见上文 #54。 |
| 55 | `7a3b5d9443e1` | 2026-06-04 | `ddl, mvservice: backport materialized view maintenance fixes (#68947)` | 混合 DDL worker 和 MV service maintenance 修复。 | PR2b；PR7 | 代码：已移植；测试：已保留。详见上文 #55。 |
| 56 | `6ba9bb6053d6` | 2026-06-05 | `parser, planner, executor, sessionctx: support full outer join (#68919)` | 通用 FULL OUTER JOIN parser/planner/executor/Mpp 支持及测试。 | 通用 prerequisite | 代码：已有通用 prerequisite，按 non-MV audit 复用；测试：相关场景已有。详见上文 #56。 |
| 57 | `3892bda264c2` | 2026-06-10 | `planner, executor, parser, expression: support MV complete delta apply (#68994)` | 实现 complete delta apply、touched rows、delta merge、planner/executor/parser 接入。 | PR5；S9/S10；FULL OUTER JOIN follow-up | 代码：主体已移植，入口 R2；测试：部分，T3/T5。关联 R2/T3/T5；详见上文 #57。 |
| 58 | `a2e127422a1d` | 2026-06-11 | `executor, parser, session, mvservice: support canceling MV jobs (#69102)` | 增加 refresh/purge cancel 语法、状态更新、controller/monitor 和 pooled session 生命周期处理。 | PR5；PR4；PR7；部分 PR2b-alter/schema | 代码：已移植；测试：已保留 runtime 场景。关联 T9；详见上文 #58。 |
| 59 | `c7b499b9bc6b` | 2026-06-12 | `executor, planner, parser, ddl: support bounded MV fast refresh (#69117)` | 增加 bounded fast refresh 语法、planner、executor、DDL metadata 和测试。 | PR5；S9/S10；PR2b shared index helper | 代码：主体已移植；planner 入口 R2；测试：已保留。关联 R2；详见上文 #59。 |
| 60 | `8ecd2a11bb59` | 2026-06-12 | `pkg/ddl, pkg/executor, pkg/mvservice: fix MV privilege check and refactor execution vars (#69126)` | 混合修正 MV privilege check、DDL/executor/service execution vars 和测试。 | 独立权限闭环；PR2b-create/alter；PR4/PR5/PR7 | 代码：已移植；测试：已保留 runtime 场景。关联 T10；详见上文 #60。 |
| 61 | `09824b200e7e` | 2026-06-13 | `ddl, executor, parser, planner: refine materialized view initialization and refresh syntax (#69159)` | 混合包含 COMPLETE 语法、schema version、initial-build state gate、admin index 限制和 refresh 初始化修复。 | PR2a；PR2b-create/alter；PR5；PR7 | 代码：部分，C6；测试：部分，T5/T15/T16。关联 C6/T5/T15/T16；详见上文 #61。 |
| 62 | `9a30eedc0650` | 2026-06-14 | `*: MV refresh/purge result reporting, metrics, and history enrichment (#69137)` | 增加 refresh/purge 结果、metrics、history 字段和 bootstrap 表初始化。 | PR1；PR4；PR5；PR7 | 代码：已移植；测试：部分，T4/T9。关联 T4/T9；详见上文 #62。 |
| 63 | `99948f2aac85` | 2026-06-14 | `*: cherry pick some non-mv related optimazations to mv branch (#69165)` | prepare dedup、plan cache、通用 session/expression 优化及大量测试结果同步。 | 非 MV；混合 branch drift | 代码：MV hunk 已移植；其余排除；测试：已保留。详见上文 #63。 |
| 64 | `fd53fbc6a7f2` | 2026-06-15 | `pkg/mvservice, pkg/session: add MV alert table and history cleanup (#69166)` | 增加 alert table、history cleanup、service 清理和系统表相关初始化/DDL 清理。 | PR1；PR7；部分 PR2b-drop | 代码：已移植；测试：已保留 runtime 场景。关联 T9；详见上文 #64。 |
| 65 | `37ff05b94b37` | 2026-06-15 | `mv,mvservice: make maintenance isolation read engines independent (#69172)` | 让 MV maintenance 的 isolation read engines 与普通 session 独立配置。 | PR4/PR5/PR7；PR2b-create initial build | 代码：已移植；测试：部分，T6。关联 T6；详见上文 #65。 |
| 66 | `90447ef0160d` | 2026-06-15 | `executor, session: record MV refresh commit TSO (#69174)` | 记录 refresh commit TSO，用于 refresh info/history 和后续调度。 | PR5；PR1 schema | 代码：已移植；测试：runtime 已保留，schema T9。关联 T9；详见上文 #66。 |
| 67 | `4a2bc184c38c` | 2026-06-15 | `executor: move MV log purge deletes out of pessimistic txn (#69183)` | 将 MLog purge delete 从 pessimistic transaction 中移出，调整执行和统计语义。 | PR4 | 代码：核心事务语义在；总流程 R1；测试：已保留专项。关联 R1；详见上文 #67。 |
| 68 | `9cef8d892f06` | 2026-06-15 | `executor, mvservice: throttle materialized view log purge (#69192)` | 增加 MLog purge 限流及 service 调度参数。 | PR4；PR7 | 代码：throttle/scheduler 已移植；purge 总流程 R1；测试：已保留。关联 R1；详见上文 #68。 |
| 69 | `fbbf288e1869` | 2026-06-16 | `ddl, executor, mvservice: impl mv attribute mview_alert_refresh_failed and enhance mysql.tidb_mview_refresh_alert (#69210)` | 增加 refresh-failed alert attribute、alert 表更新和 service 处理。 | PR2b-create/alter；PR5；PR7 | 代码：已移植；测试：runtime 已保留，T9/T28。关联 T9/T28；详见上文 #69。 |
| 70 | `a534759aca3e` | 2026-06-16 | `executor, session: guard fast refresh against hazardous mlog purge (#69227)` | 在 fast refresh 前检测危险的 MLog purge 状态，避免增量数据已被清理。 | PR4；PR5 | 代码：核心逻辑已移植；purge 总流程 R1；测试：已保留 runtime 场景。关联 R1/T9；详见上文 #70。 |
| 71 | `78e1033f4aa7` | 2026-06-16 | `ddl, executor: reject truncating mview-related tables with mlog guards (#69218)` | 拒绝会破坏 MV/MLog 依赖的 TRUNCATE/相关 DDL，并增加 worker 侧约束。 | PR2b-drop/alter | 代码：已移植；测试：已保留。详见上文 #71。 |
| 72 | `ee3965416a3f` | 2026-06-16 | `planner, executor, ddl, session: support MV privilege model and show status commands (#69233)` | 混合增加权限模型、SHOW STATUS/状态输出、planner/executor/DDL 检查。 | 独立权限闭环；PR6；PR7 | 代码：已移植权限闭环；测试：场景已有，integration 层 T27。关联 T27；详见上文 #72。 |
| 73 | `9dfaa1bc4cd6` | 2026-06-17 | `ddl, executor: cherry-pick MV log DDL enhancements (#69247)` | MLog DDL 增强，包括 create/drop/alter 约束、job args、schema tracker 和测试。 | PR2b-create/drop/alter | 代码：已移植；测试：部分，T18/T28。关联 T18/T28；详见上文 #73。 |
| 74 | `ff53f54bb001` | 2026-06-17 | `mvservice: improve manual cancel backoff handling (#69265)` | 改进手工 cancel 后的 service backoff 和任务重新调度。 | PR7；PR4/PR5 cancel | 代码：已移植；测试：已保留。详见上文 #74。 |
| 75 | `79156a515145` | 2026-06-17 | `infoschema, ddl: support materialized view metadata tables (#69271)` | 在 infoschema/DDL 中暴露 MV metadata tables，并增加读取/测试。 | PR6；部分 PR1/PR2b | 代码：已移植；测试：虚表场景保留，名称 T21。关联 T21；详见上文 #75。 |
| 76 | `6d14c9de5201` | 2026-06-17 | `ddl, parser, mvservice: support ALERT ROWS for materialized view logs (#69278)` | CREATE MLog 的 alert rows 语法和 metadata，以及 service alert 触发。 | PR2b-create；PR7 | 代码：已移植；测试：已保留并加强。详见上文 #76。 |
| 77 | `5018ef08bddd` | 2026-06-18 | `executor: support compare materialized view (#69297)` | 增加 COMPARE MATERIALIZED VIEW executor、SQL 和测试。 | PR6；FULL OUTER JOIN prerequisite | 代码：已移植，另有明确修复 B3；测试：十组 Go 场景保留，integration T27。关联 T27；详见上文 #77。 |
| 78 | `d2dc23f5c71a` | 2026-06-18 | `parser, ddl: allow clearing materialized view log purge schedule (#69280)` | 支持清除 MLog purge schedule 的 parser 和 ALTER DDL 行为。 | PR2a；PR2b-alter；PR4 metadata/runtime | 代码：已移植；测试：已保留。详见上文 #78。 |
| 79 | `2e489b053bc6` | 2026-06-18 | `executor: use baseQuery as build side for compare materialized view (#69312)` | 调整 COMPARE MV 的 join build side，降低/修正 compare 执行开销。 | PR6 | 代码：已移植；测试：无独立测试新增。详见上文 #79。 |
| 80 | `c4207360618b` | 2026-06-18 | `planner, executor: rename MV delta sides (#69307)` | 统一 MV delta apply 中 build/probe/old/new side 的命名。 | PR5；S9/S10 命名 refine | 代码：最终命名/执行器组装已移植；R2；测试：已保留核心场景。关联 R2；详见上文 #80。 |
| 81 | `f145795d551e` | 2026-06-19 | `ddl: forbid the creation of mlog for unsupported column type (#68997)` | 拒绝 MLog 复制 JSON、binary BLOB 等不支持的列类型。 | PR2b-create | 代码：已移植；测试：主体已保留，ALTER 后 DML T18。关联 T18；详见上文 #81。 |
| 82 | `e7a30c7889ba` | 2026-06-19 | `ddl: forbid the creating of mlog on partition base table (#68903)` | 拒绝对 partitioned base table 创建 MLog，并让 CREATE MV 分区表错误正确取消。 | PR2b-create | 代码：已移植；测试：已保留。详见上文 #82。 |
| 83 | `4769ef44fab9` | 2026-06-20 | `ddl: forbid unique indexes on materialized view (#68911)` | 拒绝会破坏 MV 维护语义的 unique index 操作。 | PR2b-alter | 代码：已移植；测试：已保留。详见上文 #83。 |
| 84 | `552472a43d33` | 2026-06-22 | `ddl: throw error when setting mlog and mv as target to create materialized view log (#68948)` | 拒绝将 MV/MLog/shadow table 作为 CREATE MLog 的 target。 | PR2b-create | 代码：已移植；测试：旧缺项已关闭。关联 T5；详见上文 #84。 |
| 85 | `b441fe3584eb` | 2026-06-22 | `test: cover materialized view log on generated column (#68082)` | 增加 generated column 与 MLog column capture 的测试覆盖。 | PR2b-create；PR3 | 代码：仅测试 commit，相关代码在；测试：主体已保留，T18。关联 T18；详见上文 #85。 |
| 86 | `a8bc88b4d857` | 2026-06-22 | `ddl: forbid to set system table as mlog target and unify the error info for unsupported object types (#68969)` | 拒绝 system table 作为 MLog target，并统一 unsupported object 错误。 | PR2b-create | 代码：已移植；测试：部分，T22。关联 T22；详见上文 #86。 |
| 87 | `7c01d98e8ad6` | 2026-06-23 | `ddl, parser: support IF EXISTS for dropping materialized views (#69351)` | 增加 DROP MV/MLog 的 IF EXISTS parser 和 DDL 行为。 | PR2a；PR2b-drop | 代码：已移植；测试：已保留。详见上文 #87。 |
| 88 | `d96dfd09a259` | 2026-06-23 | `ddl: avoid the upgrade of column type from TEXT to MEDIUMTEXT in MLog internal table (#69356)` | 防止 MLog 内部复制列在 DDL 过程中错误升级 TEXT 类型。 | PR2b-alter；PR3 相关 schema 同步 | 代码：已移植；测试：类型断言在，DML T18。关联 T18；详见上文 #88。 |
| 89 | `0fd4796db0c4` | 2026-06-23 | `ddl: allow comment-only modify on materialized view base columns (#69391)` | 允许不改变数据语义的 comment-only base column 修改，同时保持 MLog 约束。 | PR2b-alter | 代码：已移植；测试：已保留专项。详见上文 #89。 |
| 90 | `597952c8226e` | 2026-06-23 | `ddl: limit COMMENT strings length when creating materialized view (#68944)` | CREATE/ALTER MV 时限制 COMMENT 长度并产生正确 warning。 | PR2b-create；PR2b-alter | 代码：代码已移植；测试：部分，T19。关联 T19；详见上文 #90。 |
| 91 | `42d9a021010e` | 2026-06-25 | `*: slow query skip purge statement (#69462)` | 将 MLog purge 维护 SQL 从 slow query 统计/暴露路径中排除。 | PR4；部分 PR7 observability | 代码：已移植；测试：已保留。详见上文 #91。 |
| 92 | `2ad2da61a2ff` | 2026-06-28 | `mvservice: force status indexes for history cleanup (#69496)` | history cleanup 强制使用 status 相关索引，降低清理成本。 | PR7 | 代码：已移植；测试：已保留。详见上文 #92。 |
| 93 | `14ec59608231` | 2026-06-29 | `planner: bind MV MIN/MAX fast refresh lookup to supporting index (#69490)` | 将 MIN/MAX fast refresh lookup 绑定到支持索引。 | PR5；S9 | 代码：lookup 算法已移植；入口 R2；测试：已保留本条场景。关联 R2/T17；详见上文 #93。 |
| 94 | `8aff9064f372` | 2026-06-29 | `executor: collect stats delta for mlog purge (#69510)` | MLog purge 后收集 statistics delta。 | PR4 | 代码：已移植，stats API 适配；测试：已保留。详见上文 #94。 |
| 95 | `6645850375d3` | 2026-06-29 | `statistics: skip MV log tables in auto analyze priority queue (#69519)` | 将 MLog 从 auto-analyze priority queue 中排除或改由专门调度处理。 | PR4；PR7 | 代码：已移植；不等于 C6 已关闭；测试：已保留。关联 C6；详见上文 #95。 |
| 96 | `3c11363cc32e` | 2026-06-30 | `planner: estimate mlog commit ts filter selectivity (#69445)` | 为 MLog commit-ts filter 增加 selectivity 估算和 planner stats。 | PR4；PR5；S9 | 代码：window/stats 算法已移植；入口 R2；测试：已保留。关联 R2；详见上文 #96。 |
| 97 | `46f2873a3cc7` | 2026-07-01 | `mvservice, variable: schedule mlog analyze in MV service (#69516)` | 由 MV service 调度 MLog analyze，并增加相关变量和 statistics helper。 | PR4；PR7 | 代码：已移植并有明确加强 B4；测试：已保留并扩展。详见上文 #97。 |
| 98 | `190c9d508e3` | 2026-07-01 | `executor: avoid exposing materialized view cancel job state (#69555)` | 隐藏内部 cancel job 状态，调整用户可见结果和测试。 | PR4/PR5；PR7 | 代码：已移植；测试：已保留。详见上文 #98。 |
| 99 | `c9fd6ddba9ea` | 2026-07-02 | `executor: update error messages for materialized view log creation privileges (#69578)` | 修正 CREATE MLog 权限错误信息和对应测试。 | 独立权限闭环；PR2b-create | 代码：已移植；测试：unit 与 integration 本条场景均在。关联 T27；详见上文 #99。 |
| 100 | `19544ab81d34` | 2026-07-03 | `planner: collect predicate columns for MV maintenance SQL (#69596)` | 收集 MV maintenance SQL 的 predicate columns，改善 stats/plan 选择。 | PR5；S9 | 代码：已移植；purge session 分工 R1；测试：部分，T10。关联 R1/T10；详见上文 #100。 |
| 101 | `b1568c2e4a41` | 2026-07-09 | `metrics, mvservice: refine MV service metrics by component (#69737)` | 按组件细化 MV service metrics、reporter 和 Grafana。 | PR7 | 代码：已移植；测试：无独立 Go 测试新增。关联 C7；详见上文 #101。 |
| 102 | `17575377d73f` | 2026-07-12 | `mview: improve refresh observability and mlog purge delete (#69758)` | 混合修正 refresh observability、MLog purge delete、metrics、bootstrap 字段和测试。 | PR4；PR5；PR7；部分 PR1 | 代码：部分，R1/C7；测试：部分，T9/T25/T26。关联 C7/R1/T9/T25/T26；详见上文 #102。 |
| 103 | `06030b2809c0` | 2026-07-13 | `mvservice: refine MV refresh alert checking (#69808)` | 改进 refresh alert checker、过期/失败判断和 service 测试。 | PR7；部分 PR5 observability | 代码：已移植；测试：已保留。详见上文 #103。 |
| 104 | `7d7b687384f8` | 2026-07-13 | `executor,mvservice: avoid memory retention in MV maintenance SQL (#69778)` | 混合修正 MV maintenance SQL memory retention，同时回灌 DEPS、Makefile、cmd/mirror 等 branch drift。 | PR4/PR5/PR7 的 MV hunk；非 MV drift 不纳入 | 代码：MV hunks 已移植；C5 已关闭；测试：已保留。关联 C5；详见上文 #104。 |
| 105 | `6910cef84061` | 2026-07-17 | `mvservice: fix refresh alert checker ownership (#69849)` | 修正 refresh alert checker 的 owner/server maintainer 生命周期。 | PR7 | 代码：已移植；测试：已保留。详见上文 #105。 |
| 106 | `f5dfdf58b9c9` | 2026-08-21 | `refine bootstrap (#73)` | 合并/简化 bootstrap version，删除旧 upgrade 路径中的重复 MV system-table registry 和大量历史测试。 | PR1 | 代码：已移植，版本号/框架适配；测试：部分，T9。关联 T9；详见上文 #106。 |
| 107 | `43c6999be1c9` | 2026-08-22 | `mv: use saved schedule timezone for unix seconds (#74)` | 原保存 timezone/Unix-seconds 的变更；最终 source 由 #114 改为 UTC 求值并保存 schedule SQL mode，按此最终结果检查。 | PR1 schema；PR2b-create/alter；PR4/PR5/PR7 runtime | 代码：最终 UTC/SQL-mode 逻辑已移植，A1；测试：部分，T4/T20。关联 T4/T20；详见上文 #107。 |
| 108 | `c226d733626f` | 2026-08-22 | `executor, ddl, session: store MV refresh end time as Unix seconds (#75)` | 将 refresh end time 从 datetime 改为 Unix seconds，并调整 schema、DDL、refresh 写入和测试。 | PR1；PR2b-create/alter；PR5 | 代码：已移植；测试：runtime 已保留，schema T9。关联 T9；详见上文 #108。 |
| 109 | `24eaea3deee7` | 2026-08-23 | `mview: unify maintenance timestamp names (#76)` | 统一 refresh/purge info/history 的时间字段命名，例如 snapshot/start/end/heartbeat/update。 | PR1；PR4/PR5/PR7 | 代码：已移植最终命名；测试：runtime 已保留，schema T9。关联 T9；详见上文 #109。 |
| 110 | `bf681b1b662a` | 2026-08-25 | `mview: unify materialized view naming (#77)` | 统一 `MV`/`MView`/`MLog` 在系统表字段、Go 类型、task/service 和 planner 中的命名。 | PR1；PR2b；PR4/PR5/PR6/PR7 | 代码：部分，继承各实际缺项；测试：最终 suites 已落地，仍有 T3/T5/T6/T9/T15/T16/T28。关联 C6/R1/R2/R3/T3/T5/T6/T9/T15/T16/T28；详见上文 #110。 |
| 111 | `d05b5da91b50` | 2026-08-26 | `add mv system table rebuild sql` | 提供旧 MV 系统表清理并按最终 schema 重建的 SQL 运维脚本。 | PR1 文档/运维辅助 | 代码：artifact 缺失 C8；测试：无测试变更。关联 C8；详见上文 #111。 |
| 112 | `9439fdfa65e0` | 2026-08-29 | `parser: align materialized view options with spec (#78)` | 按 spec 重排 CREATE MV options、REFRESH、ATTRIBUTES 以及 AST Restore/parser 测试。 | PR2a；PR2b-create/alter 对 metadata 的适配 | 代码：已移植；测试：已保留。详见上文 #112。 |
| 113 | `8d2633e8e55a` | 2026-08-29 | `mview: remove unused never refresh method (#79)` | 删除未使用的 `NEVER REFRESH` refresh method、AST 分支和 DDL metadata builder 分支。 | PR2a；PR2b-create/alter | 代码：最终删除状态已保留；测试：无新增测试变更。详见上文 #113。 |
| 114 | `f6de16d6f2fa` | 2026-10-08 | `port some bug fix back to cp_mv_for_master (#81)` | 回灌 parser/DDL 修复、schedule SQL mode/UTC 求值、refresh 除法精度、shadow cleanup、cutover 依赖/原子性、infoschema v2、GC 和 planner 修复及测试。 | 混合 commit；PR2a/PR2b/PR4/PR5/PR7 及 prerequisite，按 hunk 拆分 | 代码：多数修复已移植，R3/A1 单列；测试：部分，T4/T5/T10/T11/T12/T20/T28。关联 R3/T4/T5/T10/T11/T12/T20/T28；详见上文 #114。 |
| 115 | `fd4636bb442d` | 2026-10-09 | `fix some ddl bug` | 修复 MV/MLog DDL 内部 SQL 错误后的整事务回滚、rollback schema-version 失败重试、重开事务后的 job metadata 并发检查和 infoschema v2 affected-table 保留；补充失败回滚回归。 | PR2b-create/drop/alter；共享 DDL/infoschema prerequisite | 代码：已移植；测试：已保留核心新增回归。详见上文 #115。 |

## 已覆盖范围与缺口汇总

### 尚缺失的代码及 artifact

以下均比较最终 source 与固定目标树；缺失是静态结论，未声称已复现 SQL 故障。
编号保留旧 C1–C5，新增 C6–C8。

| ID | Source commit # | 最终 source / master 证据 | 结论 |
| --- | --- | --- | --- |
| C2 | 5 | `pkg/executor/point_get.go:decodeOldRowValToChunk`：source 在 `getColInfoByID` 返回 nil 且 colID 为 `model.ExtraCommitTSID` 时构造 synthetic ColumnInfo；master 直接把返回值传给 `GetColOriginDefaultValue`，缺此分支。 | **逻辑未移植**。普通 rowcodec commit-TS 测试不能证明该 fallback；未运行 old-row-format 最小复现。 |
| C6 | 61/110 | source `pkg/statistics/handle/autoanalyze/autoanalyze.go:shouldSkipAutoAnalyzeTable` 跳过 not-ready MV；`priorityqueue/job.go:checkTableReadyForAnalyze` 被 non-partitioned/static/dynamic 三种 `ValidateAndPrepare` 调用，返回可重试。master scan/queue 只有 view/MLog 等过滤，无这些 readiness gates。 | **逻辑未移植**。#72582 的 MLog skip/analyze 不是 Building MV skip/retry；planner 读保护也不替代 queue retry。测试见 T16。 |
| C7 | 102 | `pkg/metrics/grafana/tidb.json`：source 有 `MV Refresh Duration` 面板的 success/failure 执行 P999/avg、schedule-duration P999/avg 六条查询；master 缺整个面板。现有 `MV Operation Duration` 有按类型执行时长，但无 schedule-duration 查询。 | **监控 artifact 未完整移植**。对应 metric 定义/reporter 存在；不能从 reporter 存在推断面板完整。 |
| C8 | 111 | source `docs/note/materialized_view/mv_system_tables_rebuild.sql` 存在，目标树无该文件/对应 rebuild artifact；tracking 明确将它纳入 PR1 文档。 | **运维 artifact 未移植**。不是 bootstrap runtime 缺失，未执行脚本。 |

可直接复核：[source point-get](https://github.com/pingcap/tidb/blob/fd4636bb442de52a1a5c6321f3f29092472aa6f4/pkg/executor/point_get.go#L723)、
[master point-get](https://github.com/pingcap/tidb/blob/e51a8506edf7cd4b7970c592fb40d26f55250dd1/pkg/executor/point_get.go#L753)；
[source analyze readiness](https://github.com/pingcap/tidb/blob/fd4636bb442de52a1a5c6321f3f29092472aa6f4/pkg/statistics/handle/autoanalyze/priorityqueue/job.go#L108)、
[master analyze job](https://github.com/pingcap/tidb/blob/e51a8506edf7cd4b7970c592fb40d26f55250dd1/pkg/statistics/handle/autoanalyze/priorityqueue/job.go)。

### 有实现但不满足本轮 port 标准的较大改写

这些项并非断言 master 整个功能不可用，而是不能把它标为 source 最终实现已完整移植。
没有找到能将下述整组重写认定为必要 master bug fix 的依据；明确修复只按 B 项保留。

| ID | Source commit # / 影响 | 实现对照 | 本轮判定 |
| --- | --- | --- | --- |
| R1 | 30；影响后续 38/67/68/70/94/100/102 的 purge 总流程 | source `pkg/executor/materialized_view.go:executePurgeMaterializedViewLog` 的 delete、count、schedule-eval session 分离；master `pkg/executor/mlog_purge.go` 复用 delete session 做 count、hist session 做 schedule，execution/recovery/history/finalize 流程重组。delete TiFlash threads 因而也施加于 count。 | **较大改写，未完整 port**。批次事务、checkpoint 等单独逻辑存在，不能据此关闭整个流程差异。T25 的预期已经改变，旧审计“更强测试覆盖”结论撤销。 |
| R2 | 31/33/36/51/53/57/59/80/93/96；最终命名 110 | source `pkg/planner/core/planbuilder.go:buildRefreshMaterializedViewImplement` 约 286 行；master `mview_refresh_builder.go` 约 223 行。FAST/COMPLETE switch 改成 if/else，source-plan closure、full-update lookup 组装及校验被重排/合并，相关字段/局部变量系统性改写。 | **较大改写，未完整 port**。不仅是文件移动/接口适配。`pkg/planner/mview` 的推导算法及多数 builder helpers 保留；#72227 empty-MLog 修复单独豁免，不能用它豁免其余重写。 |
| R3 | 54/114；最终命名 110 | `pkg/ddl/table.go:onRefreshMaterializedViewCompleteOutOfPlaceCutover`：source 分别验证 MV metadata/BaseTableIDs、shadow state/shape/revision；master 合并多组校验。BaseTableIDs 异常由 InvalidDDLJob 变 WrongObject；shadow 非 public 由 InvalidDDLState 变 InvalidDDLJob；revision 的 expected/actual 诊断被泛化。 | **validation 改写，未完整 port**。CAS、依赖更新、事务和 publish atomicity 保留，不把整个 cutover 记缺失；合并错误类别/丢失具体校验诊断没有作为必要修复得到证明。 |

固定实现入口：[source purge](https://github.com/pingcap/tidb/blob/fd4636bb442de52a1a5c6321f3f29092472aa6f4/pkg/executor/materialized_view.go#L2310)、
[master purge](https://github.com/pingcap/tidb/blob/e51a8506edf7cd4b7970c592fb40d26f55250dd1/pkg/executor/mlog_purge.go#L204)；
[source refresh planner](https://github.com/pingcap/tidb/blob/fd4636bb442de52a1a5c6321f3f29092472aa6f4/pkg/planner/core/planbuilder.go#L3911)、
[master refresh planner](https://github.com/pingcap/tidb/blob/e51a8506edf7cd4b7970c592fb40d26f55250dd1/pkg/planner/core/mview_refresh_builder.go#L96)；
[source cutover](https://github.com/pingcap/tidb/blob/fd4636bb442de52a1a5c6321f3f29092472aa6f4/pkg/ddl/table.go#L1232)、
[master cutover](https://github.com/pingcap/tidb/blob/e51a8506edf7cd4b7970c592fb40d26f55250dd1/pkg/ddl/table.go#L2065)。

**A1（局部适配，非上述整组重写）**：source 的 DDL/runtime schedule 私有 helper 提取为
`expression.SetMaterializedScheduleEvalSession` / `EvalMaterializedScheduleExpr`；master 先
SELECT NOW 再 parse/build expression，source 在 build 后读取 NOW，且出错时
`shouldUpdate` 返回值由 true 变 false。调用者先处理 error，未据此认定运行 bug；明确
保留这一顺序/API 差异，不能只写“同名等价”。缺失的 SQL-mode 边界验证见 T4/T20。

### 已关闭的旧代码缺口，以及允许保留的 master 修复

| ID | 关闭/保留依据 | 测试证据及限制 |
| --- | --- | --- |
| C1（关闭） | #72128：`encodeChunkKeyRows` 将单列 codec 输出与累积 group key buffer 分开，适配 codec 重置输出契约。 | `TestEncodeChunkKeyRows` 检查 `(1,"same")`/`(2,"same")`、NULL、空输入、复用 buffer/usedRows；不再只是单列 key。 |
| C3（关闭） | #72099：无 ProcessInfo 的内部 session 分配稳定非零 owner，GetLock SET/BEGIN 失败 Close/clean；INSERT 失败清理也在。 | WithoutProcessInfo、UsesProcessInfoOwner、SetupFailureReleasesSession 在；**剩余测试缺项 T8 不等于代码仍缺**。 |
| C4（关闭） | #72107：`scalarExprSupportedByFlash` allowlist 已加入 UnaryMinus。 | `TestExprPushDownToFlash` 同名测试新增对应成功下推断言。 |
| C5（关闭） | #72099：root `setMPPEncodeType` 经 SetEncodeType 尊重 EnableChunkRPC；non-root 保持 CHBlock。 | TestSetMPPEncodeTypeRespectsChunkRPCSetting 全部分支和 service chunk-RPC 回归在。 |
| B1（保留） | #72490 恢复 typed touched-row、NOT NULL/单字节 fast path、AppendCellRange/HasNull；保留 nil guard、IsFixed API 与 BIT 变长处理。 | signed-zero、NULL、sparse/bitmap、BIT、decimal 等回归在；不回退到 #71812 raw-byte 比较。 |
| B2（保留） | #72581 BIT MIN/MAX 按 ETString/变长表示处理；#72227 MLogEmpty 时跳过无输入 pipeline、lookup/index/GC 前提。 | unit/FAST SQL/integration BIT 和 empty-MLog 回归在；只豁免对应修复 hunk，不豁免 R2。 |
| B3（保留） | refresh `shadowNeedsCleanup` 同时处理 panic，成功 cutover 后先解除 cleanup 再 OnStepEnd；observe 使用 internal txn infoschema 避免重复 MDL；COMPARE output writer 应用/恢复 definition timezone。 | out-of-place failure/cleanup、timestamp output 等新增回归在；source 主体事务/结果断言仍保留。 |
| B4（保留） | service timer 包含 goroutine 调度等待；accumulation checker 在 error/panic 后也推进 next deadline；#72582 analyze 增加 run gate/locked table/time window/shutdown 与 stats ratio 再检查。 | service error/panic/timeout/cancel/lock/window 测试加强；这些修复不补齐 C6 的 not-ready MV gates。 |

### 测试直接缺项、被缩减的断言及迁移位置

T1–T5 沿用旧编号（旧 T5 的多类问题现拆开），T6–T28 为本轮细分。T2 已关闭；T27 是
**integration 层 fixture 缺失而 Go 场景仍在**，与完全没有对应断言区分。其余项都明确
指出 source 最终场景及目标剩余断言，不能用“同名测试还在”关闭。

| ID | Source commit # | Source 最终场景 | Master 检查结果 |
| --- | --- | --- | --- |
| T1 | 18 | `pkg/expression/aggregation/aggregation_test.go:TestNewDistAggFuncSumInt`：PB CompleteMode 构造具体 sumIntFunction，Update 1/2/3 得 6。 | **缺失**。构造分支在，但其他 SUM_INT SQL/pushdown 不直接覆盖这些断言。 |
| T2（关闭） | 35 | chunk `TestColumnAppendCellRangeFixed`、`...VarLen`、`TestHasNull`，以及复合 group-key 不碰撞。 | #72490 已保留三个 helper 测试；#72128 TestEncodeChunkKeyRows 补复合 key/buffer 场景，不再列开放缺项。 |
| T3 | 35/51/57/110 | `mviewdeltamergeagg/exec_test.go` 的 RuntimeStatsString/Merge：total=3、min=10ms、max=20ms、avg=15ms、reader=15ms、writer=8ms 等精确字符串；2/3 worker 不等长 merge、writer/row-op/insert/update/delete counters、clone 字段。 | **部分缺失**。目标短 `TestMergeRuntimeStats` 只检查 clone.String 一致、Tp、reader time/chunks/active 等，缺上述精确值/不等长 worker 和 counters；SQL profile 场景现已在，不再把整个 instrumentation 判缺。 |
| T4 | 62/107/114 | `TestHistTimeUsesLocation` 的 UTC/+08 精确字符串及微秒截断；`TestMaterializedScheduleRuntimeEvalUsesScheduleSQLMode` 的 caller strict 与 saved ALLOW_INVALID_DATES、Feb 31、non-date error、DATE mysql type。 | **缺失这些直接断言**。historyTime/shared evaluator 在；purge valid DATE/PIPES_AS_CONCAT/NO_BACKSLASH、UTC 和 parser SQL-mode 测试不覆盖 invalid-date/caller-vs-saved 边界。 |
| T5 | 3/25/51/57/61/114 | CREATE `REFRESH FAST START WITH` 无 NEXT 拒绝；AST IMPLEMENT Restore lower=4242/target=5252/retained=3131；FAST 忽略两种不匹配 CompleteType；直接构造 unspecified COMPLETE 的 Restore error；Mode 七分支含 unknown/-1。 | **上述断言缺失**。parser 的正常 IN/OUT/DELTA/AS OF Restore、bare COMPLETE 语法拒绝在，不能把它们当作直接非法 AST/Mode 或内部 TSO 的测试。 |
| T6 | 44/50/65/110 | `pkg/session/test/variable/variable_test.go` 的 TestMVMaintainMemQuota（默认2GiB/session独立/global新session）、TestMVMaintainIsolationReadEngines（与普通变量独立/global继承）、TestMViewMaintainImportDiskQuota（空默认/100gib/非法bad精确error）。 | **三组 SQL 专项缺失**。按目标 mview 变量名查也无相应 SQL 断言；refresh/purge apply/restore 和 service fake-global 用例已在，但不是 SET/GET scope/继承回归。 |
| T7 | 43 | `TestNewExtraCommitTSSchemaColType`：datasource 隐藏 commit-TS 列的 LONG LONG/unsigned 类型。 | **缺失**。普通 commit-TS rowcodec/SQL 结果不直接检查 schema col 类型标记。 |
| T8 | 54/114 | `pkg/session/session_test.go:TestAdvisoryLockConflictDoesNotLeakInternalSession`：INSERT 锁冲突后 internal-session/processlist 无泄漏；WithoutProcessInfo 还检查 Close 后 owner 清零。 | **缺失**。master SetupFailure 测 SET 失败，与 INSERT conflict 不同；owner 稳定性有测试，但删除了 Close→0 断言。C3 实现本身已补齐。 |
| T9 | 2/17/19/43/50/54/58/62/64/66/69/70/102/106–110 | source bootstrap 和 TestUpgradeToVer221MaterializedViewSystemTables 对最终五表检查 column type/unsigned/nullability、DATETIME(6)、decimal(18,6)、collation、索引列顺序、PKIsHandle 且非 common handle。 | **同名/替代测试被缩减**。目标 `pkg/session/test/bootstraptest/boot_test.go` 的 checkMaterializedViewBootstrapSchema 查表存在、ordinal 列名、索引名；upgrade316 从315升级也只复用它。不是 classic migration 缺失，但精确 schema contract 测试未移植。 |
| T10 | 11/60/100/114 | `pkg/ddl/backfilling_test.go:TestCreateMaterializedViewBuildSessionMVMaintenance`：build 期间 maintenance flag=true，结束恢复旧值。 | **部分缺失**。目标 precision helper 检查9/恢复2、CurrentDB；refresh SessionVars 检查配置应用，但无原 helper 的 maintenance flag 进入/恢复断言。 |
| T11 | 114 | `pkg/ddl/ddl_test.go:TestSetSchemaDiffForCreateMaterializedViewLogRollback`：rollback diff OldTableID=123、TableID=0。 | **缺失直接断言**。高层 rollback/retry 测试存在，不能替代这两个 schema-diff 字段检查。 |
| T12 | 54/114 | `gc_worker_test.go:TestGCPlacementRulesForMaterializedViewOutOfPlaceCutover` 同时安装 old/new bundles，GC 清 old 且保留 new shadow bundle。 | **部分缺失**。目标合并到 generic GC test 的 cutover 子例只安装/清理 old 40；没有安装并断言 new 41 仍在。 |
| T13 | 54 | `TestShouldUseImportIntoForMVRefreshOutOfPlace`：TiKV=true、lowercase tikv=false、TiDB=false、mock-storage=false。 | **部分缺失**。目标 BuildMVRefreshOutOfPlaceBuildSQLImportOptions 覆盖 TiKV/TiDB；lowercase/mock-storage 两个分支未找到。 |
| T14 | 25 | `pkg/session/test/common/common_test.go:TestRefreshMaterializedViewDoesNotMarkDDLExecution`。 | **缺失**。purge 的同类测试存在，但不检查 refresh statement 的 DDL execution flag。 |
| T15 | 61/110 | BlocksReadAndRefreshBeforeReady：SELECT/point/batch 拒绝、ANALYZE 拒绝、ADMIN CHECK/CHECKSUM 允许、REFRESH 拒绝、ready 转换；CheckMViewReadable 的 ready/legacy/deferred/restricted bypass。 | **部分缺失**。目标 PauseAndResume 保留读并增加 DML 拒绝；未保留 ANALYZE/ADMIN/REFRESH 子组。CheckMViewReadableShadow 只覆盖 base/building/shadow/restricted building，缺 ready/legacy/deferred 及 deferred bypass 的直接断言。 |
| T16 | 61/110 | `TestShouldSkipAutoAnalyzeTableNotReadyMV`、`TestNonPartitionedTableValidateAndPrepareRetriesNotReadyMV`。 | **两项均缺失，且对应 C6 代码也缺**。MLog skip/queue remove 测试不覆盖未完成 initial build 的 MV。 |
| T17 | 38 | `TestMaterializedViewDDLProtectsMinMaxSupportingBaseTableIndexes` 的 ALTER DROP INDEX、invisible index 建 MV 拒绝、非 clustered PK drop、备用 index drop/invisible 后 FAST 结果、WHERE 两索引最后 drop/invisible、合法 multi-schema 替换后 FAST 结果。 | **同名测试大幅缩减**。目标保留简单 DROP、最后 supporting-index invisible、multi-schema 成败及另立的并发 drop 场景，但未保留左列子组。 |
| T18 | 73/81/85/88 | MLog AddColumnBasic 的 SHOW CREATE、ALTER 后 untracked UPDATE 无日志/tracked UPDATE 的 old/new DATE/TEXT 值、rollback SHOW CREATE 无新增列；RejectsInvalidColumns 中经 ALTER 加 generated column 后 UPDATE；SupportsNewMaterializedView 后续 UPDATE/INSERT+FAST 得 `1 350 2`/`2 700 2`。 | **部分缺失**。目标直接向 MLog 填 fixture，保留 metadata/default/初建 MV 结果；不能代替 ALTER 后 base DML→MLog→FAST 链路。普通 stored/virtual generated-column writetest 在，但未经过 ALTER ADD COLUMN。 |
| T19 | 90 | `TestMaterializedViewCommentLength`：ALTER 最大长度成功、strict oversize 失败、non-strict warning 和截断。 | **ALTER 子组缺失**。目标同名测试只保留 CREATE；普通 `updated comment` ALTER 不覆盖边界。 |
| T20 | 21/107/114 | CREATE/ALTER MV/MLog 的 START WITH CURRENT_DATE；ALTER UsesUTC 在保存 PIPES_AS_CONCAT 后以 `INTERVAL (1 \|\| 2) DAY` 求得 Jan 13。 | **这些表达式路径缺失**。目标改成 NEXT CAST DATE、ALTER 常量 Jan 2；只检查保存 SQL mode/UTC 不能验证原 START 分支和 ALTER 执行所存 mode。purge runtime 的独立 PIPES 用例仍在。 |
| T21 | 4/75 | TruncatesLongPhysicalName 的64字符 ASCII 表实际 CREATE→DML→DROP/IS；NameLengthByRune 的 Unicode 碰撞后 DROP 冲突 MLog，再建更长 base 的 MLog 成功及 table count。 | **部分缺失**。目标同名 ASCII 测试是 schematracker metadata/长度检查；Unicode 只保留碰撞失败，删掉清冲突后成功路径。 |
| T22 | 86 | RejectNonBaseObject 的 local temporary target，以及带 db/table 名的完整 WrongObject 错误。 | **部分缺失/弱化**。global temporary/system/view/sequence 等还在；local temp 被删，若干精确错误变为仅 BASE TABLE substring。 |
| T23 | 38 | MaterializedViewRelatedTablesDDLRejected：rename MV/MLog、rename 带 MV 依赖 base、MLog DROP INDEX/ADD COLUMN 拒绝。 | **这些直接场景未保留**。目标有 base 仅 MLog 依赖的 rename、drop/truncate/vector/TiFlash-MLog 保护等，不能替代左列不同对象/操作组合。 |
| T24 | 41 | RestoreStmtTextForSlowLogWhenEmptySQL 的 schema-qualified FAST、内部非零 TSO=123456789 不泄漏、无关 SelectStmt 返回空。 | **部分缺失**。目标改为 COMPLETE IN PLACE 的文本与空 Refresh case，没有保留 FAST/隐藏内部字段/SelectStmt 子组。 |
| T25 | 102 | `TestPurgeMaterializedViewLogUsesDeleteTiFlashThreadsOnlyOnDeleteSession`：delete=配置值，count 不使用此 delete-only 值。 | **原断言未移植且预期反转**。目标 `...UsesConfiguredTiFlashThreadsForCountAndDelete` 要求 count/delete 均使用该值，对应 R1 的 session 合并；不能称“更强等价覆盖”。 |
| T26 | 102 | `TestBuildPurgeMaterializedViewLogDeleteSQL`：没有 lower TSO 时仍断言 row-ID 范围；没有 row-ID range 时仍断言 commit-TS 上下界。 | **组合断言缩减**。目标保留常规范围、单独条件，缺原上述两种组合中的关键断言。 |
| T27 | 72/77/99 | `tests/integrationtest/t/executor/mview_privilege.test` 及 result：SHOW PRIVILEGES、五组 COMPARE 权限/output table SQL。 | **integration fixture 缺失，场景已移到 Go**。目标整份文件已在，CREATE MLog error 也在；被删组由 TestShowPrivileges 的 OPERATE VIEW row、testCompareMaterializedViewPrivilegeSkeleton 覆盖。不能说整份未 port，也不能说原 integration 层全部保留。 |
| T28 | 11/13/38/49/69/73/114；最终命名 110 | `TestMaterializedViewDDLBasic` 的剩余 CREATE validation/metadata/依赖同步子场景（下列分项）。 | **部分缺失**。不能用拆分后 TestCreateMaterializedViewAndLog/ValidationCoverage 等的名称存在，推断约390行原场景都保留。 |

T28 的具体缺失与已有替代分开记录，避免把整条 DDLBasic 误判为未迁移：

- 缺 CREATE 入口拒绝没有 MLog、没有 COUNT(*)/COUNT(1)、AVG、non-deterministic WHERE RAND、SUM DATE/DATETIME/TIMESTAMP/TIME、
  DISTINCT/HAVING/ORDER BY/LIMIT/ROLLUP 的 SQL+错误断言；COUNT(column)、aggregate 大小写、
  缺 MLog 列、nullable SUM matching COUNT、nullable MIN/MAX 已有对应专项。
- 缺 aliased base WHERE 与 direct-output alias `k` 的原 SQL/结果断言；缺不重命名的
  `CHANGE COLUMN a a bigint` 同步 base/MLog/MV metadata 及查询结果的链路。
- 普通 MODIFY 的 base/MLog/MV 同步、WHERE-used column 拒绝和 rename 拒绝在
  `TestAlterTableWithMaterializedViewDependencies`；仍缺 source 同一列影响多个 MV
  （含 output alias）的全部 involving tables、alias 列类型/结果，以及仅由 aggregate
  expression 使用该列时的拒绝断言。两个列/一个 MV 的 multi-schema test 不替代该组合。
- 缺 CREATE 时 BaseTableIDs、默认 FAST/空 START/NEXT、alert 默认0/false、definition SQL mode
  和 timezone name/offset 捕获的原断言组；缺 CREATE ATTRIBUTES 的 unknown key/负数/非法 yes-no/
  warning>overdue 拒绝及显式 zero thresholds 检查（目标有 ALTER 的对应校验，CREATE 正常300/600也在）。
  缺 nullable group key
  明确使用 UNIQUE KEY 而非 PRIMARY KEY 的 CREATE 结果断言。目标只保留部分 reverse mapping、
  Ready、precision=9；其他 schedule/alert suites 的专项按各自实际断言计覆盖，不补写为整组齐全。
- 缺 MV hypothetical expression index create/drop、MV 自身 SET TIFLASH REPLICA 的原检查。
  普通 MV index 允许、unique/primary 拒绝、base TiFlash、comment-only MODIFY、pre-split、
  DROP dependency/cleanup 有对应 suites，**不列为缺失**。

### 确认已移动或被 source 后续替代的测试

| Source 场景 | 固定 master 的实际落点 / 判定 |
| --- | --- |
| SHOW 八个 Test helpers | `materialized_view_show_test.go:TestShowMaterializedView` 下 lowercase helpers；核对内容保留，只适配 testkit/CIStr/maintenance 名称。 |
| COMPARE 十个 Test helpers、infoschema 两个 | `materialized_view_compare_test.go:TestCompareMaterializedView`、`materialized_view_infoschema_test.go:TestInformationSchemaMaterializedView`；原断言保留，COMPARE 另增 output timestamp timezone。 |
| refresh 五类 commit-TS/result、session vars、schedule、snapshot/GC、history、cancel | `materialized_view_refresh_test.go` 的 StatementResult、SessionVars、Scheduling、FastAsOfTimestamp、GCSafePoint、MinMaxTargetSnapshot、History、Cancellation；按子场景核对，名称合并不算丢失。 |
| source 单列 FastMinMax supporting-index/nullable/separate-index 用例 | 合并到 TestMaterializedViewRefreshFastMinMax；保留 expected rows/index 使用等断言，不与 T17 的 DDL 操作后 FAST 混淆。 |
| source shadow CREATE MLog 拒绝 | TestMaterializedViewRefreshCompleteOutOfPlaceShadowTableProtected 中明确断言，旧 T5 此子项关闭。 |
| source shadow job-check interval | 三个 interval 断言合并到 TestGetJobCheckIntervalForCreateMaterializedView，已保留。 |
| source throttle 跨 row-range 四种组合 | 目标 `pkg/executor/utils_test.go` 的 throttle helper test 保留；不能因从 adaptive batch test 中删除就报漏。 |
| saved schedule timezone / NextTime / 旧 MVMerge 命名 | 已由 source #107–#114 替换为最终 UTC/SQL-mode/Unix seconds/MView；不要求恢复中间测试或名称。 |
| TestCreateMaterializedViewBuildSessionSQLMode | source 最终已删除；不能要求 target 迁移已删除测试。definition precision metadata 和 helper 应用/恢复另有真实断言。 |
| classic upgrade version221/222 | master version316/317 是编号/框架适配，classic migration 未丢；测试精度缺项仍按 T9，不以 NextGen 完全替代解释。 |
| FULL OUTER 的单独 outer-to-inner rule | 按 non-MV prerequisite 复用；四类最终 join-type 断言在 predicate-pushdown，不恢复旧 rule flag。 |
| integration assets | mview_log_dml、mview_compare 原场景保留；mview_refresh 另有 empty-MLog/BIT 扩展；mview_privilege 仅上述 T27 的 layer 差异，不能再写整份缺失。 |

## 审计命令与验证边界

交付只修改本文，使用 **Ready / documentation-only** 验证。没有修改生产代码、测试、
generated parser/Bazel/testdata，也没有执行 rebuild SQL。审计检查固定 Git objects，
工作目录中的其他分支内容不作为 master 证据。

固定边界和 inventory 的实际命令（从 TiDB 仓库运行）：

```bash
git fetch origin --prune
git fetch xufei cp_mv_for_master
git rev-parse e51a8506edf7cd4b7970c592fb40d26f55250dd1 xufei/cp_mv_for_master
git rev-list --count f08c648a20380ea723449c6c3eb5b171d96fd567..fd4636bb442de52a1a5c6321f3f29092472aa6f4
git diff --shortstat f08c648a20380ea723449c6c3eb5b171d96fd567 fd4636bb442de52a1a5c6321f3f29092472aa6f4
git log --reverse --format='%H%x09%ad%x09%s' --date=short f08c648a20380ea723449c6c3eb5b171d96fd567..fd4636bb442de52a1a5c6321f3f29092472aa6f4
```

逐项检查使用 `git show <source_commit> -- <path>` 定位历史 hunk，再比较
`git show fd4636bb442de52a1a5c6321f3f29092472aa6f4:<source_path>` 与
`git show e51a8506edf7cd4b7970c592fb40d26f55250dd1:<master_path>` 的最终函数/测试体；
`git grep` 搜索改名/拆分后的替代断言。函数名和文本相似度只辅助定位，不作独立覆盖证明。
每个上列 port commit 以 `git merge-base --is-ancestor <port_commit>
e51a8506edf7cd4b7970c592fb40d26f55250dd1` 检查所属历史。

文档验证命令：

```bash
python3 /tmp/mv-master-port-audit-20261104/validate_audit.py
git -C /home/xufei/dev/pingcap/tidb_worktree/cp_mv_for_master_base diff --check
git -C /home/xufei/dev/pingcap/tidb_worktree/cp_mv_for_master_base diff --stat
```

临时 validator 核对两张逐 commit 清单各有且仅有1–115、hash/date/subject 对应 Git inventory、
master merge commits 均为目标祖先、表格列数、C/R/T 引用和固定链接路径；它不是行为测试，
也不是仓库新增依赖。未本地验证项：运行正确性、错误路径复现、兼容性、性能、真实 TiKV/
TiFlash 行为、Grafana 展示。文档本身没有 runtime 影响；C/R/T 中涉及的风险尚未通过执行排除。
