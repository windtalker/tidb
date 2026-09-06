# MV on MV 端到端开发计划

**状态：已完成，待评审**
**日期：2026-09-05**
**关联范围文档：**[嵌套物化视图支持范围与工作量评估](mv_nested_scope_zh.md)

本文将 `mv_nested_scope_zh.md` 中的 MV on MV 范围细化为可实现、可测试和可拆分提交的
开发计划。本文中的 "source object" 指物理表或已完成 initial build 的物化视图；虽然现有
metadata 字段仍名为 `BaseTableIDs` / `BaseTableID`，实现中不再把该名称解释为仅能指向物理表。

## 实施记录

本计划已在 `nested_mv` 分支完成，提交按可独立审阅和回滚的边界拆分如下：

- `c0553cd1bd`：ready MV 上的 MLog DDL，以及 COMPLETE IN PLACE / OUT OF PLACE 的模式限制；
- `bd65fb7d7a`：以 ready MV 为 source 创建 child MV，并维护直接依赖 metadata；
- `9add297adc`：FAST 和 COMPLETE DELTA APPLY 对 parent MLog 的同事务 row-level I/U/D 写入；
- `3110b5af03`：child FAST 从 parent MV MLog 消费 delta，并覆盖 parent FAST / COMPLETE DELTA APPLY；
- `04d0d58b20`：parent MV drop 的 executor / worker 双层保护；
- `900fc11d60`：OOP cutover 对并发创建 MLog 或 child MV 的 worker-side 保护和 shadow cleanup；
- `def499d778`：parent MLog purge 按 nested child refresh checkpoint 保留未消费 delta 的回归；
- `306adc1b2e`：三级 `t -> mv1 -> mv2 -> mv3` direct dependency metadata 和逆序 drop 保护回归。

这些提交完成本文第 2 节的支持矩阵和第 4 节的阶段 A-E。第 2.3 节列出的非目标仍保持不变，
尤其是不提供自动 refresh 调度、级联 refresh、optimizer rewrite，以及 COMPLETE OUT OF PLACE
的依赖 metadata / MLog 迁移。

## 1. 目标与完成定义

交付最小端到端闭环：

```text
base table t
    -> mv1
    -> $mlog$mv1
    -> mv2
```

用户应能够执行：

```sql
CREATE MATERIALIZED VIEW mv1 (k, cnt, total)
REFRESH FAST
AS SELECT k, COUNT(*), SUM(v)
FROM t
GROUP BY k;

CREATE MATERIALIZED VIEW LOG ON mv1 (k, cnt, total);

CREATE MATERIALIZED VIEW mv2 (k, child_cnt)
REFRESH FAST
AS SELECT k, SUM(cnt)
FROM mv1
GROUP BY k;

REFRESH MATERIALIZED VIEW mv1 FAST;
REFRESH MATERIALIZED VIEW mv2 FAST;
```

当 `mv1` 使用本计划支持的 refresh 模式更新时，`$mlog$mv1` 必须在同一事务中记录可供
`mv2 FAST` 消费的 row-level delta。下列 parent refresh 模式属于本期承诺：

```sql
REFRESH MATERIALIZED VIEW mv1 FAST;
REFRESH MATERIALIZED VIEW mv1 COMPLETE DELTA APPLY;
```

完成条件不是只让上述 DDL 成功，而是同时满足：

1. child MV 的 initial build、后续 FAST refresh、MLog purge safety 和 drop 生命周期都正确；
2. parent 的 FAST 与 COMPLETE DELTA APPLY 均向自身 MLog 写入正确的 I/U/D old/new 行；
3. parent 有 MLog 或 child MV 时，COMPLETE OUT OF PLACE 被稳定拒绝，不发生依赖关系迁移；
4. 不引入自动 cascade refresh、拓扑调度或查询 rewrite。

## 2. 明确支持范围

### 2.1 SQL 和依赖范围

- 每个 MV 仍只支持当前已有的单 source、单表聚合 SQL 能力。
- source object 可以是物理表，也可以是 ready MV。
- child MV 的 source MV 必须已有 MLog，且 child 查询引用的所有 source 列都必须被该 MLog
  跟踪。
- 不新增 parser/AST 语法；现有 `CREATE MATERIALIZED VIEW`、`CREATE MATERIALIZED VIEW LOG`
  和 `REFRESH MATERIALIZED VIEW` 语法足够。
- 首个端到端验收场景是一个直接边 `t -> mv1 -> mv2`。metadata 和 DDL 代码按直接依赖 ID
  通用实现，不应人为写死层数；但本期不承诺任意深度的拓扑刷新语义。

### 2.2 Refresh 模式矩阵

| target MV 状态 | `FAST` | `COMPLETE DELTA APPLY` | `COMPLETE IN PLACE` | `COMPLETE OUT OF PLACE` |
| --- | --- | --- | --- | --- |
| 无自身 MLog、无 child MV | 保持现有行为 | 保持现有行为 | 保持现有行为 | 允许，保持现有 cutover 行为 |
| 有自身 MLog | 支持，并写自身 MLog | 支持，并写自身 MLog | 本期拒绝，避免未定义的全量 delete/insert MLog 语义和日志放大 | 拒绝 |
| 有 child MV | 仅在自身 MLog 存在且 child 通过 FAST 消费时支持 | 仅在自身 MLog 存在且 child 通过 FAST 消费时支持 | 本期拒绝 | 拒绝 |

`COMPLETE IN PLACE` 的限制只施加在 target MV 已有自身 MLog 的情况下；没有 MLog 的既有
refresh 行为不应被本功能改变。该限制避免出现 parent 数据已变化、下游是否应消费一整套
delete/insert MLog 却没有清晰契约的状态。

`COMPLETE OUT OF PLACE` 的允许条件为：

```go
mv.MaterializedViewBase == nil ||
    (mv.MaterializedViewBase.MLogID == 0 &&
        len(mv.MaterializedViewBase.MViewIDs) == 0)
```

其中 `MViewIDs` 只表示该 MV 的下游 direct child MV。target MV 自身可以是另一个 MV 的
child；只要它没有自己的 MLog、也没有下游 child MV，OOP 仍可按既有行为执行。

### 2.3 明确不包含

- parent/child 的自动 refresh 顺序、自动 cascade refresh 和 MVService 依赖图调度；
- parent refresh 失败时自动阻止 child refresh；
- parent/child 的严格 revision、snapshot 或 freshness 一致性校验；
- optimizer 将普通查询 rewrite 到 MV；
- join、subquery、window function、多 source MV、多表 fast refresh；
- COMPLETE IN PLACE 向 child MLog 传播；
- COMPLETE OUT OF PLACE 中迁移 MLog、child `BaseTableIDs` 或下游依赖图。

调用方仍必须先刷新 parent，再刷新 child：

```sql
REFRESH MATERIALIZED VIEW mv1 FAST;
REFRESH MATERIALIZED VIEW mv2 FAST;
```

独立 schedule 的 child MV 没有 parent-first 保证。本期不修改 MVService 来补足该保证。

## 3. 元数据与不变量

对于 `t -> mv1 -> mv2`，完成后的 public metadata 必须满足：

```text
mv1.MaterializedView.BaseTableIDs = [t.ID]
mv1.MaterializedViewBase.MLogID = $mlog$mv1.ID
mv1.MaterializedViewBase.MViewIDs = [mv2.ID]

$mlog$mv1.MaterializedViewLog.BaseTableID = mv1.ID

mv2.MaterializedView.BaseTableIDs = [mv1.ID]
```

不新增 metadata 字段。现有 create/drop helper 已按 object ID 维护
`MaterializedViewBase.MViewIDs` 和 `MLogID`，但所有相关的 object-type 和 state 校验必须
从 "base table" 泛化到 "physical source object"。

下列不变量需要同时在 executor 前置校验和 DDL worker 状态机中维护：

1. 被引用的 source MV 必须是 `MVInitBuildReady`。
2. source object 的 `MaterializedViewBase.MLogID` 必须非零，且对应表的
   `MaterializedViewLog.BaseTableID` 必须等于 source object ID。
3. MLog tracked columns 必须覆盖 child MV SQL 中引用的 source 列。
4. parent 存在 direct child MV 时，不能 drop parent，不能 drop parent MLog。
5. OOP cutover 前 target 仍不得拥有 MLog 或 direct child MV。
6. refresh writer 对 MV 数据和其 MLog 的写入必须落在同一 TiKV transaction；任一 MLog
   写失败，MV 数据写入也必须随 statement 回滚。

## 4. 实现阶段

### 阶段 A：允许 ready MV 拥有和管理 MLog

**目的：**建立 parent 能作为 child source 的 metadata 前提，但尚不承诺 refresh 会向该
MLog 写数据。

**涉及位置：**

- `pkg/ddl/executor.go`：调整 `isValidMaterializedViewLogBaseTable` 及相关 protected-object
  检查。
- `pkg/ddl/create_table.go`：调整 `onCreateMaterializedViewBaseCheck` 和
  `onCreateMaterializedViewLog` 的 worker-side object/state recheck。
- `pkg/ddl/materialized_view.go`：CREATE/ALTER/DROP MLog 的 source lookup、错误信息和
  dependent-MV 检查。
- `pkg/ddl/table.go`：MLog drop worker-side recheck，以及 create/drop 时
  `MaterializedViewBase` 更新。
- `pkg/executor/show.go`、`pkg/executor/infoschema_reader.go`、MLog purge 相关路径：
  确认 `BaseTableID` 指向 MV 时的展示、解析和 safe-purge 计算均可工作。

**具体步骤：**

1. 允许 `CREATE MATERIALIZED VIEW LOG ON mv1 (...)`，前提是 `mv1` 是 public、ready MV。
2. 继续拒绝普通 View、Sequence、临时表、MLog table、MV shadow table 和 initial build
   尚未完成的 MV。
3. 保持 MLog 物理表命名、列定义、tracked-column 顺序、purge metadata 和 privilege
   规则不变。
4. 复核 `ALTER MATERIALIZED VIEW LOG`、`DROP MATERIALIZED VIEW LOG`、`PURGE MATERIALIZED
   VIEW LOG`、`SHOW MATERIALIZED VIEW LOGS`、`SHOW CREATE MATERIALIZED VIEW LOG` 和
   `information_schema.tidb_mlogs` 对 MV base 的表现。
5. 验证 worker 在 executor 校验完成后仍会重新检查 source 状态，防止并发 DDL 使
   MLog 指向不再合法的对象。

**阶段验收：**

- ready MV 可创建、展示、alter 和 purge MLog；
- building MV 与 shadow MV 被拒绝；
- MLog metadata 的 `BaseTableID` 指向 MV ID；
- MLog drop 仍会因 dependent child MV 被拒绝。

### 阶段 B：支持以 ready MV 为 source 创建 child MV

**目的：**支持 `CREATE MATERIALIZED VIEW mv2 ... AS SELECT ... FROM mv1`，并让现有
direct-dependency metadata 正确落盘。

**涉及位置：**

- `pkg/ddl/materialized_view.go`：`CreateMaterializedView`、source/MLog lookup、
  `validateCreateMaterializedViewQuery`。
- `pkg/ddl/create_table.go`：`onCreateMaterializedView` 的 worker-side source recheck，
  initial build 和 metadata publish 流程。
- `pkg/ddl/create_table.go` / `pkg/ddl/table.go`：复用并验证
  `updateMaterializedViewBaseInfoOnCreate` 与 `updateMaterializedViewBaseInfoOnDrop`。
- `pkg/planner/core` 的 preprocessor / privilege 路径：仅在现有 table lookup 对 MV source
  有额外限制时做最小放宽。

**具体步骤：**

1. 将 CREATE MV 内的 source lookup 抽象为 "physical source object"，不再以
   `IsBaseTable()` 作为唯一合法条件。
2. 对 MV source 强制校验 ready 状态、自身 MLog 存在、MLog `BaseTableID` 匹配、tracked
   columns 覆盖查询引用列。
3. 沿用当前单表聚合 validator、aggregate 限制、MIN/MAX 索引限制和 privilege 语义；
   仅改变 source object 的合法类型。
4. 令 `mv2.MaterializedView.BaseTableIDs` 保存 `mv1.ID`。create worker 通过现有 ID-based
   helper 将 `mv2.ID` 加入 `mv1.MaterializedViewBase.MViewIDs`。
5. initial build 直接读取 ready `mv1` 的物理数据。initial build 只初始化 `mv2` 数据与
   refresh-info，不向 `mv2` 的 MLog 追写历史行。
6. 在 worker build 前重新确认 source MV 仍 ready、MLog 仍匹配；任何失效都取消或回滚
   child create job，不能留下半连接 metadata。

**阶段验收：**

- `mv1` ready 且有完备 MLog 时，`mv2` 创建和 initial build 成功；
- source MV 无 MLog、MLog 缺少引用列、source MV 未 ready 时，child create 有明确错误；
- `BaseTableIDs` / `MViewIDs` / `MLogID` 的 ID 关系符合第 3 节；
- child initial build 失败、取消或 rollback 后不留下 parent 的 dangling `MViewIDs`。

### 阶段 C：提供 refresh 专用的 operation-aware MLog 写入目标

**目的：**让专用 refresh writer 复用现有 MLog 存储和事务行为，同时准确表达每一行的
logical I/U/D。

现有 `tables.WrapTableWithMaterializedViewLog` 的 `MLogSourceStmt` 是 statement-level。
而 FAST merge 与 COMPLETE DELTA APPLY 都在一个内部 refresh statement 中混合 insert、
update 和 delete，不能把整个 target 固定包装为 `MLogSourceUpdate`。

**涉及位置：**

- `pkg/table/tables/mview_log.go`：新增 refresh 专用的 operation-aware wrapper/helper；
  不修改 `table.Table` 接口。
- `pkg/executor/builder.go`：为 refresh target 解析其 MLog，并构造 refresh writer 所需的
  MLog targets。
- `pkg/executor/mview_delta_merge_agg_builder.go`：
  `MViewDeltaMerge` builder 注入 refresh MLog targets。
- `pkg/executor/mviewdeltamergeagg/exec.go` 和 `writer.go`：
  FAST writer 依据 `RowOpInsert` / `RowOpUpdate` / `RowOpDelete` 选择对应 target。
- `pkg/executor/materialized_view.go`：
  COMPLETE DELTA APPLY builder/executor 按 diff op 选择对应 target。

**建议设计：**

在 `pkg/table/tables` 定义 statement-scoped 的 refresh target 集合，例如：

```go
type MViewRefreshMLogTargets struct {
    Insert table.Table
    Update table.Table
    Delete table.Table
}
```

构造时：

- base MV 无 MLog：三个字段都指向原始 target table；
- base MV 有 MLog：三个字段分别包装为产生 I/U/D 的 target；
- 每个 wrapper 仅在当前 refresh statement 内使用，不跨 statement/session 复用。

这样不用把 MLog 的 logical DML type 错误地绑定为单一的 refresh statement 类型，也不需要
在 FAST 或 COMPLETE DELTA APPLY writer 中重复 MLog row 编码逻辑。

**行级语义：**

| refresh op | MV table mutation | MLog rows |
| --- | --- | --- |
| insert | `AddRecord(new)` | `new, I, +1` |
| update | `UpdateRecord(old, new, touched)` | `old, U, -1`；`new, U, +1` |
| delete | `RemoveRecord(old)` | `old, D, -1` |
| no-op update | 不写 MV | 不写 MLog |

对 update，现有 MLog tracked-column 过滤仍应生效：未变更 tracked column 时不写 MLog。
因此创建 parent MLog 时必须包含 child refresh 所需的 parent 输出列。

**事务和错误要求：**

1. 先完成 base MV 的 `AddRecord` / `UpdateRecord` / `RemoveRecord`，再在同一 transaction
   追加对应 MLog row，沿用既有 wrapper 语义。
2. MLog 写失败必须向上返回错误，使整个 refresh statement 回滚。
3. 不能以单独 internal SQL 或异步任务写 MLog。
4. 不能把 CREATE MV initial build 接到此 refresh MLog target；它不是增量 refresh。

**阶段验收：**

- wrapper 层单测覆盖 I/U/D、old/new marker、tracked-column unchanged、MLog 写失败回滚；
- FAST 与 COMPLETE DELTA APPLY 的写路径均不再直接绕开 MLog；
- target 无 MLog 时两个 writer 的现有行为不变。

### 阶段 D：FAST refresh source 从 "base table" 泛化到 "source object"

**目的：**让 `mv2 FAST` 可以从 `$mlog$mv1` 消费 delta，而不是只接受物理表 MLog。

**涉及位置：**

- `pkg/planner/mview/mview.go`：`buildLocal`、MLog delta select 和 source validation。
- `pkg/planner/core/planbuilder.go`：FAST plan 构建中 source/MLog ID 的传递。
- `pkg/executor/materialized_view.go`：
  `resolveRefreshMaterializedViewLogInfo`、FAST refresh integrity/purge fence 校验。
- `pkg/executor/mview_delta_merge_agg_builder.go` 与
  `pkg/executor/mviewdeltamergeagg`：验证 target/source column、handle 和 min/max 重算
  假设不依赖 source 是普通表。

**具体步骤：**

1. 引入共用 source resolver，返回 source table metadata、source kind、MLog metadata 和
   source ID；物理表与 ready MV 走同一 MLog consistency check。
2. 保持 `MaterializedViewLog.BaseTableID == source.ID` 的强校验，不通过名称猜测 MLog。
3. child FAST 读取 parent MLog 时，复用已有 `_MLOG$_DML_TYPE` 与 `_MLOG$_OLD_NEW`
   delta 计算；不增加 parent/child 联合调度。
4. 验证 aggregate merge 对 parent MV 输出列的类型、nullable 语义、group handle 和
   MIN/MAX 索引检查均正确。
5. 将错误文案中的 "base table" 改为 "source object" 或在需要保留 SQL 术语处明确
   "materialized view source"，避免用户误以为 MV source 不被支持。

**阶段验收：**

- parent FAST 后 child FAST 正确消费 parent MLog；
- parent COMPLETE DELTA APPLY 后 child FAST 同样正确；
- source MLog 被 purge 到 child 上次成功 read TSO 之后时，child FAST 仍按现有完整性
  规则报错；
- child 不因 parent 尚未刷新而得到虚假的 freshness 保证。

### 阶段 E：生命周期保护和 OOP safety boundary

**目的：**避免 dangling dependency metadata，以及避免 OOP cutover 在存在下游关系时替换
物理 table ID。

**Drop 和 purge：**

1. `DROP MATERIALIZED VIEW mv1` 在 `mv1.MaterializedViewBase.MViewIDs` 非空时必须拒绝。
2. `DROP MATERIALIZED VIEW LOG ON mv1` 在 `mv1` 有 child MV 时必须在 executor 和 worker
   两层拒绝。
3. `DROP DATABASE`、CREATE rollback 和 DROP rollback 必须清理或恢复 parent/child
   direct-dependency metadata，不留下无效 ID。
4. MLog purge 的 direct dependent MV 收集继续使用 parent 的 `MViewIDs`；验证 parent 为
   MV 时 safe purge TSO 与物理表 source 时一致。

**COMPLETE OUT OF PLACE：**

在两个位置执行第 2.2 节的 OOP guard：

1. executor：在 shadow table 创建前检查，快速失败且不创建 shadow；
2. DDL worker：
   `onRefreshMaterializedViewCompleteOutOfPlaceCutover` 在读取 old MV metadata 后再次检查。

第二层检查是并发 DDL 的正确性要求。例如 refresh 开始后、cutover 前，另一个会话可能已为
target 创建 MLog 或创建了 child MV。worker 必须取消 cutover；refresh executor 的既有
defer cleanup 路径必须删除 shadow table。

本期不尝试在 OOP cutover 中迁移下列对象：

- `$mlog$mv` 的 `MaterializedViewLog.BaseTableID`；
- child MV 的 `MaterializedView.BaseTableIDs`；
- replacement MV 的 `MaterializedViewBase.MLogID`；
- replacement MV 的 `MaterializedViewBase.MViewIDs`。

这正是 OOP guard 存在的原因。

**COMPLETE IN PLACE：**

在 refresh mode 解析后的公共校验处增加 target-has-MLog 限制。错误必须明确说明本期仅支持
FAST 和 COMPLETE DELTA APPLY 向下游 MLog 传播。该检查应在实际 internal delete/insert
前执行。

**阶段验收：**

- parent/child/MLog 的正常 drop 顺序成功，错误顺序被拒绝；
- purge 不会越过 direct child 所需的 refresh checkpoint；
- OOP 对无 MLog、无 child 的 MV 保持成功；
- OOP 对有 MLog 或 child 的 MV 在 shadow 创建前失败；
- pause OOP build 后并发创建 MLog 或 child，再继续 cutover，worker 拒绝且 shadow 被清理；
- 有自身 MLog 的 MV 执行 COMPLETE IN PLACE 被拒绝。

## 5. 建议的 PR 拆分

### PR-1：MV MLog source 与 refresh mode guard

- 支持 ready MV 创建/管理 MLog；
- 覆盖 show、infoschema、purge 和 DDL worker recheck；
- 实现 COMPLETE OUT OF PLACE 的 executor + worker 双层 guard；
- 对带 MLog 的 MV 拒绝 COMPLETE IN PLACE；
- 完成 MLog-on-MV 的 DDL/lifecycle 测试。

### PR-2：Nested MV CREATE 与 direct dependency

- 支持 ready MV + matching MLog 作为 CREATE MV source；
- 完成 source column coverage 和 state 校验；
- 验证 initial build、metadata、drop/rollback；
- 不在该 PR 宣称 child FAST 已可用。

### PR-3：Refresh MLog sink

- 在 `pkg/table/tables` 引入 refresh operation-aware MLog targets；
- 将 FAST merge 和 COMPLETE DELTA APPLY 接入该 sink；
- 覆盖 I/U/D old/new 行、transaction rollback 和无 MLog 回归；
- 不改变 COMPLETE IN PLACE / OOP 的限制。

### PR-4：Nested FAST refresh 与端到端回归

- 泛化 FAST source object / MLog resolver；
- 打通 `t -> mv1 -> mv2` 的 parent FAST、parent COMPLETE DELTA APPLY 到 child FAST；
- 覆盖 purge、drop、并发 DDL 和 documentation；
- 完成本计划第 6 节的端到端验收矩阵。

每个 PR 保持可独立测试和可回滚。PR-3 与 PR-4 可以在代码依赖足够清楚时并入同一个 runtime
PR，但不要把 OOP 依赖图迁移混入其中。

## 6. 测试计划

### 6.1 DDL 和 metadata

在 `pkg/executor/test/ddl/mview_log_ddl_test.go`、
`pkg/executor/test/ddl/materialized_view_ddl_test.go` 和相邻 DDL 单测中覆盖：

1. ready MV 可创建 MLog，building MV、ordinary view、sequence、temporary table、MLog 和
   shadow MV 均被拒绝；
2. `SHOW CREATE MATERIALIZED VIEW LOG`、`information_schema.tidb_mlogs` 和
   `SHOW MATERIALIZED VIEW LOGS` 显示 MV source；
3. `mv1` MLog 存在且列完整时，`CREATE mv2 AS SELECT ... FROM mv1` 成功，initial build
   结果正确；
4. parent 无 MLog、MLog mismatch、缺少 tracked column、parent 未 ready 时，child create
   失败；
5. 创建和删除 child 后，`mv1.MaterializedViewBase.MViewIDs` 正确增加和删除；
6. child 存在时 drop parent 与 drop parent MLog 被拒绝；按 child、MLog、parent 顺序删除
   成功；
7. create child 失败、cancel 和 rollback 不遗留 parent metadata；
8. OOP early guard 和 worker-side race guard 均覆盖 "并发创建 MLog" 与 "并发创建 child"
   两种情况。

### 6.2 MLog writer

在 `pkg/table/tables`、`pkg/executor/mviewdeltamergeagg` 和
`pkg/executor/test/executor/materialized_view_refresh_test.go` 增加：

1. refresh insert 产生一条 `I / +1` MLog row；
2. refresh delete 产生一条 `D / -1` MLog row；
3. refresh update 产生完整的 `U / -1` 和 `U / +1` pair；
4. update 没有改变 tracked column 时不写 MLog；
5. no-op merge / no-op complete delta apply 不写 MLog；
6. MLog append 失败时 refresh 数据与 MLog 一起回滚；
7. 无自身 MLog 的 MV 继续使用原始 target table，现有 refresh regression 不变。

FAST 和 COMPLETE DELTA APPLY 必须各自覆盖 insert-only、update-only、delete-only、mixed
和 no-op。不要只通过最终 MV 行集断言；必须直接查询 `$mlog$mv1` 检查 DML type、
old/new marker 和 tracked column 值。

### 6.3 Nested refresh 端到端

在 executor 测试和 `tests/integrationtest/t/executor/mview_refresh.test` 中覆盖：

1. `t -> mv1 -> mv2` initial build；
2. base table 变化后 `mv1 FAST -> mv2 FAST`；
3. base table 变化后 `mv1 COMPLETE DELTA APPLY -> mv2 FAST`；
4. parent group 新建、已有 group 更新、group 删除；
5. source group key 变化导致 parent 的 delete/insert 或 update 组合；
6. `COUNT(*)`、`COUNT(column)`、nullable `SUM`、`MIN` 和 `MAX` 的 child FAST 结果；
7. parent MLog tracked columns 缺失时 child create/refresh 的明确失败；
8. child 先于 parent refresh 时只消费已有 parent MLog，不承诺自动追赶；测试只证明现有
   调用方顺序契约，不引入 scheduler 断言；
9. parent MLog purge 与 child refresh checkpoint；
10. 三级 metadata 链 `t -> mv1 -> mv2 -> mv3` 的 create/drop 回归可作为 direct-edge
    泛化检查；不将其解释为自动拓扑 refresh 支持。

### 6.4 OOP 和非支持模式

在 `pkg/executor/test/executor/materialized_view_refresh_test.go` 和 DDL cutover 测试中覆盖：

1. no-MLog/no-child target 的 OOP refresh 仍成功；
2. target 有 MLog 时 OOP 在 shadow create 前失败；
3. target 有 child MV 时 OOP 在 shadow create 前失败；
4. refresh 建 shadow 后并发创建 MLog 或 child，cutover worker 失败且 shadow 被删除；
5. target 有 MLog 时 COMPLETE IN PLACE 被拒绝；
6. target 无 MLog 时 COMPLETE IN PLACE 保持既有行为。

## 7. 验证命令与提交前检查

实现时按改动范围选择最小有效集合。MV refresh 和 DDL 现有测试使用 failpoint，因此先按
`docs/agents/testing-flow.md` 搜索并执行标准 enable/disable 流程。

建议的目标命令如下，新增测试名称以实际实现为准：

```bash
rg -n --fixed-strings -- "failpoint." pkg/executor pkg/ddl
rg -n --fixed-strings -- "testfailpoint." pkg/executor pkg/ddl

make failpoint-enable && (
  go test ./pkg/table/tables -run 'Test.*MLog.*Refresh' -tags=intest,deadlock
  go test ./pkg/executor/mviewdeltamergeagg -run 'Test.*MView.*MLog' -tags=intest,deadlock
  go test ./pkg/executor/test/executor -run 'Test.*Nested.*MaterializedView|Test.*MaterializedView.*(Fast|CompleteDeltaApply|OutOfPlace)' -tags=intest,deadlock
  go test ./pkg/executor/test/ddl -run 'Test.*(Nested|MaterializedViewLog|MaterializedView).*' -tags=intest,deadlock
  go test ./pkg/planner/mview -run 'Test.*MaterializedView' -tags=intest,deadlock
  make failpoint-disable
)

pushd tests/integrationtest
./run-tests.sh -r executor/mview_refresh
./run-tests.sh -r executor/mview_log_dml
popd

make bazel_lint_changed
git diff --check
```

如果为组织测试新增、移动或删除 Go 文件，或者改动 Bazel metadata / `go.mod` / `go.sum`，
在构建和 lint 前执行：

```bash
make bazel_prepare
```

测试执行后必须检查 integration result 变更，并在 PR 中报告精确命令。新增 bug regression
应尽可能证明 fix 前失败、fix 后通过；无法在当前分支直接复现时应说明原因。

## 8. 风险与决策点

### 8.1 正确性风险

- refresh MLog 的 DML type 或 old/new marker 错误，会使 child FAST 静默算错；
- OOP 缺少 worker-side guard，会在并发 DDL 后留下旧 ID 的 MLog 或 child dependency；
- parent MLog purge 若未将 child refresh-info 计入，会删除 child 尚未消费的 delta；
- create/drop rollback 若漏更新 `MViewIDs`，后续 DDL 或 purge 会遇到 dangling metadata。

这些风险必须由直接查询 MLog、metadata 和 failpoint 并发测试覆盖，不能只验证最终 SELECT
结果。

### 8.2 性能风险

parent 每次 FAST 或 COMPLETE DELTA APPLY 都会额外写 MLog。MLog 行数会随 parent group
变化数增长，update 产生 old/new 两行。这个成本是 nested FAST 的必要输入，不在本期通过
压缩、合并或自动 purge 优化解决。

COMPLETE IN PLACE 的全量 delete/insert 可能生成极大量 MLog，因此本期明确拒绝其在
MLog-bearing parent 上执行，而不是隐式产生不可预测的下游成本。

### 8.3 后续独立议题

- parent/child dependency-aware scheduler；
- child refresh freshness / revision contract；
- COMPLETE IN PLACE 的下游变更捕获设计；
- COMPLETE OUT OF PLACE 的原子依赖图、MLog 和 refresh-info 迁移；
- 任意深度链路的 operational observability；
- nested MV 的 optimizer rewrite。

这些议题不应阻塞本计划中的 direct MV-on-MV FAST 闭环，也不应在本期实现中以隐式行为
提前承诺。
