# 嵌套物化视图支持范围与工作量评估

本文记录在 `feature/release-8.5-materialized-view` 分支上讨论的嵌套物化视图
支持范围、现有实现的限制、预计工作量和后续 PR 拆分建议。

## 1. 目标定义

本阶段的目标是支持下面形式的嵌套物化视图：

```sql
CREATE MATERIALIZED VIEW mv1 (k, cnt, total)
AS SELECT k, COUNT(*), SUM(v)
FROM t
GROUP BY k;

CREATE MATERIALIZED VIEW LOG ON mv1 (k, cnt, total);

CREATE MATERIALIZED VIEW mv2 (k, cnt2)
AS SELECT k, SUM(cnt)
FROM mv1
GROUP BY k;
```

具体约束如下：

- `mv1` 和 `mv2` 都只支持当前已有的单表聚合能力。
- 每个 MV 的定义仍然只能引用一个 source object。
- source object 可以是物理 base table，也可以是已经完成 initial build 的 MV。
- nested MV 自己需要有对应的 MLog 才能作为下一级 MV 的 source。
- child MV 支持 `FAST` refresh。
- 本阶段不实现 parent/child MV 的 refresh 调度和顺序控制。
- 本阶段不实现 optimizer 将普通查询自动 rewrite 到 MV 的能力。

这里的 nested MV 是指 `mv2` 的 SELECT 直接读取 `mv1` 的物理表。它不是普通
View 的嵌套，也不是要求 TiDB 自动将用户对 base table 的查询改写成读取某个 MV。

## 2. Refresh 顺序边界

本阶段不修改 MVService，也不增加 parent/child refresh 的拓扑调度。

对于：

```text
base table -> mv1 -> mv2
```

调用方需要保证先刷新 `mv1`，再刷新 `mv2`：

```sql
REFRESH MATERIALIZED VIEW mv1 FAST;
REFRESH MATERIALIZED VIEW mv2 FAST;
```

如果先执行 `REFRESH mv2 FAST`，那么 `mv2` 只能消费当前已有的 `$mlog$mv1` 内容，
不能保证看到 base table 到最新状态之间的全部变化。这个限制不通过 scheduler
逻辑解决，但需要在用户文档、错误处理和测试中明确记录。

因此本阶段不包含以下内容：

- 自动保证 parent MV 先于 child MV refresh；
- parent refresh 完成后自动触发 child refresh；
- MVService 的依赖图调度；
- 跨节点 parent/child refresh 协调；
- parent refresh 失败时自动阻止 child refresh；
- parent/child refresh 的严格 revision 或 snapshot 一致性检查。

## 3. 当前实现可以复用的部分

### 3.1 MV 已经是可读的物理表

MV 的存储对象是带有 `TableInfo.MaterializedView` metadata 的物理表。initial build
完成后，普通 table reader 可以读取它，`CheckMViewReadable` 主要检查 initial build
是否完成。

因此，读取 `mv1` 的 SQL 本身不需要引入新的 parser 或 table reader 类型。

### 3.2 现有元数据可以表达直接依赖

`MaterializedViewInfo.BaseTableIDs` 保存 source object ID，source object 的
`MaterializedViewBase.MViewIDs` 保存直接依赖它的 MV ID。

这两个字段目前已经可以表达：

```text
mv1.BaseTableIDs = [base_table_id]
mv2.BaseTableIDs = [mv1_id]
mv1.MaterializedViewBase.MViewIDs = [mv2_id]
```

如果只支持单 source object，不一定需要新增元数据字段。需要做的是将现有代码中
“base table”的语义扩展为“physical table or materialized view source”。

### 3.3 单表聚合 validator 可以复用

当前 validator 已经限制了单表、`GROUP BY` 和有限的聚合函数，包括：

- `COUNT(*)` / `COUNT(1)`；
- `COUNT(column)`；
- `SUM(column)`；
- `MIN(column)`；
- `MAX(column)`。

对于 nested MV，`mv1` 的输出列也存在于 `TableInfo.Columns` 中。例如 `mv2` 的
`SUM(cnt)` 对 validator 来说仍然是对一个直接列做聚合。

## 4. 必须修改的实现

### 4.1 允许在 MV 上创建 MLog

当前 [pkg/ddl/executor.go](../../../pkg/ddl/executor.go) 中的
`isValidMaterializedViewLogBaseTable` 明确禁止 `tblInfo.MaterializedView != nil`。

需要允许已经完成 initial build 的 MV 作为 MLog source，同时继续禁止：

- 普通 View；
- Sequence；
- 临时表；
- MV shadow table；
- MLog table；
- 尚未完成 initial build 的 MV。

在 MV 上创建 MLog 时：

- `MaterializedViewLogInfo.BaseTableID` 指向 MV 的 table ID；
- MLog 的 tracked columns 从 MV 的输出列中选择；
- MLog 的列类型和列顺序仍遵循现有 MLog 规则；
- child MV 的 query validator 要求其引用的 parent MV 列出现在 parent MLog 中。

除 CREATE 之外，还要检查下面这些路径是否正确支持 MV source：

- `ALTER MATERIALIZED VIEW LOG ON mv1`；
- `DROP MATERIALIZED VIEW LOG ON mv1`；
- `PURGE MATERIALIZED VIEW LOG ON mv1`；
- MLog accumulation alert；
- `SHOW MATERIALIZED VIEW LOGS`；
- `information_schema.tidb_table_mview_dependencies`。

现有 MLog metadata 的 `BaseTableID` 命名可以保留，代码语义需要接受它也可能指向
一个 MV。

### 4.2 允许在带 MLog 的 MV 上创建新的 MV

当前 `CREATE MATERIALIZED VIEW` 会：

1. 从 SELECT 中提取一个 table name；
2. 查找对应的 `$mlog$<source>`；
3. 要求 MLog 存在；
4. 使用 source 的 metadata 和 MLog tracked columns 校验查询。

相关入口在
[pkg/ddl/materialized_view.go](../../../pkg/ddl/materialized_view.go) 的
`CreateMaterializedView` 和 `validateCreateMaterializedViewQuery`。

这部分需要调整为：

- source 可以是物理表或 MV；
- source MV 必须是 ready 状态；
- source MV 必须存在自己的 MLog；
- MLog 的 `BaseTableID` 必须等于 source MV 的 table ID；
- query validator 使用 source MV 的输出列做列解析；
- query validator 要求引用列被 source MV 的 MLog 跟踪；
- child MV 的 `BaseTableIDs` 保存直接 parent MV 的 ID。

DDL worker 中的反向依赖维护逻辑已经按 ID 列表操作，主要需要修正 source 类型校验、
状态校验和错误信息。`CREATE MATERIALIZED VIEW` 的 initial build 仍然可以复用
现有 `IMPORT INTO ... FROM SELECT` 路径，但必须确认 parent MV 已经 ready。

### 4.3 让 MV refresh 写入自身的 MLog

现有普通 INSERT/UPDATE/DELETE executor 会通过
`wrapTableWithMLogIfExists` 给带 MLog 的表增加写入 wrapper，相关代码在
[pkg/executor/builder.go](../../../pkg/executor/builder.go)。

但是 MV refresh 有专用写路径，并不是所有路径都会经过这个 wrapper：

- `MViewDeltaMerge` 通过 table ID 直接获取目标 MV；
- `MViewCompleteDeltaApply` 通过 table ID 直接获取目标 MV；
- complete in-place、complete out-of-place、initial build 的写入语义不同。

如果不修改这些路径，下面的操作会出现功能断裂：

```text
REFRESH mv1 FAST       -- mv1 数据更新成功
REFRESH mv2 FAST       -- $mlog$mv1 没有记录 mv1 的变化，mv2 无法消费 delta
```

至少需要：

- `MViewDeltaMerge` 的 target 使用 MLog wrapper；
- `MViewCompleteDeltaApply` 的 target 使用 MLog wrapper；
- 正确传递 refresh 产生的 old/new 行和 DML 类型；
- group key 变化时保持 delete/insert 或 update 语义正确；
- initial build 不写入无效的历史 MLog；
- 明确 complete refresh 是否也必须生成下游可消费的 MLog。

MLog 的 tracked columns 仍由用户在 parent MV 创建 MLog 时指定。例如 `mv2` 使用
`mv1.cnt` 和 `mv1.k`，那么 `$mlog$mv1` 必须至少跟踪这两列。

### 4.4 将 fast refresh source 泛化为 source object

当前 [pkg/planner/mview/mview.go](../../../pkg/planner/mview/mview.go) 的
`buildLocal` 和
[pkg/executor/materialized_view.go](../../../pkg/executor/materialized_view.go) 的
`resolveRefreshMaterializedViewLogInfo` 都假定 source 是“物理 base table + 该表的
MLog”。

嵌套 MV 需要把校验抽象成：

```text
source object 可以是 physical table 或 materialized view；
source object 必须有 MaterializedViewBase.MLogID；
MLog.BaseTableID 必须匹配 source object ID；
source object 的列必须覆盖 MV 定义需要的列。
```

对单表聚合而言，`buildMLogDeltaSelect` 和 delta aggregate merge 的主要算法可以
复用，因为它们读取的是 source MLog，并基于 source 的列做聚合。需要重点验证：

- `COUNT(*)`；
- `COUNT(column)`；
- nullable `SUM`；
- `MIN` / `MAX`；
- group key 变化；
- parent MV 行的 delete/insert；
- parent MV 的 primary key 或 handle；
- source MV 上用于 MIN/MAX 的索引检查。

### 4.5 父 MV 的 drop 和 MLog drop 保护

refresh 顺序可以由调用方保证，但不能允许删除仍被 child MV 引用的 parent MV：

```text
mv1 <- mv2
DROP MATERIALIZED VIEW mv1;
```

否则 `mv2.BaseTableIDs` 会指向已经不存在的 table ID。

因此需要：

- parent MV 存在 `MViewIDs` 时拒绝 DROP；
- parent MLog 存在 dependent MV 时继续拒绝 DROP；
- DROP DATABASE 清理 nested MV/MLog 时不留下 dangling metadata；
- drop/rollback/concurrent DDL 路径都维护 direct dependency metadata。

如果只支持单层嵌套，direct dependency 检查就足够。如果支持任意深度，则需要至少
对 drop 和 metadata 展示路径验证多级链路。

## 5. 不包含在本阶段的内容

本阶段明确不包含：

- MVService 的 parent/child refresh 顺序控制；
- 自动级联 refresh；
- 跨节点依赖调度；
- optimizer 的普通查询 MV rewrite；
- join、subquery、window function、多 source MV；
- 多 base table 的 fast refresh；
- 与 nested MV 无关的 refresh 性能优化。

对于 parent MV 的 refresh 方法，建议第一版先明确约束：parent 的 FAST refresh
必须能够稳定地产生下游 MLog。如果要让 complete in-place、complete out-of-place
也都能作为 nested FAST 的输入，需要额外设计它们的 change capture 和 cutover 语义，
这会扩大本阶段工作量。

## 6. 工作量估计

假设只支持一层或少量层数的 nested MV、单 source、单表聚合，并且不实现自动 refresh
顺序控制：

| 工作项 | 预计人日 |
| --- | ---: |
| 允许在 ready MV 上创建和管理 MLog | 3～5 |
| 允许使用带 MLog 的 MV 创建 child MV | 2～4 |
| MV refresh 输出写入自身 MLog | 4～7 |
| nested FAST refresh planner/executor 适配 | 2～4 |
| DROP/MLog purge/权限/错误路径 | 2～4 |
| 单元测试、executor 测试和生命周期测试 | 4～7 |
| **合计** | **15～25** |

如果第一版限制 parent MV 只通过 FAST refresh 产生下游 MLog，预计接近区间下限。
如果要求 complete in-place、complete out-of-place、COMPLETE DELTA APPLY 都能
产生完整且可消费的 parent MLog，预计接近区间上限或超过该范围。

如果后续要求任意深度、自动 refresh 调度或严格 revision 一致性，需要另行增加工作量，
预计总量会上升到约 25～40 人日。

## 7. 建议的 PR 拆分

### PR-A：MV MLog support

- 允许在 ready MV 上创建 MLog；
- 完善 MLog source 类型校验；
- 支持 MV source 的 alter/drop/purge 基础路径；
- 增加 MV MLog metadata 和权限测试。

### PR-B：Nested MV create

- 允许带 MLog 的 MV 作为 CREATE MV source；
- 复用单表聚合列校验；
- 建立 parent/child 反向依赖 metadata；
- 支持 nested initial build；
- 增加 `mv1 -> mv2` 的 CREATE 和 initial build 测试。

### PR-C：Nested MV fast refresh

- 让 parent MV 的 refresh 写入自己的 MLog；
- 让 `MViewDeltaMerge` 和 `MViewCompleteDeltaApply` 使用带 MLog 的 target；
- 让 child MV 消费 parent MV 的 MLog；
- 增加 count/sum/min/max、old/new、group key 变化等测试。

### PR-D：Dependency safety and regression tests

- 禁止删除仍有 child MV 的 parent MV；
- 验证 DROP DATABASE 和 rollback 清理；
- 验证 MLog purge 不会违反直接 dependent MV 的保留要求；
- 增加并发 DDL、drop MLog、权限和完整生命周期测试。

## 8. 结论

在不实现 refresh 顺序调度的前提下，嵌套 MV 的最小闭环是：

```text
MV 可以拥有 MLog
    -> MV 可以作为另一个 MV 的 source
    -> parent MV refresh 时写入自己的 MLog
    -> child MV FAST refresh 消费 parent MV 的 MLog
```

parser 基本不需要新增语法，真正的核心工作是 MV refresh 的输出变更捕获，以及将
当前“物理 base table + MLog”的 fast refresh 实现泛化为“source object + MLog”。

本阶段预计为中等偏大的功能，建议先按 PR-A/PR-B/PR-C/PR-D 拆分，并把 parent 先
refresh、child 后 refresh 作为明确的调用方前置条件。
