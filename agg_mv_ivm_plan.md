# TiDB 单表聚合 MV 增量计划生成设计

## 1. 目标与范围

本文聚焦“根据 MV query 与 MV log 生成增量更新计划”的设计细节，覆盖：

- 输入与元信息定义
- 增量规则库（Delta Rules）
- 计划生成流程（SQL/AST/Logical Plan）
- 语义与边界处理
 - 增量更新整体 plan（Plan B）

不涵盖：MV log 写入、MV 刷新调度、执行器实现细节。

## 2. 输入与元信息

### 2.1 输入对象

1. **MV query text**
   - 作为持久化输入，运行时解析为 AST。
2. **MV log table**
   - 认为 query 中基表对应的 MV log 即输入的 MV log。
   - MV log 物理上是普通 TiDB 表，包含 `op` 列（`I/D`）与必要投影列。
3. **MV table**
   - 目标物化视图表，用于应用增量结果。
   - **列顺序需与 MV 创建 query 的输出顺序一致**（执行器按顺序做映射）。
   - MV 仅允许刷新线程写入，禁止用户直接写入（避免 NULL 组的重复行问题）。

说明：在单表聚合场景下，query 已隐含基表与聚合元信息，因此不强制要求额外的 MV 元信息输入。MV log 物理上是普通表，但需要约定规范 schema 作为 IVM 的输入契约（包含 `op`、主键与必要投影列）。

### 2.2 逻辑计划生成策略

- 运行时对 query text 解析成 AST 并生成逻辑计划。
- 不持久化逻辑计划，避免版本不兼容与元信息漂移。

### 2.3 从 AST 提取关键信息（单表聚合）

在进入逻辑计划重写前，可基于 AST 做快速校验与信息抽取：

- `FROM` 只允许一个 `TableName`（可含别名）。
- `WHERE` 可选，保留表达式树用于列校验。
- `GROUP BY` 可选但若存在必须是列引用（或可解析为列引用的表达式）。
- `SELECT` 列表中仅允许 `group key` 与聚合函数。

抽取结果用于：

- 确定基表与别名、group key 列清单、聚合函数清单。
- 提前检查 MV log schema 是否覆盖所需列。

## 3. 增量规则库（Delta Rules）

### 3.1 基本定义

对原查询 Q 生成增量查询 ΔQ，使得：

```
MV_new = MV_old ⊕ ΔQ
```

其中 ⊕ 表示对聚合结果的合并操作。

### 3.2 Selection / Projection

- `σ_P(R)`：增量等价于对 MV log 应用同样的过滤条件 `P`。
- `π_cols(R)`：保持投影列，与 MV log 可用列对齐。

### 3.3 GroupBy + Aggregate

原查询：

```
SELECT G, AGG(F)
FROM T
WHERE P
GROUP BY G
```

增量查询：

```
SELECT G, AGG_DELTA(F, op)
FROM MV_LOG
WHERE P
GROUP BY G
```

其中 `op` 是 MV log 里的 INSERT/DELETE 标记（I/D）。

### 3.4 聚合函数增量规则

- COUNT: INSERT -> +1, DELETE -> -1
- SUM: INSERT -> +v, DELETE -> -v
- AVG: 维护 (SUM, COUNT)，最终 AVG = SUM / COUNT
- MIN/MAX: 需要辅助结构或回退为全量重算

### 3.5 现有实现的 delta 计算形态

当前实现直接把聚合改写成可执行表达式：

- `COUNT(x)` -> `SUM(IF(op='I', count_expr(x), -count_expr(x)))`
- `SUM(x)` -> `SUM(IF(op='I', x, -x))`

其中 `count_expr(x)` 复用现有 `COUNT` 的 NULL 处理语义。

### 3.6 MIN/MAX 的可行增量策略（允许多余回表）

在不引入 `min_count` 或辅助结构的情况下，可使用“安全但可能回表”的策略：

以 MIN 为例，定义：

- `old_min`：MV 表当前最小值
- `insert_min`：本批 INSERT 记录的最小值（无增量则为 NULL）
- `delete_min`：本批 DELETE 记录的最小值（无增量则为 NULL）

规则：

1. `old_min` 为 NULL：
   - `insert_min` 为 NULL → `new_min = NULL`
   - 否则 → `new_min = insert_min`
2. `old_min` 非 NULL：
   - `insert_min` 非 NULL 且 `insert_min < old_min` → `new_min = insert_min`
   - 否则：
     - `delete_min` 为 NULL → `new_min = old_min`
     - `delete_min > old_min` → `new_min = old_min`
     - `delete_min <= old_min` → 回表重算 `new_min`

说明：该策略在信息不足时宁可回表，保证结果 100% 正确。

## 4. 增量计划生成流程

### 4.1 总流程

1. 解析 MV query text，生成 AST。
2. 生成逻辑计划（Logical Plan）。
3. 执行 IVM 重写：用 MV log 作为数据源，替换基表扫描。
4. 对聚合节点应用 Δ 规则，生成增量聚合节点。
5. 生成 `LogicalMVApplyDelta` 作为根算子，应用增量结果。

### 4.2 关键重写点

- **数据源替换**：`TableScan(T)` -> `TableScan(MV_LOG_T)`
- **过滤复用**：保留 WHERE 条件，但需保证 MV log 包含相关列
- **聚合替换**：AGG -> AGG_DELTA（引入 op 维度）

### 4.3 逻辑算子级重写细节（贴近 TiDB）

以下以 TiDB 逻辑算子命名进行描述：

- `LogicalDataSource(T)` 替换为 `LogicalDataSource(MV_LOG_T)`。
- `LogicalSelection(P)` 保留，但需校验 `P` 中列都来自 MV log。
- `LogicalProjection(cols)` 保留，并追加 `op` 列供增量计算使用。
- `LogicalAggregation(GroupBy G, Aggs A)` 改写为：
  - 聚合目标替换为增量聚合表达式（通过 `op` 生成正负增量）。
  - group key 保持不变。

### 4.4 计划生成伪流程

```
Parse(queryText) -> AST
BuildLogicalPlan(AST) -> logicalPlan
ValidateSupport(logicalPlan, mvLogSchema)
RewriteToDeltaPlan(logicalPlan, mvLog, mvTable) -> mvApplyDeltaPlan
Optimize(mvApplyDeltaPlan) -> bestPlan
```

## 5. 语义与边界

### 5.1 UPDATE 处理

UPDATE 拆为 DELETE(old) + INSERT(new)，保证增量规则一致。

### 5.2 幂等与重复消费

增量计划本身不负责幂等，但需要依赖 MV log 消费水位（TSO）。

### 5.3 不支持表达式处理

若 MV query 中包含以下情况，应拒绝增量生成或降级为全量刷新：

- DISTINCT 聚合
- 子查询/窗口函数/复杂表达式
- 非确定性函数（如 RAND）

### 5.4 校验与拒绝清单（建议）

在 `ValidateSupport` 阶段做如下检查：

- 仅允许单表来源，禁止 join。
- GROUP BY 必须与 MV 表 schema 对齐。
- 聚合函数仅限 COUNT/SUM/AVG（MIN/MAX 可标记为不支持）。
- WHERE/表达式涉及的列必须存在于 MV log。
- 过滤条件中不得出现非确定性函数或外部依赖。

### 5.5 列映射与别名处理

单表场景推荐约定：MV log 中列名与基表列名一致，便于表达式复用。

- 如 query 使用表别名（`t.a`），在重写时统一绑定到 MV log 的对应列。
- 若 query 中有 `SELECT a AS x`，对增量计划无影响，但聚合表达式仍基于原列。
- 若存在表达式列（如 `SUM(a+b)`），需校验 MV log 包含 `a` 与 `b`。

## 6. 可扩展点

- DISTINCT 聚合：引入辅助计数表
- MIN/MAX：维护补偿结构（如分桶计数或局部索引）
- Join MV：在多表增量传播基础上扩展

## 7. 与现有模块接口

建议提供一个独立的接口：

```
GenerateDeltaPlan(
  queryText string,
  mvLog table.Table,
  mvTable table.Table
) -> LogicalPlan
```

输入由 MV 元数据模块提供，输出进入现有优化器路径。

## 8. Plan B：增量更新整体 plan

新增一个通用逻辑算子 `LogicalMVApplyDelta` 作为增量更新根节点：

```
LogicalMVApplyDelta(
  TargetTable, TargetInfo, TargetDBName,
  GroupByItems, AggFuncs, OpColumnName,
  GroupKeyTargetColIDs, AggMappings
)
  └── <delta logical plan>
```

该算子负责把增量结果合并回 MV 表，执行器可基于其元信息做 merge/upsert 和 drop empty。

`AggMappings` 按 MV 输出列顺序构建，不依赖列命名规则：

- 输出列 i 对应 MV 表列 i
- 通过 `agg.Schema()` 定位该列对应的聚合函数
- `first_row` 视为 group key
- `min/max` 通过同一 MV 列上的两次 agg（insert/delete）配对

## 9. 示例：逻辑计划到增量计划

原始查询：

```
SELECT c1, SUM(c2)
FROM t
WHERE c3 > 10
GROUP BY c1
```

逻辑计划（简化）：

```
LogicalAggregation(GroupBy: c1, Aggs: sum(c2))
  LogicalSelection(c3 > 10)
    LogicalDataSource(t)
```

增量计划（简化）：

```
LogicalMVApplyDelta(...)
  LogicalAggregation(GroupBy: c1, Aggs: sum(IF(op='I', c2, -c2)))
    LogicalSelection(c3 > 10)
      LogicalDataSource(mv_log_t)
```

备注：`LogicalMVApplyDelta` 是增量更新根算子，聚合已被改写为 `op` 驱动的正负增量表达式。

## 10. 物理计划选择与执行器复用

### 8.1 物理计划选择

增量计划的物理选择可以复用现有路径：

- `PhysicalTableScan` 或 `PhysicalIndexScan` 读取 MV log。
- `PhysicalHashAgg` 进行增量聚合。
- `PhysicalHashJoin` 不使用（单表场景）。

### 8.2 执行器复用与输出形态

- 复用现有聚合执行器产出 `(group_key, delta_value)`。
- 输出可直接供上层合并执行（MV 表 UPSERT）。

## 11. 失败与降级策略

- 不支持的 query：拒绝创建增量 MV 或标记为全量刷新。
- MV log 缺列：报错并提示重新建 MV log。
- 聚合规则未知：直接降级为全量刷新。
