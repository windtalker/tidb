# TiDB 单表聚合物化视图增量维护（IVM）设计草案

## 1. 背景与目标

TiDB 计划引入单表聚合（GROUP BY + 聚合函数）的物化视图（MV），并支持基于 MV log 的增量维护（Incremental View Maintenance, IVM）。该设计目标是：

- 以可工程化的方式在 TiDB 中落地单表聚合 MV 的增量维护能力。
- 通过规则化的增量计划生成，避免全量重算，提高更新吞吐与延迟。
- 兼容 TiDB 现有规划器、执行器与事务模型。

## 2. 范围与非目标

### 2.1 目标范围

- 单表聚合 MV：单基表、无 join、可带过滤条件（WHERE）。
- 聚合函数：COUNT/SUM/AVG/MIN/MAX（优先 COUNT/SUM/AVG）。
- 增量数据来源：MV log（记录基表行变更）。
- 增量计划生成：根据 MV query 自动生成增量更新 plan。
- 执行路径：支持批量增量处理（按事务或按定期窗口）。

### 2.2 非目标

- 多表 join MV 的增量维护。
- 复杂子查询、窗口函数、distinct 聚合（可后续扩展）。
- 跨表依赖的联级刷新与调度编排。

## 3. 总体设计概述

核心思路：对 MV query 生成对应的增量查询（Delta Query），对每批变更数据执行增量聚合，并把增量结果合并回 MV 表。

### 3.1 关键组件

1. **MV Log**
   - 记录基表的变更事件（INSERT/DELETE/UPDATE）。
   - 采用行级记录，包含必要的列投影（MV query 相关列）。
   - UPDATE 记录拆成 DELETE(old) + INSERT(new)。

2. **IVM Plan 生成器**
   - 输入：MV query text（或解析树 AST）、MV log schema。
   - 输出：增量更新 plan（Delta Query 逻辑计划）。
   - 基于增量规则（Delta Rules）对算子进行逐层转换。

3. **增量执行器**
   - 从 MV log 读出增量变更（按事务或时间窗口）。
   - 执行增量 plan，得到各 group key 的 delta 聚合。
   - 应用合并策略，将 delta 写回 MV 表。

4. **合并策略（Merge Policy）**
   - COUNT/SUM/AVG：按增量相加/相减。
   - MIN/MAX：需辅助结构或可降级重算（初期可选做法）。

## 4. MV Log 设计

### 4.1 基本结构

MV log 作为基表的变更日志表，记录必要列：

- 基表主键（便于去重与更新拆分）。
- MV query 中引用的列（投影）。
- 变更类型：INSERT/DELETE。
- 变更时间戳或事务时间戳（TS）。

### 4.2 UPDATE 拆分策略

UPDATE 拆为：

- DELETE old_row
- INSERT new_row

为保证一致性，MV log 的插入与基表变更在同一事务内提交。

## 5. 增量计划生成（IVM Plan Generator）

### 5.1 输入输出

- 输入：MV query text（或 AST）与 MV log schema。
- 输出：Delta Query 的逻辑计划，用于增量更新。

说明：逻辑计划不做持久化，增量计划生成在运行时把 SQL text 解析成 AST 并生成逻辑计划，再执行增量重写。

### 5.2 规则化增量转换

针对单表聚合的通用模式：

原查询：

```
SELECT G, AGG(F)
FROM T
WHERE P
GROUP BY G
```

增量查询（Δ）：

```
SELECT G, AGG_DELTA(F, op)
FROM MV_LOG
WHERE P
GROUP BY G
```

其中 `op` 表示 INSERT/DELETE，用于正负增量。

### 5.3 聚合函数增量规则

- COUNT: INSERT -> +1，DELETE -> -1
- SUM: INSERT -> +v，DELETE -> -v
- AVG: 维护 SUM 与 COUNT 的双字段
- MIN/MAX: 需要辅助结构或回退为重算（后续扩展）

### 5.4 过滤条件与投影

增量查询需与原查询保持相同的过滤条件（WHERE），且 MV log 需包含所有被引用的列。

## 6. MV 表结构与合并策略

### 6.1 MV 表结构建议

以 `(group_key, agg_state)` 形式持久化，建议包含：

- Group Key 列（与原 query 的 GROUP BY 一致）
- 聚合结果列（例如 sum_x, cnt_x, avg_x）
- 可选：最后刷新时间、版本或 TSO

### 6.2 合并策略（Merge）

当计算得到 `Δ` 后，对 MV 表做合并：

- 若 group key 存在，做增量加法或减法
- 若 group key 不存在，直接插入新行
- COUNT/SUM/AVG 支持线性合并
- MIN/MAX 初期建议不支持增量或使用补偿表

### 6.3 空结果处理

若合并后 COUNT 变为 0，则可选择：

- 删除该 group key 行
- 或保留并置为 0

## 7. 增量执行流程

### 7.1 执行阶段

1. 读取 MV log 中的变更批次（按事务或时间窗口）。
2. 构造并执行 Delta Query 计划。
3. 将增量结果合并写入 MV 表。
4. 标记 MV log 已消费进度。

### 7.2 事务一致性

- MV log 与基表更新在同一事务提交，保证变更完整。
- 增量刷新采用可重复读/快照读取，确保 log 处理一致性。
- MV 的更新可选择异步刷新，提供最终一致性语义。

## 8. 失败恢复与幂等性

### 8.1 幂等处理

增量处理需要记录消费水位（如 TSO）以防重复：

- 每次刷新记录最后已处理的 MV log TSO。
- 再次执行时按 TSO 过滤未处理的日志。

### 8.2 恢复策略

- 刷新失败时，保留 MV log，重试即可。
- 如果 MV 与基表严重不一致，可触发全量重建。

## 9. 与 TiDB 组件集成建议

### 9.1 Planner / Optimizer

- 在 MV 定义阶段持久化原 query text（可选保存 AST 与列/聚合元信息）。
- 在增量刷新阶段通过 rule-based 增量重写生成 Delta Query。
- 初期可采用独立的增量计划生成器模块，避免侵入主优化路径。

### 9.2 Executor

- 复用现有聚合执行器。
- 合并写入可走 UPSERT 或内部 merge 操作。
- 需要对 MV 表写入提供原子合并能力。

### 9.3 DDL 与元数据

- MV 元数据记录：query、基表、MV log 表、刷新方式（增量/全量）。
- 支持启用/停用增量刷新。

## 10. 版本与演进路径

### 10.1 第一阶段（MVP）

- 单表聚合（COUNT/SUM/AVG）增量维护
- 简单过滤条件
- 异步刷新 + 最终一致性
- MIN/MAX 暂不支持增量

### 10.2 后续阶段

- 支持 MIN/MAX 与 distinct 聚合（引入辅助结构）
- 支持多表 join MV
- 支持更精细的刷新调度与依赖管理

## 11. 风险与限制

- MIN/MAX 等非线性聚合需要额外结构或重算策略。
- MV log 需要额外存储与写放大。
- 延迟刷新期间查询可能读到旧数据。

## 12. 测试与验证建议

- 功能测试：INSERT/DELETE/UPDATE + GROUP BY 聚合。
- 并发测试：高并发更新下 MV 结果一致性。
- 容错测试：刷新失败重试与断点续跑。
- 性能测试：评估 MV log 写入与增量刷新成本。

