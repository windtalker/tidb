# Design: `PURGE MATERIALIZED VIEW LOG`

- Status: Ready for implementation
- Last updated: 2026-02-21

## 背景

TiDB 已经支持 Materialized View Log（下文简称 **MLog**）的建表语法，例如：

- `CREATE MATERIALIZED VIEW LOG ON <base_table> (...) [PURGE ...]`
- `ALTER MATERIALIZED VIEW LOG ON <base_table> PURGE ...`（当前实现中暂未支持执行）

MLog 作为 FAST refresh / 增量刷新能力的基础组件，通常会持续写入变更记录。如果缺少可控的清理（purge）机制，MLog 可能出现长期膨胀，带来存储和查询成本上升。

仓库中已经预留了用于记录 purge 状态/历史的系统表（在 bootstrap 时创建）：

- `mysql.tidb_mlog_purge`：记录每个 MLog 的最新 purge 状态（时间/行数/耗时等）
- `mysql.tidb_mlog_purge_hist`：记录 purge 历史（按 job 维度）

但目前缺少一个面向用户的“手动触发 purge”的 SQL 命令入口。

## 目标与非目标

### 目标

引入一个新的 SQL 命令，用于显式触发对指定 base table 对应的 MLog 的清理：

```sql
PURGE MATERIALIZED VIEW LOG ON <base_table>
```

并按分步交付方式推进，但整体流程与依赖已经明确，可以直接据此开始实现：

- 语句走 **DDL statement** 链路接入，但以 **local execution** 的方式执行（不提交 DDL job、不依赖 DDL owner；由接收该语句的 TiDB 节点直接执行）。
- 使用 `mysql.tidb_mlog_purge` 的行锁（`SELECT ... FOR UPDATE NOWAIT`）保证 **同一 `MLOG_ID` 的 purge 串行化**（跨 TiDB 节点）。
- 基于 `mysql.tidb_mview_refresh` 计算 **safe purge TSO**（覆盖“已存在 MV + 在建 MV”）。
- 删除 MLog 表中 commit ts 小于等于 safe purge TSO 的记录（commit ts 以 `_tidb_commit_ts` extra column 或等效内部实现获取）。
- 维护 `mysql.tidb_mlog_purge` / `mysql.tidb_mlog_purge_hist` 作为 purge 状态与审计面，并补齐必要测试。

### 非目标（v1）

- 不提供自动/周期性 purge（仅提供手动触发的 SQL 入口）。
- 不在 v1 做完整的性能工程与后台化（如分批/限速/异步任务/断点续跑等）；v1 以正确性优先的同步执行为主，后续再优化。
- 不在 v1 实现完整的 MV refresh 命令与执行引擎（`REFRESH MATERIALIZED VIEW` 仍属于独立工作项）；purge 仅依赖 MV 子系统写入的 watermark（`mysql.tidb_mview_refresh`）。

## SQL 语法（v1）

### 基本语法

```sql
PURGE MATERIALIZED VIEW LOG ON <base_table>
```

- `<base_table>` 使用现有 `TableName` 规则（支持显式 schema；省略 schema 时使用当前 DB）。
- 该命令定位的是 “base table 对应的 MLog 表”，而不是直接对 `$mlog$...` 物理表名操作。

### 错误与提示（v1）

v1 建议至少做到：

- 当前 DB 为空且 `<base_table>` 未指定 schema：返回 `ErrNoDB`（与现有 MV/MLog DDL 一致）。
- base table 不存在：返回 `ErrTableNotExists`。
- base table 存在但未创建 MLog：返回清晰错误（可复用现有文案风格，例如 `materialized view log does not exist for base table ...`）。
- 同一 `MLOG_ID` 上并发 purge：由于 `FOR UPDATE NOWAIT` 获取行锁失败，应返回清晰错误（提示“已有 purge 在运行，请稍后重试”）。
- `mysql.tidb_mlog_purge` 中缺失该 `MLOG_ID` 的主键行：视为系统表与元数据不一致，直接报错并中止（见后文“并发语义”章节）。

## 设计选型

### 放在 DDL 还是普通 executor？

建议优先按 **DDL statement** 接入（与 `CREATE/ALTER/DROP MATERIALIZED VIEW LOG` 对齐），但以 **local execution（不提交 DDL job）** 的方式实现：

- 优点：权限校验、语句分类、审计/trace 等路径更一致；同时不依赖 DDL owner，避免引入 DDL job 队列/owner 调度开销。
- 风险：如果 purge 最终更像 “DML 清理任务”，需要明确其并发/锁语义（是否需要 MDL、是否需要分批、是否需要后台任务化等）。

本设计明确：该语句 **由接收语句的 TiDB 直接执行**，而不是通过 DDL owner 异步执行。

### 权限模型（v1）

v1 选择：要求对 base table 具备 `ALTER` 权限（与 `CREATE MATERIALIZED VIEW LOG` 的权限策略对齐）。后续如引入后台任务化/自动调度，可再评估是否需要更强的管理权限。

## 前置依赖与假设（实现时需要满足）

- **`mysql.tidb_mlog_purge` 锁行存在性**：`CREATE MATERIALIZED VIEW LOG` 成功后必须插入一条 `mysql.tidb_mlog_purge` 记录（以 `MLOG_ID` 为主键）；purge 侧不负责补写该行，缺失则直接报错。
- **依赖 MV 列表可获取**：base table 元数据 `baseTableInfo.MaterializedViewBase.MViewIDs` 能正确反映已存在（Public）的依赖 MV IDs。
- **在建 MV 可被发现**：purge 需要能从 `mysql.tidb_ddl_job` 发现仍在执行中的 `CREATE MATERIALIZED VIEW`，并将其纳入 safe purge TSO 计算；为避免全量扫描 + decode `job_meta`，建议按 Milestone 0 增强 `table_ids`（写入 `mlog_table_id` 便于过滤）。
- **MV watermark 可读且语义稳定**：`mysql.tidb_mview_refresh.LAST_SUCCESSFUL_REFRESH_READ_TSO` 作为 MV 的消费 watermark；且在 MV init build 开始前写入一条记录作为初始 watermark（见后文“正在创建中的 MV”章节假设）。
- **MLog commit ts 可用于过滤**：purge 以行的 commit ts（`_tidb_commit_ts` extra column 或等效内部实现）作为删除过滤条件，按 safe purge TSO 删除多余数据。

## Purge 语义：Safe Purge TSO（覆盖“已存在 MV + 在建 MV”场景）

本节讨论 safe purge TSO 的推导思路，覆盖 **已经创建完成且对外可见（Public）的 MV** 以及 **正在创建中的 MV**，用于支持：

- “计算一个 MLog 的 safe purge TSO”
- “删除 safe purge TSO 之前的 MLog 数据”

v1 的删除语义明确为：在计算出 `safe_purge_tso` 后，删除 MLog 表中 **commit ts 小于等于**该 `safe_purge_tso` 的记录（commit ts 以 `_tidb_commit_ts` extra column 或等效的内部实现获取）。

后续可在不改变语义的前提下，将删除实现演进为分批/限速/后台化，以避免一次性大 DELETE 带来的长事务与资源抖动。

### 定义

对某个 base table 的 MLog，定义其 **safe purge TSO** 为一个 watermark：

- 当某条 MLog 记录的 commit ts **小于等于**该 safe purge TSO 时，说明这条记录所代表的变更已经被 **所有依赖该 base table 的 MV** 成功消费（apply），因此可以被清理。

核心点：**safe purge TSO 取决于“最慢的那个 MV”**。

### 依赖 MV 列表的来源（已存在 MV）

TiDB 的 base table 元数据里维护了依赖 MV 的列表：

- `baseTableInfo.MaterializedViewBase.MViewIDs`：依赖该 base table 的所有 MV table ID 列表

这份列表由 DDL 在 MV 创建/删除时维护（与 base<->mv 关联元数据一并更新），因此适合作为计算 safe purge 的索引入口，避免全局扫描所有表。

### 单个 MV 的已消费 watermark（已存在 MV）

对单个 MV，使用系统表 `mysql.tidb_mview_refresh` 中的字段来刻画其“已成功刷新到哪里”：

- `LAST_SUCCESSFUL_REFRESH_READ_TSO`：最近一次成功 refresh/初始 build 的 read TSO

设计假设（需要与后续 refresh 实现保持一致）：

- MV 在 `LAST_SUCCESSFUL_REFRESH_READ_TSO = T` 时，代表 MV 内容已经至少覆盖到 `T` 这一读快照对应的可见数据范围。
- 因此，对该 MV 来说，MLog 中 commit ts <= `T` 的变更应该已经被纳入 MV 的一致性边界内。

异常/降级处理建议（偏保守）：

- 对 **已存在（Public）的 MV**：预期每个 MV 在 `tidb_mview_refresh` 中都存在一条记录。若发现依赖的 Public MV 在 `tidb_mview_refresh` 中不存在记录，视为系统表与元数据不一致，purge 应直接报错并中止；若记录存在但 `LAST_SUCCESSFUL_REFRESH_READ_TSO` 为 `NULL`，则认为其 watermark 为 `0`（即不允许 purge 推进）。
- 对 **正在创建中的 MV**（通过 `mysql.tidb_ddl_job` 发现）：若在 purge 事务的 snapshot 下 **看不到**该 MV 在 `tidb_mview_refresh` 中的记录，则可将其视为“尚未开始消费”（不会限制 safe purge TSO）；若记录存在但 `LAST_SUCCESSFUL_REFRESH_READ_TSO` 为 `NULL`，则仍按 `0` 处理（保守：不推进 purge）。
- 若 `mysql.tidb_mview_refresh` 系统表缺失（异常集群状态），则 purge 应直接报错（因为无法正确计算 safe boundary）。

### 单个 MV 的已消费 watermark（正在创建中）

对 “正在创建中的 MV”（`CREATE MATERIALIZED VIEW` DDL 进行中），在 MV 初始 build 未完成之前，它还无法给出“已成功 refresh 到哪里”的真实信息；但 purge 仍需要将其纳入 safe boundary，避免 purge 推进到一个可能影响 build/后续 catch-up 的位置。

这里引入一个实现层面的约束/假设（需要与后续 `CREATE MATERIALIZED VIEW` 实现对齐）：

- 在 MV **init build 开始之前**，会在 `mysql.tidb_mview_refresh` 中为该 MV 写入一条记录；
- 且该写入必须在 init build 获取/使用 read TSO（记为 `T_build`）之前 **完成 commit**（形成明确的 happens-before 关系）；
- 该记录的 `LAST_SUCCESSFUL_REFRESH_READ_TSO` 记为 `T_init`，并保证：
  - `T_init` **小于**本次 init build 使用的 read TSO（记为 `T_build`），即 `T_init < T_build`。

在 purge 计算时，我们将该 MV 的 watermark 仍然定义为：

- `W(mv) = LAST_SUCCESSFUL_REFRESH_READ_TSO`

并采用以下规则：

- 若 purge 事务在其 snapshot 下能看到该记录，则将该 MV 的 watermark 视为 `T_init`（或后续 refresh 更新后的 watermark）。
- 若 purge 事务在其 snapshot 下看不到该记录，则可将该在建 MV 视为“尚未开始消费”，不限制 safe purge TSO。
  - 直观解释：由于该记录在 init build 开始前必须 commit，purge snapshot 看不到记录反向说明 `T_purge_start < T_build`，因此 purge 至多清理到 `T_purge_start` 仍是安全的。
- 若记录存在但 `LAST_SUCCESSFUL_REFRESH_READ_TSO` 为 `NULL`，仍按 `0` 处理（保守：不推进 purge）。

### MLog 的 safe purge TSO 计算（已存在 MV + 在建 MV）

给定 base table 的 MLog，设其依赖的 MV 集合为 `MVs`（包含已创建完成且 Public 的 MV，以及正在创建中的 MV；依赖 MV IDs 的汇总方式见后文“汇总依赖的 MV IDs（已存在 + 在建）”），每个 MV 的 watermark 为 `W(mv)`（来自 `LAST_SUCCESSFUL_REFRESH_READ_TSO`），则：

```text
safe_purge_tso(mlog) = min( W(mv) )  for mv in MVs
```

直观解释：只有当所有 MV 都已经至少成功刷新到某个 TSO 之后，MLog 才能安全地清理到该 TSO。

补充：若 `MVs` 为空（当前既没有依赖 MV，也不存在相关的在建 MV），则 v1 定义：

- `safe_purge_tso(mlog) = T_purge_start`
- `T_purge_start` 为 purge 事务的 start TSO（用于表达“无消费者时可以清理到当前事务开始时的边界”）。

工程实现上，可以对 “purge snapshot 下可见的 refresh 记录” 做聚合得到 `safe_purge_tso`（`xxx` 为汇总得到的 MV IDs 列表）。其中 `NULL` watermark 需要按 `0` 处理（保守：不推进 purge）。若聚合结果为空（例如仅存在在建 MV 且其 refresh 记录在该 snapshot 下不可见），则可回退为 `T_purge_start`：

```sql
SELECT MIN(COALESCE(LAST_SUCCESSFUL_REFRESH_READ_TSO, 0)) AS safe_purge_tso
FROM mysql.tidb_mview_refresh
WHERE MVIEW_ID IN (xxx)
```

### 如何发现“正在创建中的 MV”（信息源：`mysql.tidb_ddl_job`）

对于 “正在创建中的 MV”，关键前置是：需要能在 purge 执行时发现集群里存在 **进行中的 `CREATE MATERIALIZED VIEW` DDL**。

在 TiDB 实现中，正在排队/执行的 DDL job 会以记录形式存在于系统表 `mysql.tidb_ddl_job` 中，其 schema 在代码里定义为：

- `job_id`
- `job_meta`（`model.Job` 的 JSON 编码，包含 `RawArgs`）
- `type`（DDL action type 的数值编码）
- 以及 `schema_ids/table_ids/processing/...`

因此，从工程实现角度，`mysql.tidb_ddl_job` **可以作为发现“正在创建中的 MV”的信息源**：

1. 查询 `mysql.tidb_ddl_job` 过滤 `type = model.ActionCreateMaterializedView`（当前值为 `75`）。
2. 对每条记录读取 `job_meta` 并 decode 为 `model.Job`。
3. 进一步 decode job args（`model.GetCreateMaterializedViewArgs(job)`），即可从 `args.TableInfo.MaterializedView.BaseTableIDs` 得到其关联的 base table ID。
4. 结合 `job.State/job.SchemaState` 可判断是否仍处于执行中。

注意/限制（与 safe purge watermark 的精确计算相关）：

- 当前 `mysql.tidb_ddl_job.table_ids` 对 `ActionCreateMaterializedView` 只写入 MV 自身的 table ID，因此若要按 base table / mlog 精确过滤，通常需要 decode `job_meta`。
  - 计划增强：为提升 purge 对“正在建 MV”的发现效率，考虑在 `job2TableIDs` 中为 `ActionCreateMaterializedView` 额外写入关联的 MLog table ID（形成 `<mv_table_id>,<mlog_table_id>`），使 purge 能通过 `table_ids` 快速定位相关 job。
  - 约束：`table_ids` 会进入 `mysql.tidb_mdl_info` 用于 MDL 与长事务阻塞判断；**不建议写入 base table ID**，避免 `CREATE MATERIALIZED VIEW` 被 base 上的长事务频繁阻塞。只追加 MLog table ID 的风险更低（仍需确认普通 DML 事务不会把 MLog table ID 写入 `related_table_ids`）。
- `model.Job.SnapshotVer` 何时可见取决于 job 执行过程中的持久化更新节奏：在某些长耗时 reorg 场景中，进行中的 job 可能暂时看不到完整的 snapshot 信息。对 “正在创建中的 MV” safe purge tso 的精确规则需要单独定义（本节仅确认：能发现这些 job）。

### 汇总依赖的 MV IDs（已存在 + 在建）

当我们想计算某个 MLog 的 safe purge TSO 时，需要得到“所有依赖该 MLog/base 的 MV IDs”集合。可按两类来源汇总并去重：

1. **已创建完成（Public）的 MV**
   - 来源：base table 元数据 `baseTableInfo.MaterializedViewBase.MViewIDs`
2. **正在创建中的 MV**
   - 来源：`mysql.tidb_ddl_job`
   - 依赖 Milestone 0 的增强：`ActionCreateMaterializedView` 的 `table_ids` 包含 `<mv_table_id>,<mlog_table_id>`
   - 过滤方式：`type = ActionCreateMaterializedView` 且 `table_ids` 包含目标 `mlog_table_id`
   - 取 MV id：优先 decode `job_meta` 得到 `job.TableID`（更健壮，避免后续 `table_ids` 格式演进导致解析出错）；`table_ids` 只作为快速过滤条件

说明：

- 某些创建过程中的 MV 进入 Phase-1 后，可能已经被写入 `baseTableInfo.MaterializedViewBase.MViewIDs`，因此两路来源会有交集，汇总时需要去重。

## 并发语义：Purge 与 Purge（基于 `mysql.tidb_mlog_purge` 行锁）

本节仅讨论 **purge 与 purge** 的并发（同一个 MLog 被多次触发 purge），目标是在不引入全局 DDL job/owner 依赖的前提下，提供一个 **跨 TiDB 节点** 可用、实现成本低且语义清晰的互斥机制。

### 目标

- **同一 `MLOG_ID` 串行化**：任意时刻最多只有一个 purge 在执行（跨 TiDB 节点亦然）。
- **不同 `MLOG_ID` 可并发**：允许对不同 base table / 不同 MLog 的 purge 并行进行。
- **失败快速可重试**：当检测到冲突时不排队等待，直接返回“正在 purge”的错误，避免长时间占用资源。

### 核心思路

将每次 `PURGE MATERIALIZED VIEW LOG` 视为一个“本地执行的事务性任务”，并用系统表 `mysql.tidb_mlog_purge` 的 **单行行锁** 作为互斥锁：

- 每个 MLog 以 `MLOG_ID` 为主键在 `mysql.tidb_mlog_purge` 中对应一行；
- purge 事务开始后，先对该行执行 `SELECT ... FOR UPDATE NOWAIT` 获取行锁；
- 行锁持有到事务提交/回滚，确保同一 `MLOG_ID` 的 purge 串行化；
- `NOWAIT` 用于冲突时快速失败（TiDB 语法为 `FOR UPDATE NOWAIT`，而不是 `NO WAIT`）。

该机制的优点是：

- **不依赖 DDL owner**：符合本设计“local execution”的方向；
- **跨节点可用**：行锁由 TiDB/TiKV 事务层保证，不需要额外的分布式锁服务；
- **实现简单且可演进**：后续即使 purge 改为分批/后台任务，也可以复用同一把“逻辑互斥锁”来避免并发运行。

### 执行流程（单条 purge）

以下流程描述的是 purge 的 **并发控制 + safe boundary 计算 + 删除** 的最小闭环；其中 safe purge TSO 的计算细节复用上一章的定义（包含“已存在 MV + 在建 MV”）。

在一个 **悲观事务** 中执行：

1. **锁行必须存在（由建 MLog 写入）**
   - 约定：在 `CREATE MATERIALIZED VIEW LOG` 成功后，必须在 `mysql.tidb_mlog_purge` 中插入一行对应的记录（`MLOG_ID` 主键行存在即可，其余列可为 `NULL`）。
   - purge 侧不负责补写/修复该行，只依赖其存在性来完成互斥。

2. **获取互斥锁（冲突快速失败）**
   - 执行：`SELECT ... FROM mysql.tidb_mlog_purge WHERE mlog_id = ? FOR UPDATE NOWAIT`
   - 若返回行数为 0，说明系统表与元数据状态不一致（理论上不应发生），purge 可直接报错并中止。
   - 若返回 NOWAIT 错误（典型报错形态为 `[tikv:3572] ... NOWAIT is set`），说明该 `MLOG_ID` 已经被另一个 purge 持有行锁：
     - 当前 purge 直接返回用户可理解的错误（例如“another purge is running, please retry later”）。
     - 不进入后续的 safe purge TSO 计算与 delete，避免浪费资源。

3. **汇总依赖的 MV IDs（已存在 + 在建）**
   - 已存在 MV：从 base table 元数据 `baseTableInfo.MaterializedViewBase.MViewIDs` 获取。
   - 在建 MV：查询 `mysql.tidb_ddl_job`（并按前述建议利用 `table_ids` + decode `job_meta`）筛选出与当前 `MLOG_ID` 相关的建 MV job，并补充其 MV id。

4. **计算 `safe_purge_tso`**
   - 读取 `mysql.tidb_mview_refresh`，对依赖 MV IDs 做 `MIN(LAST_SUCCESSFUL_REFRESH_READ_TSO)` 聚合，得到 `safe_purge_tso`。
   - 缺失/NULL 按前述策略视为 `0`（保守：不推进 purge）。
   - 若依赖 MV IDs 为空，则按前文规则取 `safe_purge_tso = T_purge_start`。

5. **执行删除**
   - 执行形态（示意）：
     - `DELETE FROM <mlog_table> WHERE <commit_ts_col> <= safe_purge_tso`
   - `<commit_ts_col>` 以行 commit ts 表达为准（例如 `_tidb_commit_ts` extra column 或等效内部实现）。

6. **更新 purge 状态/历史（Milestone 3）**
   - 在同一事务内更新 `mysql.tidb_mlog_purge` 的 `LAST_PURGE_*` 字段，并向 `mysql.tidb_mlog_purge_hist` 写入一条历史记录（成功/失败都可落表，便于可观测性）。

7. **提交/回滚**
   - commit 成功：释放行锁，同时保证“删除 + 状态落表”原子一致。
   - 任一步失败：rollback，释放行锁，不产生部分写入。

### 讨论与边界

- 本节的锁仅解决 **purge 与 purge** 的并发互斥；purge 与 refresh/base DML 的一致性主要依赖 **safe purge TSO 规则** 的正确性与保守性（本设计已给出推导方式），后续可以补充更细的阻塞/性能分析与测试覆盖。
- 该互斥机制依赖 `mysql.tidb_mlog_purge` 中对应 `MLOG_ID` 的主键行存在；需要在建 MLog（以及升级/异常修复场景）时保证该行被创建/回填，否则 purge 将直接报错。
- 该互斥锁会被 delete 的耗时放大：如果单次 purge 很慢，同一 `MLOG_ID` 的其他 purge 将持续失败（NOWAIT）或等待（如果未来改为 WAIT）。这符合预期：同一 MLog 上同时跑多个 delete 通常只会产生更差的性能与更复杂的失败语义。
- 语法一次只 purge 一个 base table（对应一个 `MLOG_ID`），因此只持有单行锁，天然避免死锁；若未来扩展为一次 purge 多个 MLog，需要规定锁顺序（例如按 `MLOG_ID` 升序加锁）来规避死锁。

## 分步开发计划（建议按多个小 PR 推进）

### Milestone 0（前置）：补齐 purge 元数据与可发现性

目标：在实现 `PURGE MATERIALIZED VIEW LOG` 之前，先补齐其运行所需的前置元数据，避免 purge 侧实现“修复型兜底逻辑”，并保证并发语义与 safe boundary 计算可落地。

建议交付内容：

- **建 MLog 时插入锁行**
  - 在 `CREATE MATERIALIZED VIEW LOG` 成功后，必须插入一行 `mysql.tidb_mlog_purge`（`MLOG_ID` 主键行存在即可，其余列可为 `NULL`），用于 purge 并发互斥的行锁载体。
- **增强建 MV job 的可过滤性**
  - 对 `model.ActionCreateMaterializedView`，在 `job2TableIDs` 的返回值中包含：
    - MV table id（现有 `job.TableID`）
    - 该 MV 依赖的 MLog table id（即 base table 对应的 `$mlog$...` 表 id）
  - 不建议包含 base table id（原因：`table_ids` 会进入 `mysql.tidb_mdl_info` 用于 MDL/长事务阻塞判断，把 base table id 放进去会显著扩大被阻塞面）。

实现提示（具体落地可在代码阶段再细化）：

- 需要让建 MV job 的 args 能携带 `mlog_table_id`（例如扩展 `model.CreateMaterializedViewArgs`，或引入更通用的 related IDs 字段）。
- 增加单测：验证 `CREATE MATERIALIZED VIEW LOG` 完成后 `mysql.tidb_mlog_purge` 中存在对应 `MLOG_ID` 行。
- 增加单测，验证 `mysql.tidb_ddl_job.table_ids` 里确实是 `<mv_id>,<mlog_id>`。

最小验证命令（按需要挑选）：

```bash
go test ./pkg/ddl -run TestCreateMaterializedViewJobTableIDs -tags=intest,deadlock
```

### Milestone 1：Parser/AST 接入（只要能 parse/restore）

目标：SQL 能被 TiDB parser 正确解析、`Restore()` 输出稳定，且不会与现有 `PURGE BACKUP LOGS ...` 等语法冲突。

涉及改动（预期文件）：

- `pkg/parser/parser.y`
  - 新增 `PurgeMaterializedViewLogStmt` 产生式。
  - 将其加入 `Statement:` 顶层列表。
- `pkg/parser/ast/ddl.go`
  - 新增 `ast.PurgeMaterializedViewLogStmt`（`ddlNode`），实现 `Restore()` / `Accept()`。
- `pkg/parser/parser.go`
  - 由 `make parser` 重新生成（不要手工编辑生成文件）。
- `pkg/parser/parser_test.go` / `pkg/parser/ast/ddl_test.go`
  - 增加 parser case：`PURGE MATERIALIZED VIEW LOG ON t`、带 schema/反引号版本。

最小验证命令：

```bash
make parser_yacc
make parser_unit_test
```

### Milestone 2：Planner / 权限 / 语句分类接入（local execution 执行入口）

目标：走通从 “SQL -> plan -> executor(DDLExec) -> (local) handler” 的链路；权限校验可覆盖到；并实现 v1 的 purge 执行主流程（事务 + 行锁 + safe purge TSO + 删除）。

约束：该语句 **不提交 DDL job**，因此 **不需要 DDL owner**；执行节点就是接收 SQL 的 TiDB。

涉及改动（预期文件）：

- `pkg/planner/core/preprocess.go`
  - 设定 `stmtTp`（当前实现暂归类为 `TypeAlter`；TODO：后续再确认是否需要新增/调整 statement type 分类）。
- `pkg/planner/core/planbuilder.go`
  - 补齐 visitInfo（权限校验），并确保 `ErrNoDB` 等错误行为一致。
- `pkg/parser/ast/ast.go`
  - `GetStmtLabel()` 增加新 statement label（便于 metrics / stmt summary）。
- `pkg/executor/ddl.go`
  - 在 DDLExec `switch` 中分发新 AST。
- `pkg/ddl/executor.go`
  - DDL `Executor` interface 增加 `PurgeMaterializedViewLog(...)`。
- `pkg/ddl/materialized_view.go`（或新建文件，视代码组织）
  - 实现 purge 的本地执行（悲观事务 + `SELECT ... FOR UPDATE NOWAIT` 行锁 + 计算 `safe_purge_tso` + 执行删除）。
- `pkg/ddl/sanity_check.go`、`pkg/ddl/schematracker/*`
  - 让 schema tracker / sanity check 认识新语句（至少不 panic / 不误判）。

最小验证命令（按需要挑选）：

```bash
go test ./pkg/parser/... -run PurgeMaterializedViewLog -tags=intest,deadlock
go test ./pkg/planner/core/... -run PurgeMaterializedViewLog -tags=intest,deadlock
go test ./pkg/executor/... -run PurgeMaterializedViewLog -tags=intest,deadlock
```

说明：建议写回归用例锁定以下行为，避免后续迭代时语义漂移：

- 能 parse/restore，且权限校验（`ALTER` on base table）路径正确；
- 并发 purge 冲突时的错误行为稳定（NOWAIT 冲突直接报错）；
- `mysql.tidb_mlog_purge` 锁行缺失时直接报错（系统表与元数据不一致）。

### Milestone 3：状态落表（先落 `tidb_mlog_purge`，审计表后续补齐）

目标：补齐 purge 的可观测性与审计面，便于排障与运维，同时为后续性能优化（分批/限速/后台化）预留扩展点。

建议交付内容：

- 接入系统表：
  - 在 purge 成功/失败后，更新 `mysql.tidb_mlog_purge` 的最新状态（时间/行数/耗时）。
- 预留 failpoint（用于后续测试错误分支、重试语义等）。

TODO（后续补齐审计能力）：

- 接入 `mysql.tidb_mlog_purge_hist`：
  - 插入历史记录并维护 `IS_NEWEST_PURGE` 标记（对齐 refresh hist 的表设计习惯）。

验证建议：

- 单测覆盖：系统表不存在/权限不足/写入失败等分支（可参考 `mview_ddl_test.go` 里对 refresh 系统表的 failpoint 测试方式）。

### Milestone 4：执行方式与性能迭代（避免长事务）

目标：将当前“单语句 = 单事务”的 purge 执行方式改为“单语句 = 多事务分批删除”，降低大事务导致的 OOM/写放大风险，同时保持 safe purge 语义不变。

本里程碑先聚焦于“分批执行 + 语义稳定”，暂不引入后台任务化/checkpoint 表等复杂机制。

#### 设计决策（本里程碑）

- **执行 session 保持内部 session**
  - `PURGE MATERIALIZED VIEW LOG` 的实际删除事务继续在 DDL 内部 session 中执行（沿用当前 DDL 子系统模式）。
  - 用户 session 负责解析/权限校验/接收 warning，不直接承载 purge 的内部事务循环。
- **新增分批参数**
  - 增加 session/global 变量：`tidb_mlog_purge_batch_size`。
  - 建议默认值：`100000`。
  - 用途：控制每个 purge 子事务最多删除的行数。
- **safe purge tso 仅计算一次**
  - 在首个成功拿到锁的 purge 子事务中计算 `safe_purge_tso`。
  - 后续 batch 固定复用该值，不重新计算，不向前推进边界。
- **允许部分成功**
  - 一个 statement 中，如果至少有一个 batch 已成功删除并提交，后续 batch 因锁冲突中断时，statement 返回成功并附带 warning。
  - 如果一个 batch 都没成功提交就发生锁冲突，则返回 error。

#### 详细执行流程（建议实现顺序）

1. **入口阶段（用户 session）**
   - 解析 base table -> mlog 元数据（沿用现有逻辑）。
   - 读取当前 session 的 `tidb_mlog_purge_batch_size`。
   - 初始化 statement 级统计项：`totalDeletedRows`、`safePurgeTSO`、`batchCount`、`startTime`。

2. **batch 循环（内部 session，每批一个悲观事务）**
   - 开启新事务。
   - 执行 `SELECT ... FOR UPDATE NOWAIT` 获取 `mysql.tidb_mlog_purge` 行锁。
   - 若是首批：收集依赖 MV IDs 并计算 `safe_purge_tso`（逻辑与 M2/M3 一致）。
   - 若 `safe_purge_tso > 0`：执行带 `LIMIT` 的删除语句，例如：
     - `DELETE ... WHERE _tidb_commit_ts <= safe_purge_tso LIMIT batch_size`
   - 读取本批 `affected_rows`，累计到 `totalDeletedRows`。
   - 更新 `mysql.tidb_mlog_purge` 的最新状态（行数写累计值，耗时写 statement 级累计耗时）。
   - 提交事务。

3. **循环退出条件**
   - 本批 `affected_rows == 0`：表示无可删数据，结束。
   - 本批 `affected_rows < batch_size`：表示已到边界尾部，结束。
   - 本批 `affected_rows == batch_size`：继续下一批。

4. **错误与 warning 语义**
   - **锁冲突（NOWAIT）**
     - `totalDeletedRows == 0`：返回 error（与当前行为一致）。
     - `totalDeletedRows > 0`：向用户 session `StmtCtx` 追加 warning，返回成功。
   - **非锁冲突错误**（系统表缺失、safe tso 计算失败、delete 执行失败、状态表写失败）
     - 直接返回 error（即使已有部分 batch 成功，已提交部分不回滚）。

#### 语义与兼容性说明

- 该改动引入明确 tradeoff：statement 不再保证“全有或全无”。
- 但 purge 语义仍保持幂等推进：重复执行仅会继续清理 `safe_purge_tso` 之前尚未删除的数据。
- 在并发场景下，`MLOG_ID` 级别仍通过 `FOR UPDATE NOWAIT` 保证“每个子事务串行化”。

#### `tidb_mlog_purge_batch_size` 约束建议

- 变量类型：`GLOBAL | SESSION`，整型正数。
- 默认值：`100000`。
- 建议范围：`[1, 1000000]`（超范围按现有系统变量风格截断并给 warning）。
- 实现细节建议：
  - purge 执行时读取用户 session 的变量值，作为该 statement 的固定 batch size。
  - 即使执行发生在内部 session，也不在循环中动态变更 batch size。

#### 测试与验收建议（M4）

- **变量行为**
  - `set/show`、边界值、非法值、warning 行为与现有系统变量风格一致。
- **分批正确性**
  - 构造超过单批规模的数据，验证 statement 会发生多批提交，且最终删除完整。
- **锁冲突语义**
  - 首批即冲突：报错。
  - 首批成功、后续批冲突：statement 成功且 `show warnings` 可见提示信息。
- **safe tso 只算一次**
  - 通过 failpoint/观测点验证后续 batch 不重复计算 `safe_purge_tso`。
- **状态落表**
  - `LAST_PURGE_ROWS` 为 statement 累计删除行数，不是单批行数。
  - `LAST_PURGE_TIME` / `LAST_PURGE_DURATION` 在结束后可反映本次 statement 的最终状态。

#### 后续迭代（不在本里程碑内）

- 可中断/可重试（checkpoint 表或任务表）。
- 分批限速（sleep/令牌桶/负载反馈）。
- 更强可观测性（batch 级 rows/s、失败分类、耗时拆解）。

### Milestone 5：集成测试与文档补齐

在真实 purge 行为落地后，补齐更高层测试与用户文档：

- `tests/integrationtest`：端到端 SQL 行为与结果集验证。
- 补充用户文档（语法、权限、典型用法、查看 purge 状态/历史的推荐 SQL）。

## 风险与关注点（提前列出来，便于后续对齐）

- **正确性风险**：purge 规则若不严谨，可能导致 MV refresh 读取不到所需的增量数据，属于高风险语义变更。
- **元数据一致性风险**：purge 依赖 `mysql.tidb_mlog_purge` 的主键行作为互斥锁载体；若建 MLog/升级/异常修复时未能回填该行，会导致 purge 直接报错不可用。
- **性能风险**：purge 可能涉及大量数据删除，需避免长事务、热点写放大、对 TiKV/GC 的冲击。
- **可运维性**：需要可观测性（rows/s、耗时、失败原因）、可控的限速/分批策略、必要时可取消。

## 里程碑验收标准（每个 milestone 都应可独立合入）

- M1：parser/restore + unit test 通过；语法不会影响现有 `PURGE BACKUP LOGS ...`。
- M2：走通 DDL 链路、权限校验覆盖、错误信息稳定；且执行不依赖 DDL owner（不提交 DDL job）；新增/修改单测通过。
- M3：`mysql.tidb_mlog_purge` 状态落表可用（成功/失败均更新）；失败分支可测；基础可观测性具备；`mysql.tidb_mlog_purge_hist` 作为 TODO。
- M4：性能与可运维性有明确迭代路径（分批/限速/可重试），并有对应测试与压测结论支撑。
- M5：文档补齐、典型运维路径清晰（查看状态/历史、失败排查）。
