# 物化视图日志连续性与 OOP 刷新设计

**状态：设计草案，待讨论，尚未实现**
**日期：2026-10-13**
**更新日期：2026-10-14**
**适用分支：`nested_mv`**

本设计支持已有自身 MLog 或下游 MV 的物化视图执行 `COMPLETE OUT OF PLACE`。
OOP 成功后，下游仍可查询已有数据，但旧的增量消费基线失效；下游通过 COMPLETE
重新建立基线，再恢复 FAST。普通 COMPLETE 的基线恢复只更新运行状态，不增加 DDL 流程。

状态模型按 source 与 MV 的多对多关系设计。当前 SQL 仍只支持单 source，单 source
使用同一套协议的一条入边；未来多 source SQL 和增量计算算法不应要求重做失效与恢复机制。
本次不支持普通表 TRUNCATE，也不放开对 MV、MLog 内部存储对象的直接 TRUNCATE。

MLog 代际是通用机制，是否推进代际由操作语义决定。当前设计范围内，普通表尚不支持
会导致其 MLog 代际变化的操作；MV 的 COMPLETE OUT OF PLACE 会导致其自身 MLog
代际推进。普通表 DML、MV FAST 和 COMPLETE DELTA APPLY 均保持对应 MLog 的代际不变。

## 1 背景与设计边界

### 1.1 当前分支行为

[已交付的 nested MV 实施计划](mv_nested_implementation_plan_zh.md)记录了当前能力：

- ready MV 可以拥有 MLog，并作为另一 MV 的 source。
- FAST 和 COMPLETE DELTA APPLY 在同一事务中更新目标 MV 并写入其自身 MLog。
- 已有自身 MLog 的 MV 拒绝 COMPLETE IN PLACE；有自身 MLog 或 child 的 MV 拒绝 OOP。
- 依赖关系按对象 ID 保存，parent-first 刷新顺序由调用方保证。

当前拒绝 OOP 的主要原因有两项：全量替换没有产生下游所需的完整 delta；cutover 使用
shadow table ID 替换原 MV ID，尚未迁移其自身 MLog 和下游引用。
[cutover worker](../../../pkg/ddl/table.go)的入口为
`onRefreshMaterializedViewCompleteOutOfPlaceCutover`。

本设计是后续扩展，不改变已有实施计划对当前实现的描述。

### 1.2 本次范围

1. 支持受控 MV 写入下的 parent OOP、下游 FAST 失效及 COMPLETE 恢复。
2. 从一开始表达多个 source、多个 consumer，以及同时作为 consumer 和 source 的中间层 MV。
3. 迁移 cutover 涉及的结构依赖和运行基线，并保证 schema 与运行状态原子提交。
4. 将日志保留、刷新提交、initial build、drop、rollback 和升级纳入同一协议。
5. 保持现有 SQL 语法；不隐式将失败的 FAST 降级成 COMPLETE。

本次不实现多 source SQL、join 增量算法、自动级联、依赖图调度、严格的传递 freshness
保证或普通表 TRUNCATE。COMPLETE IN PLACE 在带自身 MLog 的 MV 上保持现有限制。

### 1.3 MV 写入受控是 OOP 的前置条件

parent OOP 持有目标 MV 的维护排他权，等待已有 refresh 数据事务结束后，阻止其他
FAST、DELTA APPLY 和会修改该 MV 数据的维护入口进入切换临界区。shadow build 不是
对 serving MV 的写入，也不向 serving MV 的 MLog 追加历史数据。

划分新旧日志代际前，需要确认对目标 MV 的旧 refresh 数据事务及其自身 MLog 写入已经
结束。切换完成后的写者必须重新解析新 MV 身份和 MLog 绑定。物理 MV ID 改变时，维护锁
与 schema 发布的交接也必须维持这个保证，不能仅假设使用旧 ID 的 advisory lock 会自动
覆盖新 ID。

以 `t -> mv1 -> mv2` 为例，`MLog(t)` 的直接 source 是普通表 t，随用户 DML 持续写入；
`MLog(mv1)` 的直接 source 是 mv1，随 mv1 的 refresh 写入。对 mv1 执行 OOP 时，
需要控制的是 mv1 与 `MLog(mv1)` 的写入，并推进 `MLog(mv1)` 的代际。上游 t 的 DML
及 `MLog(t)` 写入可以继续，`MLog(t)` 的代际不变，快照之后的变化仍供 mv1 后续 FAST 消费。

普通表有任意用户写事务；它们可能跨过 TRUNCATE 的切换边界。仅递增 generation
不能证明这些写事务及日志属于正确的 source 实例。因此普通表 TRUNCATE 不属于本次
支持范围，也不作为本设计已经解决的并发问题。

## 2 OceanBase 参考与 TiDB 的差异

参考版本为 OceanBase `v4.4.2_CE_BP2`，commit
`e9fc1e5f01f0cf7cc3a801af6c2c05c34d491d77`。

OceanBase 在 `__all_mview_dep` 的每条依赖记录中保存
`IS_COMPLETE_REFRESH_ONLY_FLAG`。切换时替换依赖的容器表 ID 并置位，FAST 检查该标记，
COMPLETE 成功时重建目标 MV 自己的依赖记录，使其再次支持 FAST。这提供了按消费者
独立恢复的语义。[标记定义](https://github.com/oceanbase/oceanbase/blob/e9fc1e5f01f0cf7cc3a801af6c2c05c34d491d77/src/sql/resolver/mv/ob_mv_dep_utils.h#L37)、
[切换时置位](https://github.com/oceanbase/oceanbase/blob/e9fc1e5f01f0cf7cc3a801af6c2c05c34d491d77/src/sql/resolver/mv/ob_mv_dep_utils.cpp#L604)、
[FAST 检查](https://github.com/oceanbase/oceanbase/blob/e9fc1e5f01f0cf7cc3a801af6c2c05c34d491d77/src/sql/resolver/mv/ob_mv_provider.cpp#L427)、
[依赖重建](https://github.com/oceanbase/oceanbase/blob/e9fc1e5f01f0cf7cc3a801af6c2c05c34d491d77/src/rootserver/mview/ob_mview_dependency_service.cpp#L197)

OceanBase 保留逻辑 MV ID，切换它的 `data_table_id`，并为新容器重建 MLog。容器、依赖、
刷新信息的切换属于同一个 DDL 事务；切换前也检查构建依赖的 schema version。
[OOP 实现](https://github.com/oceanbase/oceanbase/blob/e9fc1e5f01f0cf7cc3a801af6c2c05c34d491d77/src/rootserver/ob_ddl_service.cpp#L23856)

TiDB 当前的 MV ID 就是物理 table ID，OOP 会改变它。本设计沿用按依赖独立恢复的契约，
但使用日志代际与消费基线表达有效性；不将需要频繁确认的运行状态放进 `TableInfo`。
普通 COMPLETE DELTA APPLY 可以通过数据事务恢复入边基线，无需额外 schema 变更。

## 3 多对多模型与核心不变量

```mermaid
graph LR
    A[Source A] --> C[MV C]
    B[Source B] --> C
    A --> D[MV D]
    B --> D
    C --> E[MV E]
    D --> E
```

source 指普通物理表或 ready MV。边 `S -> M` 表示 M 的定义直接读取 S。
结构图中的一条边对应一条消费基线；同一 source 被多个 MV 使用时，每个 consumer
有自己的记录。同一 source 在 SQL 中出现多次，不以 SQL alias 重复建立消费基线。

本设计采用以下不变量：

1. `TableInfo` 是结构依赖的权威定义；运行基线表不能单独建立或删除结构依赖。
2. 结构边集合与运行基线记录集合一致；缺行、重复身份或无效绑定不能被 FAST 默认为有效。
3. 日志身份由 `(MLOG_ID, LOG_GENERATION)` 共同确定。新建 MLog 即使 generation 相同，
   也不能继承旧 MLog 的有效基线。
4. FAST 要求所有入边都具有当前日志身份的有效基线，且日志覆盖各自的消费区间。
5. 一次成功刷新原子提交目标数据、自身 MLog、全部入边基线和整体刷新成功信息。
6. COMPLETE 确认的是实际读取的 source 身份和日志代际；不能确认提交时新看到的代际。
7. OOP 原子切换对象身份、结构依赖、日志代际和基线记录的键。任一步失败都不能留下
   已切换的部分运行状态。
8. 维护排他权保护生产端写入；source 日志状态上的提交屏障保护 consumer 的基线确认。
   两者分别承担不同的并发责任。

### 3.1 有效性判定

对一条结构边 `S -> M`，定义：

```text
Current(S) = (source_id, mlog_id, generation, generation_start_tso)
Baseline(M, S) = (baseline_mlog_id, baseline_generation, read_tso)

Valid(M, S) =
    source / MLog 绑定一致
    AND baseline 各字段非 NULL
    AND baseline_mlog_id == Current(S).mlog_id
    AND baseline_generation == Current(S).generation
    AND read_tso >= Current(S).generation_start_tso
    AND 当前日志保留与 purge 完整性检查通过

CanFast(M) = 对所有结构入边 S -> M，Valid(M, S) 均成立
```

generation 匹配只能证明未跨过已声明的日志断点，不能代替 purge 完整性检查。
`CanFast` 也不表示所有祖先数据已经最新；parent-first 刷新顺序仍由调用方保证。

### 3.2 状态由基线推导

| 状态 | 判定 | 允许的恢复 |
| --- | --- | --- |
| 未建立基线 | 基线字段为 NULL | initial build 或 COMPLETE |
| 有效 | 日志身份、边集合和保留范围均匹配 | FAST 或 COMPLETE |
| 需要 COMPLETE | 代际、MLog 身份不匹配，或消费区间存在日志缺口 | COMPLETE |
| 元数据不一致 | 应有记录缺失、owner 不匹配或出现多余边 | 报一致性错误，先恢复有效结构与运行记录 |

不保存整个 MV 的 `REQUIRES_COMPLETE_REFRESH` 布尔值，也不通过手工修改 generation
匹配值来恢复 FAST。运行记录必须由实际的数据构建结果建立。

## 4 持久化布局

以下表名和字段是草案，尚未加入 bootstrap 或 schema model。

### 4.1 结构依赖继续保存在 TableInfo

现有字段已经按 ID 集合表达正反向关系：

- consumer 的 `MaterializedView.BaseTableIDs`。
- source 的 `MaterializedViewBase.MViewIDs`。
- source 的 `MaterializedViewBase.MLogID`。
- MLog 的 `MaterializedViewLog.BaseTableID`。

定义见 [model/table.go](../../../pkg/meta/model/table.go)，维护入口见
[create_table.go](../../../pkg/ddl/create_table.go)的
`updateMaterializedViewBaseInfoOnCreate`。本机制不另建一套结构图作为第二个权威来源。

### 4.2 新增每条依赖的消费基线表

```sql
CREATE TABLE mysql.tidb_mview_source_refresh_info (
    MVIEW_ID BIGINT NOT NULL,
    SOURCE_ID BIGINT NOT NULL,
    BASELINE_MLOG_ID BIGINT DEFAULT NULL,
    BASELINE_LOG_GENERATION BIGINT UNSIGNED DEFAULT NULL,
    LAST_SUCCESS_READ_TSO BIGINT UNSIGNED DEFAULT NULL,
    PRIMARY KEY (MVIEW_ID, SOURCE_ID),
    KEY idx_source (SOURCE_ID, MVIEW_ID)
);
```

主键支持读取一个 MV 的全部入边；反向索引支持枚举一个 source 的 consumer、迁移 source
ID 和计算日志保留范围。`SOURCE_ID` 是当前结构引用，`BASELINE_MLOG_ID` 是数据基线
实际确认过的日志，两者用途不同。OOP 改键时不能顺便把下游确认值改成新日志身份。

未完成 initial build 的记录保留三个 NULL 基线字段。建立或恢复基线时一起填写；字段
部分非 NULL 视为元数据不一致。consumer OOP 改变自身 ID 时，自己的入边记录也要改键。

每条边单独保存水位，使 future 多 source 算法不依赖“所有 source 永远共用一个字段”的
假设。当前单 source 和 COMPLETE 重算仍可使用现有统一读取快照。

### 4.3 扩展 MLog 运行状态

草案建议先复用 `mysql.tidb_mlog_purge_info`，增加以下字段，仅新增第 4.2 节的一张表：

| 新字段 | 用途 |
| --- | --- |
| `SOURCE_ID` | 当前 owner 身份的运行副本；必须与 MLog 的 `BaseTableID` 一致 |
| `LOG_GENERATION` | 当前日志代际，初始化为 1；断点时单调递增 |
| `GENERATION_START_TSO` | 当前代际的日志区间下界；初始兼容值为 0 |

这些字段统一用于普通表和 MV 的 MLog，不按 source 类型另设代际模型。本次定义的代际
推进操作是 MV OOP；普通表当前支持的操作不会触发其 MLog 的代际推进。

`SOURCE_ID` 的副本用于拒绝 schema cache 与日志运行状态不属于同一次切换的组合，
不能用它覆盖 `TableInfo` 的结构定义。generation 溢出时拒绝切换，不回绕。

`LAST_PURGED_TSO` 继续记录实际物理日志删除边界，不用它冒充 generation，也不在 OOP
时盲目清零。两者分别表示日志断点与日志保留范围。

复用 purge-info 会共享一行的锁粒度。purge batch 与刷新提交可能相互等待，这一取舍
列入第 13 节的讨论项；普通 source DML 不需要每行更新这条 generation 记录。
现有系统表定义见 [bootstrap.go](../../../pkg/session/bootstrap.go)。

### 4.4 整体 refresh-info 保留任务职责

`mysql.tidb_mview_refresh_info` 继续承担目标 MV 的刷新行锁、调度、整体成功时间与水位。
当前 `LAST_SUCCESS_READ_TSO` 与唯一入边水位同步提交；未来多 source 的 FAST 正确性
及 purge 保留范围从边记录判断，整体字段的汇总语义需要明确，不能代替逐 source 基线。

普通 COMPLETE 不改 `TableInfo`。OOP 本来就有 cutover DDL，在该事务中同时处理
运行状态；这不增加一个仅用于清除失效标记的 DDL job。

## 5 OOP 后的日志策略

### 5.1 草案优先保留 MLog 物理 ID

MV 写入受控的前提下，OOP 可以保留自身 MLog 的物理 ID、tracked columns、索引和
purge schedule，迁移其 owner 并推进 generation。保留 MLog 物理 ID 的方案是本草案
优先讨论的实现选择；OceanBase 重建 MLog 的方式是可比较的另一种选择。

旧日志可以继续物理存在，但不得被已确认新代际的 consumer 当成新的 delta。无需为每个
MLog 数据行新增 generation 字段，前提是第 5.2 节的生产端区间不变量成立。

### 5.2 切换区间下界

设：

- `R_build`：shadow build 实际读取上游数据的 TSO。
- `F`：实际执行 cutover 事务的 start TSO，候选来源为当前 worker 的事务 startTS。
- `C`：cutover commit TSO，用于描述时序，不要求预先知道其数值。

目标 MV 维护排他权已等待旧 refresh 数据事务结束，且期间没有对 serving MV 及其自身
MLog 的写入，所以该 MLog 的旧日志记录 commit TSO 均小于 `F`。这一条件不限制上游
普通表及其 MLog 的并发写入。切换后恢复的目标 MV 写者产生的日志 commit TSO 大于
`C`，也大于 `F`。在 cutover 同事务设置：

```text
LOG_GENERATION := old_generation + 1
GENERATION_START_TSO := F
SOURCE_ID := new_mv_id
```

因此当前代际的 delta 只来自 `commit_ts > F` 的区间。generation 不匹配的 consumer
先 COMPLETE，不读取旧日志来跨过断点。新基线的读取快照须在观察到已提交新代际之后
取得，所以新基线水位也晚于实际切换提交。

`R_build` 更新目标 MV 的上游消费基线，`F` 划分目标 MV 自己的输出日志。这两个时间
不能互换。也不能仅用 `read_tso >= F` 判定恢复成功：child 可以在 `F` 之后、`C` 之前
读到旧 MV，仍须用身份与 generation 验证它实际读取的基线。

如果无法证明旧写者已经结束，或者 worker 事务重试时不能维持上述区间，保留 MLog ID
的方案就不成立；应改用新 MLog 实例，不能以推进 generation 掩盖日志混用。

### 5.3 刷新模式与输出代际

| 目标 MV 的刷新方式 | 恢复自己的入边基线 | 自身输出 MLog |
| --- | --- | --- |
| FAST | 在有效代际内推进水位 | 写完整 I/U/D，generation 不变 |
| COMPLETE DELTA APPLY | 从完整重算结果建立全部入边基线 | 写实际差异，generation 不变 |
| COMPLETE OUT OF PLACE | 从 shadow build 建立全部入边基线 | 不补写全量 I/U/D，generation 递增 |
| COMPLETE IN PLACE | 无自身 MLog 时沿用既有行为并确认入边 | 带自身 MLog 时本次仍拒绝 |

即使 OOP 的最终行集没有变化，也按断点处理；本方案没有通过 diff 证明其输出历史连续。
DELTA APPLY 必须沿用本模式已有的 SQL 和 schema 限制，不承诺对任意定义都可用。

## 6 刷新协议

### 6.1 捕获 source 身份与读取快照

刷新捕获完整的入边集合，以及每条边的 `(SOURCE_ID, MLOG_ID, LOG_GENERATION)`。
`TableInfo`、MLog owner 和运行 `SOURCE_ID` 不一致时重新解析，不能拼接不同切换版本
的元数据。看到已提交的运行身份之后再取得本次 COMPLETE 数据读取快照，并固定
查询使用的 source 物理身份和必要 schema version。

该顺序保证读取快照不会早于已经观察到的日志代际切换。允许并发的上游普通 DML 仍使用
正常事务快照；本设计不会要求上游普通表在整个 refresh 期间停止写入。

OOP 将捕获的 source 令牌和 shadow build 实际读取 TSO 传入 cutover job。传入的
令牌要参与重试校验，不能在 worker 提交时直接替换成 source 最新令牌。

### 6.2 FAST

1. 获取目标维护排他权及现有 refresh-info 行锁。
2. 从结构元数据枚举全部 source，检查基线行集合与入边集合完全一致。
3. 检查各边的日志身份、generation、水位和 purge 完整性。
4. 在已捕获的区间中执行增量计划；数据及自身 MLog 写入仍在同一刷新事务内。
5. 执行第 7 节提交屏障，确认读取的 source 身份和依赖集合仍然有效。
6. 原子更新全部边水位、整体 refresh-info 并提交。

generation 不匹配时返回需要 COMPLETE 的错误，说明具体 source 和日志身份。
即使 FAST 的指定上界等于旧水位，也先校验基线有效性，再决定是否返回 no-op。
`FAST AS OF TIMESTAMP` 继续遵守既有区间限制；不允许利用历史上界绕过代际失效。

### 6.3 COMPLETE DELTA APPLY 与普通 COMPLETE

COMPLETE 可以从无有效增量基线的状态开始，但结构依赖和当前 MLog 绑定必须有效。
它读取全部 source 重新计算结果，并捕获实际使用的 source 令牌。

提交屏障通过后，在同一数据事务中一起提交：目标 MV 数据、自身 MLog 的行级差异、
全部入边的日志身份与水位、整体成功信息。任一步失败一起回滚；不先提交恢复状态再写数据。
基线字段使用本次实际读取的令牌，不从“最新 generation”生成恢复值。

同一个 parent 的其他 consumer 记录不受这次恢复影响。中间层 MV 通过 DELTA APPLY
恢复入边时，其自身 MLog 仍连续，这次恢复不会使其下游对应的消费边失效。

### 6.4 OOP build 与 cutover

build 阶段保持 shadow 受保护、不可作为合法 source，不改 serving MV 及其输出 MLog。
允许 child 对旧 serving MV 做 refresh；这样的基线可能在随后的 cutover 后失效。

cutover 在目标维护排他权仍有效的条件下，执行 source 提交屏障、对象 revision 校验和
第 8 节的完整迁移。目标自身入边改成 build 实际确认的基线，输出日志推进代际；所有
下游 consumer 的确认值保留原值，随后由有效性判定识别断点。

cutover 失败时旧 MV、旧代际和全部旧基线继续成立，清理 shadow；不能留下已递增的
generation 或已经改键的部分边记录。

## 7 并发协议与事务排序

### 7.1 提交屏障

仅在 refresh 开始检查 generation 不足以保证正确性。数据构建结束、确认新基线之前，
consumer 在自己的提交事务中对全部 source 的日志运行状态建立提交屏障：

1. 按 `MLOG_ID` 升序获取状态行保护，使用最新已提交状态进行校验。
2. 比较当前 owner、MLog ID、generation 与构建捕获值，校验目标及依赖集合的身份版本。
3. 所有校验通过后，写入全部基线和成功信息，持有保护直到数据事务提交或回滚。

普通 refresh 只在提交阶段持有 source 屏障，不在整个扫描或重算期间锁住所有 source。
源 OOP 的 generation 更新必须与这个屏障冲突，从而在数据库事务层建立先后顺序。
不能在独立系统 session 中做一次最新值查询，再无保护地提交数据。

实现可使用同一运行状态行上的事务锁或条件写冲突，但要验证 DDL KV 事务与 refresh SQL
事务保护的是相同 key，且保护覆盖最终提交。DDL helper 的具体 API 绑定是实现前的
验证项；“再读一次 generation”本身不构成提交屏障。

OOP 的保护集合包括其全部入边日志和自身输出日志，统一排序。先取得日志保护，再修改
消费基线记录；不能让另一条路径先锁边记录、再反向等待日志状态。保持目标 MV 已有的
维护互斥职责，generation 更新不逐个锁住所有 child 的整体 refresh-info 行。

### 7.2 多 source 与恢复竞争

```text
C COMPLETE 捕获 A:g1、B:g1，开始重算
B OOP 获得 B 的提交屏障，切换并提交 B:g2
C 提交时取得全部 source 屏障，发现 B:g2 != 捕获的 B:g1
C 回滚数据及全部基线，报告 source 已变化，可重新执行 COMPLETE
```

C 不能提交 A 的新基线并保留 B 的旧基线，也不能把 B 的确认值改成 g2。
如果 C 先取得屏障并提交，B OOP 随后推进 g2，C 的基线在 B 切换后失效，结果同样正确。
一次成功刷新确认完整的 source 集合；当前协议不提供单独恢复一条入边的操作。

### 7.3 多 consumer 独立恢复

```text
A OOP：A:g1 -> A:g2
C COMPLETE：确认 C<-A 的 g2 基线
D 尚未 COMPLETE：D<-A 仍确认 g1
```

C 可以 FAST，D 必须 COMPLETE。两者只在短暂的 A 状态行提交保护阶段发生竞争，
不会相互覆盖确认值，也不必一起重算。

### 7.4 多个 parent 同时 OOP

A、B 同时作为 C、D 的 source 时，A 和 B 更新各自日志代际。多个入边的失效不会压缩
成 C 或 D 的一个布尔值。共享 child 的 `TableInfo` 仍会被两个 cutover 修改，这部分
需要 DDL 涉及对象的排他调度与 worker 重读，避免用旧副本覆盖另一条 source ID 更新。

cutover 的 `InvolvingSchemaInfo` 必须覆盖实际修改的 source、目标、shadow、MLog
和直接 consumer。构建期间新增 MLog、增删 child 或依赖集合变化时，如果实际写集合
已经超出捕获和调度的对象集合，就拒绝本次 cutover 并重新规划；不静默漏迁移新依赖。

### 7.5 目标或 source 身份变化

consumer 构建捕获旧 source ID，parent OOP 已将边记录改键时，提交校验失败；consumer
不能用 UPSERT 重新创建旧 source ID 的记录。consumer 自己 OOP 期间 source 也 OOP，
同样拒绝使用旧 build 的结果确认新 source 身份。

跨节点 schema cache 暂未收敛时，运行 owner 与缓存的 MLog owner 不一致，应重读或返回
可重试错误。旧 MV 的锁释放前后、新 ID 上的写入入口以及 schema 同步需要一起验证。
现有 schema checker 和 MDL 可以参与身份保护，不能未经验证就代替日志代际屏障。

### 7.6 事务重试与崩溃

DDL 事务重试重新校验 owner、generation、revision 和写集合，并用实际重试事务计算 F。
上游 generation 已变化时，旧 shadow build 不可继续使用；需要重新构建数据。
重复处理已经完成的 cutover 不能再次递增 generation。

崩溃在 cutover commit 之前时全部迁移回滚；commit 之后即使历史记录尚未 finalise，
schema、日志状态和基线已经共同生效。恢复任务依据持久化 job 和对象身份确认结果，
不能用历史状态仍为 running 来重复切换或回退 generation。

## 8 Cutover 的完整迁移集合

假设目标 `M` 的旧 ID 为 `M_old`，shadow ID 为 `M_new`，拥有输出 MLog `L`。
下面所有变更属于同一个 cutover 事务：

| 对象 | 变更 |
| --- | --- |
| serving MV | 使用新 ID 承接名称、定义、ready 状态及 source ID 集合 |
| 全部上游 source | 反向 `MViewIDs` 中将 `M_old` 替换为 `M_new` |
| 目标的 `MaterializedViewBase` | 将自身 MLog ID 与下游 `MViewIDs` 迁至新 MV |
| 输出 MLog 的 `TableInfo` | `BaseTableID` 从 `M_old` 改为 `M_new`，保留日志定义 |
| 输出日志运行状态 | owner 改为 `M_new`，generation 递增，设置 F |
| 全部直接 child | `BaseTableIDs` 中将 `M_old` 替换为 `M_new`，保留其他 source |
| 目标的入边基线 | `MVIEW_ID` 改键到 `M_new`，确认 build 捕获的令牌与水位 |
| child 的消费基线 | `SOURCE_ID` 改键到 `M_new`，保留旧确认值与水位 |
| 整体 refresh-info | 迁移目标 ID 并更新 build 成功信息与 schedule |
| schema diff | 发布所有被修改的 TableInfo，不能只发布新旧目标 |

对象可能属于不同 schema，迁移时逐个解析所属 schema，不能把 source 和 child 一律当成
`job.SchemaID` 下的表。改键保留基线行数量和其他入边内容，冲突或多余记录视为一致性错误。

当前 [schema_version.go](../../../pkg/ddl/schema_version.go)的
`SetSchemaDiffForMViewRefreshOutOfPlaceCutover` 以及
[infoschema/builder.go](../../../pkg/infoschema/builder.go)的 cutover apply 路径需要覆盖
扩展后的 affected objects。只更新 meta KV 而不发布对应 TableInfo 会留下缓存中的旧引用。

运行状态写入沿用 [table.go](../../../pkg/ddl/table.go)中
`migrateMViewRefreshInfoForOutOfPlaceCutover` 的原则：通过 table API 写入同一个 DDL
事务，不使用会提前完成 statement staging 的独立 SQL UPDATE。新基线表是联合主键，
handle 的构造要依据其实际 schema，不能照搬 refresh-info 的 `kv.IntHandle` 假设。

## 9 日志保留与 Purge

### 9.1 从每条消费边计算保留水位

当前 `calcMaterializedViewLogSafePurgeTSO` 从整体 refresh-info 取多个 MV 的最小水位，
见 [materialized_view.go](../../../pkg/executor/materialized_view.go)。新机制改为按当前
source 的结构 consumer 集合，读取对应的边记录。

第一版采用保守的代际保留策略。设当前日志状态为 `(L, g, F)`：

```text
有效且确认 L:g 的消费边：保护水位 = 该边 LAST_SUCCESS_READ_TSO
尚未建立或已因断点失效的消费边：保护水位 = F
safe_purge_tso = min(purge transaction startTS, 所有保护水位)
没有 consumer / building consumer 时，沿用事务 startTS 上界
```

在第 5.2 节的无写入区间不变量下，`commit_ts <= F` 都属于旧日志区间，可以删除。
失效 consumer 从完整数据恢复，不再需要旧区间；保留 `commit_ts > F` 的全部新日志，
则覆盖它未来 COMPLETE 快照之后的变化。

这会让迟迟不恢复的 consumer 阻止新代际日志继续回收，是第一版明确接受的保留成本。
更细的进行中基线保护可以降低该成本，但需要另外设计持久化保护水位及失败清理，不能
简单忽略失效 consumer 来提高 purge 上界。

### 9.2 Initial build 与 COMPLETE 保护

initial build 在取得数据读取快照之前，先注册全部 source 的未建立基线记录，并沿用
building DDL job 的保护。COMPLETE 从旧有效基线恢复时，旧水位仍保护构建期间的日志；
从失效状态恢复时，F 保护整个新代际区间。成功后一次推进全部水位，失败不解除原保护。

对 public consumer，缺少应有基线行是元数据不一致，purge 失败；不能把缺行解释成没有
消费者。对合法 building consumer，NULL 基线表示需要保护。结构引用与运行基线集合
不一致时不能继续删除日志。

### 9.3 Purge 与 generation 切换并发

每个 purge batch 在已有日志行锁下同时读取 owner、generation 和物理删除水位。
generation 或 owner 变化后，旧 purge run 停止并重新解析保护集合，不能继续使用旧 source
ID 或旧 safe-purge 计算结果。cutover 与 purge 对同一状态行的修改建立事务顺序。

现有 `LAST_PURGED_TSO` 和 hazardous purge history 检查继续生效。历史记录建议携带
run 捕获的 generation，使旧 run 的 running / failed 记录不会错误归属到新代际。
物理删除水位的单调性仍需保留，不能因为开始新代际而否认已经发生的删除。

## 10 生命周期与用户行为

### 10.1 CREATE 与 initial build

注册结构依赖时一起创建 NULL 基线记录；initial build 验证 source 令牌并成功完成后，
确认全部入边。source 在 build 期间 OOP 时，以令牌和身份校验拒绝旧构建结果。
create rollback / cancel 删除本 MV 的运行记录和结构反向引用，不遗留日志保护。

### 10.2 DROP 与日志重建

DROP MV 删除自身入边记录；作为 source 时仍按已有下游依赖保护拒绝非法 DROP。
DROP MLog 保持既有 dependent MV 限制。本机制可以表达日志实例变化，但本次不因此
放开新的 DROP、重建或普通表 TRUNCATE SQL。

DROP DATABASE 清理该 schema 对象对应的运行记录；跨 schema 依赖继续遵守 DDL 限制，
不能仅删除记录来掩盖外部 consumer 的悬空引用。

### 10.3 错误和恢复

建议将以下错误原因区分展示，错误代码和最终文案在实现时确定：

- source 的日志身份或 generation 已变化，要求目标执行 COMPLETE。
- source 的日志保留范围已越过消费水位，要求 COMPLETE。
- source 在本次构建期间变化，本次刷新回滚，可以重新执行。
- 结构、owner 或运行基线不一致，报告元数据一致性错误。

报错标识具体 source、目标 MV、确认及当前日志身份，不只报告整个 MV 的布尔状态。
旧 MV 数据仍可查询；日志连续性失效不等于 initial build 未完成，也不修改 ready 状态。
自动调度的 FAST 同样报错并沿用现有失败观测，不擅自触发成本更高的 COMPLETE。

### 10.4 三级关系的恢复

```text
A OOP -> C<-A 失效
C COMPLETE DELTA APPLY -> C<-A 恢复，C 的输出 MLog 连续，E<-C 不因此失效
C COMPLETE OUT OF PLACE -> C<-A 恢复，C 的输出代际变化，E<-C 失效
```

失效沿发生日志断点的直接出边判断，不立即将整个后代图的日志代际都改掉。
E 依赖 C、D 时，全部入边有效才可 FAST。它仍不能从这个判定推断 C、D 的祖先已经最新。

## 11 升级与兼容性

旧版本既不检查 generation，也不维护边基线。仅创建新表不足以安全开启 parent OOP。
上线流程需要满足以下顺序条件：

1. 创建新表、扩展日志状态，并保持 parent OOP 的现有限制。
2. 所有可执行 MV 维护的节点具备新协议能力，排空仍在运行的旧协议任务。
3. 按目标 MV 的维护互斥或等价 CAS 回填边基线，随后所有刷新一起维护边记录与整体信息。
4. 完成元数据和基线一致性检查后，再通过集群能力门槛开放 parent OOP。

当前旧格式只支持单 source：在源和 MLog 绑定确定、整体水位非 NULL、且没有发生新机制
断点时，可以用既有水位建立 generation 1 的基线。仍需保留 purge 完整性检查，不能让
已经丢日志的 MV 因回填恢复 FAST。整体水位为 NULL 的对象保留未建立基线状态。

不能在旧节点仍会只更新整体水位时提前回填并启用边基线，否则 MV 数据已刷新而边水位
落后，会造成 delta 重复应用。对于未来无法从旧格式证明逐 source 基线的升级，不把
一个汇总水位直接复制成所有入边的有效基线。

滚动升级、旧协议任务排空和降级限制需要明确实现门槛。已经发生新机制断点后，禁止
忽略新字段的旧协议节点继续执行 FAST；具体能力门槛与恢复运维流程属于评审项。

## 12 实现边界与验收

### 12.1 建议的实现拆分

| 边界 | 涉及入口 | 可评审结果 |
| --- | --- | --- |
| 运行 schema 与状态读取 | session bootstrap、日志状态 resolver、新边记录 helper | 逐 source 身份、基线和集合校验 |
| 刷新数据事务 | executor refresh、`persistRefreshSuccess` | 数据与全体基线原子确认、提交屏障 |
| OOP 结构迁移 | ddl cutover、job args、schema diff、InfoSchema | 全部入边与出边改键、输出代际推进、失败回滚 |
| 保留与生命周期 | purge、CREATE initial build、DROP / rollback | 新旧日志区间与 building 基线安全 |
| 激活与兼容 | bootstrap upgrade、维护任务能力检查 | 旧水位回填、新协议激活顺序 |

新增接口之前先验证提交屏障、联合主键 table API 和 cutover staging 原子性。
现有 [刷新说明](mv_refresh.md)、[purge 说明](mv_log_purge.md)和
[initial build 状态](mv_init_build_state.md)随实现更新；本草案不表示这些路径已经完成适配。

### 12.2 当前 SQL 可执行的验收

- parent OOP 后 child FAST 报断点错误，child COMPLETE 后 FAST 恢复。
- 同一 source 的两个 child 先后恢复，确认其中一个不会恢复另一个。
- parent 多次 OOP，child 只确认实际读取的代际；OOP no-op 也产生断点。
- parent OOP 前后的 I/U/D 均进入正确日志区间，旧日志不会在恢复后重复应用。
- 中间层 DELTA APPLY 与 OOP 对下游连续性具有第 10.4 节的不同效果。
- FAST no-op / bounded FAST 不能绕过失效检查；purge hazard 检查仍有效。
- source / consumer ID 改键后正反依赖、MLog owner、边行集合与 InfoSchema 都一致。
- OOP 在每个迁移步骤后注入失败，schema、generation、基线均保持全旧或全新。
- create / rollback / DROP DATABASE 清理运行记录，不解除其他 consumer 的保护。
- 升级回填不会造成重复消费；旧节点及旧任务不能跨激活边界忽略边基线。

### 12.3 多对多机制验收

当前 SQL parser / validator 尚不接受多 source MV，机制层仍应以 `A、B -> C、D -> E`
构造多个 source 令牌和边记录进行事务测试。将来启用多 source SQL 时再增加对应 SQL
用例，不把机制测试推迟到 SQL 功能完成后。

至少覆盖：

1. C 确认全部 source 成功时全部边一起推进；任一确认失败，数据和全部边回滚。
2. A、B 同时 OOP，不丢失共享 child 的任一 source ID 迁移。
3. C COMPLETE 期间 A 或 B 切换，按第 7.2 节验证两种提交先后关系。
4. C、D 独立恢复同一个 source，不覆盖对方基线。
5. A -> C 与 B -> C 的水位不同，purge 各自使用对应边的保护水位。
6. 日志实例替换后相同 generation 数值仍被 MLog ID 区分。
7. source 集合变化、缺边、多余边及并发 drop 不被确认成成功基线。
8. 多个 source 锁按固定顺序获得，事务冲突与重试不绕过旧 build 令牌检查。

并发测试在 capture、数据构建、提交屏障及 cutover mutation 之间设置确定性的暂停点。
断言既包含行集，也包含基线向量、日志区间和对象引用；不以最终 SELECT 正确代替事务证明。

## 13 待讨论决策与实现风险

| 决策 | 草案倾向 | 需要确认的内容 |
| --- | --- | --- |
| 运行状态布局 | 新增一张逐 source 基线表，复用日志 purge-info 增加代际字段 | source 运行 owner 副本、字段名及锁粒度 |
| 输出 MLog | 受控写入下保留物理 ID 并推进代际 | 所有 writer 的静默区间、F 与事务重试；若不能证明则重建日志 |
| 提交屏障 | 多 source 日志状态上短暂的事务保护 | DDL KV 与 refresh SQL 的共同 key、锁序、死锁和重试行为 |
| 保留策略 | 失效或未建立基线的边保护到 F | 日志积累成本、现有 building job 与代际 history 的衔接 |
| 整体水位 | 当前单 source 保持原语义，正确性依据逐边水位 | future 多 source 汇总与可观测性定义 |
| 升级激活 | 所有 writer 支持新协议并完成回填后开放 OOP | 集群能力门槛、旧任务排空、降级行为 |

正确性风险集中在 source 令牌与读取数据不对应、普通 SQL UPDATE 提前提交 cutover
运行状态、漏迁移共享 child 的引用、错误确认新 generation，以及 purge 漏保护基线。
这些属于实现验收条件，不能仅用设计中的字段存在来宣称解决。

性能方面，边记录数量与结构边数相同，每次刷新更新自身入边，OOP 仍需要遍历实际迁移的
上下游引用。source 状态行的提交保护会短暂串行化多个 consumer，purge batch 也会竞争
同一行；应测量持锁时长与扇出成本，必要时拆分运行锁职责，但保持同一提交不变量。

本次讨论最终需要确认的是完整多对多状态模型及其事务协议。普通表 TRUNCATE 不属于
这组实现决策；它需要另行证明任意写事务与数据、日志实例切换的关系。
