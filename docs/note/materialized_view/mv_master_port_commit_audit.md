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
      8ca8747cbb3ced8e87f82d047b0e8f6f865566bd

head: xufei/cp_mv_for_master
      8d2633e8e55a6e7d09649e650df39f1c9f64a7f2

master: origin/master
        fe7ae3611c83bdf64c911d496ec0509ef63cb27a
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
| `5e8a1a229a7` | parser: port materialized view DDL syntax (#70744) | PR2a parser / AST / 语法 |
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
| PR2a | MV/MLog parser、AST、Restore、Digest 和语法测试 | 已进入 master |
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

## 全部 Commit 审计

| # | Source commit | 日期 | Subject | 主要最终语义 | Port 归属 | Master 对照和结论 |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | `f3af7d5a83fd` | 2026-02-05 | `pkg/meta: store mview/mlog dependency metadata in TableInfo (#66023)` | 在 `TableInfo` 中加入 base table、MV、MLog 之间的反向依赖元数据。 | PR2b-create/drop/alter；元数据基础 | **重构后覆盖**。依赖字段和 clone/metadata 逻辑已经随 #70789、#70874、#70927 进入 master；最终实现还包含 `DependentMViewIDs` 等后续设计，不能按原 patch 直接复制。 |
| 2 | `e0d58ec5ca8c` | 2026-02-06 | `pkg/session: bootstrap mview refresh/purge system tables (#66024)` | 首次 bootstrap MV refresh/purge 信息、历史和 alert 相关系统表，并补充 bootstrap 测试及 restore 行为。 | PR1 | **已覆盖**。最终系统表 schema 以 master #70599 为准；source 后续的字段、索引和命名 refine 必须合并看，不能只 port 这个早期版本。 |
| 3 | `96a16b284bc8` | 2026-02-06 | `*: add materialized view DDL syntax (#66022)` | 增加 CREATE/DROP/ALTER MV/MLog 语法、AST、Restore、关键字和 parser 测试。 | PR2a | **重构后覆盖**。master #70744 提供最终 parser/AST 基础；source 后续 parser spec refine 和 `NEVER REFRESH` 删除也要以最终 parser diff 合并判断。 |
| 4 | `d56cc857183c` | 2026-02-07 | `executor: support CREATE MATERIALIZED VIEW LOG (#66080)` | 实现 CREATE MLog 的 DDL dispatcher、job args、schema tracker、内部表构造和测试。 | PR2b-create | **已覆盖**。master #70789 已按当前 DDL framework 重写并覆盖 CREATE MLog。 |
| 5 | `6fb36360b1cd` | 2026-02-09 | `*: Cherry-pick pr-66089 into materialized_view branch (#66163)` | 大量 branch 同步内容，包含 `_tidb_commit_ts` extra column、planner/point-get/sample 适配以及大量通用测试结果变化。 | 混合 commit；commit-ts prerequisite；非 MV 测试 drift | **混合 commit**。MV 后续使用的 commit-ts 底层能力在 master 已有或由对应 slice 适配；大量 planner/testdata churn 不应整体 port。 |
| 6 | `1587a903f980` | 2026-02-12 | `table,executor: sync base-table DML to materialized-view log tables (#66204)` | base-table INSERT/UPDATE/DELETE 等 DML 生成并写入 MLog，新增 MLog table 实现和 writetest/integration test。 | PR3 | **已覆盖**。master #70941 已包含最终 MLog DML capture 逻辑和测试；source 旧测试文件组织不直接照搬。 |
| 7 | `b476f8f72604` | 2026-02-13 | `table, sessionctx: fix bug where mlog consumes reserved row IDs from base table (#66246)` | 修正 MLog 写入对 base table 保留 row ID/handle 的影响。 | PR3 | **已覆盖**。master #70941 包含对应 row ID 处理和回归覆盖。 |
| 8 | `581718f52f96` | 2026-02-14 | `planner, executor: support nulleq for IndexJoin and IndexHashJoin (#66017) (#66199)` | 为通用 IndexJoin/IndexHashJoin 增加 NULL-safe equality 支持。 | 通用 prerequisite | **master 已有等价 prerequisite**。该能力不是 MV 专属 port；MV 后续 planner 直接复用 master 实现。 |
| 9 | `6ac0a2fb86f5` | 2026-02-14 | `*: add aggregate function sum_int (#66085)` | 增加 `SUM_INT` 的 parser、expression、executor、tipb pushdown 和测试。 | 通用 prerequisite；PR5/快速刷新只适配 | **master 已有等价 prerequisite**。master 已有 #69457 等价能力，不重复移植 source 的旧 aggregate 实现。 |
| 10 | `f7f7e7f85cda` | 2026-02-15 | `pkg/ddl: include base table id for create mlog MDL table ids (#66266)` | CREATE MLog 的 metadata lock involving tables 加入 base table，保证相关 DDL 串行化。 | PR2b-create | **已覆盖**。master CREATE MLog 的 involving-table 逻辑已经包含 base table。 |
| 11 | `a76fec7f4d03` | 2026-02-15 | `executor: support CREATE/DROP MATERIALIZED VIEW (#66083)` | 初始 CREATE/DROP MV/MLog DDL worker、依赖元数据更新、清理和测试。 | 拆分到 PR2b-create、PR2b-drop | **重构后覆盖**。原 commit 已被拆成 master #70789、#70874；不能把该 commit 整体作为一个 PR port。 |
| 12 | `800fad39c987` | 2026-02-19 | `*: sync AGENTS/.gitignore and Bazel helper updates from master (#66308)` | 同步 AGENTS、`.gitignore`、Makefile/build helper 和开发流程内容。 | 非 MV | **非 MV，不纳入**。这些是 branch drift，不属于 MV 功能。 |
| 13 | `76ef4adbc110` | 2026-02-21 | `ddl, executor: fix CREATE MATERIALIZED VIEW validation (#66316)` | 修正 CREATE MV 的对象类型、查询和依赖 validation。 | PR2b-create | **已覆盖**。master #70789 的最终 CREATE validation 已覆盖该语义，并结合后续 DDL review 修复。 |
| 14 | `4bd68061a54b` | 2026-02-22 | `*: backport _tidb_commit_ts support for unistore cop request (#66325)` | 为 unistore/cop/rowcodec 等路径传递 commit timestamp，并加入独立测试。 | commit-ts prerequisite；PR4/PR5 使用时适配 | **部分覆盖**。master 已有底层 commit-ts metadata 和传递能力；普通 SQL 直接引用 `_tidb_commit_ts` 的策略不应由本 commit 单独推断，随 purge/refresh slice 处理。 |
| 15 | `0f22124d57d0` | 2026-02-24 | `ddl: allow SET TIFLASH REPLICA on MV-dependent base tables (#66328)` | 放宽 MV 相关表的 TiFlash replica placement DDL，同时保留其他 base-table DDL 约束。 | PR2b-create/drop/alter | **已覆盖**。该行为已经在 master 的 MV-related DDL constraint 中实现；实现形式按当前 master DDL API 适配。 |
| 16 | `7fe4ee1528fa` | 2026-02-24 | `ddl: prewrite mv refresh info before init build (#66330)` | 在 MV initial build 前预写 refresh-info 行，避免 build 阶段缺少系统表状态。 | PR2b-create；部分涉及 PR5 initial build | **部分覆盖**。CREATE MV 的 prewrite 和初始化元数据已随 #70789 覆盖；真正 refresh executor/runtime 的后续行为仍属于 PR5。 |
| 17 | `a2036935163f` | 2026-02-25 | `pkg/session, pkg/ddl, pkg/executor: update mview/mlog system table schemas (#66341)` | 更新 MV/MLog 系统表字段、类型及 DDL/executor 引用。 | PR1；PR2b 对最终 schema 的引用 | **重构后覆盖**。最终 schema 已由 master #70599 建立，CREATE/DROP/ALTER 代码使用 master 的最终列名和类型。 |
| 18 | `049564311187` | 2026-02-25 | `expression/aggregation: support sum_int in NewDistAggFunc (#66360)` | 为 `SUM_INT` 增加分布式聚合构造。 | 通用 prerequisite；PR5 适配 | **master 已有等价 prerequisite**。master 已包含对应 dist-agg 支持，不重复 port。 |
| 19 | `d5ca6127eea7` | 2026-02-25 | `session, executor: update mview system table schema (#66382)` | 继续调整 MV 系统表字段和初始化逻辑。 | PR1；PR2b/后续 runtime 引用 | **已覆盖 schema，runtime 部分待后续**。master bootstrap 使用最终 schema；refresh/service 对字段的运行时使用仍按 PR5/PR7 审计。 |
| 20 | `78096e182edc` | 2026-02-25 | `*: add aggregate function min_count/max_count (#66250)` | 增加 `MIN_COUNT`/`MAX_COUNT` 及 expression、executor、tipb 和测试。 | 通用 prerequisite；PR5 快速刷新适配 | **master 已有等价 prerequisite**。master 已有 #69642 等价能力，后续 MV operator 只适配现有实现。 |
| 21 | `e8829b004dcc` | 2026-02-26 | `pkg/ddl, pkg/executor: validate MV START WITH/NEXT expr type and remove defaults (#66383)` | 校验 schedule expression 类型，移除不明确的默认行为。 | PR2b-create/alter；PR5/PR7 schedule runtime | **部分覆盖**。CREATE/ALTER 的 metadata validation 已在 #70789/#70927 覆盖；运行时 schedule 求值和 service 调度仍待 PR4/PR5/PR7。 |
| 22 | `a36d83807816` | 2026-02-27 | `*: refine AGENTS.md style and wording guidance (#66556) (#66558)` | 调整 agent 文档风格和措辞。 | 非 MV | **非 MV，不纳入**。 |
| 23 | `b06308c62738` | 2026-02-27 | `ddl, executor: derive MV NEXT_TIME in create flow with safe eval session (#66555)` | CREATE MV 时使用安全 eval session 求值 `NEXT_TIME`/schedule。 | PR2b-create；PR5/PR7 运行时 schedule | **部分覆盖**。CREATE 路径的 schedule 初始化已在 #70789 适配；refresh/purge/service 的运行时 helper 不属于已合入的 DDL 主线。 |
| 24 | `58bba0d7a9cd` | 2026-02-27 | `pkg/ddl, pkg/parser, pkg/executor: support pre-split options for create materialized view (#66286)` | CREATE MV 支持 pre-split/scatter 等建表选项，并传入 physical table build。 | PR2b-create | **已覆盖**。master CREATE MV 已按当前 `createTableWithInfoPost`/建表流程适配。 |
| 25 | `c0cdf05369c5` | 2026-02-28 | `*: basic mv refresh support (#66385)` | 新增基本 refresh executor、refresh SQL、initial build 相关逻辑和测试。 | PR5 | **待后续 MV slice**。master 当前已具备 CREATE MV 所需的部分初始化 metadata，但完整 MV refresh executor 尚未作为本范围内的 master PR 合入。 |
| 26 | `3b8b6e4a7230` | 2026-02-28 | `planner, executor, sessionctx: reject explicit dml on mview / mlog tables (#66396)` | 拦截用户直接对 MV/MLog 的 DML，并覆盖 planner fast path 和 integration test。 | PR3 | **已覆盖**。master #70941 已包含显式写入拦截和相关测试适配。 |
| 27 | `e5fdd577b2a7` | 2026-03-02 | `*: Cherry pick topsql adding network field to feature branch (#66290)` | TopSQL 增加 network in/out bytes 统计及相关字段。 | 非 MV | **非 MV，不纳入**。该改动不属于 MV observability 的必要语义。 |
| 28 | `85f88a3ff5b3` | 2026-03-10 | `planner: block UPDATE/DELETE on mview/mlog via point-get fast path (#66805)` | 修正 point-get fast path 绕过 MV/MLog DML 禁止检查的问题。 | PR3 | **已覆盖**。master #70941 已覆盖 point-get 路径的限制。 |
| 29 | `7185487da7dd` | 2026-03-12 | `importer: add disk quota support for IMPORT INTO SELECT FROM (#66902)` | 通用 IMPORT INTO SELECT 的 disk quota 支持，同时被 MV initial build 的 import option 使用。 | 混合 commit；PR2b-create 的必要 hunk；通用 importer 部分不纳入 | **混合 commit**。通用 importer quota 不作为独立 MV port；CREATE MV 如需 thread/disk quota，则只适配对应的 MV build hunk。 |
| 30 | `6f309157d091` | 2026-03-17 | `*: support mv log purge (#66660)` | 实现 MLog purge executor、purge schedule、purge info、DDL dispatcher 和基础测试。 | PR4 | **待后续 MV slice**。当前 master 尚未合入完整 MLog purge。 |
| 31 | `c447836a4989` | 2026-03-19 | `planner: support fast refresh mview for count/sum (#66595)` | 增加 count/sum fast refresh planner、MV merge plan 和 planner casetest。 | PR5；S9 fast refresh planner | **待后续 MV slice**。通用 aggregate prerequisite 已在 master，但 MV fast-refresh planner 尚未合入。 |
| 32 | `ce48aec35f24` | 2026-03-25 | `table, executor: avoid deep-copying tracked MLog datums (#67160)` | 优化 MLog tracked-column datum 复制，降低 base-table DML capture 的开销。 | PR3 | **已覆盖**。master #70941 的最终 MLog 写入实现已包含相应数据处理方式。 |
| 33 | `bb1f16b69cb5` | 2026-04-10 | `planner: add min/max full-update lookup for mv refresh (#67146)` | 为 MIN/MAX fast refresh 增加 full-update lookup 和 planner 支持。 | PR5；S9 | **待后续 MV slice**。属于 refresh planner，不应因为 DDL 已完成而标记为已 port。 |
| 34 | `2e944507d529` | 2026-04-18 | `mvservice,domain,server,session,metrics: add materialized view service (MVS) framework (#66242)` | 新增 MV service、任务分配、backpressure、metrics、domain/server/session 集成和大量 service 测试。 | PR7 | **待后续 MV slice**。master 当前没有完整 MV service framework。 |
| 35 | `17ad85353d3e` | 2026-05-21 | `executor: implement MViewDeltaMergeAgg operator (#66326)` | 新增 MViewDeltaMergeAgg executor、merge count/sum/min/max、spill 和测试。 | PR5；S10 | **待后续 MV slice**。这是 refresh delta apply 的核心 executor，不属于已合入的 DDL/DML port。 |
| 36 | `e3d29c0b773d` | 2026-05-26 | `executor: add mview delta merge agg builder (#68636)` | 将 delta merge aggregate 接入 executor builder、planner 和 refresh 测试。 | PR5；S10 | **待后续 MV slice**。依赖后续 refresh planner/operator。 |
| 37 | `72d1670b394f` | 2026-05-27 | `planner/mview: clean up mvmerge naming (#68655)` | 将 `mvmerge` 等内部 planner/operator 名称统一为 mview 命名。 | PR5；S9/S10 命名 refine | **待后续 MV slice**。属于 refresh planner/operator 的内部命名，不能仅因 source 有 rename 就提前 port。 |
| 38 | `adc727df5a8b` | 2026-05-28 | `*: several ddl enhancement for mv (#68649)` | 大量 DDL worker、job args、schema tracker、multi-schema-change、依赖约束和测试增强。 | PR2b-create/drop/alter | **重构后覆盖**。最终语义已拆入 #70789、#70874、#70927；其中普通表 DDL 约束和 worker 侧 recheck 以 master 最终实现为准。 |
| 39 | `1cd7b66e493d` | 2026-05-28 | `OWNERS: Auto Sync OWNERS files from community membership (#67717) (#68706)` | 同步 OWNERS 和 OWNERS_ALIASES。 | 非 MV | **非 MV，不纳入**。 |
| 40 | `b21036c39400` | 2026-05-28 | `executor, parser, planner: support SHOW CREATE for materialized views (#68698)` | 增加 SHOW CREATE MV/MLog 的 parser、executor、planner 和测试。 | PR6 | **待后续 MV slice**。CREATE DDL 已进入 master，但 SHOW CREATE 用户可见功能不在当前已合入主线中。 |
| 41 | `d8a6eac392cc` | 2026-05-28 | `executor: several refinements for MV refresh (#68704)` | refresh executor/session 使用、结果处理和测试修正。 | PR5 | **待后续 MV slice**。其中若有 CREATE initial-build 共享 helper，只能按最终 hunk 适配，不能整体视为 DDL port。 |
| 42 | `25e037cb02e4` | 2026-05-29 | `expression: keep unary minus flen for signed int/decimal (#68723)` | 修正通用 unary-minus 的 field length 推导。 | 非 MV prerequisite candidate | **非 MV，不纳入**。除非后续 MV aggregate 证明存在直接依赖，否则不进入 MV port。 |
| 43 | `edbfd5121f7f` | 2026-05-29 | `executor, mvservice: use unsigned TSO for materialized view metadata (#68719)` | 将 refresh/maintenance metadata 中的 TSO 表示统一为 unsigned 语义。 | PR1 schema；PR5 refresh；PR7 service | **部分覆盖**。相关系统表字段/DDL 初始化已按 master 最终 schema 适配；refresh/service runtime 仍待 PR5/PR7。 |
| 44 | `74b10f298fa2` | 2026-05-29 | `executor, sessionctx/variable: add MV maintenance memory quota (#68736)` | 增加 MV maintenance memory quota 变量和 executor/service 使用。 | PR2b-create 的 build option；PR5/PR7 runtime | **部分覆盖**。CREATE initial build 所需变量可以随 #70789 适配；完整 refresh/service maintenance quota 仍待后续 slice。 |
| 45 | `f432536c5f34` | 2026-05-29 | `ddl: fix some bug in create mv/ create mv log (#68766)` | 修正 CREATE MV/MLog dispatcher、schema tracker 和 DDL validation。 | PR2b-create | **已覆盖**。master #70789 已包含最终行为和 review 后修复。 |
| 46 | `5f3246d2f28a` | 2026-05-30 | `ddl, mvservice: notify alter MV and MV log events (#68776)` | 增加 ALTER MV/MLog notifier event，并让 service 感知相关变化。 | PR2b-alter；PR7 | **部分覆盖**。DDL notifier 和 ALTER metadata 语义已随 #70927 覆盖；MV service 消费事件仍待 PR7。 |
| 47 | `ffbb0cef41b6` | 2026-05-30 | `executor: fix MV refresh internal-session usage collection (#68778)` | 修正 refresh 使用 pooled/internal session 时的资源统计。 | PR5；PR7 | **待后续 MV slice**。这是 refresh runtime/session observability，不属于已合入 DDL。 |
| 48 | `c0f0d1b212b4` | 2026-06-01 | `executor, parser: add show materialized views and show materialized view logs (#68808)` | 增加 SHOW MATERIALIZED VIEWS/MATERIALIZED VIEW LOGS。 | PR6 | **待后续 MV slice**。parser 语法和 SHOW executor 的用户可见行为尚未作为独立 slice 合入 master。 |
| 49 | `3cfcfc49d821` | 2026-06-02 | `ddl, parser: support materialized view alert attributes (#68810)` | 支持 MV alert attributes 的 parser、DDL metadata 和 notifier/service 引用。 | PR2b-create/alter；PR7 | **部分覆盖**。属性解析和 DDL metadata 已随 #70789/#70927 覆盖；alert checker/service runtime 待 PR7。 |
| 50 | `725e4f64ad7d` | 2026-06-02 | `*: backport mv metadata, import options, and refresh support fixes (#68849)` | 混合回灌 MV metadata、IMPORT options、DDL schema/job args 和 refresh 修复。 | 混合 commit；按 hunk 拆到 PR2b-create、PR5 | **混合 commit**。DDL/create hunk 已覆盖；refresh/runtime hunk 待 PR5；不能整体 cherry-pick。 |
| 51 | `c99ca71584bd` | 2026-06-02 | `*: add MV refresh observability and backport related fixes (#68774)` | 增加 refresh observability、结果/metrics/history、expression pushdown 和大量 refresh 测试。 | PR5；部分 PR7 | **待后续 MV slice**。bootstrap 字段/schema 可能已覆盖，但 observability executor/runtime 未进入 master。 |
| 52 | `259810a1abf8` | 2026-06-02 | `executor,mvservice: unify MV maintenance session vars (#68877)` | 统一 refresh/purge/service 的 maintenance session variables 和 restore 逻辑。 | PR4/PR5/PR7 | **待后续 MV slice**。当前 master 的 CREATE DDL 只保留其需要的变量适配，完整 maintenance session framework 尚未 port。 |
| 53 | `bdb304c4d378` | 2026-06-03 | `ddl, executor, planner: backport duplicate count expr support for nullable MV aggs (#68893)` | 修正 nullable MV aggregate 的 duplicate count expression，并更新 planner/executor/DDL 测试。 | PR5；S9/S10 | **待后续 MV slice**。属于 fast/complete refresh aggregate 语义。 |
| 54 | `f30199fba937` | 2026-06-03 | `*: support complete refresh out of place (#68894)` | 实现 out-of-place complete refresh、cutover、临时表、DDL worker、notifier 和测试。 | PR5 | **待后续 MV slice**。out-of-place cutover 与 refresh 强耦合，应随 refresh port，不单独归 CREATE/DROP。 |
| 55 | `7a3b5d9443e1` | 2026-06-04 | `ddl, mvservice: backport materialized view maintenance fixes (#68947)` | 混合 DDL worker 和 MV service maintenance 修复。 | PR2b；PR7 | **部分覆盖**。其中 DDL hunk 已在 master DDL PR 中覆盖，service/maintenance hunk 待 PR7。 |
| 56 | `6ba9bb6053d6` | 2026-06-05 | `parser, planner, executor, sessionctx: support full outer join (#68919)` | 通用 FULL OUTER JOIN parser/planner/executor/Mpp 支持及测试。 | 通用 prerequisite | **master 已有等价 prerequisite**。master 已有 #69999、#70185、#70462、#70562；MV compare/complete refresh 后续直接复用，不重复 port。 |
| 57 | `3892bda264c2` | 2026-06-10 | `planner, executor, parser, expression: support MV complete delta apply (#68994)` | 实现 complete delta apply、touched rows、delta merge、planner/executor/parser 接入。 | PR5；S9/S10 | **待后续 MV slice**。这是完整 refresh 的核心逻辑。 |
| 58 | `a2e127422a1d` | 2026-06-11 | `executor, parser, session, mvservice: support canceling MV jobs (#69102)` | 增加 refresh/purge cancel 语法、状态更新、controller/monitor 和 pooled session 生命周期处理。 | PR5；PR4；PR7 | **待后续 MV slice**。cancel 必须跟随实际 refresh/purge/service owner port，不能提前只 port parser 或 history 标记。 |
| 59 | `c7b499b9bc6b` | 2026-06-12 | `executor, planner, parser, ddl: support bounded MV fast refresh (#69117)` | 增加 bounded fast refresh 语法、planner、executor、DDL metadata 和测试。 | PR5；S9/S10 | **待后续 MV slice**。完整 bounded refresh 尚未进入 master。 |
| 60 | `8ecd2a11bb59` | 2026-06-12 | `pkg/ddl, pkg/executor, pkg/mvservice: fix MV privilege check and refactor execution vars (#69126)` | 混合修正 MV privilege check、DDL/executor/service execution vars 和测试。 | 独立权限闭环；PR2b；PR4/PR5/PR7 | **混合 commit**。OPERATE VIEW 权限闭环已由 #70694 覆盖；runtime execution-vars 部分随各 owning slice 适配。 |
| 61 | `09824b200e7e` | 2026-06-13 | `ddl, executor, parser, planner: refine materialized view initialization and refresh syntax (#69159)` | 混合包含 COMPLETE 语法、schema version、initial-build state gate、admin index 限制和 refresh 初始化修复。 | PR2a；PR2b-create/alter；PR5；PR7 | **部分覆盖**。parser/DDL metadata 和 initial-build gate 的必要部分已随 #70744/#70789/#70927 覆盖；refresh runtime、auto-analyze 和完整 service 行为待后续 slice。 |
| 62 | `9a30eedc0650` | 2026-06-14 | `*: MV refresh/purge result reporting, metrics, and history enrichment (#69137)` | 增加 refresh/purge 结果、metrics、history 字段和 bootstrap 表初始化。 | PR1；PR4；PR5；PR7 | **部分覆盖**。最终系统表 schema 的基础部分已进入 master；结果写入、history/metrics runtime 尚待后续 slice。 |
| 63 | `99948f2aac85` | 2026-06-14 | `*: cherry pick some non-mv related optimazations to mv branch (#69165)` | prepare dedup、plan cache、通用 session/expression 优化及大量测试结果同步。 | 非 MV；混合 branch drift | **非 MV，不纳入**。其中与 MV 无关的性能优化、Bazel/testdata 变化不进入 MV port。 |
| 64 | `fd53fbc6a7f2` | 2026-06-15 | `pkg/mvservice, pkg/session: add MV alert table and history cleanup (#69166)` | 增加 alert table、history cleanup、service 清理和系统表相关初始化/DDL 清理。 | PR1；PR7；部分 PR2b-drop | **部分覆盖**。系统表/schema 及 DROP 清理的必要部分已覆盖；service history cleanup 和 alert lifecycle 待 PR7。 |
| 65 | `37ff05b94b37` | 2026-06-15 | `mv,mvservice: make maintenance isolation read engines independent (#69172)` | 让 MV maintenance 的 isolation read engines 与普通 session 独立配置。 | PR4/PR5/PR7 | **待后续 MV slice**。属于 maintenance runtime/session 配置。 |
| 66 | `90447ef0160d` | 2026-06-15 | `executor, session: record MV refresh commit TSO (#69174)` | 记录 refresh commit TSO，用于 refresh info/history 和后续调度。 | PR5；PR1 schema | **待后续 MV slice**。字段初始化可以随 schema 适配，但 refresh commit TSO 的 runtime 写入尚未 port。 |
| 67 | `4a2bc184c38c` | 2026-06-15 | `executor: move MV log purge deletes out of pessimistic txn (#69183)` | 将 MLog purge delete 从 pessimistic transaction 中移出，调整执行和统计语义。 | PR4 | **待后续 MV slice**。属于 purge executor 的事务/性能实现。 |
| 68 | `9cef8d892f06` | 2026-06-15 | `executor, mvservice: throttle materialized view log purge (#69192)` | 增加 MLog purge 限流及 service 调度参数。 | PR4；PR7 | **待后续 MV slice**。purge executor 和 service 尚未合入 master。 |
| 69 | `fbbf288e1869` | 2026-06-16 | `ddl, executor, mvservice: impl mv attribute mview_alert_refresh_failed and enhance mysql.tidb_mview_refresh_alert (#69210)` | 增加 refresh-failed alert attribute、alert 表更新和 service 处理。 | PR2b-create/alter；PR5；PR7 | **部分覆盖**。attribute 的 DDL metadata 已覆盖；alert 表写入、检查和 service 行为待 PR5/PR7。 |
| 70 | `a534759aca3e` | 2026-06-16 | `executor, session: guard fast refresh against hazardous mlog purge (#69227)` | 在 fast refresh 前检测危险的 MLog purge 状态，避免增量数据已被清理。 | PR4；PR5 | **待后续 MV slice**。这是 refresh/purge 交互保护。 |
| 71 | `78e1033f4aa7` | 2026-06-16 | `ddl, executor: reject truncating mview-related tables with mlog guards (#69218)` | 拒绝会破坏 MV/MLog 依赖的 TRUNCATE/相关 DDL，并增加 worker 侧约束。 | PR2b-drop/alter | **已覆盖**。master 的普通 DROP/TRUNCATE/base-table DDL 约束已在 #70874/#70927 及后续 review 修复中覆盖。 |
| 72 | `ee3965416a3f` | 2026-06-16 | `planner, executor, ddl, session: support MV privilege model and show status commands (#69233)` | 混合增加权限模型、SHOW STATUS/状态输出、planner/executor/DDL 检查。 | 独立权限闭环；PR6；PR7 | **混合 commit**。OPERATE VIEW 权限已由 #70694 覆盖；SHOW/status 和 service 状态输出待 PR6/PR7。 |
| 73 | `9dfaa1bc4cd6` | 2026-06-17 | `ddl, executor: cherry-pick MV log DDL enhancements (#69247)` | MLog DDL 增强，包括 create/drop/alter 约束、job args、schema tracker 和测试。 | PR2b-create/drop/alter | **重构后覆盖**。最终 DDL 语义已分散进入 #70789/#70874/#70927。 |
| 74 | `ff53f54bb001` | 2026-06-17 | `mvservice: improve manual cancel backoff handling (#69265)` | 改进手工 cancel 后的 service backoff 和任务重新调度。 | PR7；PR4/PR5 cancel | **待后续 MV slice**。依赖完整 MV service/controller。 |
| 75 | `79156a515145` | 2026-06-17 | `infoschema, ddl: support materialized view metadata tables (#69271)` | 在 infoschema/DDL 中暴露 MV metadata tables，并增加读取/测试。 | PR6；部分 PR1/PR2b | **待后续 MV slice**。系统表 bootstrap 已进入 master，但 infoschema 用户可见读取尚未作为独立 slice 合入。 |
| 76 | `6d14c9de5201` | 2026-06-17 | `ddl, parser, mvservice: support ALERT ROWS for materialized view logs (#69278)` | CREATE MLog 的 alert rows 语法和 metadata，以及 service alert 触发。 | PR2b-create；PR7 | **部分覆盖**。CREATE MLog 的字段/解析已可随 #70789 适配；service alert 触发和监控待 PR7。 |
| 77 | `5018ef08bddd` | 2026-06-18 | `executor: support compare materialized view (#69297)` | 增加 COMPARE MATERIALIZED VIEW executor、SQL 和测试。 | PR6；FULL OUTER JOIN prerequisite | **待后续 MV slice**。FULL OUTER JOIN 底层已在 master，但 compare 功能本身未 port。 |
| 78 | `d2dc23f5c71a` | 2026-06-18 | `parser, ddl: allow clearing materialized view log purge schedule (#69280)` | 支持清除 MLog purge schedule 的 parser 和 ALTER DDL 行为。 | PR2a；PR2b-alter；PR4 metadata/runtime | **部分覆盖**。parser/ALTER metadata 已随 #70744/#70927 覆盖；purge service 对 schedule 变化的处理待 PR4/PR7。 |
| 79 | `2e489b053bc6` | 2026-06-18 | `executor: use baseQuery as build side for compare materialized view (#69312)` | 调整 COMPARE MV 的 join build side，降低/修正 compare 执行开销。 | PR6 | **待后续 MV slice**。这是 compare executor 的内部实现。 |
| 80 | `c4207360618b` | 2026-06-18 | `planner, executor: rename MV delta sides (#69307)` | 统一 MV delta apply 中 build/probe/old/new side 的命名。 | PR5；S9/S10 命名 refine | **待后续 MV slice**。随 refresh planner/executor 一起 port，不能单独当作 DDL 已覆盖。 |
| 81 | `f145795d551e` | 2026-06-19 | `ddl: forbid the creation of mlog for unsupported column type (#68997)` | 拒绝 MLog 复制 JSON、binary BLOB 等不支持的列类型。 | PR2b-create | **已覆盖**。master #70789 已包含最终列类型 validation。 |
| 82 | `e7a30c7889ba` | 2026-06-19 | `ddl: forbid the creating of mlog on partition base table (#68903)` | 拒绝对 partitioned base table 创建 MLog。 | PR2b-create | **已覆盖**。master CREATE MLog validation 已包含该限制。 |
| 83 | `4769ef44fab9` | 2026-06-20 | `ddl: forbid unique indexes on materialized view (#68911)` | 拒绝会破坏 MV 维护语义的 unique index 操作。 | PR2b-alter | **已覆盖**。master ALTER/index constraint 已覆盖该行为。 |
| 84 | `552472a43d33` | 2026-06-22 | `ddl: throw error when setting mlog and mv as target to create materialized view log (#68948)` | 拒绝将 MV/MLog 本身作为 CREATE MLog 的 target。 | PR2b-create | **已覆盖**。master CREATE MLog target validation 已覆盖。 |
| 85 | `b441fe3584eb` | 2026-06-22 | `test: cover materialized view log on generated column (#68082)` | 增加 generated column 与 MLog column capture 的测试覆盖。 | PR2b-create；PR3 | **部分覆盖/已覆盖语义**。CREATE MLog 的 generated-column validation 和 DML capture 已分别随 #70789/#70941 适配，测试位置可能已重组。 |
| 86 | `a8bc88b4d857` | 2026-06-22 | `ddl: forbid to set system table as mlog target and unify the error info for unsupported object types (#68969)` | 拒绝 system table 作为 MLog target，并统一 unsupported object 错误。 | PR2b-create | **已覆盖**。master CREATE MLog validation 已包含。 |
| 87 | `7c01d98e8ad6` | 2026-06-23 | `ddl, parser: support IF EXISTS for dropping materialized views (#69351)` | 增加 DROP MV/MLog 的 IF EXISTS parser 和 DDL 行为。 | PR2a；PR2b-drop | **已覆盖**。parser 由 #70744、DROP worker 由 #70874 覆盖。 |
| 88 | `d96dfd09a259` | 2026-06-23 | `ddl: avoid the upgrade of column type from TEXT to MEDIUMTEXT in MLog internal table (#69356)` | 防止 MLog 内部复制列在 DDL 过程中错误升级 TEXT 类型。 | PR2b-alter；PR3 相关 schema 同步 | **已覆盖**。master 的 base-table/MLog column synchronization 已按最终 DDL 逻辑适配。 |
| 89 | `0fd4796db0c4` | 2026-06-23 | `ddl: allow comment-only modify on materialized view base columns (#69391)` | 允许不改变数据语义的 comment-only base column 修改，同时保持 MLog 约束。 | PR2b-alter | **已覆盖**。master ALTER/base-table DDL constraint 已包含该特例。 |
| 90 | `597952c8226e` | 2026-06-23 | `ddl: limit COMMENT strings length when creating materialized view (#68944)` | CREATE MV 时限制 COMMENT 长度并产生正确 warning。 | PR2b-create | **已覆盖**。master CREATE validation 和测试已覆盖。 |
| 91 | `42d9a021010e` | 2026-06-25 | `*: slow query skip purge statement (#69462)` | 将 MLog purge 维护 SQL 从 slow query 统计/暴露路径中排除。 | PR4；部分 PR7 observability | **待后续 MV slice**。属于 purge runtime observability，不能随 DDL 一起标记完成。 |
| 92 | `2ad2da61a2ff` | 2026-06-28 | `mvservice: force status indexes for history cleanup (#69496)` | history cleanup 强制使用 status 相关索引，降低清理成本。 | PR7 | **待后续 MV slice**。依赖 MV service/history cleanup。 |
| 93 | `14ec59608231` | 2026-06-29 | `planner: bind MV MIN/MAX fast refresh lookup to supporting index (#69490)` | 将 MIN/MAX fast refresh lookup 绑定到支持索引。 | PR5；S9 | **待后续 MV slice**。属于 refresh planner 优化。 |
| 94 | `8aff9064f372` | 2026-06-29 | `executor: collect stats delta for mlog purge (#69510)` | MLog purge 后收集 statistics delta。 | PR4 | **待后续 MV slice**。属于 purge executor/statistics 维护。 |
| 95 | `6645850375d3` | 2026-06-29 | `statistics: skip MV log tables in auto analyze priority queue (#69519)` | 将 MLog 从 auto-analyze priority queue 中排除或改由专门调度处理。 | PR4；PR7 | **待后续 MV slice**。完整行为包含 statistics 和 service 调度。 |
| 96 | `3c11363cc32e` | 2026-06-30 | `planner: estimate mlog commit ts filter selectivity (#69445)` | 为 MLog commit-ts filter 增加 selectivity 估算和 planner stats。 | PR4；PR5；S9 | **待后续 MV slice**。依赖 MLog purge/refresh 查询计划。 |
| 97 | `46f2873a3cc7` | 2026-07-01 | `mvservice, variable: schedule mlog analyze in MV service (#69516)` | 由 MV service 调度 MLog analyze，并增加相关变量和 statistics helper。 | PR4；PR7 | **待后续 MV slice**。MLog analyze service 尚未 port。 |
| 98 | `190c9d508e3` | 2026-07-01 | `executor: avoid exposing materialized view cancel job state (#69555)` | 隐藏内部 cancel job 状态，调整用户可见结果和测试。 | PR4/PR5；PR7 | **待后续 MV slice**。依赖完整 cancel/controller/service 实现。 |
| 99 | `c9fd6ddba9ea` | 2026-07-02 | `executor: update error messages for materialized view log creation privileges (#69578)` | 修正 CREATE MLog 权限错误信息和对应测试。 | 独立权限闭环；PR2b-create | **已覆盖**。权限语义由 #70694、CREATE MLog 检查由 #70789 按 master 最终错误路径适配。 |
| 100 | `19544ab81d34` | 2026-07-03 | `planner: collect predicate columns for MV maintenance SQL (#69596)` | 收集 MV maintenance SQL 的 predicate columns，改善 stats/plan 选择。 | PR5；S9 | **待后续 MV slice**。属于 refresh/purge planner 运行时优化。 |
| 101 | `b1568c2e4a41` | 2026-07-09 | `metrics, mvservice: refine MV service metrics by component (#69737)` | 按组件细化 MV service metrics、reporter 和 Grafana。 | PR7 | **待后续 MV slice**。service framework 和 metrics 尚未完整进入 master。 |
| 102 | `17575377d73f` | 2026-07-12 | `mview: improve refresh observability and mlog purge delete (#69758)` | 混合修正 refresh observability、MLog purge delete、metrics、bootstrap 字段和测试。 | PR4；PR5；PR7；部分 PR1 | **待后续 MV slice为主**。最终 schema 的部分已覆盖；runtime observability/purge delete 尚待对应 slice。 |
| 103 | `06030b2809c0` | 2026-07-13 | `mvservice: refine MV refresh alert checking (#69808)` | 改进 refresh alert checker、过期/失败判断和 service 测试。 | PR7；部分 PR5 observability | **待后续 MV slice**。属于 service alert runtime。 |
| 104 | `7d7b687384f8` | 2026-07-13 | `executor,mvservice: avoid memory retention in MV maintenance SQL (#69778)` | 混合修正 MV maintenance SQL memory retention，同时回灌 DEPS、Makefile、cmd/mirror 等 branch drift。 | PR4/PR5/PR7 的 MV hunk；非 MV drift 不纳入 | **混合 commit**。MV runtime hunk 待后续 slice；DEPS/cmd/mirror/build helper 等通用变化不 port。 |
| 105 | `6910cef84061` | 2026-07-17 | `mvservice: fix refresh alert checker ownership (#69849)` | 修正 refresh alert checker 的 owner/server maintainer 生命周期。 | PR7 | **待后续 MV slice**。依赖完整 MV service。 |
| 106 | `f5dfdf58b9c9` | 2026-08-21 | `refine bootstrap (#73)` | 合并/简化 bootstrap version，删除旧 upgrade 路径中的重复 MV system-table registry 和大量历史测试。 | PR1 | **重构后覆盖**。master #70599 已有最终 bootstrap version 顺序和 registry；不应把旧 upgrade 函数机械搬到 master。 |
| 107 | `43c6999be1c9` | 2026-08-22 | `mv: use saved schedule timezone for unix seconds (#74)` | 保存 schedule timezone，按定义/session timezone 求值，再将 schedule 存为与 timezone 无关的 Unix seconds。 | PR1 schema；PR2b-create/alter；PR4/PR5/PR7 runtime | **部分覆盖**。CREATE/ALTER metadata 和 Unix-seconds 初始化已随 #70789/#70927 适配；purge/refresh/service runtime 仍待后续 slice。 |
| 108 | `c226d733626f` | 2026-08-22 | `executor, ddl, session: store MV refresh end time as Unix seconds (#75)` | 将 refresh end time 从 datetime 改为 Unix seconds，并调整 schema、DDL、refresh 写入和测试。 | PR1；PR2b-create/alter；PR5 | **部分覆盖**。最终系统表/CREATE 初始化已经使用 Unix-seconds 字段；refresh runtime 写入和历史/observability 仍待 PR5。 |
| 109 | `24eaea3deee7` | 2026-08-23 | `mview: unify maintenance timestamp names (#76)` | 统一 refresh/purge info/history 的时间字段命名，例如 snapshot/start/end/heartbeat/update。 | PR1；PR4/PR5/PR7 | **部分覆盖**。bootstrap 和已合入 DDL 引用使用最终命名；purge/refresh/service 代码和测试待对应 slice。 |
| 110 | `bf681b1b662a` | 2026-08-25 | `mview: unify materialized view naming (#77)` | 统一 `MV`/`MView`/`MLog` 在系统表字段、Go 类型、task/service 和 planner 中的命名。 | PR1；PR2b；PR4/PR5/PR6/PR7 | **部分覆盖**。master 已合入的 bootstrap/DDL/DML 路径已按最终命名适配；未合入的 refresh/purge/service/show 代码仍按后续 slice 处理。 |
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

