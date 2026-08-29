# MV Master Port Tracking

本文档记录把固定 source diff 的 MV 相关最终改动 port 到 `master` 时，PR 应该如何拆分。

这里的核心原则不是按源 commit 顺序切，而是按**最终 diff 的语义边界**切：

- 一个 PR 对应一个最终可解释、可验证的功能面。
- PR 的边界以最终行为为准，不以原始 commit 为准。
- cancel、observability 之类的跨切关注点，跟随它真正服务的 owning PR。
- 如果一个功能簇仍然太大，再按最终行为边界继续细拆，不能退回到 commit-by-commit port。

source commit 的新增或修正只用于更新最终 diff 边界，不直接对应一个 port PR。
例如 bootstrap 合并之后的 schedule timezone、Unix seconds 和命名 refine 改动，
需要按最终影响的系统表、DDL、refresh、purge 和 service 功能面拆到对应 PR。

## Source 边界

```text
base commit: f08c648a20380ea723449c6c3eb5b171d96fd567
head commit: 9439fdfa65e065e838559f5f5f9429c661072852

source range:
  f08c648a20380ea723449c6c3eb5b171d96fd567..9439fdfa65e065e838559f5f5f9429c661072852
```

当前 source diff 规模：

```text
500 files changed, 97367 insertions(+), 25104 deletions(-)
```

后续 inventory 和 review 都以固定 commit range 为准：

```bash
git diff --name-status f08c648a20380ea723449c6c3eb5b171d96fd567..9439fdfa65e065e838559f5f5f9429c661072852
git diff --stat f08c648a20380ea723449c6c3eb5b171d96fd567..9439fdfa65e065e838559f5f5f9429c661072852
```

### 新增 source commits

相对原 tracking 文档的 head `6910cef840612ee85e171adb19d3e427697a65da`，新增以下 6 个 commit。
它们已经纳入上面的最终 source range，但 port 时仍然必须按最终 diff 的语义边界拆分。

| Commit | 最终改动 | 主要 port 归属 |
| --- | --- | --- |
| `43c6999be1` | 保存 refresh/purge schedule timezone，并按该 timezone 求值后转换为 Unix seconds；ALTER schedule 只在提供 schedule expression 时更新 timezone | PR1 的最终调度字段 schema；PR2 的 CREATE/ALTER schedule metadata；PR4/PR5 的 purge/refresh runtime schedule；PR7 的 service schedule loading |
| `c226d73362` | `LAST_SUCCESS_REFRESH_ENDTIME datetime(6)` 改为 `LAST_SUCCESS_REFRESH_END_UNIX_SECONDS bigint`，并适配 create、refresh、out-of-place cutover、schedule duration | PR1 的 refresh-info schema；PR2 的 create/cutover metadata 初始化；PR5 的 refresh runtime 和 refresh observability |
| `24eaea3dee` | refresh/purge history 和 alert 表的时间字段、索引名称统一为明确的 start/end/request/heartbeat/snapshot/update 命名 | PR1 的最终 schema/index；PR4/PR5/PR7 的 SQL、executor、service 和测试引用 |
| `bf681b1b66` | 统一 MV/MLog 相关系统表字段和 Go 内部命名，例如 `MV_SCHEMA/MV_NAME -> MVIEW_SCHEMA/MVIEW_NAME`、`MVInitBuild* -> MViewInitBuild*`、`mv/mvLog -> mviewTask/mlogPurgeTask` | PR1 的最终系统表字段；PR2/PR4/PR5/PR6/PR7 各自 owning 模块的代码、测试和文档 |
| `d05b5da91b` | 新增基于最终 MV 系统表 schema 的 rebuild SQL 脚本，包含删除旧表和重建最终结构的维护步骤 | PR1 的 bootstrap / system-table migration 文档 |
| `9439fdfa65` | 按 spec 重排 `CREATE MATERIALIZED VIEW` 的 table options、`REFRESH`、`ATTRIBUTES`；同步 parser grammar、AST Restore/visitor、`SHOW CREATE` 和 parser/DDL 测试 | PR2a 的 parser/AST/语法 |

其中 `f5dfdf58b9c98b21e8f384e40c11ff77cacd7222` 已经在原 tracking head 之后的历史中完成 bootstrap version 合并，
本次 source boundary 更新也明确把这个 bootstrap 合并纳入 PR1 的最终 port 范围。

## PR 拆分规则

| PR | 范围 | 说明 |
| --- | --- | --- |
| PR1 | bootstrap 相关 | 只做最终 MV 系统表 bootstrap 和初始化。包括 bootstrap version 合并后的 5 张 MV 专用系统表、最终字段类型、字段/index rename，以及 `NEXT_*_UNIX_SECONDS` 和 `LAST_SUCCESS_REFRESH_END_UNIX_SECONDS` 的最终 schema；不把 parser / DDL / runtime 一起卷进来。 |
| PR2 | create/drop MV / MLog 相关 | 只做建表、删表、schema tracker、job args、validation 这一层；带入 CREATE/ALTER 时保存 schedule timezone、Unix-seconds schedule 初始化，以及 create/cutover 对最终 refresh-info schema 的适配。 |
| PR3 | MLog 写入相关 | base-table DML 到 MLog 的同步、row 处理、显式 DML 拦截等。 |
| PR4 | MLog purge 相关 | purge executor、cancel purge、purge history、hazard guard、purge 侧 observability；带入 `NEXT_PURGE_UNIX_SECONDS`、保存的 purge schedule timezone，以及 purge history 的最终字段/index 命名。 |
| PR5 | MV refresh 相关 | manual refresh、complete / fast / bounded refresh、cancel refresh、refresh history / metrics；带入 `NEXT_REFRESH_UNIX_SECONDS`、`LAST_SUCCESS_REFRESH_END_UNIX_SECONDS`、refresh schedule timezone、schedule duration 和 refresh history 的最终字段命名。 |
| PR6 | MV 其他用户可见功能 | `show`、`show create`、`compare`、status / metadata 展示等。 |
| PR7 | MV service 相关 | service framework、scheduler、backoff、history cleanup、alert checking、maintenance vars；带入 Unix-seconds schedule loading/rescheduling、alert 表最终命名和 service 内部 MV/MLog task 命名。 |

## 归属规则

下面这些东西不单独起 PR，直接跟着它们所属的功能面走：

- `cancel purge` -> PR4
- `cancel refresh` -> PR5
- refresh observability -> PR5
- service observability / backoff / alert / history cleanup -> PR7
- schedule timezone metadata -> 按 CREATE/ALTER、refresh、purge、service 的 owning PR 拆分，不单独起 PR
- Unix-seconds schedule / refresh-end storage -> 按 bootstrap schema、CREATE/cutover、refresh/purge runtime、service loading 分别归属
- timestamp 和 MV/MLog 命名 refine -> 按最终字段/模块归属跟随 PR1/PR2/PR4/PR5/PR6/PR7，不按 commit 单独 port
- 任何只服务于某个功能面的补丁、测试、回归结果，都跟着那个功能面的 PR 走

## PR 拆分

系统表阶段已经拆成两个子 PR：`PR1a` 是 MV 业务系统表，已合入
`master`；`PR1b` 是权限相关系统表，当前权限分支上的改动。

第二阶段统一定义为 MV/MLog DDL 生命周期，并拆成两个有先后依赖的子 PR：

```text
PR2a parser/AST/语法  ->  PR2b metadata/job args/DDL 实现
```

| PR | 范围 | 说明 |
| --- | --- | --- |
| PR2a | MV/MLog parser 和语法 | 覆盖 `CREATE`、`DROP`、`ALTER MATERIALIZED VIEW`、`ALTER MATERIALIZED VIEW LOG` 的全部 grammar、AST、Restore、Digest、关键字和 parser 测试；只负责语法和 AST，不实现 DDL 行为。 |
| PR2b | MV/MLog DDL 实现 | 包含 `pkg/meta/model` 的 MV/MLog `TableInfo` 和 job args、DDL dispatcher、schema tracker、validation、notifier、rollback/sanity check，以及 create/drop/alter 的完整 DDL 执行和测试。 |

PR2b 使用最终 MV 系统表 schema 作为 source of truth。schedule timezone、Unix-seconds
字段和 MV/MLog 命名 refine 在 CREATE/ALTER DDL 路径中的适配归 PR2b；purge、refresh
和 service runtime 的对应逻辑分别归后续 owning PR。

如果 PR2b 仍然过大，可以继续拆成 `PR2b1`（metadata + create/drop）和 `PR2b2`
（alter + schema tracker/validation），但不应把 metadata/job args 放到 PR2a，或推迟
到 refresh/service PR。

## Port Slice

| ID | Slice | Source 路径 / 符号 | 分类 | Master 状态 | Port 动作 | 验证 | 状态 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| S0 | 跟踪文档 | `docs/note/materialized_view/mv_master_port_tracking.md` | 流程 | N/A | 创建 tracking doc | N/A | `已移植` | 每完成一个有意义的 port 步骤后更新本文档。 |
| S1 | 设计文档 | `docs/note/materialized_view/{mv_refresh,kill_refresh_purge,mv_log_purge,mv_compare,mv_init_build_state,mv_refresh_observability}.md` | docs | 待处理 | port 有用的设计文档，或保留为内部参考 | 文档 review | `待处理` | 文档内容要和最终 master 实现保持一致。 |
| S2 | Parser、AST 和用户语法 | `pkg/parser`、`pkg/parser/ast`、`pkg/parser/mysql/privs.go`、`pkg/parser/mview_stmt_options.go` | direct MV | 待处理 | 基于当前 parser grammar 重写语法和 AST | parser unit test 和相关 integration test | `待处理` | 归 `PR2a`；包含 create/drop/alter/refresh/purge/cancel/show/compare 语法。 |
| S3 | 元数据模型和 DDL job args | `pkg/meta`、`pkg/meta/model`、`TableInfo`、MV/MLog 依赖元数据、job args | direct MV | 待处理 | port metadata field 和 job argument 编解码 | model / job args tests | `待处理` | 归 `PR2b`；必须兼容 master 当前 metadata versioning 和 BR restore 语义。 |
| S4 | Bootstrap、变量和 internal session prerequisite | `pkg/session`、`pkg/sessionctx`、`pkg/domain`、`pkg/privilege`、`docs/note/materialized_view/mv_system_tables_rebuild.sql` | direct MV / prerequisite | 待处理 | port MV system table、sysvar、privilege hook、internal-session 行为 | bootstrap / sysvar / session tests | `待处理` | rebuild SQL 脚本随 PR1 的最终 bootstrap/system-table 结构归属；需要确认 master 是否已有等价 internal-session helper。 |
| S5 | MV/MLog DDL 执行 | `pkg/ddl/materialized_view.go`、`pkg/ddl/mview_schedule_expr.go`、`pkg/ddl/create_table.go`、DDL guard、notifier、schema tracker | direct MV | 待处理 | 基于 master 当前 DDL framework 重写 DDL flow | DDL executor tests | `待处理` | 归 `PR2b`，与 S3 合并；包含依赖校验、schedule expression、alter/drop/truncate guard。 |
| S6 | MLog table 和 base-table DML capture | `pkg/table/tables/mview_log.go`、executor write path、`pkg/executor/internal/util/touched_rows.go` | direct MV | 待处理 | port mlog row 生成和事务写入行为 | writetest 和 integration DML tests | `待处理` | 验证 insert/update/delete、rollback、generated column、partition 行为。 |
| S7 | Manual refresh、show 和 infoschema executor | `pkg/executor/materialized_view.go`、`pkg/executor/show.go`、`pkg/executor/infoschema_reader.go` | direct MV | 待处理 | port refresh executor、show command、infoschema reader | refresh 和 infoschema tests | `待处理` | 包含 complete refresh 变体和用户可见 metadata 输出。 |
| S8 | MV service 框架 | `pkg/mvservice`、`pkg/domain`、`pkg/server`、service metrics | direct MV | 待处理 | port 后台调度、cancel、backpressure 框架 | mvservice unit tests | `待处理` | 依赖 metadata、bootstrap table 和 internal session 变量。 |
| S9 | Fast refresh planner | `pkg/planner/mview`、`pkg/planner/core`、plan guard、mview casetest | direct MV | 待处理 | 基于当前 planner 重写 fast-refresh plan derivation | planner casetest 和 unit tests | `待处理` | 包含 count/sum/min/max 和 bounded fast refresh planning。 |
| S10 | Delta merge agg executor 和 aggregate prerequisite | `pkg/executor/mviewdeltamergeagg`、`pkg/executor/aggfuncs`、`pkg/expression/aggregation` | direct MV / prerequisite | aggregate prerequisite 部分 `master 已有` | port fast refresh 需要的 MV operator；`SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 直接适配 `origin/master` 现有实现 | executor aggregate tests 和 mviewdeltamergeagg tests | `待处理` | `SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 已有 parser / expression / executor / tipb pushdown / checker 支持。 |
| S11 | Observability、metrics、stats 和 GC 处理 | `pkg/metrics`、`pkg/statistics`、`pkg/store/gcworker`、refresh observability | direct MV / hardening | 待处理 | port metrics、history、analyze skip/schedule、GC safeguard | targeted metrics / stats / gc tests | `待处理` | 通常放在 core refresh 和 service 之后更稳。 |
| S12 | BR / import / restore / system-table 交互 | `br`、`pkg/executor/import_into.go`、importer tests、realtikv import test | prerequisite / candidate | 待处理 | 审计 MV system table 和 initial build 是否依赖这些改动 | 如果 port，则跑 targeted BR/import tests | `待处理` | 部分改动可能 master 已有，或者和 MV 无关。 |
| S13 | 非 MV candidate drift | `cmd/mirror`、`full_outer_join`、`active_active/commit_ts`、TopSQL、build helpers、root metadata | non-MV candidate | 部分已确认 | agent docs、`.gitignore`、build helper、`cmd/mirror`、`google/skylark` 删除、root metadata、TopSQL network bytes、prepare dedup / plan cache、active-active commit TS 独立测试均不 cp；FULL OUTER JOIN 转入 prerequisite 审计；`_tidb_commit_ts` 底层能力 `origin/master` 已有，但 SQL 可引用策略仍需在 MV port 中处理；`SUM_INT`、`MAX_COUNT`、`MIN_COUNT` 已确认 `master` 已有 | 只有 port 时才跑 targeted test | `审计中` | integration result 不直接 cp，后续按 master port 后实际行为重新录制。 |
| S14 | Bazel 和生成文件元数据 | `BUILD.bazel`、`DEPS.bzl`、`go.mod`、`go.sum`、generated parser output | build metadata | 待处理 | 源码改完后基于 master 重新生成 | `make bazel_prepare`；需要时跑 parser 生成命令 | `待处理` | 需要生成时不要手改 generated artifact。 |
| S15 | Integration 和 regression tests | `tests/integrationtest`、executor/DDL/planner tests | tests | 待处理 | 按 slice port 测试和行为 | scoped integration / unit commands | `待处理` | 避免从无关 planner 改动带来大量 result churn。 |

## 非 MV / prerequisite 处理原则

以下内容不进入 MV 主线 PR，除非后面另有单独决定：

- full outer join 相关 prerequisite
- `_tidb_commit_ts` / commit-ts 相关 prerequisite
- TopSQL、build helper、root metadata、`AGENTS.md` / `.gitignore` 之类的 branch drift

这些内容如果需要 port，应该走各自独立的 prerequisite / drift 处理，不和 MV 主线混在一起。

## 备注

- PR1 这里限定为 bootstrap 相关改动，不强行把 `TableInfo` 元数据塞进来。
- PR1 使用最终系统表 schema 作为 source of truth；后续 PR 只引用这些最终列名和类型，不再 port 旧的 `NEXT_TIME`、`LAST_SUCCESS_ENDTIME`、`MV_SCHEMA/MV_NAME` 版本。
- `RefreshScheduleTimeZone` / `PurgeScheduleTimeZone` 属于 metadata/DDL/runtime 的跨 PR 依赖，不能因为最终字段在 PR1 中定义就把全部实现一起塞进 PR1。
- 如果某个 PR 在按最终 diff 切分后仍然过大，就继续按最终行为边界拆，不要回退到 commit 顺序。

## 工作日志

| 日期 | 目标分支 | 动作 | 结果 |
| --- | --- | --- | --- |
| 2026-08-25 | `cp_mv_for_master_base` | 更新 source boundary 和最终 diff 统计 | head 从 `6910cef840` 更新为 `bf681b1b66`，纳入 bootstrap 合并、schedule timezone、Unix seconds、timestamp/MV naming refine 的后续 commit |
| 2026-08-29 | `cp_mv_for_master_base` | 更新 source boundary，纳入 `cp_mv_for_master` 最新语法提交 | head 更新为 `9439fdfa65`，同时纳入 `d05b5da91b` 的 system-table rebuild SQL；最终 source diff 为 500 files changed、97367 insertions、25104 deletions，`9439fdfa65` 的语法调整归 PR2a |
