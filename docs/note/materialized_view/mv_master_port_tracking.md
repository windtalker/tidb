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
head commit: bf681b1b662a04c01dfdbf62507af90790df453d

source range:
  f08c648a20380ea723449c6c3eb5b171d96fd567..bf681b1b662a04c01dfdbf62507af90790df453d
```

当前 source diff 规模：

```text
499 files changed, 97274 insertions(+), 25112 deletions(-)
```

后续 inventory 和 review 都以固定 commit range 为准：

```bash
git diff --name-status f08c648a20380ea723449c6c3eb5b171d96fd567..bf681b1b662a04c01dfdbf62507af90790df453d
git diff --stat f08c648a20380ea723449c6c3eb5b171d96fd567..bf681b1b662a04c01dfdbf62507af90790df453d
```

### 新增 source commits

相对原 tracking 文档的 head `6910cef840612ee85e171adb19d3e427697a65da`，新增以下 4 个 commit。
它们已经纳入上面的最终 source range，但 port 时仍然必须按最终 diff 的语义边界拆分。

| Commit | 最终改动 | 主要 port 归属 |
| --- | --- | --- |
| `43c6999be1` | 保存 refresh/purge schedule timezone，并按该 timezone 求值后转换为 Unix seconds；ALTER schedule 只在提供 schedule expression 时更新 timezone | PR1 的最终调度字段 schema；PR2 的 CREATE/ALTER schedule metadata；PR4/PR5 的 purge/refresh runtime schedule；PR7 的 service schedule loading |
| `c226d73362` | `LAST_SUCCESS_REFRESH_ENDTIME datetime(6)` 改为 `LAST_SUCCESS_REFRESH_END_UNIX_SECONDS bigint`，并适配 create、refresh、out-of-place cutover、schedule duration | PR1 的 refresh-info schema；PR2 的 create/cutover metadata 初始化；PR5 的 refresh runtime 和 refresh observability |
| `24eaea3dee` | refresh/purge history 和 alert 表的时间字段、索引名称统一为明确的 start/end/request/heartbeat/snapshot/update 命名 | PR1 的最终 schema/index；PR4/PR5/PR7 的 SQL、executor、service 和测试引用 |
| `bf681b1b66` | 统一 MV/MLog 相关系统表字段和 Go 内部命名，例如 `MV_SCHEMA/MV_NAME -> MVIEW_SCHEMA/MVIEW_NAME`、`MVInitBuild* -> MViewInitBuild*`、`mv/mvLog -> mviewTask/mlogPurgeTask` | PR1 的最终系统表字段；PR2/PR4/PR5/PR6/PR7 各自 owning 模块的代码、测试和文档 |

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
