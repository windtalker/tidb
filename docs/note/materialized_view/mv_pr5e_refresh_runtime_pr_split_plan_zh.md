# Refresh runtime PR 拆分建议

## 结论与范围

建议将 `mv_pr5e_refresh_runtime_for_master` 按依赖和代码职责拆成 4 个 PR。
FAST、COMPLETE IN PLACE、COMPLETE DELTA APPLY 和 COMPLETE OUT OF PLACE 的完整
执行流程保留在同一个 runtime PR 中，SQL 入口最后统一接通。

拆分时直接使用当前分支的最终实现，不引入临时 stub、简化执行路径、占位返回值，
也不为中间态增加后续需要删除的适配代码。

本建议记录于 2026-10-17，基于以下固定快照：

| 项目 | 分支或 commit |
| --- | --- |
| 待拆分分支 | `mv_pr5e_refresh_runtime_for_master` |
| Runtime 快照 | `9464ba47994838613bee48daf4e4c3a57b80ae56` |
| 对照基线 | `origin/master`，`f40ad1d845cbb1bce94253b4d8965a7389177dd7` |
| 代码 diff | 31 个文件，新增 5425 行、删除 67 行 |

上面的规模不包含本文档。后续 master 或 runtime 分支变化时，需要重新核对 diff
和依赖关系；本文不是已经完成拆分或已经通过各 PR 编译的记录。

## 推荐的 4 个 PR

| PR | 内容 | 预计新增行数 |
| --- | --- | ---: |
| 1. 通用前置修复 | 内部 session advisory lock、失败清理、MPP encode type | 约 150 |
| 2. 算子构建与执行统计 | DeltaMerge / CompleteDeltaApply builder、MIN/MAX recompute、runtime stats、key 序列化修复 | 约 1200 |
| 3. 完整 refresh runtime | 所有刷新模式的执行流程，以及状态、历史、事务、锁、调度和 session 配置 | 约 3100 |
| 4. 诊断实现与 SQL 入口 | DRY RUN / PROFILE 具体实现，以及 refresh 相关入口的统一接入 | 约 1000 |

行数是按代码职责估算并取整的结果，不是实际拆分后的统计。各 PR 还需包含对应的
imports、Bazel 元数据和现有回归测试；如需将已有代码移入永久保留的公共文件，
文件头等也会影响最终行数。

### PR 1：通用前置修复

主要涉及：

- `pkg/session/session.go` 中内部 session 的稳定 advisory lock owner ID，
  包括没有 process info 时的处理，以及 session 关闭时清理 owner ID。
- `pkg/session/advisory_locks.go` 中获取锁的初始化失败后释放借用 session。
- `pkg/executor/internal/mpp/local_mpp_coordinator.go` 中遵循 chunk RPC 设置的
  encode type 选择。
- 与这些修复对应的现有回归测试。

这些改动可以基于 master 独立提交，不需要 refresh executor。仅在 refresh 使用的
`AttachStatsCollectorForInternalSession` 及显式事务限制分别留给 PR 3 和 PR 4。

### PR 2：算子构建与执行统计

主要涉及：

- `pkg/executor/mview_delta_merge_agg_builder.go` 的 DeltaMerge executor 构建、
  aggregate mapping 和 MIN/MAX recompute 构建。
- `pkg/executor/mview_refresh_executor_builder.go` 中 CompleteDeltaApply 的
  builder 和列映射校验辅助函数。
- `pkg/executor/builder.go` 中上述两个内部算子的分派、构建状态字段，以及
  `newDataReaderBuilderWithSnapshot`。
- `pkg/executor/mview_complete_delta_apply.go` 和
  `pkg/executor/mviewdeltamergeagg/` 的执行统计，以及
  `pkg/util/execdetails/runtime_stats.go` 中对应的统计类型。
- 适配 `codec.SerializeKeys` 预分配行为的复合 key 序列化修复及回归测试。

master 已有对应的 planner 类型和底层执行算子，可以先补齐这些构建和统计逻辑。
此 PR 不接入 REFRESH、DRY RUN 或 PROFILE 的 utility statement 入口。

`mview_refresh_executor_builder.go` 同时包含算子 builder 和 utility statement
builder，需要按函数归属拆分，不能将整个文件一次放入 PR 2。

### PR 3：完整 refresh runtime

主要涉及 `pkg/executor/mview_refresh.go` 中所有刷新模式的最终执行实现，包括：

- 刷新目标与 MLog 解析、read TSO、完整性检查和 purge fence。
- Advisory lock、事务、失败回滚、成功状态持久化及调度时间更新。
- Refresh history、心跳、取消请求监测、失败告警和最终状态更新。
- FAST、COMPLETE IN PLACE、COMPLETE DELTA APPLY、COMPLETE OUT OF PLACE
  的完整流程，包括 shadow table 的构建和切换。
- 内部 session 配置及恢复、维护标记和 stats collector 接入。
- 与上述实现配套的共享 history helper、purge 调用调整和调度指标。

这个 PR 同时提交 runtime 使用的 observer 接口和步骤定义，详见下一节。
`RefreshMaterializedViewExec` 的类型、`Next` 和实际执行方法一起保留，但 utility
statement 的 executor builder 和 SQL 分派留在 PR 4。

取消监测属于 runtime；接收用户取消命令的 `requestRefreshHistCancel`、
`resolveCancelRefreshJobPrivilegeTarget` 及其调用方一起放到 PR 4。

### PR 4：诊断实现与 SQL 入口

主要涉及：

- `pkg/executor/mv_refresh_observability.go` 中 DRY RUN、PROFILE、collector
  及结果格式化的具体实现。
- `mview_refresh_executor_builder.go` 中 REFRESH、DRY RUN、PROFILE 的三个
  utility statement builder，以及 `builder.go` 中对应的分派。
- `mview_task.go` 中 refresh 取消请求的类型校验、权限解析和执行分派。
- `session.go` 中 REFRESH 的显式事务限制，以及剩余入口配套逻辑和依赖入口的测试。

该 PR 合入后统一接通 REFRESH、DRY RUN、PROFILE 和 refresh 取消请求。
前面的 PR 直接积累最终实现，避免出现只支持部分刷新模式的临时执行版本。

## 必须保留的依赖边界

### Observer 契约随 runtime 先提交

`mview_refresh.go` 已经依赖 `mvRefreshStepObserver`、步骤类型、步骤常量和
`newMVRefreshStepSet`，其中 `emitMVRefreshStepPlanRows` 还依赖 `clonePlanRows`。
这些稳定定义约 150 行，需要跟随 PR 3，必要时放到永久保留的公共文件中。

DRY RUN、PROFILE 和 collector 的具体实现留在 PR 4。PROFILE 会调用真实的
refresh 执行方法，所以具体实现不能先于 runtime 提交。沿用现有最终实现的
observer 接口和可选 observer 行为即可，不需要新增临时实现。

### 状态与历史 helper 随完整执行流程提交

不建议为了平均行数，把状态和历史 helper 单独提前做成一个 PR。它们与刷新主流程
联系紧密，尚无调用方的私有函数还可能触发仓库启用的 unused 检查。
不应为绕过检查而增加无意义引用、改成导出函数或添加临时抑制规则。

因此核心 runtime PR 仍预计约 3000 行。若要继续明显缩小它，需要先设计永久的
结构重构，其工作量和风险已超过单纯拆分现有改动。

### 按最终代码归属拆分，不按已有 commit 直接切分

当前分支后续几个修复 commit 跨越 session、算子和 runtime 等不同层。
拆分时应将每个修复直接归入所属 PR 的最终实现，避免先合入已知有问题的版本，
再依赖后一个 PR 修正。

合入顺序建议为 PR 1、PR 2、PR 3、PR 4。前两个 PR 的内容没有直接相互依赖，
可以分别基于 master 准备；PR 3 需要这些前置能力，PR 4 需要完整 runtime。

## 测试安排与验证状态

- 现有的最小回归测试随对应修复或实现一起拆分。
- 依赖真正执行 REFRESH SQL 的测试，应随 PR 4 的入口接入一起加入并运行。
- 完整的 refresh 测试 port 仍按此前安排放在后续单独的测试 PR；上述四个 PR
  的行数估算不包含这部分尚未 port 的完整测试。
- 实际拆分后，每个中间分支都需要验证编译、适用的 lint 和目标测试，并按
  `AGENTS.md` 更新和检查 Bazel 元数据。
- 最后需要核对所有拆分 PR 合并后的代码与本文固定的 runtime 快照逻辑一致，
  包括后续补齐的修复，防止拆分过程中遗漏代码。

本次结论来自 diff、函数引用关系及 lint 配置的静态检查。以下命令固定使用上述
代码快照，避免本文档提交后也被计入原始代码规模：

```bash
refresh_split_base=f40ad1d845cbb1bce94253b4d8965a7389177dd7
refresh_split_tip=9464ba47994838613bee48daf4e4c3a57b80ae56
git diff --stat "${refresh_split_base}...${refresh_split_tip}"
git diff --numstat "${refresh_split_base}...${refresh_split_tip}"
```

尚未创建拆分分支，也未实际验证各拆分 PR 的编译、lint 或测试。
