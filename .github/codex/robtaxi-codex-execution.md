# Codex 单平台无人值守研发执行手册（v2）

本手册供 Codex 桌面端项目定时任务显式读取。GitHub 是正式状态，本地工作区和 `.local/` 缓存可丢弃。新闻生产、行业研究和复盘仍由现有 GitHub Actions 运行。

## 授权与费用

- 最新主分支 `.github/robtaxi-autonomy.json` 是唯一机器授权；当前 shadow 只允许健康对账、inspect 和 heartbeat。
- pilot 只允许 `pilot_issue`，active 才开放合格队列。每天最多领取一个任务、合并一个 PR。
- 进入 pilot 前必须把只读调度、网络恢复、普通交付门禁模拟和费用关闭证据写入 `activation_evidence`；其中 `normal_delivery` 是固定证据的无写入交付模拟，不代表已合并。进入 active 还必须补充 #69 的真实 `pilot_delivery` 证据。
- Codex 使用 ChatGPT 套餐；不得读取或切换到 API Key，不得充值或启用额外付费渠道。
- 90 分钟由 GitHub 租约限制写入和交付。桌面端没有已验证的硬进程终止保证，因此租约不是模型费用硬上限。

## 每日顺序

1. 确认 Shadowrocket、GitHub、项目目录和 Codex 登录可用；读取最新 main 的治理文件。网络失败时不领取任务。调度器自身 worktree 作为只读控制工作区，开发使用独立的同仓库任务 worktree，不能在控制工作区切换到任务分支。
2. 按健康闭环依次完成正式状态重建、决策重算和 apply；严重事故只冻结研发合并，不阻止简报生产。
3. 运行 `python3 -m app.development_cycle inspect --out .local/robtaxi-development/snapshot.json`。
4. 严格处理 `next_action`：review → delivery → execute → plan。单项失败不改动其他任务状态。
5. 健康对账成功后运行 heartbeat；研发无变化时不制造通知。

## 规划与领取

plan 在当前运行内完成，不再启动 `codex exec`。合同使用 `robtaxi-execution-v1`，包含 issue、version、base_sha、goal、acceptance、allowed_paths、relevant_paths、risk、risk_reason、dependencies、tests、production、rollback_conditions 和 reserved_decisions。

合同写入文件：

```json
{"event":"contract","contract":{}}
```

然后运行 `python3 -m app.development_cycle checkpoint --issue N --event FILE`。脚本验证版本、主分支基线、实际依赖和保留事项，并更新自动化标签。无关提交不会使合同失效；命中 allowed/relevant 路径才重新规划。

重新 inspect 后运行 `python3 -m app.development_cycle claim --issue N`。领取会在控制 Issue 和任务 Issue 写入同一个 `task_claimed` 事件，包含动作、合同摘要和90分钟截止时间。重复运行或当天第二项会被正式事件拒绝。

## 开发与PR

- 在控制工作区的同级目录创建或恢复独立任务 worktree，分支固定为 `codex/development-N`。开始前查询同一 Issue 的开放 PR；已有一个就续做，超过一个立即停止对账。所有领取、检查点和交付脚本从保持在最新 main 的控制工作区运行，代码与测试在任务 worktree 运行。
- 只修改合同 allowed_paths。先做最小复现，再实现、定向测试、全量 pytest、配置校验、compileall 和 diff-check；内容逻辑还需固定输入业务回放。
- 每次有实际修改但验证失败，记录一次 `run_failed`；同一合同最多两轮实际修复。网络重试不计为修复轮次。
- 使用 `Primary task: Fixes #N` 或需要生产观察时使用 `Primary task: Refs #N`，添加 `codex-development` 标签并将状态设为待验证。
- 进度用 `checkpoint` 事件记录 planned、code_changed、tests_passed、pr_created、waiting_ci 或 blocked；摘要不得包含凭据、完整错误输出、提示词或模型思考。

## 复核、交付与恢复

普通任务通过可信检查后可在当前租约内交付。高影响任务必须等到PR创建后的下一天，由新的定时运行领取 review；复核事件必须绑定当前 head、main 和合同摘要，PR变化后自动失效。

`python3 scripts/development_delivery.py --pr N` 从最新可信 main 重查范围、依赖、复核、CI、分支、提交和每日额度。不得使用 admin、强推或绕过规则。合并后无生产观察任务可关闭；其他任务进入观察中，由实际生产运行证据完成或重新排队。

回退只通过 revert PR，且必须满足合同预定义条件、已有验收版本、无冲突和必要测试。不能安全回退时写发布冻结并通知用户。

## 运行环境

本机需保持开机并登录，Codex 桌面端与 Shadowrocket 设置为登录启动和自动重连。定时任务使用独立 worktree，模型 `gpt-5.6-sol`、推理强度 medium、北京时间每日10:30。超过36小时没有成功 heartbeat 时，现有云端复盘工作流去重告警。
