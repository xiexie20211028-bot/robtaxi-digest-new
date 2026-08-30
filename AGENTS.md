# Robotaxi Digest 开发 Agent 工作流

本仓库的研发任务以 GitHub Project **Robotaxi Digest 产品研发总盘** 为唯一排期和状态来源；GitHub Issue 是唯一可执行工作单元。

详细规则见 [`.github/codex/robtaxi-development-workflow.md`](.github/codex/robtaxi-development-workflow.md)。本文件优先级高于一般开发习惯。

## 强制门禁

在修改任何 Git 跟踪文件以实现 Bug、需求、优化、技术债或监控任务前，必须：

1. 检查总盘和 GitHub Issues 是否已有同一事项；优先复用已有 Issue，避免重复登记。
2. 使用正式仓库 Issue（不能使用 Project Draft）作为主任务，并确认它已加入总盘。
3. 填完 Status、Priority、Task Type、Area、Impact、Urgency、Reach、Recurrence、Effort、Priority Score、Target、Remedy、Route、Assignee、依赖和验收标准。
4. 确认没有开放的 `Blocked by`，使用状态迁移工具将状态设为“开发中”，并运行：

   ```bash
   python scripts/validate_project_task.py --issue <number-or-url> --phase preflight
   ```

校验失败、GitHub/Project 不可访问、任务未评估或没有明确用户授权时，停止修改代码并说明原因。

## 范围与新发现

- 只读调查、方案设计、状态查询和 Issue/Project 管理本身不需要新 Issue。
- 执行中发现的问题只有在与当前 Issue 同一根因、且不扩大验收边界时才能补充到当前任务。
- 其他新 Bug 或需求必须先登记、去重和评估；除非它是完成已批准验收标准的必要子任务，否则不得顺手实现。
- 自动 Health Issue 是事故证据，不得直接作为修复主任务；必须关联正式工程 Issue。
- P0 的 break-glass 例外仅在用户明确授权后适用，且在补登记和门禁通过前不得合并、部署或推送生产。

## 结束工作

完成实现后必须运行适当的定向和全量验证，更新 Issue 的验收结果、测试证据、风险及 PR/commit 关联，并用 `python scripts/transition_project_task.py --issue <number> --status 待验证` 写后读回确认总盘状态。

非 Draft PR 的主任务必须处于“待验证”。PR 使用 `Primary task: Fixes #<number>` 关联主任务。只有 PR 合并关闭 Issue，或用户明确验收无 PR 工作后，才可设为“已完成”。唯一例外是首次引入本门禁的 bootstrap PR：它必须用 `Bootstrap task: Refs #<number>` 保持治理 Issue 开放，并在合并后补做 Token、手动校验和 Ruleset 验证。
