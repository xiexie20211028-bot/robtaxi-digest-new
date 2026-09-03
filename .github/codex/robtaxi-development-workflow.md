# 个人用户快速研发工作流

本流程服务 Robotaxi Digest 的唯一非技术用户：优先保护重要事件召回、事实准确、安全和成本边界，同时缩短需求与修复从批准到上线的时间。

## 开始前

1. 搜索总盘和现有 Issues，复用同一根因或目标；自动 Health Issue 只作证据。
2. 正式 Issue 加入总盘，填 Status、Priority、Task Type、Change Risk、Target、Route 和 Assignee。
3. 普通任务写“问题或目标”“验收标准”；Medium 另写“证据”“验证与回退”；High 另写“根因与风险”“实施方案”“上线与监控”。
4. 没有开放 Blocked by 后，将状态设为“开发中”，运行 `python3 scripts/validate_project_task.py --issue <issue> --phase preflight`。
5. 从最新 `origin/main` 建立独立工作区，不触碰已有 WIP。

## 风险与交付

| 风险 | 用户“批准执行”允许的终点 |
|---|---|
| Low | 测试、PR、合并，下一次正常定时运行采用 |
| Medium | 测试、PR、合并；只在明确需要时观察首个相关运行 |
| High | 完整测试和 Draft PR；需“批准合并上线”才能合并 |

High 的 Ready PR 必须带 `high-risk-approved` 标签。Low/Medium 不增加第二次人工审批。默认一个主 Issue 对应一个主 PR。

## 结束与透明度

- PR 标明 `Primary task: Fixes #<number>` 或 `Primary task: Refs #<number>`。
- 非 Draft PR 可处于“开发中”或“待验证”；状态同步不再阻塞已通过测试的 PR。
- 合并后，只有实际需要生产证据的任务进入“观察中”；无关日历等待不是验收。
- 结束汇报必须按“代码、测试、提交、上传、PR、合并、总盘、上线、线上验证、下一次定时运行、剩余事项”逐项说明。

## 例外

GitHub/Project 不可访问时，已批准任务可仅在隔离工作区本地测试；不得 push、Ready、合并或上线。P0 break-glass 需要明确授权，且不自动涵盖部署、通知、密钥或数据删除。
