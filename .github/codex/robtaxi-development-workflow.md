# 研发总盘驱动的开发工作流

## 适用范围

适用于所有会修改 Git 跟踪文件的 Bug、需求、优化、技术债和监控实现。新闻抓取、Agent、生产 profile、部署和通知仍遵循各自的额外审批边界。

## 开始前

1. 在总盘、开放 Issue、关闭 Issue 和观察池中搜索重复事项。
2. 复用匹配的工程 Issue；没有匹配项时，先创建标准 Issue 并加入总盘。不能把 Project Draft 作为执行任务。
3. 填写总盘字段和 Issue 正文，创建原生依赖/子任务关系。
4. 分配负责人，使用 `python scripts/transition_project_task.py --issue <number> --status 开发中` 将 Status 写后读回确认。
5. 运行 `python scripts/validate_project_task.py --issue <issue> --phase preflight`；仅在通过后修改仓库文件。

## 实施中

同一根因的补充证据或不扩大验收标准的调整可写入当前 Issue。其他发现先登记并评估：必要子任务可在当前明确授权范围内继续；无关、产品方向变化、高风险或生产范围变化必须等待新的批准。

不要以普通 Issue 评论驱动状态迁移，避免触发 `issue_comment` 自动化。优先更新 Project 字段和 Issue 正文。

## 结束与合并

1. 执行定向测试和仓库规定的全量校验。
2. 在 Issue 正文补充验收结果、测试证据、风险和遗留任务。
3. PR 正文以 `Primary task: Fixes #<number>` 指向主任务；无关闭意图时使用 `Refs #<number>`。
4. 使用 `python scripts/transition_project_task.py --issue <number> --status 待验证` 写后读回确认，再运行 postflight 校验。
5. 合并关闭 Issue 后由 Project 自动化更新为“已完成”。

## 失败与例外

Project/API/Token 缺失时 fail closed。只允许用户明确批准的 P0 break-glass 例外；例外工作不得合并、部署或推送生产，直到补登记并通过门禁。

首次引入本门禁的 PR 是唯一的 bootstrap 例外：使用 `Bootstrap task: Refs #<number>`，不得以关闭关键字提前关闭治理 Issue。合并后必须配置只读 Token、手动运行 postflight 并启用 Ruleset；完成这些步骤后才能关闭该治理 Issue。
