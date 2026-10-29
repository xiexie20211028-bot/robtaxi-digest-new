# 每日研发结果与待决策

运行结束前、finish之前，由本轮有效run-id调用`python3.11 -m app.development_outcomes --run-id ID --decision FILE --health-sync FILE --trigger schedule`。正式回执必须绑定同轮、原decision摘要和完整状态；旧回执、缺证据不能报巡检完成。

每天北京时间日期只有一条正式研发日报。每轮结果保存到控制任务的run_outcome，日报及独立严重异常使用现有串行通知入口。手动协助必须用`--trigger manual`，保存结果但不占计划日报、不额外推送、不增加计划停滞次数。离线时保留本地产物，不能声称消息已送达。

程序分别输出巡检、证据新鲜度、当前/最近已知事故、推进/合并/验收任务、排除原因、预算、待决策和渠道回执。有进展仍可同时存在critical；产物迟到不能清除原异常。可执行任务连续两次计划运行没有新版本或可验证阶段变化时告警，普通评论和重复检查点不计进展。

待决策首次24小时提醒，此后每72小时汇总。明确处理后使用：

```bash
python3.11 -m app.development_outcomes resolve --run-id ID --decision-id KEY --resolution approved --evidence 明确的用户处理依据
```

resolution只能是approved/rejected/cancelled/implemented；收到、知悉和旧记录缺字段均不算批准。处理记录仅结束提醒，不授予策略、密钥、合并或生产权限；各受保护门禁仍独立执行。没有用户明确处理依据不得由执行者自行填approved。

新增模块的受审计命令前缀在仓库规则内供评审；受保护策略放行时再核对安装到用户层，不能把仓库内规则存在描述成宿主权限已启用。
