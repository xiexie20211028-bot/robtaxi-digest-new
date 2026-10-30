# 每日研发结果与待决策

计划运行开始使用`inspect --start-run --run-id ID --trigger schedule`固定触发类型；手动协助用manual，旧记录缺字段按manual读取。正常结束统一调用`python3.11 -m app.development_cycle finish --run-id ID --decision FILE --health-sync FILE --outcome RESULT`，从正式开始事件继承触发类型。入口先保存本地现场、程序生成正式结果和待发送项，再结束整轮。不要再先调用独立report后继续开发；report仅保留兼容和只读协助用途。健康apply或研发控制器失败也自动进入failed收尾；已收尾后只核对结果，不能再次写入。

完整回执绑定同轮及decision摘要才能报全部完成；逐项成功可显示部分完成，同时列阻塞原因。缺回执/缺文件仍生成巡检未完成结果。权限、过期租约或GitHub不可写时仅保存completion.json，不伪造正式报送；已有待发送由正常云端入口补偿。消息请求保存或dispatch失败不能阻止尝试结束仍有效的整轮，响应未知不盲目重发。

每天北京时间日期只有一条正式研发日报。每轮结果保存到控制任务的run_outcome，日报及独立严重异常使用现有串行通知入口。手动协助必须用`--trigger manual`，保存结果但不占计划日报、不额外推送、不增加计划停滞次数。离线时保留本地产物，不能声称消息已送达。

程序分别输出巡检、证据新鲜度、当前/最近已知事故、推进/合并/验收任务、排除原因、预算、待决策和渠道回执。有进展仍可同时存在critical；产物迟到不能清除原异常。可执行任务连续两次计划运行没有新版本或可验证阶段变化时告警，普通评论和重复检查点不计进展。

待决策首次24小时提醒，此后每72小时汇总。明确处理后使用：

```bash
python3.11 -m app.development_outcomes resolve --run-id ID --decision-id KEY --resolution approved --evidence 明确的用户处理依据
```

resolution只能是approved/rejected/cancelled/implemented；收到、知悉和旧记录缺字段均不算批准。处理记录仅结束提醒，不授予策略、密钥、合并或生产权限；各受保护门禁仍独立执行。没有用户明确处理依据不得由执行者自行填approved。

新增模块的受审计命令前缀在仓库规则内供评审；受保护策略放行时再核对安装到用户层，不能把仓库内规则存在描述成宿主权限已启用。
