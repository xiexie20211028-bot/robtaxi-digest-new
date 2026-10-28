# 研发通知送达与补偿

正式请求与逐渠道回执保存在控制 Issue；本地文件仅缓存。请求键是稳定事件ID的摘要，渠道尝试键包含请求键、渠道、尝试次数和结果。两渠道都成功才是完整送达；未配置属于失败，未知结果不能按成功处理。

本地仅在已登记的有效研发运行内提交摘要：

```text
python3.11 -m app.development_notifications enqueue --run-id ID --request .local/robtaxi-development/runs/ID/notification.json --out .local/robtaxi-development/runs/ID/notification-result.json
```

请求JSON包含 `event_id` 与 `text`；每日摘要使用 `daily:YYYY-MM-DD`，严重异常使用稳定事故及状态变更ID。正文不得包含凭据、原始日志或私密内容，只放摘要和证据链接。相同ID不同正文会拒绝，不能通过换ID绕过去重。同日后续严重变化使用独立事故事件。

入口先保存正式待发送记录，再触发现有 review 工作流的 `notification_only=true`。仅通知路径不执行生产或复盘模型；云端复用既有密钥、固定并发组和正常复盘时的补偿检查。本地不读取Webhook密钥。dispatch失败保留待发送记录供下轮恢复；“已入队”不代表送达。

使用正常产业简报前缀改善机器人关键词兼容性；机器人实际规则须在真实验证时核实，不关闭关键词安全校验。已确认关键词错误不进行同轮重试。暂时连接失败最多三次，等待30/90秒；限流遵守Retry-After并受总预算约束。

每次发送前先保存 sending；进程崩溃或响应丢失后按unknown处理，不盲重发。已成功渠道不再发送；另一渠道提示未知结果。双路失败或unknown保留失败Actions与正式回执，不能声称用户已收到。人工核验未知送达后，只有确切证据才可补记同请求/渠道/attempt的success回执；未核验前保持unknown。

受保护规则需先批准合并上线。真实测试消息须在发送授权范围内进行；离线测试与真实渠道成功回执分开记录，不手动触发新闻生产凑验收。

English: Requests and per-channel receipts are durable; enqueueing is not delivery. Unknown outcomes require reconciliation rather than blind retries.
