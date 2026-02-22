# Robtaxi 行业简报 2.0（实施版）

当前版本已改为分层流水线：`fetch -> parse -> filter_relevance -> summarize -> render -> notify_feishu`。

## 运行接口
```bash
python -m app.fetch --date YYYY-MM-DD --sources ./sources.json --out ./artifacts/raw --report ./artifacts/reports
python -m app.parse --date YYYY-MM-DD --in ./artifacts/raw --out ./artifacts/canonical --report ./artifacts/reports
python -m app.filter_relevance --date YYYY-MM-DD --in ./artifacts/canonical --out ./artifacts/filtered --sources ./sources.json --report ./artifacts/reports
python -m app.summarize --date YYYY-MM-DD --in ./artifacts/filtered --out ./artifacts/brief --provider deepseek --report ./artifacts/reports
python -m app.render --date YYYY-MM-DD --in ./artifacts/brief --out ./site/index.html --report ./artifacts/reports --sources ./sources.json
python -m app.notify_feishu --date YYYY-MM-DD --html-url <pages_url> --in ./artifacts/brief --report ./artifacts/reports
```

## 配置与依赖
- 配置文件统一为：`./sources.json`
- 依赖：`pip install -r requirements.txt`
- DeepSeek Key：`DEEPSEEK_API_KEY`
- 飞书推送：`FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`FEISHU_RECEIVE_OPEN_ID`

## 调度策略
- 生产：GitHub Actions（每日 UTC 01:00 / 北京时间 09:00）
- 本地 launchd：仅开发调试，不作为生产链路

## 质量规则
- 去重：L1 URL、L2 标题、L3 TF-IDF 语义去重（阈值 0.85）
- 相关性过滤：高精度模式（核心词/公司别名命中 + 分源阈值 + 负向词扣分）
- 摘要：优先 DeepSeek，失败降级规则摘要
- 输出：必须包含中文摘要、原文链接、运行状态

## 运行报告
- 报告文件：`artifacts/reports/<date>/run_report.json`
- 关键字段：
  - `run_id`
  - `stage_status`
  - `source_stats`
  - `dedupe_drop_count`
  - `summarize_fail_count`
  - `feishu_push_status`
