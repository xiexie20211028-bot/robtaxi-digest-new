# Robtaxi 行业简报 4.2（单次日跑 + 严格时间窗口 + 结构化摘要）

本项目用于每日生成 Robtaxi 行业简报并发布到 GitHub Pages，同时推送到飞书和企业微信机器人。

## 核心规则（v4.2）
- 统计窗口固定为北京时间前一自然日：`[D-1 00:00:00, D 00:00:00)`（左闭右开）。
- 旧闻禁止：窗口外新闻一律淘汰。
- `published_at` 缺失或不可解析一律淘汰。
- 每天北京时间 `09:00` 运行一次完整链路。
- 每条摘要强制结构：`What / Why / So what`，并标注“影响对象”。

## 流水线
- `fetch -> parse -> filter_relevance -> enrich -> summarize -> render -> deploy -> notify`

对应模块：
- `app/fetch.py`：抓取原始数据
- `app/parse.py`：标准化与 L1/L2 去重
- `app/filter_relevance.py`：相关性过滤 + 时间窗口硬约束
- `app/enrich.py`：正文补全（短摘要条目拉取全文）
- `app/summarize.py`：摘要与 L3 去重
- `app/render.py`：生成 `site/index.html`
- `app/notify_feishu.py`：飞书推送
- `app/notify_wecom.py`：企业微信推送
- `app/validate_sources.py`：配置校验

## 配置文件
- 主配置：`./sources.json`
- 关键默认项（`defaults`）：
  - `window_mode = "prev_natural_day"`
  - `window_timezone = "Asia/Shanghai"`
  - `drop_if_published_missing = true`
  - `drop_if_published_unparseable = true`
  - `fast_pass_window_hours` 仅用于 fast-pass 内部新鲜度辅助，不作为主时间准入
  - `summary_style = "what_why_so_what"`
  - `summary_sentence_min = 2`
  - `summary_sentence_max = 3`
  - `impact_target_taxonomy = ["运营方","车企","供应链","监管","资本市场"]`
  - `summary_require_so_what = true`
  - `summary_ban_phrases = ["详见原文","建议查看原文"]`

## 环境变量
- DeepSeek：`DEEPSEEK_API_KEY`
- 搜索补充（可选）：`SERPAPI_API_KEY`（无 key 继续失败告警）
- 飞书（推荐 webhook）：
  - `FEISHU_WEBHOOK_URL`
  - `FEISHU_WEBHOOK_SECRET`（可选）
- 飞书（备选 app/open_id）：
  - `FEISHU_APP_ID`
  - `FEISHU_APP_SECRET`
  - `FEISHU_RECEIVE_OPEN_ID`
- 企业微信：
  - `WECOM_WEBHOOK_URL`

## 本地运行
1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 校验配置

```bash
python -m app.validate_sources ./sources.json
```

3. 分阶段执行

```bash
DATE_BJ="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
python -m app.fetch --date "$DATE_BJ" --sources ./sources.json --out ./artifacts/raw --report ./artifacts/reports
python -m app.parse --date "$DATE_BJ" --in ./artifacts/raw --out ./artifacts/canonical --report ./artifacts/reports
python -m app.filter_relevance --date "$DATE_BJ" --in ./artifacts/canonical --out ./artifacts/filtered --sources ./sources.json --report ./artifacts/reports
python -m app.enrich --date "$DATE_BJ" --in ./artifacts/filtered --out ./artifacts/enriched --report ./artifacts/reports
python -m app.summarize --date "$DATE_BJ" --in ./artifacts/enriched --out ./artifacts/brief --provider deepseek --report ./artifacts/reports --sources ./sources.json
python -m app.render --date "$DATE_BJ" --in ./artifacts/brief --out ./site/index.html --report ./artifacts/reports --sources ./sources.json
```

4. 包装器入口

```bash
python3 ./scripts/robtaxi_digest.py --date "$DATE_BJ" --sources ./sources.json --output ./site/index.html
```

## GitHub Actions（生产）
工作流：`./.github/workflows/robtaxi-digest-pages.yml`

- 定时：`0 1 * * *`（UTC），即北京时间 `09:00`
- 链路：`fetch -> parse -> filter -> enrich -> summarize -> render -> deploy -> notify`
- 手动触发默认不推送，`send_notify=true` 才推送
- 同一北京日期按渠道独立锁（飞书/企微），避免重跑重复推送

## 运行报告
报告路径：`artifacts/reports/<date>/run_report.json`

重点字段：
- `window_mode`
- `window_start_bj`
- `window_end_bj`
- `relevance_total_in`
- `relevance_kept`
- `relevance_dropped`
- `relevance_drop_by_reason_zh`
- `source_stats`
- `summary_structured_count`
- `summary_structured_valid_count`
- `summary_structured_invalid_count`
- `summary_retry_count`
- `impact_target_distribution`

兼容字段（本版不展示，保留一个版本便于回溯）：
- `daily_pool_size`
- `baseline_*`
- `recall_at_20`
- `recall_guard_*`

## 排障
- 查看过滤结果：
  - `artifacts/filtered/<date>/filtered_items.jsonl`
  - `artifacts/filtered/<date>/dropped_items.jsonl`
- 查看失败源摘要：`run_report.json` 里的 `source_stats`
- 若通知失败，优先检查：
  - `FEISHU_WEBHOOK_URL` / `WECOM_WEBHOOK_URL`
  - 对应 step 日志错误码
