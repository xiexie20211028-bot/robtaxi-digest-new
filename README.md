# Robtaxi 行业简报 2.1（DeepSeek + 飞书/企业微信机器人）

该项目用于生成 Robtaxi 行业简报，并发布到 GitHub Pages。  
当前生产链路分为两种运行模式：
- `hourly`：每小时增量采集，更新当天候选池（不推送）
- `daily`：北京时间 09:00 基于当天候选池生成摘要页并推送

## 目标
- 按“当日新闻”口径产出国内/国外 Robtaxi 简报。
- 覆盖 `rss`、`search_api`、`query_rss`、`structured_web` 四类来源。
- 避免“搜索引擎可见但简报为空”，并用覆盖率指标可观测。

## 目录结构
- `app/fetch.py`：抓取原始数据，输出 `artifacts/raw/<date>/raw_items.jsonl`
- `app/parse.py`：标准化与 L1/L2 去重，输出 `artifacts/canonical/<date>/canonical_items.jsonl`
- `app/filter_relevance.py`：行业相关性过滤，输出 `artifacts/filtered/<date>/filtered_items.jsonl`
- `app/daily_pool.py`：合并当天增量池，输出 `artifacts/daily_pool/<date>/pool_items.jsonl`
- `app/recall_guard.py`：搜索对账与召回指标，写入 `run_report.json`
- `app/summarize.py`：L3 语义去重 + DeepSeek 摘要，输出 `artifacts/brief/<date>/brief_items.jsonl`
- `app/render.py`：生成 `site/index.html`
- `app/notify_feishu.py`：飞书推送
- `app/notify_wecom.py`：企业微信推送
- `app/validate_sources.py`：校验 `sources.json`
- `artifacts/reports/<date>/run_report.json`：运行报告

## 配置文件
- 主配置：`./sources.json`
- 支持 `source_type`：
  - `rss`
  - `search_api`
  - `query_rss`（Google News RSS 查询发现源）
  - `structured_web`

### 关键默认字段
- `defaults.strict_today_mode=true`：北京时间当日新闻硬约束
- `defaults.discovery_query_recency="when:1d"`：发现源查询时效词
- `defaults.discovery_max_results_per_query=30`
- `defaults.discovery_query_groups=["topic","brand","context"]`

### Structured 外链白名单
- 对于新闻入口页跳转到第三方新闻稿的场景，可使用：
  - `external_link_allow_domains`
- 用于放行“主站 -> 外链新闻稿”链路，避免 `url_not_in_allow_patterns` 误杀。

## 环境变量
- 摘要：`DEEPSEEK_API_KEY`
- 搜索补充（可选）：`SERPAPI_API_KEY`
- 飞书（推荐 webhook）
  - `FEISHU_WEBHOOK_URL`
  - `FEISHU_WEBHOOK_SECRET`（可选）
- 飞书（备选 app/open_id）
  - `FEISHU_APP_ID`
  - `FEISHU_APP_SECRET`
  - `FEISHU_RECEIVE_OPEN_ID`
- 企业微信
  - `WECOM_WEBHOOK_URL`

## 本地开发运行
1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 校验配置

```bash
python -m app.validate_sources ./sources.json
```

3. 按阶段执行（推荐）

```bash
DATE_BJ="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
python -m app.fetch --date "$DATE_BJ" --sources ./sources.json --out ./artifacts/raw --report ./artifacts/reports
python -m app.parse --date "$DATE_BJ" --in ./artifacts/raw --out ./artifacts/canonical --report ./artifacts/reports
python -m app.filter_relevance --date "$DATE_BJ" --in ./artifacts/canonical --out ./artifacts/filtered --sources ./sources.json --report ./artifacts/reports
python -m app.daily_pool --date "$DATE_BJ" --in ./artifacts/filtered --out ./artifacts/daily_pool --report ./artifacts/reports
python -m app.recall_guard --date "$DATE_BJ" --in ./artifacts/daily_pool --sources ./sources.json --report ./artifacts/reports --top-n 20 --min-recall 0.7
python -m app.summarize --date "$DATE_BJ" --in ./artifacts/daily_pool --out ./artifacts/brief --provider deepseek --report ./artifacts/reports
python -m app.render --date "$DATE_BJ" --in ./artifacts/brief --out ./site/index.html --report ./artifacts/reports --sources ./sources.json
python -m app.notify_feishu --date "$DATE_BJ" --html-url "https://<username>.github.io/<repo>/" --in ./artifacts/brief --report ./artifacts/reports
python -m app.notify_wecom --date "$DATE_BJ" --html-url "https://<username>.github.io/<repo>/" --in ./artifacts/brief --report ./artifacts/reports
```

4. 包装器入口

```bash
python3 ./scripts/robtaxi_digest.py --date "$DATE_BJ" --sources ./sources.json --output ./site/index.html
```

## GitHub Actions（生产）
工作流文件：`./.github/workflows/robtaxi-digest-pages.yml`

- 定时：
  - `10 * * * *`：每小时增量采集（hourly）
  - `0 1 * * *`：北京时间 09:00 完整发布（daily）
- `hourly` 路径：`fetch -> parse -> filter -> daily_pool`
- `daily` 路径：`fetch -> parse -> filter -> daily_pool -> recall_guard -> summarize -> render -> deploy -> notify`
- 通知只在 `daily` 执行，避免小时级轰炸
- 同一北京日期按渠道独立通知锁，避免重复推送

`workflow_dispatch` 支持：
- `run_mode`：`daily|hourly`
- `send_notify`：仅 `daily` 且显式为 `true` 时推送

## 质量与可靠性
- 相关性过滤：
  - 阶段0：当日硬约束 + URL 规则 + 发布时间规则
  - 阶段1：标题直通规则（`fast_pass`）
  - 阶段2：高精度评分（`high_precision`）
- 发布时间解析：
  - 支持绝对时间与常见相对时间（如 `2小时前`、`yesterday`）
  - 无法解析单独记录 `published_unparseable`
- 覆盖率监控：
  - `recall_guard` 对账 Google News RSS 基线，输出 `recall_at_20`
  - 当 `recall_at_20 < 0.7` 或 `baseline_count>0 且 pool=0`，报告触发覆盖率告警

## 关键报告字段
- 抓取：`non_search_fail_count`、`search_api_missing_key_count`
- 发现源：`discovery_items_raw_count`、`discovery_today_raw_count`、`discovery_items_canonical_count`、`discovery_today_canonical_count`
- 过滤：`fast_pass_kept_count`、`stage2_kept_count`、`published_missing_drop_count`、`published_unparseable_count`、`not_today_drop_count`
- 当天池：`daily_pool_size`
- 覆盖率：`baseline_count`、`baseline_matched_count`、`baseline_unmatched_count`、`recall_at_20`

## 常用排障
- 运行报告：`artifacts/reports/<date>/run_report.json`
- 过滤结果：`artifacts/filtered/<date>/filtered_items.jsonl`、`artifacts/filtered/<date>/dropped_items.jsonl`
- 当天池：`artifacts/daily_pool/<date>/pool_items.jsonl`
- 覆盖率对账：`baseline_unmatched_samples`
- 若飞书/企微推送失败，优先检查对应 secret 与返回错误码
