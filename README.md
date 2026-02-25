# Robtaxi 行业简报 2.0（DeepSeek + 飞书应用机器人）

该项目每天北京时间 **09:00** 自动生成 Robtaxi 行业简报 HTML，并发布到 GitHub Pages。  
生产链路：`fetch -> parse -> filter_relevance -> summarize -> render -> deploy -> notify_feishu`。

## 目标
- 每天生成国内/国外 Robtaxi 简报，包含中文摘要、原文链接、运行状态。
- 覆盖 `rss`、`search_api`、`structured_web` 三类信息源。
- 覆盖 `rss`、`search_api`、`query_rss`、`structured_web` 四类信息源。
- 发布成功后推送到飞书应用机器人（个人 `open_id`）。

## 目录结构
- `app/fetch.py`: 拉取原始数据，输出 `artifacts/raw/<date>/raw_items.jsonl`
- `app/parse.py`: 结构化与 L1/L2 去重，输出 `artifacts/canonical/<date>/canonical_items.jsonl`
- `app/filter_relevance.py`: 行业相关性过滤，输出 `artifacts/filtered/<date>/filtered_items.jsonl`
- `app/summarize.py`: L3 语义去重 + DeepSeek 摘要，输出 `artifacts/brief/<date>/brief_items.jsonl`
- `app/render.py`: 生成 `site/index.html`
- `app/notify_feishu.py`: 飞书推送（open_id）
- `app/validate_sources.py`: 校验 `sources.json`
- `artifacts/reports/<date>/run_report.json`: 运行报告

## 配置文件
- 主配置：`./sources.json`
- 支持 `source_type`:
  - `rss`
  - `search_api`
  - `query_rss`（基于 Google News RSS 的查询驱动发现源，无需 key）
  - `structured_web`（`extractor`: `css_selector` / `json_ld` / `sitemap`）
- 相关性配置新增：
  - `core_keywords_domestic/foreign`
  - `context_keywords_domestic/foreign`
  - `brand_keywords_domestic/foreign`
  - `keyword_pair_rules`（L3/L4、货运词配对约束）
  - `fast_pass_*`（二阶段过滤中的“标题直通规则”）

## 环境变量
- 摘要：`DEEPSEEK_API_KEY`
- 搜索补充（可选）：`SERPAPI_API_KEY`
- 飞书推送（推荐：群自定义机器人 Webhook，不需要企业审核）：
  - `FEISHU_WEBHOOK_URL`
  - `FEISHU_WEBHOOK_SECRET`（可选，开启“签名校验”才需要）
- 飞书推送（备选：飞书应用机器人 open_id，需要租户/企业权限）：
  - `FEISHU_APP_ID`
  - `FEISHU_APP_SECRET`
  - `FEISHU_RECEIVE_OPEN_ID`

说明：
- 当前默认将 Search API 作为“告警哨兵源”保留启用；若未配置 `SERPAPI_API_KEY`，报告会保留失败告警，避免静默漏报。
- `query_rss` 查询发现源默认启用，不依赖 `SERPAPI_API_KEY`。

## 本地开发运行
1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 校验配置

```bash
python -m app.validate_sources ./sources.json
```

3. 按阶段执行

```bash
DATE_BJ="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
python -m app.fetch --date "$DATE_BJ" --sources ./sources.json --out ./artifacts/raw --report ./artifacts/reports
python -m app.parse --date "$DATE_BJ" --in ./artifacts/raw --out ./artifacts/canonical --report ./artifacts/reports
python -m app.filter_relevance --date "$DATE_BJ" --in ./artifacts/canonical --out ./artifacts/filtered --sources ./sources.json --report ./artifacts/reports
python -m app.summarize --date "$DATE_BJ" --in ./artifacts/filtered --out ./artifacts/brief --provider deepseek --report ./artifacts/reports
python -m app.render --date "$DATE_BJ" --in ./artifacts/brief --out ./site/index.html --report ./artifacts/reports --sources ./sources.json
python -m app.notify_feishu --date "$DATE_BJ" --html-url "https://<username>.github.io/<repo>/" --in ./artifacts/brief --report ./artifacts/reports
```

4. 兼容旧入口（包装器）

```bash
python3 ./scripts/robtaxi_digest.py --date "$DATE_BJ" --sources ./sources.json --output ./site/index.html
```

## GitHub Actions（唯一生产调度）
工作流文件：`./.github/workflows/robtaxi-digest-pages.yml`

- 定时：`0 1 * * *`（UTC 01:00 = 北京时间 09:00）
- 顺序：`fetch -> parse -> filter_relevance -> summarize -> render -> deploy -> notify`
- 手动运行默认不推送飞书（`send_notify=false`），避免非定时时段误推送；需要手动推送时在 Run workflow 勾选 `send_notify=true`
- 同一北京日期启用“通知日锁”（daily lock），避免重复触发导致同日重复推送

需要在 GitHub Secrets 配置：
- `DEEPSEEK_API_KEY`
- `FEISHU_WEBHOOK_URL`（推荐）
- `FEISHU_WEBHOOK_SECRET`（可选）
- `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `FEISHU_RECEIVE_OPEN_ID`（备选）
- `SERPAPI_API_KEY`（可选）

## 本地 launchd（仅开发调试）
仅用于本机调试，不作为生产调度。

```bash
./scripts/install_launchd.sh
```

## 质量与可靠性
- 相关性过滤（高精度默认）：
  - 阶段0硬约束：时间窗、URL 规则、发布时间规则
  - 阶段1直通：标题命中中英核心词 + 公司词或运营/监管词 + 48小时内，直接保留
  - 阶段2评分：仅对命中候选信号（公司/品牌/上下文/语义）的条目执行高精度评分
  - 候选门控：未命中候选信号直接剔除（`candidate_gate_miss`）
  - 核心词/上下文词/品牌词/公司别名命中、负向词扣分、分源阈值
  - L3/L4、无人驾驶货运等关键词需满足自动驾驶语义配对
  - 通用媒体源要求“核心词或公司信号”
  - 当前默认关闭通用媒体单源限额（`enable_general_media_source_cap=false`）
  - 默认丢弃无发布时间条目（`general_media/newsroom`）；`regulator` 可按配置例外保留
- 去重：
  - L1: URL 规范化去重
  - L2: 标题标准化去重
  - L3: TF-IDF 余弦相似度去重（阈值 0.85）
- 摘要：优先 DeepSeek；失败自动降级规则摘要
- 单源失败不阻塞总产出
- 飞书推送失败不回滚网页发布，状态写入 `run_report.json`
- 运行报告新增关键字段：
  - 稳定性：`non_search_fail_count`、`search_api_missing_key_count`
  - 发现源：`discovery_items_raw_count`、`discovery_items_canonical_count`
  - 过滤链路：`fast_pass_kept_count`、`fast_pass_drop_count`、`stage2_scored_count`、`stage2_kept_count`、`candidate_gate_drop_count`、`published_missing_drop_count`
  - 产出量：`brief_count`

## 常用排障
- 查看运行报告：`artifacts/reports/<date>/run_report.json`
- 查看过滤结果：`artifacts/filtered/<date>/filtered_items.jsonl`、`artifacts/filtered/<date>/dropped_items.jsonl`
- 查看健康检查：`./scripts/test_sources_health.sh`
- 若飞书失败，先检查 `FEISHU_*` 三个变量和应用权限范围
- 若发现同日重复推送，先确认是否重复手动运行且 `send_notify=true`；工作流已内置同日去重锁
- 页面失败源仅展示“失败源名称 + 中文原因摘要”；详细错误在页面折叠区与报告 `source_stats[].error_raw`
