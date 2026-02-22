# Robtaxi 行业简报 2.0（DeepSeek + 飞书应用机器人）

该项目每天北京时间 **09:00** 自动生成 Robtaxi 行业简报 HTML，并发布到 GitHub Pages。  
生产链路：`fetch -> parse -> filter_relevance -> summarize -> render -> deploy -> notify_feishu`。

## 目标
- 每天生成国内/国外 Robtaxi 简报，包含中文摘要、原文链接、运行状态。
- 覆盖 `rss`、`search_api`、`structured_web` 三类信息源。
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
  - `structured_web`（`extractor`: `css_selector` / `json_ld` / `sitemap`）

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

需要在 GitHub Secrets 配置：
- `DEEPSEEK_API_KEY`
- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `FEISHU_RECEIVE_OPEN_ID`
- `SERPAPI_API_KEY`（可选）

## 本地 launchd（仅开发调试）
仅用于本机调试，不作为生产调度。

```bash
./scripts/install_launchd.sh
```

## 质量与可靠性
- 相关性过滤（高精度默认）：
  - 时间窗、URL 规则、关键词/公司别名命中、负向词扣分、分源阈值
  - 通用媒体源要求“核心词或公司信号”，且每源每日默认最多 2 条
- 去重：
  - L1: URL 规范化去重
  - L2: 标题标准化去重
  - L3: TF-IDF 余弦相似度去重（阈值 0.85）
- 摘要：优先 DeepSeek；失败自动降级规则摘要
- 单源失败不阻塞总产出
- 飞书推送失败不回滚网页发布，状态写入 `run_report.json`

## 常用排障
- 查看运行报告：`artifacts/reports/<date>/run_report.json`
- 查看过滤结果：`artifacts/filtered/<date>/filtered_items.jsonl`、`artifacts/filtered/<date>/dropped_items.jsonl`
- 查看健康检查：`./scripts/test_sources_health.sh`
- 若飞书失败，先检查 `FEISHU_*` 三个变量和应用权限范围
