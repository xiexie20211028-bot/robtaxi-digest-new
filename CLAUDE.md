# CLAUDE.md — Robtaxi 行业简报系统

## 项目介绍

自动化的 Robotaxi 行业每日简报系统（v4.2）。每天北京时间 09:00 触发，从 20+ 国内外媒体和企业官网抓取新闻，经过标准化、去重、相关性过滤后，调用 DeepSeek API 生成结构化摘要（What / Why / So what），最终发布到 GitHub Pages，并推送到飞书和企业微信。

监控范围：Waymo、Tesla、百度 Apollo/萝卜快跑、小马智行、文远知行、滴滴自动驾驶等 19 家国内外头部公司。

**核心时间规则**：统计窗口固定为北京时间前一自然日 `[D-1 00:00:00, D 00:00:00)`，窗口外新闻一律丢弃。`published_at` 缺失或不可解析的条目同样丢弃。

---

## 技术栈

| 层次 | 技术 |
|------|------|
| 编程语言 | Python 3.11 |
| 唯一第三方依赖 | `beautifulsoup4>=4.12.0`（HTML 解析） |
| AI 摘要 | DeepSeek API（`DEEPSEEK_API_KEY`） |
| 新闻搜索补充 | SerpAPI / Google News RSS |
| 数据格式 | JSONL（中间产物）、JSON（配置/报告）、HTML（输出） |
| 去重算法 | TF-IDF + 余弦相似度，三级去重（L1 URL、L2 标题、L3 语义） |
| 自动化运行 | GitHub Actions（cron `0 1 * * *` = 北京时间 09:00） |
| 页面托管 | GitHub Pages |
| 通知渠道 | 飞书 Webhook / App API、企业微信 Webhook |

---

## 项目结构

```
app/                      # 核心 Pipeline 模块
  common.py               # 共享工具：数据类、HTTP 客户端、解析工具
  fetch.py                # Stage 1：从 RSS / SerpAPI / 结构化网页抓取原始数据
  parse.py                # Stage 2：标准化 + L1/L2 去重
  filter_relevance.py     # Stage 3：关键词相关性过滤 + 时间窗口硬约束
  enrich.py               # Stage 3.5：正文补全（短摘要条目拉取全文）
  summarize.py            # Stage 4：DeepSeek 摘要 + L3 语义去重
  render.py               # Stage 5：渲染 site/index.html
  notify_feishu.py        # 推送飞书
  notify_wecom.py         # 推送企业微信
  validate_sources.py     # 配置合法性校验
  report.py               # 运行报告读写工具

scripts/
  robtaxi_digest.py       # 完整 Pipeline 包装器入口（本地一键运行）
  run_if_due.sh           # 本地 launchd 防重运行守卫
  install_launchd.sh      # macOS 定时任务安装脚本

artifacts/                # 运行产物（按日期分区，不提交 Git）
  raw/<date>/             # Stage 1 输出：raw_items.jsonl
  canonical/<date>/       # Stage 2 输出：canonical_items.jsonl
  filtered/<date>/        # Stage 3 输出：filtered_items.jsonl / dropped_items.jsonl
  enriched/<date>/        # Stage 3.5 输出：enriched_items.jsonl
  brief/<date>/           # Stage 4 输出：brief_items.jsonl
  reports/<date>/         # 各 Stage 运行报告：run_report.json

site/
  index.html              # 最终发布页面

sources.json              # 主配置文件（数据源、公司、搜索查询）
.github/workflows/
  robtaxi-digest-pages.yml  # GitHub Actions 生产工作流
```

---

## 开发规范

### 代码风格

- 所有模块顶部使用 `from __future__ import annotations`（PEP 563 延迟求值）
- 全面使用类型注解，返回值、参数均标注
- 结构化数据统一使用 `@dataclass`，当前四个核心数据类：
  - `RawItem`：抓取原始条目
  - `CanonicalItem`：标准化条目
  - `BriefItem`：摘要条目
  - `SourceStat`：数据源统计
- JSON 输出统一 `ensure_ascii=False`，文件编码统一 `utf-8`
- 禁止使用第三方 HTTP 库（requests 等），HTTP 工具封装在 `common.py` 的 `http_get_bytes` / `http_post_json` 中，内置重试 + curl 兜底

### 每个 Stage 模块的约定

- 暴露 `main() -> int` 函数，通过 `argparse` 接收 CLI 参数
- 入口为 `if __name__ == "__main__": raise SystemExit(main())`
- 模块以 `python -m app.<module_name>` 方式调用，不直接执行脚本
- 每个 Stage 完成后更新 `artifacts/reports/<date>/run_report.json`，使用 `report.mark_stage` 和 `report.patch_report`

### 配置修改

- 唯一配置文件：`sources.json`（`sources.yaml` 为参考副本，不用于运行）
- 添加新数据源：在 `sources` 数组中追加，`enabled: true/false` 控制开关
- 添加新监控公司：在 `companies` 数组中追加，设置 `aliases` 用于去重匹配
- 修改后务必用 `python -m app.validate_sources ./sources.json` 校验

### 数据流

```
sources.json
    ↓ fetch.py
artifacts/raw/<date>/raw_items.jsonl
    ↓ parse.py
artifacts/canonical/<date>/canonical_items.jsonl
    ↓ filter_relevance.py
artifacts/filtered/<date>/filtered_items.jsonl
    ↓ enrich.py
artifacts/enriched/<date>/enriched_items.jsonl
    ↓ summarize.py
artifacts/brief/<date>/brief_items.jsonl
    ↓ render.py
site/index.html
```

---

## 常用命令

### 环境准备

```bash
pip install -r requirements.txt
```

### 校验配置

```bash
python -m app.validate_sources ./sources.json
```

### 分阶段运行（调试时按需单跑）

```bash
# 设置当前北京日期
DATE_BJ="$(TZ=Asia/Shanghai date +%Y-%m-%d)"

# Stage 1：抓取
python -m app.fetch --date "$DATE_BJ" --sources ./sources.json --out ./artifacts/raw --report ./artifacts/reports

# Stage 2：解析 + 去重
python -m app.parse --date "$DATE_BJ" --in ./artifacts/raw --out ./artifacts/canonical --report ./artifacts/reports

# Stage 3：相关性过滤
python -m app.filter_relevance --date "$DATE_BJ" --in ./artifacts/canonical --out ./artifacts/filtered --sources ./sources.json --report ./artifacts/reports

# Stage 3.5：正文补全
python -m app.enrich --date "$DATE_BJ" --in ./artifacts/filtered --out ./artifacts/enriched --report ./artifacts/reports

# Stage 4：AI 摘要（需要 DEEPSEEK_API_KEY）
python -m app.summarize --date "$DATE_BJ" --in ./artifacts/enriched --out ./artifacts/brief --provider deepseek --report ./artifacts/reports --sources ./sources.json

# Stage 5：渲染 HTML
python -m app.render --date "$DATE_BJ" --in ./artifacts/brief --out ./site/index.html --report ./artifacts/reports --sources ./sources.json
```

### 一键完整运行

```bash
DATE_BJ="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
python3 ./scripts/robtaxi_digest.py --date "$DATE_BJ" --sources ./sources.json --output ./site/index.html
```

### 本地通知推送（测试用）

```bash
# 飞书
python -m app.notify_feishu --date "$DATE_BJ" --html-url "http://localhost" --in ./artifacts/brief --report ./artifacts/reports

# 企业微信
python -m app.notify_wecom --date "$DATE_BJ" --html-url "http://localhost" --in ./artifacts/brief --report ./artifacts/reports
```

### 排障

```bash
# 查看过滤掉的条目及原因
cat artifacts/filtered/<date>/dropped_items.jsonl | python3 -m json.tool | less

# 查看运行报告
cat artifacts/reports/<date>/run_report.json | python3 -m json.tool

# 查看数据源健康状态
cat .state/source_health_latest.tsv
```

---

## 环境变量

| 变量 | 用途 | 是否必须 |
|------|------|---------|
| `DEEPSEEK_API_KEY` | AI 摘要（Stage 4） | 必须 |
| `SERPAPI_API_KEY` | SerpAPI 搜索补充源 | 可选（缺失时该源跳过，仅告警） |
| `FEISHU_WEBHOOK_URL` | 飞书 Webhook 推送 | 推送时必须 |
| `FEISHU_WEBHOOK_SECRET` | 飞书 Webhook 签名校验 | 可选 |
| `FEISHU_APP_ID` | 飞书 App 模式（备选） | 可选 |
| `FEISHU_APP_SECRET` | 飞书 App 模式（备选） | 可选 |
| `FEISHU_RECEIVE_OPEN_ID` | 飞书 App 模式接收人 | 可选 |
| `WECOM_WEBHOOK_URL` | 企业微信 Webhook 推送 | 推送时必须 |
