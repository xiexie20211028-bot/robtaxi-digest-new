# CLAUDE.md — Robotaxi 与 L3/L4 乘用车产业简报系统

## 1. 项目目标与边界

本项目每日生成 Robotaxi、L3/L4 乘用车及其直接相关供应链、监管和安全简报，发布到 GitHub Pages，并推送到飞书和企业微信。

纳入：

- Robotaxi 技术、准入、运营、商业化和安全事件。
- 明确属于 L3 或 L4 的乘用车准入、量产、上路和责任转移。
- 绑定明确 L3/L4 车型、客户、定点、认证或 Robotaxi 项目的核心供应链。
- 与上述范围直接相关的监管和安全信息。

排除：Robotruck、Robovan、矿区/港口无人车、普通 L2/L2+ 营销内容。

主时间窗口是北京时间前一自然日 `[D-1 00:00:00, D 00:00:00)`。`optimized` 和 `agent_domestic` 可按配置补录 72 小时内首次发现的重要事件。无法验证发布时间的内容不得入选。

## 2. 配置与 Profile

`sources.json` 是唯一信源配置（schema v3），不使用 YAML 副本。

| Profile | 用途 | 当前状态 |
|---|---|---|
| `legacy` | 现有国内外固定信源生产链 | `sources.json.active_profile` 默认值 |
| `optimized` | 结构化信源优化影子链 | 独立运行，不部署、不通知 |
| `agent_domestic` | 国内 Agent 主发现＋9 个可用监管骨干入口；海外沿用 legacy | 14 天复盘并人工批准后才切换 |

本地 CLI 的 `--profile` 优先级高于环境变量和 `active_profile`。GitHub Actions 通过仓库变量 `ROBTAXI_ACTIVE_PROFILE` 注入 `ROBTAXI_PROFILE`，不自动改写 `sources.json`。

修改配置后必须执行：

```bash
python -m app.validate_sources ./sources.json
```

## 3. 真实自动化时序

| 北京时间 | Workflow | 职责 |
|---|---|---|
| 08:00 | `robtaxi-industry-agent.yml` | 独立搜索、证据核验和事件输出；45 分钟超时 |
| 09:00 | `robtaxi-digest-pages.yml` | 主构建、Pages 部署、通知、自检与健康 Issue |
| 09:30 | `robtaxi-digest-shadow.yml` | `optimized` 独立影子运行，不部署、不通知 |
| 10:30 | `robtaxi-agent-review.yml` | Agent、legacy、optimized 和次日回看的盲态事件级复盘 |
| Issue 评论触发 | `robtaxi-agent-approval.yml` | 验证 `/agent-review approve <review_id>` 并切换 profile |
| PR 触发 | `robtaxi-pr-checks.yml` | 配置校验、测试、编译和差异检查；按标签运行真实 DeepSeek 演练 |

### 3.1 生产主流程

```text
sources.json
  ↓
fetch（列表发现、正文抓取、信源健康）
  ↓
artifacts/raw/<date>/raw_items.jsonl
  ↑ agent_domestic 时，import_events 在这里导入当日 AgentEvent
  ↓
parse（标准化、时间处理、L1/L2 去重）
  ↓
filter_relevance（范围硬门槛、时间窗口、评分与补录）
  ↓
enrich（短文本回源补全正文）
  ↓
summarize（What / Why / So what、事件聚类、L3 语义去重）
  ↓
artifacts/brief/<date>/brief_items.jsonl
  ├─ editorial_digest → artifacts/digest/<date>/daily_digest.{json,txt} → 通知
  └─ render → site/index.html → GitHub Pages 部署 → 通知中的完整页面链接

飞书＋企业微信
  ↓ finalize_notify（汇总双渠道结果）
self_check（构建、测试、部署、通知、信源和产物检查）
  ↓
health_issue（异常 Issue 、恢复关闭和飞书/企微告警）
```

`editorial_digest` 和 `render` 都读取 `brief_items.jsonl`。主编摘要主要服务于聊天工具推送，HTML 页面仍由 `render` 根据 brief 生成。

Agent 只在 `agent_domestic` profile 中导入主流程；`legacy` 和 `optimized` 调用 `import_events` 时为可预期的不导入。这个边界不得改成“Agent 读取固定源候选后补漏”。

## 4. 模块责任

### 4.1 主 Pipeline

| 模块 | 责任 |
|---|---|
| `app/fetch.py` | 抓取调度器；根据 `source_type` 分发，不承担所有站点解析细节 |
| `app/fetch_rss.py` | RSS/Atom 获取与解析 |
| `app/fetch_structured.py` | Sitemap、CSS、JSON-LD 等结构化站点抓取 |
| `app/fetch_discovery.py` | API、搜索结果、query RSS 等发现源 |
| `app/parse.py` | Canonical 字段标准化、URL/标题去重、已见历史与首次发现时间 |
| `app/filter_relevance.py` | 过滤阶段调度、补录限制和运行报告 |
| `app/filter_rules.py` | 范围、证据、时间窗口等硬规则 |
| `app/filter_scoring.py` | 相关性、重要性和候选排序 |
| `app/site_rules.py` | 专站 URL、日期和白名单规则 |
| `app/enrich.py` | 回源读取文章页，补齐过短正文 |
| `app/summarize.py` | 结构化摘要、语义去重、摘要缓存和已见状态 |
| `app/editorial_digest.py` | 生成可直接推送的主编摘要；DeepSeek 不可用时生成本地降级摘要 |
| `app/render.py` | 选稿、质量指标和 HTML 页面渲染 |

### 4.2 配置、分类与健康

| 模块 | 责任 |
|---|---|
| `app/source_config.py` | schema v3 元数据默认值、profile 解析和信源启停 |
| `app/validate_sources.py` | 配置结构与业务约束校验 |
| `app/taxonomy.py` | Robotaxi/L3/L4/供应链/监管分类与排除范围 |
| `app/source_health.py` | 35 天信源健康历史和 `silent_dead` 识别 |
| `app/quality_metrics.py` | 7/30 天覆盖、一手来源、地区、发现依赖和单源集中度 |
| `app/social_provider.py` | 社交平台统一接口；当前仅无登录态的人工种子源 |

### 4.3 Agent、通知与运维

| 模块 | 责任 |
|---|---|
| `app/industry_agent/runner.py` | 三段式研究、预算/搜索次数停止和事件输出 |
| `contracts.py` / `providers.py` | Model、Search、PageReader、Evidence 的合约与 DeepSeek 实现 |
| `page_reader.py` / `verifier.py` | 通用网页读取、官方域名、日期和证据独立性验证 |
| `import_events.py` | 在主流程 raw 层导入结构化 AgentEvent |
| `review.py` / `approval.py` | 盲态复盘、14 天门槛和人工批准 |
| `runtime_profile.py` | 运行时 profile 和 Agent 连续失败回滚 |
| `app/notify_feishu.py` / `notify_wecom.py` | 双渠道推送 |
| `app/finalize_notify.py` | 归一化并汇总双渠道结果 |
| `app/self_check.py` | 最终上线健康检查与修复请求产物 |
| `app/health_issue.py` | 稳定事件 ID、GitHub Issue 同步、恢复和告警 |
| `app/report.py` | 各阶段共用的 `run_report.json` 读写 |

## 5. 目录与产物

```text
app/                         核心模块
app/industry_agent/          独立国内行业 Agent
app/digest_template.html     页面模板
scripts/                     本地包装器、launchd、配置迁移与回放工具
tests/                       单元、fixture、范围黄金集和回滚测试
.github/workflows/           生产、影子、Agent、复盘、批准和 PR 检查
.github/codex/               健康事件诊断与批准规则
sources.json                 唯一信源配置

artifacts/raw/<date>/raw_items.jsonl
artifacts/canonical/<date>/canonical_items.jsonl
artifacts/filtered/<date>/{filtered_items,dropped_items}.jsonl
artifacts/enriched/<date>/enriched_items.jsonl
artifacts/brief/<date>/brief_items.jsonl
artifacts/digest/<date>/{daily_digest.json,daily_digest.txt}
artifacts/reports/<date>/run_report.json
artifacts/health/<date>/{health_report.json,health_report.md,repair_request.json}
artifacts-agent/<date>/{agent_events.jsonl,agent_trace.jsonl,agent_run_report.json}
site/index.html
```

运行产物不提交 Git。`.state/`、`.state-shadow/`和 `.state-agent/` 分别保存生产、optimized 和 Agent 的独立状态，GitHub Actions 通过 cache 跨日恢复。不得让影子或本地演练共用生产去重状态。

## 6. 本地运行

使用 Python 3.11；`.python-version` 与 `pyproject.toml` 是本地和工具链的版本事实来源：

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m app.validate_sources ./sources.json
python -m pytest -q
```

一键构建：

```bash
DATE_BJ="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
python3 ./scripts/robtaxi_digest.py \
  --date "$DATE_BJ" \
  --sources ./sources.json \
  --output ./site/index.html
```

本地包装器执行的范围与生产 build job 一致：

```text
fetch → import_events → parse → filter_relevance → enrich
      → summarize → editorial_digest → render
```

`--dry-run` 仍执行到 `editorial_digest`，只跳过 HTML 渲染。`--health-report` 只执行 fetch 并输出信源统计。本地包装器不默认执行 deploy、notify、finalize_notify、self_check 或 health_issue，避免产生外部副作用。

单独运行 Agent：

```bash
python -m app.industry_agent.runner \
  --date "$DATE_BJ" \
  --config ./sources.json \
  --out ./artifacts-agent \
  --state ./.state-agent

python3 ./scripts/robtaxi_digest.py \
  --profile agent_domestic \
  --date "$DATE_BJ" \
  --agent-handoff ./artifacts-agent \
  --dry-run
```

手工测试通知时必须明确传入 digest，且会真实发送消息：

```bash
python -m app.notify_feishu --date "$DATE_BJ" --html-url "http://localhost" --in ./artifacts/brief --digest-root ./artifacts/digest --report ./artifacts/reports
python -m app.notify_wecom --date "$DATE_BJ" --html-url "http://localhost" --in ./artifacts/brief --digest-root ./artifacts/digest --report ./artifacts/reports
```

## 7. 开发约定

### 7.0 研发任务治理

所有会修改 Git 跟踪文件的 Bug、需求、优化、技术债和监控实现，必须遵循根目录 [`AGENTS.md`](AGENTS.md) 与 [`.github/codex/robtaxi-development-workflow.md`](.github/codex/robtaxi-development-workflow.md)：以正式 Issue 加入“Robotaxi Digest 产品研发总盘”、按风险填写必要信息并通过 preflight。不得以自动 Health Issue 直接作为修复主任务；结束时必须向非技术用户清楚说明代码、PR、合并和上线状态。

- 新模块使用 `from __future__ import annotations` 和类型注解。
- 代码注释优先使用中文。
- Stage CLI 统一以 `python -m app.<module>` 运行，暴露 `main() -> int`。
- JSON/JSONL 使用 UTF-8 和 `ensure_ascii=False`。
- HTTP 请求优先复用 `app.common` 的客户端和缓存/重试逻辑，不在站点模块中重复实现。
- CSS/JSON-LD 专站修改必须配固定 HTML fixture；不以当时在线页面“碰巧可抓”作为验收。
- 筛选先程序硬门槛，后模型评分；搜索摘要不能作为最终证据。
- 新增字段或 Stage 时，同步 dataclass/contract、run report、测试、README 和本文档。
- 修改 Pipeline 时，同时检查本地包装器、生产 workflow、optimized shadow 和 PR rehearsal，避免链路再次漂移。

## 8. 环境变量

| 变量 | 用途 |
|---|---|
| `DEEPSEEK_API_KEY` | 结构化摘要、主编摘要、行业 Agent 和复盘评审 |
| `DEEPSEEK_BASE_URL` / `DEEPSEEK_MODEL` | DeepSeek 端点和模型覆盖 |
| `SERPAPI_API_KEY` | 可选搜索发现源；默认不是 P0 依赖 |
| `ROBTAXI_PROFILE` | 运行时 profile；GitHub Actions 由 `ROBTAXI_ACTIVE_PROFILE` 注入 |
| `FEISHU_WEBHOOK_URL` / `FEISHU_WEBHOOK_SECRET` | 飞书 Webhook 通知 |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `FEISHU_RECEIVE_OPEN_ID` | 飞书 App 备选通知 |
| `WECOM_WEBHOOK_URL` | 企业微信通知 |

## 9. 排障入口

```bash
# 运行报告
python3 -m json.tool artifacts/reports/<date>/run_report.json

# 入选和淘汰候选
less artifacts/filtered/<date>/filtered_items.jsonl
less artifacts/filtered/<date>/dropped_items.jsonl

# 主编摘要
cat artifacts/digest/<date>/daily_digest.txt

# Agent 运行状态与可审计轨迹（不保存模型隐式思维过程）
python3 -m json.tool artifacts-agent/<date>/agent_run_report.json
less artifacts-agent/<date>/agent_trace.jsonl
```

健康等级为 `healthy < warning < error < critical`。生产非健康结果由 `health_issue` 创建或更新 GitHub Issue，恢复后自动关闭。`.github/codex/robtaxi-health-diagnosis.md` 和 `robtaxi-health-approval.md` 定义后续诊断与人工批准规则。
