# Robotaxi 与 L3/L4 乘用车产业简报

本项目用于每日生成 Robotaxi、L3/L4 乘用车及其直接相关核心供应链、监管与安全简报，并发布到 GitHub Pages，同时推送到飞书和企业微信机器人。

## 核心规则（信源配置 v3）
- 统计窗口固定为北京时间前一自然日：`[D-1 00:00:00, D 00:00:00)`（左闭右开）。
- optimized profile 允许补录 72 小时内首次发现的重要内容，每日最多 2 条并标记“补录”。
- `published_at` 缺失或不可解析一律淘汰。
- 每天北京时间 `09:00` 运行一次完整链路。
- 只覆盖 Robotaxi、L3/L4 乘用车、直接绑定这些项目的核心供应链和监管安全；排除 Robotruck、Robovan、矿区/港口无人车及普通 L2/L2+ 营销新闻。
- P0 为监管、数据集、公告、IR 和企业正式发布；X/公众号只作为 P1 线索发现，不是生产必需依赖。
- 国内新增 Agent-first 影子链：Agent 自主研究行业事件；通过 14 天门槛后，国内固定源只保留 9 个已验证可用的监管入口，海外继续使用 legacy。
- 每日最多 12 条，每家公司和每个信源最多 2 条，搜索与社交直接入选合计不超过 25%。
- 每条摘要强制结构：`What / Why / So what`，并标注“影响对象”。
- 搜索发现链路默认使用“搜索结果页 + 回源验证”，不再依赖 Google News 包装链接解析。

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
- `app/taxonomy.py`：范围硬门槛与结构化分类
- `app/source_health.py`：35 天信源健康历史与静默失效识别
- `app/social_provider.py`：社交补漏统一接口（第一版仅启用无登录态的人工种子 provider）
- `app/industry_agent/`：独立行业研究 Agent、DeepSeek/Web Search Provider、通用网页读取、证据核验、导入、复盘与回滚

## 国内行业 Agent-first

Agent 不读取旧信源候选，输入只有时间窗口、五类覆盖范围、精简企业别名和 35 天事件指纹。每日执行：

1. 行业扫描：自主搜索 Robotaxi、L3、L4、绑定项目的供应链和监管安全。
2. 盲区审计：对没有覆盖或证据薄弱的领域换角度继续搜索。
3. 证据整理：回到监管/企业原文，或取得两家独立媒体证据。
4. 程序硬核验：原始链接、发布日期、范围、证据独立性和 65 分门槛。

发现方式和证据质量分别记录：`discovery_method=agent_search` 只表示如何找到事件，最终的 `evidence_type` 仍按监管、企业公告或媒体计算。

首发 Provider：

- `ModelProvider=deepseek`，模型 `deepseek-v4-flash`
- `SearchProvider=deepseek_web`，使用 DeepSeek Anthropic 兼容接口的服务器端 Web Search
- `PageReader=GenericPageReader`，只使用 JSON-LD、通用 meta 和正文选择器，不增加企业专站适配器
- 每日最多 20 次 Web Search，估算费用达到 2 元即停止扩展，仅保留已核验事件

`agent_domestic` profile 保留国内工信部、交通运输部、市场监管总局召回以及北京、北京经开区、上海、广州、深圳、武汉共 9 个已验证可用的监管入口；已知返回 403 的公安部交管入口保持停用，海外启用源与 `legacy` 完全一致。

### 搜索发现说明
- 生产发现链路分两类：
  - 骨干真实源：`rss / structured_web / official_api`
  - 搜索发现源：`search_result`
- `search_result` 当前试点提供方：
  - 国内：`toutiao_news`
  - 国外：`bing_news`
- 搜索结果页只负责发现候选；最终是否入选，仍以真实文章页的发布时间为准。

### 社交补漏说明

- X 使用 Bing 限域查询发现官方账号 `/status/` 永久链接，不依赖 X API。
- 微信公众号不使用登录态、手机 RPA 或采购 API；可把人工确认的种子写入 `.state/manual_social_seeds.json`。
- 种子格式为 `{"items":[{"platform":"wechat|x","account":"官方账号精确名称","permalink":"永久链接","published_at_utc":"ISO-8601","text":"正文或标题","outbound_urls":[]}]}`。
- 账号、永久链接或发布时间无法验证时一律拒绝；社交候选为空或失败不触发 P0 告警。

## 配置文件
- 唯一配置：`./sources.json`（schema v3）
- `active_profile` 当前保持 `legacy`；`optimized` 与行业 Agent 分别影子运行。
- 核心 CLI 支持 `--profile legacy|optimized|agent_domestic`。
- 14 天审批通过后由仓库变量 `ROBTAXI_ACTIVE_PROFILE=agent_domestic` 切换生产，不自动改写配置文件。
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
- 运行时 profile：`ROBTAXI_PROFILE`（本地可选；GitHub Actions 使用仓库变量 `ROBTAXI_ACTIVE_PROFILE`）
- 搜索补充（可选）：`SERPAPI_API_KEY`（默认关闭；无 key 不产生生产失败告警）
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
python -m app.industry_agent.import_events --date "$DATE_BJ" --sources ./sources.json --in ./artifacts-agent --raw ./artifacts/raw --report ./artifacts/reports
python -m app.parse --date "$DATE_BJ" --in ./artifacts/raw --out ./artifacts/canonical --report ./artifacts/reports
python -m app.filter_relevance --date "$DATE_BJ" --in ./artifacts/canonical --out ./artifacts/filtered --sources ./sources.json --report ./artifacts/reports
python -m app.enrich --date "$DATE_BJ" --in ./artifacts/filtered --out ./artifacts/enriched --report ./artifacts/reports
python -m app.summarize --date "$DATE_BJ" --in ./artifacts/enriched --out ./artifacts/brief --provider deepseek --report ./artifacts/reports --sources ./sources.json
python -m app.render --date "$DATE_BJ" --in ./artifacts/brief --out ./site/index.html --report ./artifacts/reports --sources ./sources.json
```

4. 包装器入口

```bash
python3 ./scripts/robtaxi_digest.py --date "$DATE_BJ" --sources ./sources.json --output ./site/index.html

# optimized 仅用于本地或影子验证
python3 ./scripts/robtaxi_digest.py --profile optimized --date "$DATE_BJ" --sources ./sources.json --dry-run

# 独立运行国内行业 Agent（业务失败也会写出 failed 报告供主流程降级）
python -m app.industry_agent.runner --date "$DATE_BJ" --config ./sources.json --out ./artifacts-agent --state ./.state-agent

# 验证 Agent 事件进入末端合并；仅 agent_domestic 会实际导入
python3 ./scripts/robtaxi_digest.py --profile agent_domestic --date "$DATE_BJ" --agent-handoff ./artifacts-agent --dry-run
```

## GitHub Actions（生产）
工作流：`./.github/workflows/robtaxi-digest-pages.yml`

- 定时：`0 1 * * *`（UTC），即北京时间 `09:00`
- 链路：`fetch -> parse -> filter -> enrich -> summarize -> editorial_digest -> render -> deploy -> notify -> self_check`
- 手动触发默认不推送，`send_notify=true` 才推送
- 同一北京日期按渠道独立锁（飞书/企微），避免重跑重复推送
- build 失败时仍上传基础 artifact；飞书和企微独立尝试，最终统一聚合通知状态
- `workflow_dispatch.self_check_fixture` 可人工注入 `warning/error/critical` 做验收，生产保持 `none`
- `.state` 通过 Actions cache 跨日持久化，保存 35 天已见内容、摘要缓存、HTTP 条件请求缓存和健康历史。

## optimized 影子运行

工作流：`./.github/workflows/robtaxi-digest-shadow.yml`

- 每天在生产任务后运行 `optimized` profile。
- 使用独立 `.state-shadow`，不污染生产去重历史。
- 不部署、不通知，只保留 35 天产物供 14 天验收。
- 上线前检查 P0 成功率、正文/日期解析率、黄金集精度与召回、一手来源占比、发现源依赖度及严重健康事件。
- `app.rollout_gate` 每日计算并保存上线门槛；未满 14 天或任一指标未达标时只报告，不自动切换生产。
- 历史产物到位后可运行 `python scripts/replay_production.py --input <历史产物根目录>`，默认要求至少 8 个统计日。

## Agent 工作流与上线

- `.github/workflows/robtaxi-industry-agent.yml`：北京时间 08:00 独立搜索与核验；保存 `agent_events.jsonl`、`agent_trace.jsonl`、`agent_run_report.json` 35 天。
- `.github/workflows/robtaxi-digest-pages.yml`：09:00 恢复当天 Agent 交接产物；影子期不导入，通过审批后才在末端合并。
- `.github/workflows/robtaxi-agent-review.yml`：10:30 进行来源盲态的事件级复盘；第 7 天提醒，第 14 天创建/更新上线复盘 Issue，第 16 天未审批再次提醒。
- `.github/workflows/robtaxi-agent-approval.yml`：校验 `/agent-review approve <review_id>`，自动门槛和人工推翻率合格后切换到 `agent_domestic`。

上线门槛包括 14 个有效日、至少 13 个成功日、无连续两日失败、重要事件召回率 90%、精度 85%、旧流程重要事件复现率 90%、链接和日期验证率 95%、强证据占比 90%、日费用 P95 不超过 2 元，并且必须完成最多 20 条人工抽检。

切换后的第一阶段继续运行旧企业专站 shadow 7 天；稳定后仓库变量进入 `phase2` 并停止 optimized shadow。Agent 单日失败时只发布国内监管骨干，连续两日失败会在激活后 30 天内自动恢复 `legacy`。稳定 30 天后自动创建站点适配器清理 Issue。

## 运行自检与诊断审批

- 自检入口：`python -m app.self_check`
- 每次输出：`health_report.json`、`health_report.md`；非健康时额外输出 `repair_request.json`
- 等级：`healthy < warning < error < critical`，所有非健康状态都会创建或更新 GitHub 健康 Issue
- 来源异常按 `check_id + source_id + reason_code` 生成稳定事件；跨日复用原 Issue，同一 GitHub Run 重跑不重复计数，恢复后自动关闭并标记 `health-recovered`
- GitHub Issue 是异常诊断队列，标签含义为：
  - `proposal-pending`：等待 Codex 诊断
  - `proposal-ready`：用户批准后，Codex 已将方案回写 Issue 并准备执行
  - `no-fix-required`：人工确认无需代码修改时可用于归档
  - `health-superseded`：旧事件已被新的来源级跟踪方式替代
- Codex 11:00 自动诊断提示词：`.github/codex/robtaxi-health-diagnosis.md`
- 用户批准后的执行规则：`.github/codex/robtaxi-health-approval.md`
- Scheduled Task 对 GitHub 完全只读；同一 `request_id` 的多个来源 Issue 作为一个批次诊断，完整 proposal 仅保存在 Codex Scheduled 收件箱；`proposal_id` 对同一 request 固定为 `rp_<request_id>_v1`
- 只有明确批准 `ready_for_approval` 的 `proposal_id` 后，Codex 才使用本地 GitHub CLI 回写方案、校验版本并创建 `codex/health-<proposal_id>` 分支和 PR
- 不需要 `OPENAI_API_KEY`；Scheduled Task 使用现有 Codex 套餐额度，额度不足时 Issue 会继续保留在队列

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
- `active_profile`
- `scope_drop_count`
- `late_arrival_kept_count`
- `quality_metrics`（滚动 7/30 天主题、一手来源、地区、发现源依赖、单源集中度和静默失效）
- `summary_structured_count`
- `summary_structured_valid_count`
- `summary_structured_invalid_count`
- `summary_retry_count`
- `impact_target_distribution`
- `search_result_raw_count`
- `search_result_fetch_success_count`
- `search_result_fetch_fail_count`
- `search_result_verified_count`
- `search_result_unverified_drop_count`
- `agent_import_status`
- `agent_imported_count`
- `domestic_agent_notice`
- `quality_metrics.*.agent_verified_evidence_share`
- `quality_metrics.*.agent_strong_evidence_share`

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
