# Robotaxi 与 L3/L4 乘用车产业简报

## 项目目标与验收标准

**每天稳定交付一份覆盖充分、事实可靠、去重清晰，并带简短影响解读的产业新闻日报。Agent 和 legacy 都服务于这个目标。** 本节是当前产品目标的唯一入口，研发指令和历史方案均引用本节；实现与目标的差距应如实记录，不能用当前实现反向缩减目标。

范围：Robotaxi、L3/L4 乘用车，以及直接相关的核心供应链、监管和安全事件。排除 Robotruck、Robovan、矿区/港口无人车和普通 L2/L2+ 营销内容。交付到日报网页、飞书和企业微信，不新增周报。

| 目标 | 必需交付与验收方法 |
|---|---|
| 覆盖充分 | 有明确的关键词、企业和信源覆盖清单，重要新闻尽量不漏。用人工核对的当日重要新闻清单检查漏报，定位漏在检索、读取还是筛选；不能仅凭执行过搜索认定覆盖完成。 |
| 查证可靠 | 原文可追溯、事实和日期可信；重复报道合并，旧闻重发不能冒充新进展。抽查误收与误删，不能靠过度过滤制造表面准确率。 |
| 工作流稳定 | 每天按计划执行并形成明确结果，局部失败保留已有成果，未完成环节能够定位。检查必要步骤是否完成、是否按时交付，以及失败后能否继续处理已有结果。 |
| 简短解读 | **每条必须保留 What / Why / So what 和影响对象**，简短说明发生什么、为什么、有什么影响；有事实依据，不编造原因，不把影响推断写成既成事实。抽查是否清楚、有用、没有空话。 |

稳定不要求每天必须有新闻，但必须区分：**已完成且无合格新闻、只完成部分检索、运行失败**。零条结果、网页成功部署或任务退出成功，都不能单独证明当天没有相关新闻。

评价方法先统一，数值门槛根据实际样本另行确定；不擅自承诺百分之百召回或固定每日条数。新闻条数、搜索次数、费用和测试通过数只作辅助信息，不能单独证明日报质量。

### 目标工作流程与边界

**维护关键词和信源 → 收集相关链接 → 读取原文 → 筛选、核验和去重 → 生成简短解读 → 发布日报。**

- 关键词和信源是覆盖基础，Agent 可以补充检索，不能完全依赖模型临时决定当天查什么。
- 补证解决具体疑点，应当有边界，不能持续挤占其他领域检索和当天交付。
- 深度专题研究暂不作为每日必需交付；简短影响解读继续是必需交付。
- Agent 接管 legacy、自主搜索次数更多、工作流更复杂，都不是当前成功标准。现阶段继续双路正式供稿，保留真实路线标签、三端展示和每日 5 元 Agent 预算。

以上是后续优化的目标与验收方向，**不表示当前实现已全部达成**。关键词覆盖充分性、证据利用、补证分配及失败恢复仍需逐项审查；本次目标对齐只整理文档，不修改程序、生产配置或研究流程。

### 后续优化的三个问题

每项优化在现有 Issue 的目标、验收或风险说明中回答，不新增审批或 Project 字段：

1. 具体改善哪项目标？
2. 用什么案例或运行结果证明改善？
3. 是否损害其他目标，或增加不必要的步骤？

安全、数据保护、证据、预算与既有研发授权边界继续有效。满足目标时优先选择更简单、成本更低、便于理解的实现。

## 当前生产状态

- `hybrid_domestic` 双路正式供稿：完整 legacy 信源与国内 Agent 已核验事件合并去重；海外由 legacy 提供。
- 三端条目显示“Agent 研究 / Legacy 采集 / 共同发现”；三类数量互斥，原媒体或官方机构另列为来源，统一 AI 摘要不改变发现路线。
- 主工作流北京时间 09:00 开始调度 Agent 和 legacy，完成后合并、部署并通知；09:00 是计划启动时间，不是保证送达时间。独立 Agent 工作流只保留手动入口。
- 当日 Agent 未完成就使用可用 legacy；部分完成只采用已完整核验事件。legacy 失败但 Agent 有合格事件时发布可用内容并提示海外覆盖可能缺失。两路均失败保留上一期并提示更新失败，不冒充当天日报。
- 已正式上线，无需等待影子观察期或旧接管门槛。历史质量问题与未通过的门槛仍如实保留，不因上线或文档更新变成通过。

实施细节、历史发布证据与已知边界见[双路上线说明](docs/domestic-hybrid-production.md)。

## 当前实现规则（信源配置 v3）

- 统计窗口固定为北京时间前一自然日：`[D-1 00:00:00, D 00:00:00)`（左闭右开）。
- `hybrid_domestic`、`agent_domestic` 和 `optimized` profile 允许补录 72 小时内首次发现的重要内容，每日最多 2 条并标记“补录”。
- `published_at` 缺失或不可解析一律淘汰。
- 主简报每天北京时间 `09:00` 启动双路任务；optimized shadow 和自动复盘分别在 `09:30`、`10:30` 运行，它们不构成双路上线等待条件。
- 只覆盖 Robotaxi、L3/L4 乘用车、直接绑定这些项目的核心供应链和监管安全；排除 Robotruck、Robovan、矿区/港口无人车及普通 L2/L2+ 营销新闻。
- P0 为监管、数据集、公告、IR 和企业正式发布；X/公众号只作为 P1 线索发现，不是生产必需依赖。
- 当前 `hybrid_domestic` 保留完整 legacy 信源；只保留 9 个国内监管入口的 `agent_domestic` 是历史单路方案，不是当前生产目标。
- 当前展示配置为每日最多 12 条，每家公司和每个信源最多 2 条；这些是现有选稿限制，不是覆盖充分的验收标准，本次不调整。legacy/optimized 限制搜索与社交直接入选依赖；Agent 事件以最终证据质量而不是发现方式评估。
- 每条摘要强制结构：`What / Why / So what`，并标注“影响对象”。
- 搜索发现链路默认使用“搜索结果页 + 回源验证”，不再依赖 Google News 包装链接解析。

## 当前实现流水线

生产主链：

`Agent 研究 + legacy fetch -> 同次运行工件校验 -> import_events -> parse -> filter_relevance -> enrich -> summarize -> editorial_digest -> render -> deploy -> notify -> finalize_notify -> self_check -> health_issue`

`import_events` 在 `hybrid_domestic` 和兼容的 `agent_domestic` 中导入当日 Agent 事件；`legacy`、`optimized` 不导入。双路生产校验同次运行工件的日期、提交、运行 ID、必需文件与哈希，不用模糊缓存选择当天研究结果。`editorial_digest` 和 `render` 都读取 `brief_items.jsonl`：前者生成通知文案，后者生成 GitHub Pages 页面。

对应模块：
- `app/fetch.py`：抓取调度器，按信源类型分发到 `fetch_rss.py` / `fetch_structured.py` / `fetch_discovery.py`
- `app/parse.py`：标准化与 L1/L2 去重
- `app/filter_relevance.py`：相关性过滤 + 时间窗口硬约束
- `app/enrich.py`：正文补全（短摘要条目拉取全文）
- `app/summarize.py`：摘要与 L3 去重
- `app/editorial_digest.py`：生成飞书/企微可直接阅读的主编摘要
- `app/render.py`：生成 `site/index.html`
- `app/notify_feishu.py`：飞书推送
- `app/notify_wecom.py`：企业微信推送
- `app/finalize_notify.py`：汇总双渠道通知结果
- `app/self_check.py` / `app/health_issue.py`：最终自检、健康事件和恢复管理
- `app/validate_sources.py`：配置校验
- `app/taxonomy.py`：范围硬门槛与结构化分类
- `app/source_health.py`：35 天信源健康历史与静默失效识别
- `app/social_provider.py`：社交补漏统一接口（第一版仅启用无登录态的人工种子 provider）
- `app/industry_agent/`：独立行业研究 Agent、DeepSeek/Web Search Provider、通用网页读取、证据核验、导入、复盘与回滚

## 国内研究 Agent：当前实现与待优化边界

当前 `industry_agent.workflow_version=2`：六领域检索 → 候选及证据池合并 → 回源及事实核验（最多两轮补证）→ 事实增量检查 → 最终评分 → 正式候选池。模型仍参与候选整理；当前运行方式不等于上文目标工作流程已完成优化。

- Provider 保持现有 DeepSeek 模型 `deepseek-v4-flash` 和 `deepseek_web` 搜索；通用读取器支持网页与文本型 PDF。
- 当前每日预算 5 元、最多 48 次实际 Web Search、每次请求预留 4 次搜索溢出余量。资源不足时停止新增请求并保留可用结果；这不代表覆盖已经充分或当天没有相关新闻。
- 企业别名和证券代码从配置读取；35 天事件指纹与 180 天精简事实索引用于去重和增量判断。
- `discovery_routes` 与路线记录表示如何发现；媒体、官方原文和 `evidence_type` 表示出处与证据质量。统一摘要不将 legacy 事件改标为 Agent。
- 当前 Agent 独立检索，不以 legacy 候选作为输入。这是现状描述，不是禁止后续复用关键词、信源或读取能力的产品约束；后续方案按上文目标验收，真实发现记录必须保留。

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
- `active_profile` 当前为 `hybrid_domestic`，`industry_agent.mode=production`；`legacy` 保留为回退入口，`optimized` 继续独立影子运行。
- 核心 CLI 支持 `--profile legacy|optimized|agent_domestic|hybrid_domestic`。
- GitHub Actions 使用仓库变量 `ROBTAXI_ACTIVE_PROFILE`；当前值为 `hybrid_domestic`。旧审批和阶段晋级不能把双路改回 Agent 单路；生产路线变更仍需用户授权。
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

以下是各阶段及本地包装器用法，不会自动启动国内 Agent。默认 profile 已是 `hybrid_domestic`；导入 Agent 需要带有相符日期、提交、运行 ID 和文件哈希的 handoff manifest（参见 `app.industry_agent.handoff`）。缺失时按降级处理，不能据此判断 Agent 无新闻。仅演练 legacy 时显式传入 `--profile legacy`。本地输出与 `.state*` 应与生产隔离。

1. 准备 Python 3.11（仓库通过 `.python-version` 和 `pyproject.toml` 锁定 3.11）

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python --version
```

2. 安装依赖

```bash
pip install -r requirements.txt
```

3. 校验配置

```bash
python -m app.validate_sources ./sources.json
```

4. 分阶段执行

```bash
DATE_BJ="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
python -m app.fetch --date "$DATE_BJ" --sources ./sources.json --out ./artifacts/raw --report ./artifacts/reports
python -m app.industry_agent.import_events --date "$DATE_BJ" --sources ./sources.json --in ./artifacts-agent --raw ./artifacts/raw --report ./artifacts/reports
python -m app.parse --date "$DATE_BJ" --in ./artifacts/raw --out ./artifacts/canonical --report ./artifacts/reports
python -m app.filter_relevance --date "$DATE_BJ" --in ./artifacts/canonical --out ./artifacts/filtered --sources ./sources.json --report ./artifacts/reports
python -m app.enrich --date "$DATE_BJ" --in ./artifacts/filtered --out ./artifacts/enriched --report ./artifacts/reports
python -m app.summarize --date "$DATE_BJ" --in ./artifacts/enriched --out ./artifacts/brief --provider deepseek --report ./artifacts/reports --sources ./sources.json
python -m app.editorial_digest --date "$DATE_BJ" --in ./artifacts/brief --out ./artifacts/digest --provider deepseek --report ./artifacts/reports --sources ./sources.json
python -m app.render --date "$DATE_BJ" --in ./artifacts/brief --out ./site/index.html --report ./artifacts/reports --sources ./sources.json
```

5. 包装器入口

```bash
python3 ./scripts/robtaxi_digest.py --date "$DATE_BJ" --sources ./sources.json --output ./site/index.html

# --dry-run 仍会生成 ./artifacts/digest，仅跳过 HTML 渲染

# optimized 仅用于本地或影子验证
python3 ./scripts/robtaxi_digest.py --profile optimized --date "$DATE_BJ" --sources ./sources.json --dry-run

# 独立运行国内行业 Agent（业务失败也会写出 failed 报告供主流程降级）
python -m app.industry_agent.runner --date "$DATE_BJ" --config ./sources.json --out ./artifacts-agent --state ./.state-agent

# 历史 agent_domestic 兼容导入演练，不代表当前生产路线
python3 ./scripts/robtaxi_digest.py --profile agent_domestic --date "$DATE_BJ" --agent-handoff ./artifacts-agent --dry-run
```

## GitHub Actions（生产）
工作流：`./.github/workflows/robtaxi-digest-pages.yml`

- 定时：`0 1 * * *`（UTC），即北京时间 `09:00`
- 链路：`Agent 研究 + legacy fetch -> 同次运行工件校验 -> import_events -> parse -> filter_relevance -> enrich -> summarize -> editorial_digest -> render -> deploy -> notify -> finalize_notify -> self_check -> health_issue`
- 手动触发默认不推送，`send_notify=true` 才推送
- 同一北京日期按渠道查询保留 35 天的 GitHub artifact 锁；只有渠道明确返回 `sent=true` 才写锁，避免重跑重复推送
- build 失败时仍上传基础 artifact；飞书和企微独立尝试，最终统一聚合通知状态
- `workflow_dispatch.self_check_fixture` 可人工注入 `warning/error/critical` 做验收，生产保持 `none`
- `.state` 通过 Actions cache 跨日持久化，保存 35 天已见内容、摘要缓存、HTTP 条件请求缓存和健康历史。

## optimized 影子运行（独立实验，不是双路上线门槛）

工作流：`./.github/workflows/robtaxi-digest-shadow.yml`

- 每天在生产任务后运行 `optimized` profile。
- 使用独立 `.state-shadow`，不污染生产去重历史。
- 不部署、不通知，只保留 35 天产物供 14 天验收。
- 上线前检查 P0 成功率、正文/日期解析率、黄金集精度与召回、一手来源占比、发现源依赖度及严重健康事件。
- `app.rollout_gate` 每日计算并保存上线门槛；未满 14 天或任一指标未达标时只报告，不自动切换生产。
- 历史产物到位后可运行 `python scripts/replay_production.py --input <历史产物根目录>`，默认要求至少 8 个统计日。

## Agent 工作流与历史方案

- `robtaxi-digest-pages.yml` 统一调度国内 Agent 和完整 legacy，使用本次运行 artifact 交接，正式生成和发布双路日报。
- `robtaxi-industry-agent.yml` 只保留手动研究入口，产物保留 35 天；不重复每日定时研究。
- `robtaxi-agent-review.yml` 保留既有复盘；`hybrid_domestic` 不参与旧接管审批提醒或阶段晋级。历史复盘结果不是完整覆盖的独立真值。
- `robtaxi-agent-approval.yml` 保留历史审批兼容，但禁止覆盖当前 `hybrid_domestic`。

**历史设计，已被双路生产取代：** Agent-first 影子观察后接管、缩减国内固定源、稳定后清理适配器，以及连续两日失败才回滚，均不约束当前双路供稿。旧文档曾写 7 天，兼容复盘配置保留 14 天；两者都不是当前上线等待条件，也不因此修改用户的历史 WIP 或门槛结果。原设计可查阅 [本次目标对齐前的 README](https://github.com/xiexie20211028-bot/robtaxi-digest-new/blob/ba02cee85b4c844574a3532c778e35c098a2b519/README.md)。

当前生产状态见本文上方，2026-10-08 发布记录见[双路上线说明](docs/domestic-hybrid-production.md)。上线后的观察用于发现问题与回退，不作为再次等待条件。

## 运行自检与诊断审批

- 自检入口：`python -m app.self_check`
- 每次输出：`health_report.json`、`health_report.md`；非健康时额外输出 `repair_request.json`
- 等级：`healthy < warning < error < critical`，所有非健康状态都会创建或更新 GitHub 健康 Issue
- 来源异常按 `source_id + reason_code` 生成稳定事件；`check_id` 只保留为证据。跨日复用原 Issue，同一 GitHub Run 重跑不重复计数。
- GitHub Issue 是异常诊断队列，标签含义为：
  - `proposal-pending`：等待 Codex 诊断
  - `proposal-ready`：用户批准后，Codex 已将方案回写 Issue 并准备执行
  - `no-fix-required`：人工确认无需代码修改时可用于归档
  - `health-superseded`：旧事件已被新的来源级跟踪方式替代
- Codex 桌面端每日健康与研发闭环：`.github/codex/robtaxi-health-loop.md`、`.github/codex/robtaxi-codex-execution.md`
- 阶段 A 只读取和归类已有产物；阶段 B 必须经单独批准和白名单启用后，才可处理限定的单信源修复。

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

已废弃空兼容字段：`daily_pool_size`、`baseline_count`、
`baseline_matched_count`、`baseline_unmatched_count`、
`baseline_unmatched_samples`、`recall_at_20`、`recall_guard_alert` 和
`recall_guard_message`。新生成的 `run_report.json` 不再包含这些字段；读取和
更新旧报告时仍保留原有未知字段，便于历史产物继续使用和回退。

生产者/消费者盘点：

| 范围 | 盘点结论 | 处理 |
|---|---|---|
| 代码 | `app/report.py::default_report` 是唯一生产者；其他报告模块只做通用读取或增量更新，没有按字段消费 | 删除新报告的默认生产；旧报告字段原样保留 |
| 测试 | 原有测试没有依赖这些字段 | 增加新报告不生成、旧字段为空、旧字段缺失三类兼容回归 |
| fixture | 原来没有固定旧报告样本 | 增加 `tests/fixtures/run_report_legacy_compat.json` 作为旧输入兼容证据 |
| 工作流 | 简报与复盘工作流只传递 `run_report.json` 文件，不读取这些字段名 | 无需修改工作流 |
| 文档 | 本节曾把这些字段描述为仍保留的兼容字段 | 改为正式废弃说明和回退边界 |

若发现仓库外消费者依赖这些字段，直接 revert 本次变更，恢复默认生成，再单独设计带截止日期的迁移方案。

## 排障

- 查看过滤结果：
  - `artifacts/filtered/<date>/filtered_items.jsonl`
  - `artifacts/filtered/<date>/dropped_items.jsonl`
- 查看失败源摘要：`run_report.json` 里的 `source_stats`
- 若通知失败，优先检查：
  - `FEISHU_WEBHOOK_URL` / `WECOM_WEBHOOK_URL`
  - 对应 step 日志错误码

## 备选方案（未启用）：公众号订阅监测（RedFoxHub）

> **状态：备用方案，未启用。** 项目当前采用 Agent 与完整 legacy 双路正式供稿。本方案作为公众号数据通道的备用路线，启用需人工决策，并同步修订上文"社交补漏说明"中"微信公众号不使用采购 API"的政策。调研与验证完成于 2026-08-19。

### 目标

订阅式监测 17 家国内企业公众号（`sources.json` 已登记公众号名的 15 家 + 如祺出行、曹操出行），每日拉取新发文（标题、发布时间、阅读数、点赞数、原文链接），作为国内信源的结构化补充。

### 已验证结论（2026-08-19 实测）

- 服务：RedFoxHub（redfox.hk），REST API，请求头 `X-API-Key` 认证，按次计费约 ¥0.4/次，17 账号全量监测预估 ~¥1/天。
- **关键端点**：广域库 `POST /story/api/gzh/data/queryWorkList`。注意与优质库 `/story/api/gzhData/queryWorkList` 区分——优质库对企业官方号覆盖差（小马智行停更在 6-26、萝卜快跑完全未收录），广域库覆盖完整。
- 数据新鲜度：T+1（小马智行 8-18 17:04 发布的 Q2 财报文，8-19 即可拉到）。
- 实现要点：
  - 服务端日期过滤参数（`publishTimeStart/End`）不生效，需翻页拉全（每账号最多 5 页 × 20 条）后按统计窗口本地过滤。
  - 返回列表不按日期排序，最新文章可能在任意页。
  - `code 3108` = 频率限制，等 5 秒重试；`code 3203` = 广域库未收录该账号。
  - 文章正文仅摘要级，全文需回源 `mp.weixin.qq.com` 原文链接（可复用现有 enrich 阶段）。
- 参考实现：GitHub `redfox-data/redfox-community` 仓库 `skills/gzh-subscribe`（含订阅管理、拉取、日报生成，915 行 Python）。

### 账号 ID 解析进度（14/17 已完成）

| 公司 | 公众号 | account ID |
|------|--------|-----------|
| 小马智行 | Pony.ai小马智行 | `pony_ai` |
| 文远知行 | 文远知行WeRide | `WeRide_ai` |
| 萝卜快跑 | 萝卜快跑自动驾驶 | `ApolloGO_2020` |
| 滴滴自动驾驶 | 滴滴自动驾驶 | `DiDiAD1908` |
| Momenta | Momenta | `MomentaAI` |
| 轻舟智航 | 轻舟智航QCraft | `QCraftAI` |
| 小鹏汽车 | 小鹏汽车（主号） | `XPENGMOTORS` |
| 蔚来 | 蔚来 | `NIO-Wechat` |
| 理想汽车 | 理想汽车 | `lixiangzhizao` |
| 极氪 | 极氪Zeekr | `gh_ae4a3687be5f` |
| 地平线 | 地平线HorizonRobotics（主号） | `horizonrobotics` |
| 禾赛科技 | 禾赛科技 | `hesaitech_sh` |
| 速腾聚创 | RoboSense | `RoboSenseLiDAR` |
| 如祺出行 | 如祺出行 | `ruqichuxing` |

待人工查 3 家（RedFox 账号搜索返回全错，需微信内查公众号"微信号"）：

| 公司 | 要找的账号 | 搜索误配结果（勿用） |
|------|-----------|---------------------|
| AutoX | AutoX无人驾驶 | DJI 无人驾驶 |
| 黑芝麻智能 | 黑芝麻智能（芯片公司） | 南方黑芝麻（食品） |
| 曹操出行 | 曹操出行（乘客主号） | 曹操出行司机官方 |

踩坑记录：`searchUser` 排序常把"招聘号"排在前面（小鹏、地平线初次误配到招聘号，已修正为主号）；以上 14 个 ID 均已在广域库逐一验证收录（code 2000）。

### 若启用的实施路径（备忘，未执行）

1. 新增 `app/fetch_wechat.py`（新 source_type：`wechat_gzh`）：按账号调广域库端点，翻页拉全 + 本地按统计窗口过滤，输出标准 RawItem 进现有 `parse -> filter_relevance` 流水线（后端零改动）。
2. 正文依赖 enrich 回源微信原文链接补全文。
3. 互动数据（阅读/点赞/评论）可喂 `filter_scoring` 做重要性加权。
4. Key 管理：GitHub Secrets 增加 `REDFOX_API_KEY`；本地开发已存 `~/.robtaxi-digest/env`（chmod 600，仓库外）。
5. 建议先以 shadow 模式运行 3-5 天验证数据质量后再决定是否进入生产 profile。
