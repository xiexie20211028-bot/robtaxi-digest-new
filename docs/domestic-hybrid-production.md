# 国内研究 Agent 与 legacy 双路正式发布

主任务：#120；流程修复：#119；事实增量：#62 的本次边界；artifact 交接：#59 的本次边界。

2026-10-08 用户明确授权实施、合并、部署，直接正式双路上线，不等待影子观察或 14 天门槛。历史质量门槛继续保留其真实结果，未通过不等于通过。

## 最终行为

- 有效 profile 为 `hybrid_domestic`，完整 legacy 信源集合保留；国内 Agent 已核验事件一起进入正式候选池，海外由 legacy 提供。
- 原始、标准化、聚类、摘要及编辑结果保留 `discovery_routes` 和路线证据。URL、标题、聚类及页面选稿去重合并路线；统一摘要模型不能修改路线。
- 网页使用文字颜色标签；飞书、企微每条标题前缀及其他入选条目显示“Agent 研究 / Legacy 采集 / 共同发现”。顶部数量三类互斥。原媒体仍是来源。
- 编辑 JSON 升级 v2，按可信条目 ID 回填标题、链接、路线和来源；未知 ID 回退本地摘要；读取 v1 时不猜测归属。
- 主工作流先独立运行 Agent 与完整 legacy，再以同次运行 artifact 合并。日期、提交、运行 ID、必需文件及哈希均校验；独立 Agent 只保留手动入口。
- Agent 缺失或整次失败当日立即使用 legacy；部分完成仅采用完整核验事件。legacy 全失败则用 Agent 并提示海外可能缺失。两路均失败取回上一期页面加更新失败提示，不能取回时阻止覆盖旧部署。
- 同日重跑保留当天入选内容；已完成 Agent 研究复用，失败重试保留当日累计费用。每日发送锁继续阻止重复外部日报通知。

## 研究闭环

六领域逐个检索，零有效候选换角度；真实查询与链接关联保留。稳定候选 ID 合并同文章标题改写，已有相关搜索结果优先进入原文核验，最多两轮按缺口补证。供应商搜索实际计数，48 次上限、每请求 4 次溢出预留、5 元预算和运行时限阻止新增请求。

评分在回源、事实检查和增量判断后执行，评分模型不能改写已核验摘要。普通 65 分、迟到 80 分与原有 72 小时窗口保留。转载来源及相同正文不能充当独立证据；官方原文可满足一手要求。

正文读取包含财务表格和通用文本 PDF。网页日期、文件日期、事实披露日期分开记录。财报回查同报告期官方公告，同期同指标同数值由程序拦截；无法确认增量进入待复核。事实索引 180 天，诊断、评分、片段、正文哈希、补证、待复核及逐领域状态保留 35 天。

## 小马样本边界

2026-09-25 的真实失败：同一 Stockstar URL 被拆为两个候选，已经返回的 Autohome 第二链接未进入核验。本次验证稳定候选与已返回第二来源进入证据池的机制，拒绝原因可以逐 URL 定位。

[8 月 18 日公司官方公告](https://ir.pony.ai/news-releases/news-release-details/pony-ai-inc-reports-second-quarter-2026-financial-results-total)已经披露 1,975 辆车队，半年 Robotaxi 收入表为 20,643 / 3,256 千美元，增长约 534%。9 月新文件或媒体日期不能让这些事实变成首次披露。小马中报最终能否收录，仍取决于原文是否具有重要新增事实，不预设必须收录。

证券代码来自公司 IR：[小马](https://ir.pony.ai/)、[文远](https://ir.weride.ai/ir-resources/investor-faqs)，由配置提供给研究和主体校验。

## 验证与回退

新增验收覆盖来源三类、全部去重阶段、模型失败本地回退、未知条目 ID、PDF、日期边界、补证上限、供应商溢出、每日预算、同日重跑、两路失败矩阵、错误日期/提交/运行 ID/哈希，以及旧审批不能覆盖 hybrid。

验证命令：

```bash
python -m pytest -q
python -m app.validate_sources sources.json
python -m compileall -q app scripts
actionlint -shellcheck= -pyflakes=
git diff --check
```

生产回退入口：仓库变量 `ROBTAXI_ACTIVE_PROFILE=legacy`。观察用于发现问题和回退，不是上线等待条件。正式日报保持北京时间每日 09:00；GitHub 实际调度可能延迟。

今天已有成功部署和飞书/企微发送，上线核验关闭日报通知，从下一期正式推送生效。工程测试并不代表历史质量门槛达标，线上首期结果单独记录到 Issue #120。

English: Launch Agent and complete legacy as production discovery routes, preserve deterministic provenance, validate same-run handoffs, and fall back immediately to any usable route.
