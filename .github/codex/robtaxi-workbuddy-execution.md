# WorkBuddy 无人值守研发执行手册（v1）

本手册需要 WorkBuddy **显式读取**；不依赖自动识别 Codex Skill。
总盘与正式 Issue 是唯一任务/排期/状态来源，本地 `.workbuddy/` 可丢弃。

## 当前授权与启用

用户已批准 Codex 规划、WorkBuddy 执行的目标架构。机器实际授权只取最新已合并主分支
`.github/robtaxi-autonomy.json`，不能从任务分支读取或自行改开关。当前为单任务 `pilot`：
只允许规划、执行和交付 `pilot_issue` 指定的 Issue #70；其他研发任务继续只读，不允许执行。
受控入口关闭自动记忆、API Key/helper、自动付费 fallback、后台任务与子 Agent，并用系统沙箱
限制写入范围；CLI 自报费用不为 0 时整次执行失败关闭。回退和 active 全队列仍未开放。
旧 `.github/robtaxi-health-autofix.json` 与新模式互斥。

启用依次需要：最小真实交接 → 普通/高影响/恢复三类模拟 → 一个普通试点及真实生产验收
→ active。没有固定三天等待；#55 既有阶段 A 证据继续累计。不要填写虚构的启用凭证。
`activation_evidence` 中每项必须指向实际日志/Issue 评论/Actions，当前空对象表示未验收。

仅套餐认证不等于证明自动充值关闭；必须实际确认两个宿主都无自动额外扣费。
WorkBuddy CLI 已完成真实后代进程与进程组终止探针；提示词里的时间限制仍不算证据，
必须由可信调度器强制执行。不得把唯一低风险 pilot 声称为全无人值守已上线。

## 每日 10:30 唯一入口

复用现有任务 `automation-1786952416344`。调度工作区必须独立、无 WIP，先更新到最新 main；
不在用户原工作区切换、清理、stash 或覆盖。WorkBuddy 本身不手动触发新闻生产。

1. 恢复认证和正式状态，下载上次成功检查以来的正常生产产物（分页查询，不能只取最近一天）。
   若离线多天，按实际运行时间依次处理未看过的证据，不补跑历史研发任务。
2. 按健康闭环手册，完成 `health_loop_sync` 重建 → `health_loop` → `health_loop_sync --mode apply`。
   保留事件去重、已有工程任务关联、正常生产连续两次来源恢复规则。
3. `python3 -m app.development_cycle inspect --out .workbuddy/development/snapshot.json`。
   查询实际依赖/Issue/PR，不依赖总盘视图名称。观察、取消、Epic、明确暂停不执行。
4. 有 batch 时，`python3 -m app.development_cycle plan --codex /Applications/ChatGPT.app/Contents/Resources/codex`。先验证该文件存在、`login status` 为 ChatGPT 套餐认证；不可用时停止并保留到下一天，不回退旧 Homebrew 断链、API Key 或付费入口。
   每日一次最多三项；额度先写 GitHub，失败不返还，不切换付费 API。没有事项则跳过。
5. 仅 pilot/active 且宿主验收通过时：`python3 -m app.development_cycle execute`。
   调用策略中固定 `worker_argv`，通过标准输入传交接包，整个子进程组最多运行 60 分钟。
6. 快照出现 `delivery` 时，仅对该 PR 查询可信检查；全部成功后运行
   `python3 scripts/development_delivery.py --pr N`。pilot 必须再次核对主任务就是 #70。
7. 健康对账成功后用 `python3 -m app.development_cycle heartbeat --health-sync <本次同步回执>`
   写成功心跳；仅打开应用/规划成功不算成功巡检。

同一时间单仓库锁，同一天一个执行窗口、一个常规合并。远端预留记录先于模型和合并，
中断后查 GitHub；不因为本地无缓存而重领、重建 PR、重合并。失败任务无新证据不重复诊断，
不阻挡无依赖的其他任务；严重生产问题写发布冻结，先诊断，冻结时不合并。

## 执行工作区与交付

- 从交接指定最新 main 创建/续用 `workbuddy/development-<issue>` 独立工作区。
  开始先查询同 Issue 开放 PR；有一个就续做，有多个则停下对账。
- 可信控制器建立工作区并完成测试及 GitHub 写操作。WorkBuddy 模型只获文件读写能力，不提供终端，
  不能写 Git 元数据、调用 GitHub、提交、上传或创建 PR；模型退出后控制器重新验收再交付。
- 提交 PR 前同步最新 main；即使主分支只发生无关变化，也应把它纳入候选分支历史，避免补丁表现为删除新提交。
- 校验执行说明，检查相关文件是否变动、实际依赖是否已关闭。失效则移回待规划。
- 负责人/必填字段/验收完整后，设“开发中”，运行现有 preflight；不能绕过。
- 仅实现允许范围。每次有实际代码修改的失败测试写 `attempt` 事件：
  `contract_digest`、`head_sha`、`changed=true`、`passed=false`、复现/测试证据。
  两次失败转 Codex 重新规划；单纯网络重试或重复旧日志不算新修复，也不允许无限重试。
- 定向测试、全量 `python3 -m pytest -q`、配置检查、编译、补丁检查都必须运行。
  内容逻辑增加可信固定输入 `scripts/development_replay.py`，不能修改门槛或删样本求通过。
- 提交、上传到同一分支，创建唯一主 PR，标签 `workbuddy-development`，状态“待验证”。
  无生产观察用 `Primary task: Fixes #N`；需生产观察用 `Primary task: Refs #N`，
  不在其他位置写关闭关键词。普通任务通过门禁后交付；High 等 Codex 绑定当前提交复核。
- 从可信调度工作区运行 `python3 scripts/development_delivery.py --pr N`；
  脚本重新检查范围/复核/依赖/CI/最新 head 并预留日配额，禁止 `--admin` 绕过。
- 合并不是生产采用。源修复沿用健康闭环证据；通用任务还须实现专属正常生产验证适配器，
  在适配器未接好前保持观察，不接受模型自填 `acceptance_passed` 作为关闭依据。

## 失败、回退与通知

回退必须先有任务预定义条件的真实证据、已验收版本以及无依赖/冲突证明，再建立 revert PR，
通过同样的可信检查才合并。不得 reset/强推主分支；无法安全回退冻结发布。
首版回退自动化未完成实机验收前不能启用，已发简报也不能靠 revert 撤回。

普通记录留在 GitHub，每周一次汇总；新阻塞、预算耗尽、严重异常即时提醒，无变化静默。
云端现有每日复盘工作流检查 36 小时心跳，去重提醒/恢复，不调用研发模型。

## 迁移检查清单

- 核对唯一 WorkBuddy 10:30 任务并更新提示，保留旧提示作为恢复文件。
- 核对旧 `com.robtaxi.digest` 本机 09:00 服务：bootout 停用，不删除 plist、脚本或历史文件；
  恢复为显式 bootstrap 原 plist。不得打印其 EnvironmentVariables（可能含密钥）。
- 总盘原生规则关闭“设完成即关 Issue”“关联 PR 即开发中”“PR 合并即完成”；
  可保留新任务 Inbox，关闭 Issue→已完成（但先确认是实际验收关闭）。
- 视图只是展示；修正“未阻塞”错误名字/过滤，脚本始终直接查询 `blockedBy`。
- 先合并本工程并核验可信门禁生效，再记录证据修改运行模式。不得在迁移分支直接开启新授权。

## 已知安全边界

Issue 评论用受信任账户过滤，不是签名系统。当前 Codex/WorkBuddy 共用同一用户凭据时，
不能防御恶意执行者冒充复核者；代码路径门禁防止普通任务改规则，但不是账户权限隔离。
正式 active 前必须取得真实生产采用凭证；Issue #70 只验证低风险文档交付链路，不能充当
`pilot_production`。当前 Codex/WorkBuddy 共用同一用户凭据，仍不能宣称独立账户级授权。

English summary: Codex plans/reviews; WorkBuddy executes in an isolated supervised workspace.
GitHub is the durable source of truth. Shadow is the default until real handoff, timeout,
billing, trusted-gate and pilot-production evidence is verified. No automatic paid fallback.
