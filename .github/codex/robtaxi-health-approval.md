# Robtaxi 修复审批执行规范

当用户在异常对应的 Codex 任务中输入“批准 `<proposal_id>`”后，按以下规则执行：

1. 从最近一次 `Robtaxi 健康 Issue 自动诊断` Scheduled 收件箱结果中读取完整 proposal，确认：
   - `proposal_id` 与用户批准的 ID 完全一致，格式为 `rp_<request_id>_v1`；
   - 结论为 `ready_for_approval`，不是 `no_code_change`；
   - 方案包含 Issue URL、`request_id`、`base_commit_sha`、证据、预计文件、风险、验证命令和回滚方案。
2. 通过 GitHub 重新读取对应健康 Issue，确认：
   - Issue 仍开放且带有 `proposal-pending`；
   - Issue 正文中的 `request_id`、`base_commit_sha` 与 proposal 一致；
   - Issue 未恢复健康，也没有更新为新的 run attempt；
   - 仓库中不存在同一 `proposal_id` 的历史 marker。
3. 获取最新远端状态，要求 `origin/main` 与 `base_commit_sha` 完全一致。若不一致：
   - 不修改代码；
   - 使用本地 GitHub CLI 在 Issue 记录“方案因代码漂移作废”；
   - 保留 `proposal-pending`，等待新的健康 run 创建新 request；
   - 要求用户批准新 request 对应的 proposal。
4. 校验通过后，先用本地已授权 GitHub CLI 将完整 proposal 以新评论回写 Issue，评论包含固定 marker `<!-- robtaxi-repair-proposal:<proposal_id> -->`；再移除 `proposal-pending`、添加 `proposal-ready`。评论和标签写入成功前不得修改代码。
5. 创建独立分支/worktree：`codex/health-<proposal_id>`。
6. 严格按 proposal 修改，先运行定向测试，再运行：

   ```bash
   PYTHONPATH=. python -m pytest -q
   python -m app.validate_sources ./sources.json
   ```

7. 测试全部通过后提交、推送并创建 PR。PR 正文关联健康 Issue，并使用 `Fixes #<issue_number>` 让合并后自动关闭。
8. 不自动合并 `main`。测试失败时保留分支和测试证据，使用本地 GitHub CLI 在 Issue 追加失败说明，不创建声称成功的 PR。

任何缺失、冲突或无法验证的审批条件都按未批准处理。
