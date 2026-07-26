# Robtaxi 修复审批执行规范

当用户在异常对应的 Codex 任务中输入“批准 `<proposal_id>`”后，按以下规则执行：

1. 通过 GitHub 重新读取对应健康 Issue，确认：
   - Issue 仍为开放状态且带有 `proposal-ready`；
   - `proposal_id` 在该 Issue 和仓库中唯一；
   - proposal 评论含固定 marker，且 `created_at == updated_at`，没有被编辑；
   - proposal 的 `request_id`、`base_commit_sha` 与 Issue 中的 `repair_request` 一致；
   - proposal 结论为 `ready_for_approval`，不是 `no_code_change`。
2. 获取最新远端状态，要求 `origin/main` 与 `base_commit_sha` 完全一致。若不一致：
   - 不修改代码；
   - 在 Issue 追加“方案因代码漂移作废”的评论；
   - 移除 `proposal-ready`，重新添加 `proposal-pending`；
   - 等待自动诊断生成新版本，并要求用户重新批准。
3. 校验通过后，创建独立分支/worktree：`codex/health-<proposal_id>`。
4. 严格按 proposal 修改，先运行定向测试，再运行：

   ```bash
   PYTHONPATH=. python -m pytest -q
   python -m app.validate_sources ./sources.json
   ```

5. 测试全部通过后提交、推送并创建 PR。PR 正文关联健康 Issue，并使用 `Fixes #<issue_number>` 让合并后自动关闭。
6. 不自动合并 `main`。测试失败时保留分支和测试证据，在 Issue 追加失败说明，不创建声称成功的 PR。

任何缺失、冲突或无法验证的审批条件都按未批准处理。
