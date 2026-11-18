# LAW-02 配对回放验收

此目录的输入清单绑定 2026 年 10 月 21–25 日及 11 月 8 日六次正常影子运行。每条记录包含运行、attempt、当时代码、GitHub 归档及规范化候选文件的摘要。复核者可按运行编号下载同名 `robotaxi-l3-l4-shadow-<run>-1`，解压到同一输入目录下分别以运行编号命名的子目录。

在拥有 PR86 及第一父提交对象的完整仓库运行：

```sh
python3.11 scripts/replay_law02_comparison.py --input-root /path/to/archives --out /tmp/law-02-replay.json
```

脚本读取固定旧代码、配置和相同候选，校验两版输出分别完整覆盖且互斥。生成的每批决策摘要覆盖所有候选，送审清单只含至少一版入选的候选身份、输入摘要与去留。原文及盲审材料单独交给评审人，评审前隐藏两版去留。每次回放记录 Python 和依赖版本。

独立评审人根据可追溯原文判定“相关/噪声/未知”。只有标题、搜索摘要、无法取得原文或无法判定的项目保留未知。通过受保护 PR 审核标注文件 `law-02-paired-labels.json`，结构如下。内容摘要取实际阅读的原文快照，须保留快照供复查。

```json
{
  "schema": "law-02-independent-labels-v1",
  "replay_digest": "<回放 JSON 内容摘要>",
  "reviewer": "<独立评审人或可追溯的评审记录>",
  "labels": {
    "<run>:<attempt>:optimized:<candidate-id>": {
      "sample_digest": "<规范化候选摘要>",
      "source_url": "<原文 HTTPS 链接>",
      "source_content_sha256": "<评审所用原文字节摘要>",
      "reason": "<与业务标准对应的判断依据>",
      "verdict": "unknown"
    }
  }
}
```

沿用 #73 / PR86 的历史交付登记并新增修订，保持原范围、检查项和合并版本。生产绑定继续用 `kind=task`、`mode=offline_replay`、`verifier=law_filter_v1`，设置 `verifier_version=2` 与 `shadow_comparison={"mode":"paired_offline","replay_digest":"…","labels_digest":"…"}`。两个摘要都由 `app.development_policy.digest` 对解析后的 JSON 计算。只允许从可信 main、已有有效运行租约按既有登记和验收流程写回。

业务通过要求原有三路线行为成立、至少 20 条负例零误收、并且同一组真实样本上的误收数满足 `100 × 新误收 ≤ 110 × 旧误收`。旧数为零时，增长率显示不适用，零到零通过，零到正数失败。缺任一原文标注或登记绑定时继续观察。旧版验收记录仍按其原绑定读取。
