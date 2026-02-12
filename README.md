# Robtaxi 每日新闻摘要（RSS + Search API，HTML 版）

该项目会在**北京时间每天 09:00**自动更新本地 HTML 页面，分成两个板块：
- 【国内 Robtaxi 最新动态】
- 【国外 Robtaxi 最新动态】

## 架构特点
- 优先使用直接原文 RSS（不走 Google 跳转）。
- 支持 `search_api` 补充召回（默认预留 SerpAPI 配置）。
- 单源失败不阻塞整体生成。
- 页面底部显示“今日有效源数量”和“抓取失败源列表”。

## 关键文件
- 源池配置：`/Users/jianjie/Documents/New project/sources.yaml`
- 主脚本：`/Users/jianjie/Documents/New project/scripts/robtaxi_digest.py`
- 配置校验：`/Users/jianjie/Documents/New project/scripts/validate_config.py`
- 源健康检查：`/Users/jianjie/Documents/New project/scripts/test_sources_health.sh`

## 1. 安装定时任务

```bash
cd "/Users/jianjie/Documents/New project"
./scripts/install_launchd.sh
```

安装后会创建：
- 实际输出页面：`~/.robtaxi-digest/robtaxi_digest_latest.html`
- 项目快捷入口：`/Users/jianjie/Documents/New project/robtaxi_digest_latest.html`

## 2. 配置检查

```bash
./scripts/validate_config.py ./sources.yaml
```

## 3. 源健康检查

```bash
./scripts/test_sources_health.sh
```

健康报告输出：
- `id`
- `status`
- `fetched`（抓取条数）
- `kept`（过滤后保留条数）
- `error`

并保存到：`/Users/jianjie/Documents/New project/.state/source_health_latest.tsv`

## 4.（可选）启用 Search API

如需启用 `search_api` 源，请配置 API Key（以 SerpAPI 为例）：

```bash
export SERPAPI_API_KEY="你的_key"
```

未配置 Key 时：
- RSS 源照常工作；
- `search_api` 源会自动跳过（查询次数与命中条数为 0）。

## 5. 立即手动生成一次

```bash
cd "/Users/jianjie/.robtaxi-digest"
./scripts/robtaxi_digest.py --sources ./sources.yaml --output ./robtaxi_digest_latest.html
```

## 6. 查看运行状态

```bash
launchctl list | grep robtaxi
```

日志：
- `~/.robtaxi-digest/logs/launchd.out.log`
- `~/.robtaxi-digest/logs/launchd.err.log`
- `~/.robtaxi-digest/logs/robtaxi_digest.log`

## 7. 云端最小化部署（电脑关机也自动更新）

如果你希望在电脑不打开时也能每天 09:00（北京时间）自动更新，推荐使用 GitHub Actions + GitHub Pages。

### 已提供文件
- 工作流：`/Users/jianjie/Documents/New project/.github/workflows/robtaxi-digest-pages.yml`

### 一次性配置步骤
1. 把当前项目推送到 GitHub 仓库（默认分支如 `main`）。
2. 在仓库里配置 Secret：
   - `Settings -> Secrets and variables -> Actions -> New repository secret`
   - 名称：`SERPAPI_API_KEY`
   - 值：你的 SerpAPI key
3. 开启 GitHub Pages：
   - `Settings -> Pages`
   - `Source` 选择 `GitHub Actions`
4. 到 `Actions` 页面手动运行一次 `Robtaxi Digest Pages`（`workflow_dispatch`）。

### 运行方式
- 定时触发：每天 UTC 01:00（即北京时间 09:00）。
- 输出文件：`site/index.html`
- 访问地址：`https://<你的GitHub用户名>.github.io/<仓库名>/`
