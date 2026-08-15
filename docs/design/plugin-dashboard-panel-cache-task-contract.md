# 插件 Dashboard 面板派生缓存任务合同

## 1. 目标

Dashboard 可以从 builtin 或 installed generation 的 `dashboard_panel*.ts/tsx` 构建浏览器
模块，但编译结果只能写入当前 workspace 的 Core-owned runtime cache，不能改写插件源码或
不可变 artifact，也不能因此触发 PluginWatcher 自重载。

## 2. Owner 与状态边界

- 插件 generation 拥有 TypeScript/CSS 源码和已随 artifact 发布的 JavaScript；
- Dashboard host 拥有当前进程需要的派生 JavaScript cache；
- PluginManager/Watcher 继续把插件 artifact 当作 immutable source identity；
- cache 位于 `<workspace>/runtime/dashboard-panels/`，不是 plugin-data、用户素材或外部
  canonical source。

```text
plugin artifact (read-only)
  ├─ dashboard_panel.tsx ── esbuild ─┐
  ├─ dashboard_panel.css             │
  └─ dashboard_panel.js (optional)   │
                                     ▼
                    workspace/runtime/dashboard-panels/<source-key>/
                                     │
                                     ▼
                           /plugins/<id>/<panel>.js
```

## 3. 行为合同

1. 已存在且不早于 TS/TSX 的 artifact JavaScript 继续直接读取，不复制或重编译；
2. 缺失或过期 JavaScript 编译到 source-key 隔离的 runtime cache；esbuild 输入仍是 exact
   plugin source，source-key 覆盖 esbuild 可消费的本地前端输入并排除 `.venv` 等安装期
   runtime tree，输出路径不得位于 plugin root；
3. plugin list 和 JavaScript 下载路由使用同一份 resolved panel mapping；CSS 仍只读原
   artifact；
4. app 建立时清理本 workspace 的上次 crash cache，正常 shutdown 在 compile task 结束后
   删除本轮 cache；每个 app 独占自己的 deferred-build queue，不能替其他 app 编译；
   `runtime` 或 cache root 任一级符号链接必须 fail-loud；shutdown 取消 npx probe 时必须先
   terminate/kill 并 drain 子进程；
5. 编译失败保持现有“该 panel 不可用并记录 warning”的边界，不伪造 JavaScript；
6. `dashboard.py` 及其相对 Python 依赖以 source-only loader 执行，不在 artifact 下生成
   `__pycache__`；
7. builtin/installed manifest、Dashboard 动态 route、SessionDB 和正式 plugin-data 语义不变。

## 4. 验收

- 单测证明 read-only installed plugin 的 TSX 产物位于 runtime cache、可通过公开 route 读取、
  相对 Python import 正常、包含 bytecode 在内的 source tree 摘要不变且 lifespan 后 cache 消失；
- 单测证明已有 fresh JavaScript 仍直接使用 artifact；
- 单测证明 transitive TypeScript 输入变化会换 key 并重编译，`.venv` runtime metadata
  变化不会触发无关重编译，stale JavaScript 在编译失败时不可用；
- 单测证明中间层符号链接 fail-loud，两个 app 的 deferred build 与 shutdown 不会串扰；
- Citation/Meme WebUI E2E 证明真实 Docker runtime 启动后 Meme artifact 没有写入且不会产生
  watcher reload；
- Pyright、相关 Dashboard tests、公开 change-impact Gate 与 `git diff --check` 通过。

恢复点：`backup/plugin-dashboard-panel-cache-before-20260816`。
