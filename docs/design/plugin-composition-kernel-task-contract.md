# 插件组合内核第一阶段任务合同

## Role

- 负责范围：Python 组合内核、Core 候选接入点、行为回执、实验插件和隔离 workspace 验证。
- 当前阶段：complete

## Goal

新增一条可真实运行的新插件路径：插件只实现 `apply(ctx)` 并通过 Service 组合能力，Core 仍以完整 generation snapshot 完成候选验证和晋升；现有插件行为不变。

## Success criteria

- [x] Root Context 能挂载插件、提供 Service、等待必需依赖、响应可选依赖、逆序清理 Effect，并在依赖恢复后只激活最新 epoch。
- [x] 候选回执由 Core 观察生成，至少列出未满足依赖、Fiber 状态、已注册 Effect、写入和外部效果；插件不能自行声称通过。
- [x] 新实验插件在隔离 workspace 完成 candidate → validate → promote，随后撤除 provider 时 consumer 回到 pending，最终全部资源归零。
- [x] 正式 workspace、正式 plugin home、SessionDB、memory、渠道和外部 API 零写入；旧插件测试保持通过。
- [x] 相关验证已运行，未运行项和原因已说明。

## Evidence

- 必须先读取：`docs/INDEX.md`、`docs/WORKFLOW.md`、`docs/projectneed.md` 插件与 workspace 条款、决策 0008/0026/0036、递归自验证设计、Cordis 能力等价设计、持久化状态地图。
- 已核对事实：当前 `PluginManager` 和 `PluginContributions` 枚举所有领域贡献；现有 `PluginScope` 能逆序清理但不拥有响应式 Service 依赖；DeepSeek Harness 的 Cordis 关键语义集中在 Context、Service、Inject、Fiber、Effect 和 Scope。
- 未确认事实：现有真实插件迁移顺序；缺失 legacy phase dependency 的最终产品语义；真实外部写型插件的验证端点。
- 关键假设：第一阶段使用全新、无外部效果的实验插件，不需要改变现有插件 manifest 格式或正式安装链。

## Change intent

```yaml
change_type: refactor
semantic_delta: compatible
capability_owner: mixed
consumer_scope:
  - new-style experimental plugins
  - plugin generation publisher
runtime_patch: required
runtime_patch_reason: "Core 必须为新式插件提供 generation 级 Root Context 和不可伪造的候选回执；只在插件侧实现会复制 publication owner。"
authoritative_state_owner: "Core owns generation publication; each plugin owns its domain data beneath the Core-assigned root."
client_only_alternative: ""
invariants:
  - stable 只接受通过行为验证的完整拓扑
  - 一个请求从 admission 到结束只看同一 snapshot
  - Effect 逆序清理且失败全部保留
  - 缺失必需依赖和冲突在候选发布前 fail-loud
protected_state:
  - existing plugin lifecycle and contribution order
  - formal workspace and plugin-data
  - sessions, memory, channels, external APIs
  - stable/latest parent-Turn promotion semantics
allowed_paths:
  - agent/plugin_composition/**
  - agent/plugins/generation.py
  - agent/plugins/snapshot.py
  - agent/plugins/manager.py
  - tests/**plugin_composition**
  - tests/fixtures/plugins/composition/**
  - examples/plugin_composition/**
  - scripts/plugin_composition_experiment.py
  - tests_scenarios/contracts/impact.toml
  - tests_scenarios/contracts/coverage-baseline.json
  - docs/INDEX.md
  - docs/NOW.md
  - docs/decisions/README.md
  - docs/decisions/0036-plugin-composition-keeps-promotion-owner.md
  - docs/design/cordis-plugin-capability-parity.md
  - docs/design/plugin-composition-kernel-task-contract.md
forbidden_paths:
  - frontend/**
  - migrations/**
  - external plugin canonical sources
allowed_effects:
  - create and remove one run-identified isolated workspace
  - write experiment receipts beneath that isolated workspace
forbidden_effects:
  - modify formal workspace, plugin home, manifest, cache or plugin-data
  - send channel messages or call real external APIs
  - install, promote or unload formal plugins
validation:
  - focused composition unit and lifecycle tests
  - existing plugin snapshot and rollout regression tests
  - isolated workspace experiment with before/after write-set evidence
rollback: "/mnt/data/coding/akasic-agent/.backups/20260814-pre-plugin-composition-kernel-f2e8dd02.bundle"
worktree_writer: "/mnt/data/coding/akasic-agent-worktrees/plugin-composition-kernel"
handoff_head: "f2e8dd023b0cab188726f2bfe51d5190f03c6cce"
external_revisions:
  - "deepseek-harness@47f943859bef60e4160492346772ded9b24f765a"
schema_lineages: []
```

## Autonomy

- 可自主执行：主 Agent 串行修改上述代码和文档、运行无外部效果测试、创建带 run identity 的隔离 workspace；只有边界独立且确实缩短总耗时的只读审查或验证才并行委托。
- 执行前需确认：写正式 workspace/plugin-data、改变现有插件行为、调用真实外部 API、安装或推广正式插件、修改数据库 schema。

## Output

- 交付文件或字段：组合内核、Core 接入点、候选回执、实验插件、实验脚本、回归测试和文档对账。
- 必须附带的证据：base/head、测试命令和结果、实验 workspace identity、写集、Fiber/Effect 终态、正式状态零写入说明。

## Stop rules

- 满足全部成功标准后停止第一阶段。
- 若新入口必须改变旧插件语义、正式持久状态或外部效果才能成立，停止并报告新的审批边界。
- 若 Cordis 关键生命周期语义无法在 Python 中以确定性测试表达，保留 stable 路径并报告阻塞，不用静默降级获得通过。

## Final evidence

- 源码基线：`f2e8dd023b0cab188726f2bfe51d5190f03c6cce`；实施 worktree：`/mnt/data/coding/akasic-agent-worktrees/plugin-composition-kernel`。
- 聚焦组合测试：`32 passed`；旧插件、hot-reload、snapshot、control lineage 与 mobile plugin scheduler 回归：`378 passed`。
- 新增代码、实验和测试的 Basedpyright：`0 errors, 0 warnings`；相关 `compileall` 与 `git diff --check` 通过。
- 隔离运行：`run_id=2a271aeb-48cf-494b-ae6c-2c95415d9b3d`，workspace 为 `/tmp/akashic-plugin-composition-final.SSJoyp/workspace`，晋升 snapshot 为 `f65425e0540e6e0f`；最终 Fiber、Service、Effect 全为空，外部效果为空。
- `strace -f -e trace=%file` 记录 5704 行文件系统调用，没有命中正式 workspace 或 plugin home 的写型 syscall；两棵正式状态树的前后元数据指纹分别保持 `a28ed15820e42579964008811ea3803d78adddd3153157a586a66d8a080cffa9` 与 `75b19c7f6d421fca3e20a732f975e416f545f14924df71d10eeadf03722e02db`。
- 未进入正式 manifest/install 链，未迁移 Citation、Meme、GitHub Watch 或其他现有插件；这些属于下一阶段差分回放试点。
