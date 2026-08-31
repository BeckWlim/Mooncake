# Mooncake Notes 审计索引

> 审计基线：Git HEAD `b97f13cd` 及当前工作区，2026-08-22。
> 本目录是本地阅读笔记，不替代仓库中的 `README.md`、`CONTRIBUTING.md`
> 与 `docs/`。

## 审计结论

| 文件 | 定位 | 本次处理 | 使用建议 |
|------|------|----------|----------|
| `guide/source-reading-guide.md` | 当前源码导航 | 新增 | 新一轮阅读从这里开始 |
| `guide/project-overview.local.md` | 仓库概览 | 修正 EP/PG 入口与不存在的测试脚本 | 用于建立目录级认识 |
| `guide/study-notes.md` | Store、HiCache、TENT 深入笔记 | 修正入口文件和顶层架构图 | 按主题查阅，不建议从头顺读 |
| `guide/architecture.md` | 推理与缓存概念背景 | 统一主要 ASCII 图，移除图内 Markdown 链接 | 用于理解概念，不作为 API 事实来源 |
| `mooncake-store-report.md` | Store 专题快照 | 修正构建开关、目录描述和架构图 | 结合当前源码阅读，避免依赖行数统计 |
| `tent-transfer-engine-report.md` | TENT 专题快照 | 修正目录拼写和架构图 | 以 `tent/include` 的公开类型为准 |
| `mooncake-l4-gds-code-reading.md` | Store L4 与 TENT GDS 代码阅读笔记 | 新增 | 沿 offload/Get/promotion 与 FileSegment/cuFile 两条真实调用链阅读 |
| `mooncake-local-disk-l4-load-path.md` | `LOCAL_DISK` L4 装载路径专题 | 新增 | 聚焦 holder RPC、Host staging、TE 回传与 buffer 生命周期 |
| `mooncake-replica-placement-nof-l4.md` | Replica 放置、写模式与 NoF L4 专题 | 新增 | 核对 quota、Put 提交语义、NoF 独立存活及其生产定位 |
| `tent-descriptor-dfs-gds-adaptation-report.md` | TENT descriptor-based DFS/GDS 架构与适配方案 | 新增 | 从源码能力边界出发，比较架构选项并规划 typed FileSegment、resolver 与 GDS 路线 |
| `dev-focus-2026-q3.md` | 指定时间窗的 Git 热度分析 | 保留原统计，整理图形表达 | 仅作历史趋势参考，不代表当前能力清单 |

本次把文档分成三类事实：

1. **当前源码事实**：类名、文件路径、构建开关和调用关系，已经与工作区核对。
2. **概念模型**：L1～L4、Prefill/Decode、控制面/数据面，用于解释而非对应单一类型。
3. **历史或外部事实**：提交热度、论文指标、SGLang/vLLM 行号，保留来源语境，不能用来替代当前代码。

## 阅读入口

- 想快速理解整个仓库：先读 [源码阅读指引](guide/source-reading-guide.md)。
- 想跟一遍 Store Put/Get：读 `guide/study-notes.md` 的第 2～4、7 节。
- 想研究 TENT FileSegment/GDS：读 `guide/study-notes.md` 第 8 节，再回到 TENT 源码。
- 想核对 Store SSD L4 与 TENT GDS 为什么尚未端到端接通：读
  [L4 与 GDS 代码阅读笔记](mooncake-l4-gds-code-reading.md)。
- 想逐步跟踪 `LOCAL_DISK` 从 Master 命中到 holder 读取、TE 回传和临时 buffer
  回收：读 [LOCAL_DISK L4 装载路径](mooncake-local-disk-l4-load-path.md)。
- 想确认 `ReplicateConfig` 如何决定 MEMORY/NoF 布局、三种 write mode 如何提交，
  以及 NoF 是否只是“影子副本”：读
  [Replica 放置与 NoF L4](mooncake-replica-placement-nof-l4.md)。
- 想评估 TENT 当前文件传输能力边界，比较 path scheme、共享 resolver、provider
  transport 与 Store 旁路方案，并规划 descriptor-based DFS/GDS 接入顺序：读
  [TENT descriptor-based DFS 与 GDS 适配报告](tent-descriptor-dfs-gds-adaptation-report.md)。
- 想了解近期演进背景：最后读 `dev-focus-2026-q3.md`，不要反过来用热度报告推断调用链。

## ASCII 图约定

- 方框使用等宽字符和英文标签，中文解释放到图外，避免终端对 CJK 宽度处理不同造成错位。
- 树形目录使用 `├──` / `└──`；调用链使用 `->`；进程或职责边界才使用完整方框。
- 图内不放 Markdown 链接。链接或源码路径紧跟在图后，以免渲染后的字符数破坏边界。
