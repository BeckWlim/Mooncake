# Mooncake 近期开发关注点分析（2026年6月–8月）

> 统计范围：`2026-06-01` 至 `2026-08-04`，基于 git log 分析生成。

## 总体数据

| 指标 | 数值 |
|------|------|
| 近两个月 commit 总数 | **425** |
| 变更文件数 | 2,164 |
| 新增代码行 | 126,590 |
| 删除代码行 | 27,715 |
| 活跃贡献者 | 15+ |

## 模块热度排名

| 排名 | 模块 | 近两月 commit | 近一月 commit | 代码变更 |
|------|------|------------|------------|----------|
| 🥇 | **Store** | 97 | 40 | +44k/-12k |
| 🥈 | **TE (TransferEngine)** | 50 | 16 | +40k/-4.6k |
| 🥉 | **TENT** | 40 | 29 | 含在 TE 内 |
| 4 | **CI/Build** | 41 | 24 | +2.6k/-1.4k |
| 5 | **Doc** | 40 | 12 | +8k/-2.3k |
| 6 | **wheel** | 9 | 8 | +12k/-0.9k |
| 7 | **Bugfix** | 15 | 7 | — |
| 8 | **PG** | 8 | 2 | +2.9k/-2.3k |
| 9 | **EP** | 6 | 2 | +9.1k/-1.3k |

## 整体趋势

```text
Store    ######################  持续高热度，架构深化
TENT     #################       近期加速，可观测性为重点
Wheel    ###########             结构化数据传输 API 快速迭代
CI/Build #########               稳定投入，发布自动化
Docs     ########                文档体系重构完成
EP       ####                    功能集中建设
PG       ##                      通信与成员解耦推进中
```

**总体判断**：项目处于 0.3.x 版本快速迭代期，核心精力集中在 **Store 多租户与持久化**以及**传输层可观测性**两大方向，Python SDK 的结构化数据 API 也在快速成熟。

---

# Store 存储扩展

## 整体数据

| 指标 | 数值 |
|------|------|
| 近三个月 commits | **~60+** |
| 源码文件数（.h/.cpp） | **344** |
| 最热文件 | `master_service.cpp`（+7773 行，49 commits） |

## 在存储扩展方面的现状

Store 已构建**从内存到 SSD 到云存储的完整分层体系**，并围绕**多租户隔离**和**故障恢复**做了大量加固。整体处于从单机 KV 缓存向**多租户分布式分层存储平台**演进的阶段。

### 一、分层存储架构（SSD 冷热分层）

这是存储扩展的**核心主线**，已形成 L1（内存）→ L2（本地 SSD）→ S3（云存储）三级体系。

```text
L1 memory (hot)
    ^ promotion-on-hit / background retry
    |
L2 local SSD (warm)
    | eviction / offload
    v
S3 or object storage (cold)
```

**已落地能力：**

| 能力 | 状态 |
|------|------|
| SSD 离线存储基础框架 | ✅ 已成熟 |
| BatchEvict 逐出策略（可配置） | ✅ |
| SSD 空闲率优先分配策略 | ✅ |
| 本地优先分配策略 | ✅ |
| L2→L1 热度晋升（V1.1） | ✅ |
| BatchOffload >4GiB 修复 | ✅ |
| SSD 容量快照持久化 | ✅ |
| offload 队列限流可配 | ✅ |

### 二、多租户隔离体系

近期投入**第二大方向**，从单租户 KV 缓存升级为严格多租户配额管理平台。

| 能力 | 状态 |
|------|------|
| 规范 TenantId | ✅ |
| 严格多租户配额准入 | ✅ |
| etcd 配额连接器 | ✅ |
| 配额表从 MasterService 抽离 | ✅ |
| Host 段配额 pinning | ✅ |
| 客户端可配 tenant_id | ✅ |
| TenantId 零拷贝热路径优化 | ✅ |

**核心文件：**
- `mooncake-store/include/tenant_quota.h` — `TenantQuotaSnapshot`、`TenantQuotaUsage`
- `mooncake-store/include/tenant_quota_sharded.h` — 分片配额实现
- `mooncake-store/include/tenant_quota_policy_store.h` — 配额策略存储
- `mooncake-store/include/tenant_id.h` — 规范租户 ID 类型

### 三、快照与故障恢复

Master 快照/持久化/恢复经历**5 步大规模重构**：

| 步骤 | 内容 |
|------|------|
| Step 1 | 抽取 MasterSnapshotManager |
| Step 2 | 抽取 snapshot orchestration |
| Step 3 | 抽取 master snapshot codec |
| Step 4 | 抽取 snapshot restore path 为分层架构 |
| Step 5 | 清理并文档化 snapshot 重构 |

**亮点：**
- `OffsetAllocator` 支持可配置持久化，重启后可恢复
- Snapshot 编解码器从 MasterService 解耦
- `make-before-break` 重挂载策略

**核心文件：**
- `master_snapshot_manager.h` / `.cpp`
- `master_snapshot_repository.h` / `.cpp`
- `offset_allocator.h` / `.cpp`

### 四、S3 / 云存储集成

处于**早期但可用**阶段：S3 客户端配置环境变量化、ListObjects 分页修复、快照对象存储。

### 五、存储扩展能力矩阵

```text
Tenant quota        [########--] 80%
SSD tiering         [########--] 80%
Snapshot/recovery   [#######---] 70%
S3 integration      [#####-----] 50%
Data structures     [######----] 60%
RPC/metadata        [#######---] 70%
```

---

# TENT 传输引擎

## 架构

TENT（**T**ransport **E**ngine **N**ext-Gen **T**ransport）路径：`mooncake-transfer-engine/tent/`。提供统一 C API 和 C++ 内部实现，支持多种底层传输方式（RDMA、NVLink、MNNVL、SHM、GDS、TCP、io_uring、AscendDirect、SunriseLink、TPU），围绕 QoS 调度、可观测性和传输策略选择构建完整运行时层。

```
tent/
├── include/tent/
│   ├── common/          # 类型定义、配置、并发原语
│   ├── runtime/         # 准入队列、QoS契约、传输选择、段管理
│   ├── transport/       # 各传输实现 (rdma/shm/nvlink/mnnvl/gds/...)
│   ├── metrics/         # Prometheus 指标暴露
│   ├── platform/        # 多平台 (cuda/rocm/ascend/tpu/sunrise/cpu)
│   ├── metastore/       # etcd/http/redis
│   └── rpc/             # RPC 通信
├── src/                 # 对应实现
├── plugins/             # GPU 插件 (cuda/rocm)
└── tests/               # 单元测试
```

## 开发方向（按热度排序）

### 1. QoS 调度与准入控制（12 commits，最热）

核心主线：RFC #2519 分步交付计划。

| 步骤 | 内容 |
|------|------|
| Step 1 | SelectionPolicy: per-policy SL/TC/qp_pool schema |
| Step 2 | Opt-in EDF (earliest-deadline-first) dispatch |
| Step 3 | Deadline-infeasible drop + degradation hook |
| — | Per-entry priority promotion |
| — | IntentType enum → Transfer Intent API |
| — | Bind transport policies to intent type |
| — | Live RDMA bandwidth → admission degradation policy |
| — | Deadline proximity promotion for dispatch |
| — | QoS contract schema resolver |
| — | Receiver-credit ledger model |
| — | QoS metrics baseline → tebench |

**重点文件：**
- `tent/include/tent/runtime/admission_queue.h` — `QueueLimits`（deadline_aware、mlu_local_threshold、promotion_slack_ns）
- `tent/src/runtime/admission_queue.cpp` — 准入队列实现（+506/-19）
- `tent/include/tent/runtime/qos_contract.h` — `QosPolicyFields`、`QosRequestContext`、`EffectiveQosPolicy`
- `tent/tests/admission_queue_test.cpp` — 准入队列测试（+933 行）

### 2. 可观测性 / Metrics（5 commits）

- Transport 标签化指标（按传输类型拆分）
- 暴露 RDMA NIC 负载统计
- 清理死 metrics 配置 flag，`validateConfig` 接入 `initialize()`

**重点文件：**
- `tent/include/tent/metrics/tent_metrics.h` — 使用 ylt/metric + coro_http Prometheus 暴露
- `tent/src/metrics/tent_metrics.cpp` — +295/-194

### 3. RDMA / NIC 网络层（6 commits）

- RDMA NIC allow/deny list（`MC_FILTER_NIC` / `MC_FILTER_NIC_EXCLUDE`）
- Per-pool QP 分配 + per-pool SL/TC
- RailMonitor 跨 NUMA rail 映射优化
- SHM relocation mapping 线程间复用
- Deadline-aware NIC 带宽仲裁
- Best-effort RDMA task cancellation

**重点文件：**
- `tent/src/transport/rdma/workers.cpp` — +234/-43
- `tent/src/transport/rdma/rdma_transport.cpp`
- `tent/include/tent/transport/rdma/rail_monitor.h`

### 4. 传输选择与策略（6 commits）

- 统一 `transportTypeName` → `types.h` 单一事实来源
- 传输策略绑定到 intent 类型

**重点文件：**
- `tent/src/runtime/transport_selector.cpp` — +198/-104
- `tent/include/tent/runtime/transport_selector.h`

### 5. Bugfix（3 commits）

- 修复 TPU 大数据传输静默数据损坏
- 修复 GDS `cuFileBatchIOGetStatus` 语义不匹配
- SIGTERM/SIGINT 优雅关闭

## 重点文件热力图

```
tent/src/runtime/transfer_engine_impl.cpp    +989/-259  (17 commits)
tent/tests/admission_queue_test.cpp          +933/-11    (7 commits)
tent/src/runtime/admission_queue.cpp         +506/-19    (7 commits)
tent/src/metrics/tent_metrics.cpp            +295/-194   (7 commits)
tent/src/transport/rdma/workers.cpp          +234/-43    (5 commits)
tent/src/runtime/transport_selector.cpp      +198/-104   (5 commits)
tent/include/tent/runtime/admission_queue.h  +199/-4     (7 commits)
```

## 方向总结

TENT 处于 **QoS 体系快速构建期**，从"能传"向"传得好、可观测、有 SLA 保障"演进。核心是 RFC #2519 的 deadline-aware 调度 + receiver-credit 账本模型，辅以 per-transport metrics 和 RDMA 带宽仲裁。
