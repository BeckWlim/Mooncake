# Mooncake Store（分布式 KV 缓存存储）— 总体调研报告

> 调研日期：2026-08-04
> 源码路径：`mooncake-store/`
> 编译开关：`-DWITH_STORE=ON`

---

## 1. 概述

Mooncake Store 是 Mooncake 项目中的**分布式 KV 缓存存储系统**，专门用于 AI 推理场景中的高性能数据存储与检索。它的核心场景是 LLM 推理中的 **KV Cache 分布式共享**——多个推理节点通过 Mooncake Store 共享 KV Cache，避免重复计算，显著提升吞吐量。

Mooncake Store 采用 **Master-Worker 架构**：
- **Master 节点**（`mooncake_master`）：集中式元数据管理，负责对象注册、副本管理、段分配、租约管理、驱逐策略
- **Client 节点**（`RealClient`）：实际持有存储内存的节点，负责数据的物理读写、传输、本地缓存管理

架构关键特征：
- **分布式内存池** — 多个节点贡献本地内存段（segment）组成全局内存池
- **多副本机制** — 支持可配置副本数（replicate_config），实现数据冗余与本地化访问
- **零拷贝传输** — 利用 Transfer Engine 实现 RDMA/TCP 直接内存访问
- **软硬引脚**（Soft/Hard Pin）— 支持对象的租约管理，保护热点数据不被驱逐
- **分层存储** — 支持 MEMORY → LOCAL_DISK → NOF_SSD 的多级存储层次
- **高可用**（HA）— 支持主备切换、OpLog 复制、快照持久化

---

## 2. 核心架构

```text
+----------------------------------------------------------------+
| API: Python / C++ / C / Go / Rust                              |
+----------------------------------------------------------------+
| RealClient                                                     |
| - API adaptation and process-local resources                   |
| - Client + ClientBuffer + local/offload services               |
+-------------------------------+--------------------------------+
                                | control-plane RPC
                                v
+----------------------------------------------------------------+
| MasterService                                                  |
| metadata shards | segments | replicas | lease/quota            |
| allocation      | eviction | tasks    | snapshot/HA            |
+-------------------------------+--------------------------------+
                                | descriptors
                                v
+----------------------------------------------------------------+
| Data plane                                                     |
| Transfer Engine: memory/RDMA/TCP | Store backends: SSD/NoF/DFS |
+----------------------------------------------------------------+
```

---

## 3. 核心组件详解

### 3.1 RealClient — 存储客户端

**文件**: `include/real_client.h`, `src/real_client.cpp`

`RealClient` 是 Mooncake Store 的核心客户端实现，继承自 `PyClient`。每个使用 Store 的进程都持有一个 `RealClient` 实例。

**主要职责**：

| 职责 | 说明 |
|------|------|
| **内存段管理** | Mount/Unmount 本地内存段到 Master，贡献内存到全局池 |
| **KV 操作** | put/get/upsert/remove/isExist/getSize 等基本操作 |
| **零拷贝操作** | put_from/get_into/put_parts — 直接在注册内存上操作 |
| **批量操作** | batch_put/batch_get/batch_remove 等批量 API |
| **Buffer 管理** | register_buffer/unregister_buffer 注册/注销与 TE 共享的内存 |
| **Segment 生命周期** | mountSegment/allocateAndMountSegment/unmountSegment |
| **热缓存** | acquire_hot_cache/release_hot_cache 本地热点缓存引用计数管理 |
| **IPC Server** | UDS (Unix Domain Socket) 服务器，用于进程间共享内存 FD 传递 |
| **Dummy Client 代理** | 支持子进程（如 vLLM worker）通过共享内存访问 Store |

**关键内部数据结构**：
- `registered_buffer_sizes_` — 注册内存的地址→大小映射
- `mounted_segment_records_` — mmap 文件段记录
- `allocated_segment_records_` — 动态分配内存段记录
- `shm_contexts_` — 每个 Dummy Client 的共享内存上下文
- `offload_rpc_server_` — SSD offload 的 RPC 服务端

### 3.2 MasterService — Master 服务端

**文件**: `include/master_service.h`, `src/master_service.cpp`

`MasterService` 是 Mooncake Store 的"大脑"，负责所有元数据的管理和决策。

**核心职责**：

| 职责 | 关键方法 |
|------|----------|
| **段管理** | `MountSegment`、`UnmountSegment`、`ReMountSegment` |
| **对象生命周期** | `PutStart/PutEnd`、`UpsertStart/UpsertEnd`、`Remove`、`BatchRemove` |
| **副本管理** | `GetReplicaList`、`AddReplica`、`CopyStart/MoveStart` |
| **租约管理** | `GrantLease`（在 `ObjectMetadata` 中）、`IsLeaseExpired` |
| **驱逐调度** | `BatchEvict`（LRU 近似驱逐）、`EvictionThreadFunc` |
| **客户端心跳** | `Ping` — 接收心跳，检测死节点，触发 remount |
| **高可用** | 支持 OpLog 复制、快照持久化、主备切换 |
| **多租户** | `TenantQuota` 配额管理、租户隔离 |
| **Task 管理** | Copy/Move/Drain job 的创建、调度、状态追踪 |
| **KvEvent 发布** | 可选的对象变更事件发布（`KvEventPublisher`） |

**内部架构**：

```
MasterService
├── metadata_shards_[1024]     # 分片元数据（1024 个分片）
│   └── TenantState            # 每租户的元数据
│       ├── metadata           # key → ObjectMetadata（副本列表+租约状态）
│       ├── processing_keys    # 正在写入的 key
│       ├── replication_tasks  # 复制任务
│       ├── offloading_tasks   # 卸载任务
│       └── promotion_tasks    # 提升任务
├── SegmentManager             # 内存段管理器
├── NoFSegmentManager          # NVMe-oF SSD 段管理器
├── AllocationStrategy         # 分配策略
├── TaskManager                # Copy/Move/Drain 任务管理
├── ShardedTenantQuotaTable    # 多租户配额表
├── MasterSnapshotManager      # 快照管理器（持久化）
├── KvEventPublisher           # KV 事件发布器
├── DeadlineScheduler          # 延迟调度器（优雅卸载）
└── CountMinSketch             # 频率估计器（Promotion-on-hit）
```

**并发控制**：
- 1024 个分片的 `MetadataShard`，每个有独立的 `SharedMutex`（读写锁）
- 锁顺序严格定义避免死锁：client_mutex_ → tenant_quota_policy_mutex_ → snapshot_mutex_ → metadata_shards_ → segment_mutex_
- 4096 个 `object_operation_locks_` 条带化锁，防止同一 key 的并发写冲突

### 3.3 Segment 抽象

**文件**: `include/segment.h`

**段类型**：
- `Segment` — 内存段（DRAM/VRAM），有协议、基地址、大小、TE endpoint
- `NoFSegment` — NVMe-oF SSD 段
- `LocalDiskSegment` — 本地磁盘段（用于 offload/eviction）

**段生命周期**：
```
Mount → OK → DRAINING → DRAINED → UNMOUNTING → 移除
                    ↓
            GRACEFULLY_UNMOUNTING
```

**段标识**：
- `segment_id` (UUID) — 全局唯一 ID
- `segment_name` — 逻辑名称（如 `{ip}:{port}`），用于优选分配

### 3.4 Replica（副本）

**文件**: `include/replica.h`

每个存储对象可以有多个副本，每个副本有类型和状态：

**副本类型**（`ReplicaType`）：
- `MEMORY` — 内存副本（DRAM/VRAM 中的 KV Cache）
- `DISK` — 磁盘副本（传统磁盘）
- `LOCAL_DISK` — 本地磁盘副本（NVMe SSD 上的持久化存储）
- `NOF_SSD` — NVMe-oF SSD 远程副本

**副本状态**（`ReplicaStatus`）：
```
INITIALIZED → PROCESSING → COMPLETE → REMOVED
                            ↓
                          FAILED
```

**副本描述符**（`Replica::Descriptor`）包含：
- `replica_id` — 全局唯一副本 ID
- `segment_id` / `segment_name` — 所在段
- `buffer_address` / `size` — 在段内的偏移和大小
- `protocol` — 传输协议
- `memory_type` — 内存类型（DRAM/VRAM）
- `handle` — TE 传输句柄（用于 RDMA 直接访问）

### 3.5 BufferAllocator 体系

**文件**: `include/allocator.h`, `include/cachelib_memory_allocator/`

两种内存分配器：

| 分配器类型 | 说明 |
|-----------|------|
| **CachelibBufferAllocator** | 基于 Facebook CacheLib 的 Slab 分配器，支持多种 allocation class |
| **OffsetBufferAllocator** | 基于偏移量（bitmap）的分配器，适合大块连续内存 |

`AllocatedBuffer` 是分配结果的 RAII 封装，自动管理生命周期。

### 3.6 AllocationStrategy（分配策略）

**文件**: `include/allocation_strategy.h`

| 策略 | 说明 |
|------|------|
| `RANDOM` | 纯随机分配 |
| `FREE_RATIO_FIRST` | 优先分配空闲比例最高的段 |
| `CXL` | CXL 共享内存专用策略 |
| `SSD_FREE_RATIO_FIRST` | SSD 空闲比例优先 |
| `LOCAL_FIRST` | 优先本地主机，然后远程有序 fallback |

### 3.7 EvictionStrategy（驱逐策略）

**文件**: `include/eviction_strategy.h`

驱逐机制采用**两阶段 LRU 近似**：
1. **第一阶段**：只驱逐无 soft pin 的对象，目标达到 `eviction_ratio`（默认 5%）
2. **第二阶段**：如果未达下界，允许驱逐 soft pinned 对象（若 `allow_evict_soft_pinned_objects_` 为 true）

驱逐触发条件：
- 内存分配失败（`NO_AVAILABLE_HANDLE`）→ 设置 `need_mem_eviction_` 标志
- 驱逐线程每 10ms 检查一次，发现标志后执行 `BatchEvict`

关键配置：
- `eviction_ratio` — 驱逐比例（默认 5%）
- `eviction_high_watermark_ratio` — 高水位线（默认 90%）

---

## 4. 分层存储（Tiered Storage）

Mooncake Store 支持多级存储层次：

```
层级 1: MEMORY (DRAM/VRAM)
   ↓ eviction/offload
层级 2: LOCAL_DISK (NVMe SSD)
   ↓ 进一步降级
层级 3: NOF_SSD (NVMe-oF 远程 SSD)
   ↓
层级 4: DFS (3FS/HF3FS 分布式文件系统)
```

### 4.1 Offload-on-Evict

当内存压力触发驱逐时，对象可以 **offload 到本地 SSD** 而非直接丢弃：
- 启用条件：`enable_offload_` + `offload_on_evict_`
- offload 通信：通过 `offload_rpc_server_`（coro_rpc）进行客户端间传输
- 元数据追踪：Master 为每个 offload 对象记录 `StorageObjectMetadata`（bucket_id, offset, size）

### 4.2 Promotion-on-Hit

**热点数据自动提升**机制：
1. 当 `GetReplicaList` 发现某个 key 只有 `LOCAL_DISK` 副本（即已被驱逐到 SSD）
2. 对该 key 进行频率估计（`CountMinSketch`）
3. 超过 `promotion_admission_threshold_`（默认 2 次）后，将 key 推入 promotion 队列
4. Holder 客户端在心跳中拉取 promotion 任务
5. 从 SSD 读回数据 → 分配 MEMORY 副本 → RDMA 写入新 MEMORY 副本 → 完成提升

**配置项**：
- `promotion_on_hit_` — 开关（默认关闭）
- `promotion_queue_limit_` — 队列容量（默认 50000）
- `promotion_admission_threshold_` — 准入频率阈值（默认 2）
- `promotion_max_per_heartbeat_` — 每次心跳最多取的任务数

### 4.3 Storage Backend

**文件**: `include/storage_backend.h`, `include/file_storage.h`

底层文件存储支持：
- **POSIX File** — 标准文件 I/O
- **io_uring File** — Linux io_uring 异步 I/O
- **SPDK** — 用户态 NVMe 驱动
- **HF3FS (3FS)** — 分布式文件系统（通过 `hf3fs_adapter`）
- **通用 DFS Adapter** — `distributed_storage_backend.h` 抽象接口

---

## 5. 高可用（High Availability）

**文件**: `include/ha/` 目录

Mooncake Store 的 HA 架构基于 **OpLog 复制 + 快照** 模式：

### 5.1 组件层次

```
MasterService (Primary)
    │
    ├── OpLogManager
    │   ├── OpLogStore (etcd / localfs)
    │   ├── OpLogChangeNotifier (etcd watch / polling)
    │   ├── OpLogReplicator → Standby
    │   └── OpLogApplier (Standby 端)
    │
    ├── LeaderCoordinator (etcd / redis / k8s)
    │   ├── Leader 选举
    │   ├── Leader Label Reconciler
    │   └── MasterServiceSupervisor
    │
    └── Snapshot System
        ├── MasterSnapshotManager
        ├── MasterSnapshotCodec (msgpack 序列化)
        ├── SnapshotCatalogStore (embedded / redis)
        └── SnapshotObjectStore (local / S3)
```

### 5.2 Standby 节点

**文件**: `include/ha/standby_controller.h`, `include/standby_state_machine.h`

- `StandbyController` — 备机控制器，管理 OpLog 消费和状态同步
- `StandbyStateMachine` — 状态机：`INIT → CATCHING_UP → STANDBY → PROMOTED`
- `HotStandbyService` — 热备服务入口
- `MetadataStore` — 备机的内存元数据存储（轻量级 kv map）

### 5.3 Leader 选举后端

| 后端 | 说明 |
|------|------|
| **etcd** | 基于 etcd lease + transaction 的选举 |
| **Redis** | 基于 Redis SETNX + TTL 的选举 |
| **K8s** | 基于 Kubernetes Lease 资源 |

### 5.4 OpLog（操作日志）

**文件**: `include/ha/oplog/`

- `OpLogSerializer` — 将操作序列化为二进制格式
- `OpLogStore` — 持久化存储后端（etcd / localfs）
- `OpLogReplicator` — 将 OpLog 从 Primary 复制到 Standby
- `OpLogApplier` — Standby 端将 OpLog 应用到本地 MetadataStore
- `OpLogChangeNotifier` — 变更通知机制（etcd watch / polling）

### 5.5 Snapshot（快照）

**文件**: `include/ha/snapshot/`

- **Catalog Store**: 快照元数据目录（embedded / Redis）
- **Object Store**: 快照数据存储（local file / S3）
- **MasterSnapshotCodec**: msgpack 序列化/反序列化 Master 状态
- **Snapshot 触发**: 定时（`snapshot_interval_seconds_`，默认 600s）+ 手动

---

## 6. 数据操作流程

### 6.1 Put 操作（写入）

```
Client                              Master
  │                                    │
  │ PutStart(key, size, config)        │
  ├───────────────────────────────────►│
  │                                    │ 1. 检查 key 是否已存在
  │                                    │ 2. 选择副本数 = config.replica_num
  │                                    │ 3. AllocationStrategy 选择段
  │                                    │ 4. 在每个段上分配 buffer
  │                                    │ 5. 创建 ObjectMetadata (INITIALIZED)
  │◄───────────────────────────────────│ 返回 Replica::Descriptor 列表
  │                                    │
  │ Transfer data to replicas (RDMA)   │
  │                                    │
  │ PutEnd(key)                        │
  ├───────────────────────────────────►│
  │                                    │ 标记副本状态: INITIALIZED → COMPLETE
  │                                    │ 设置 lease_timeout
  │◄───────────────────────────────────│ OK
```

**关键步骤**：
1. **PutStart** — Master 选择副本位置，分配 buffer，返回描述符
2. **数据传输** — Client 使用 TE 通过 RDMA/TCP 将数据写入副本位置
3. **PutEnd** — Master 将副本标记为 COMPLETE，设置租约

**副本策略**（`ReplicateConfig`）：
- `replica_num` — 副本数量
- `with_soft_pin` — 启用软固定（延长驱逐保护）
- `preferred_segment` — 优选段名称

### 6.2 Get 操作（读取）

```
Client                              Master
  │                                    │
  │ GetReplicaList(key)                │
  ├───────────────────────────────────►│
  │                                    │ 1. 查询 metadata_shards_
  │                                    │ 2. 检查 lease 是否过期
  │                                    │ 3. 返回 COMPLETE 状态的副本列表
  │                                    │ 4. GrantLease（延长租约）
  │                                    │ 5. 触发 Promotion-on-hit（若仅 LOCAL_DISK）
  │◄───────────────────────────────────│ 返回 Replica::Descriptor 列表
  │                                    │
  │ 选择最优副本（本地 > 远程）        │
  │ Transfer data from replica (RDMA)  │
  │                                    │
```

**零拷贝 Get** (`get_into`)：直接读取到预注册的本地 buffer，避免中间拷贝。

### 6.3 Upsert 操作

Upsert = Update or Insert，针对有租约保护的热点数据做原地更新：

- **Case A**: key 不存在 → 等同 Put
- **Case B**: key 存在且大小相同 → 复用现有 buffer 原地更新
- **Case C**: key 存在但大小不同 → 删除旧副本 + 分配新副本

Upsert 是性能优化的关键——避免了 LLM 推理中对同一 KV Cache 条目反复 Put/Remove 的开销。

---

## 7. 租约（Lease）与引脚（Pin）

### 7.1 Hard Lease（硬租约）

- 每次 `GetReplicaList` 自动续约
- 租约过期后对象可被驱逐
- 默认 TTL: `DEFAULT_DEFAULT_KV_LEASE_TTL = 10000ms`

### 7.2 Soft Pin（软固定）

- 通过 `ReplicateConfig.with_soft_pin = true` 设置
- 对象在软固定期间不会被第一阶段驱逐
- 默认 TTL: `DEFAULT_KV_SOFT_PIN_TTL_MS = 30min`

### 7.3 Hard Pin（硬固定）

- 不可变属性，创建时设置
- 硬固定的对象永不被驱逐

### 7.4 Group Routing

支持将多个 key 绑定到同一个 `group_id`，确保它们被路由到同一节点——对于需要本地化访问的相关 KV Cache 块很重要。

---

## 8. 多租户（Multi-Tenancy）

**文件**: `include/tenant_quota.h`, `include/tenant_quota_sharded.h`

- 每个对象属于一个 `TenantId`（默认：`"default"`）
- 每个租户可以有独立的**内存配额**（quota_bytes）
- 配额超限时：`TENANT_QUOTA_EXCEEDED` 错误
- 配额驱逐：`EvictTenantMemoryForQuota` — 专为超配 tenant 触发驱逐
- 配额策略持久化：`TenantQuotaPolicyStore`

**配额操作**：
- `ReserveTenantQuota` — 预留配额（PutStart 时）
- `CommitTenantQuota` — 确认配额（PutEnd 时）
- `AbortTenantQuota` — 释放预留（PutRevoke 时）
- `ReleaseTenantQuota` — 释放已确认配额（Remove 时）

---

## 9. Task 系统

**文件**: `include/task_manager.h`

支持异步数据管理任务：

| 任务类型 | 说明 |
|----------|------|
| **Copy Task** | 将对象的副本复制到新段 |
| **Move Task** | 将对象的副本从一个段移动到另一个段 |
| **Drain Job** | 优雅排空一个/多个段（迁移所有数据后安全卸载） |

**Drain Job 流程**：
1. `CreateDrainJob` — 指定源段和目标段
2. `ProcessDrainJobs` — 分发线程处理排空任务
3. `RefreshDrainJobTasks` — 扫描段的剩余对象
4. `ScheduleDrainJobTasks` — 创建 Copy/Move 子任务
5. `MaybeCompleteDrainJob` — 检测是否排空完成
6. 排空完成 → 段状态变为 `DRAINED` → 安全卸载

---

## 10. API 层级

### 10.1 Python API（面向用户的主要接口）

```python
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
store.setup("localhost", "http://localhost:8080/metadata",
            512*1024*1024, 128*1024*1024, "tcp", "", "localhost:50051")

# 基本操作
store.put("key", b"value")
data = store.get("key")

# 零拷贝
store.put_from("key", buffer_ptr, size)
store.get_into("key", buffer_ptr, size)

# PyTorch Tensor
store.put_tensor("my_tensor", tensor)
retrieved = store.get_tensor("my_tensor")

# 批量
store.put_batch(keys, values)
store.get_batch(keys)

# 复制配置
config = ReplicateConfig()
config.replica_num = 3
config.with_soft_pin = True
store.put("key", b"value", config)
```

### 10.2 C API

**文件**: `include/store_c.h`

- 面向 C 语言和 FFI
- 函数以 `mooncake_store_` 为前缀

### 10.3 Go 客户端

**文件**: `go/mooncakestore/store.go`

- 通过 CGO 调用 C API
- 提供 Go 风格的错误处理和配置管理
- 包含集成测试（`go/tests/integration_test.go`）

### 10.4 Rust 客户端

**文件**: `rust/src/store.rs`

- Rust FFI 封装
- 提供 `MooncakeStore` 结构体
- 错误映射为 Rust `Result` 类型

---

## 11. 配置系统

**默认配置文件**: `conf/master.json`, `conf/master.yaml`

核心配置结构（`MasterServiceConfig`）：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `default_kv_lease_ttl` | 10000ms | 默认 KV 租约 TTL |
| `default_kv_soft_pin_ttl` | 30min | 软固定 TTL |
| `eviction_ratio` | 5% | 驱逐比例 |
| `eviction_high_watermark_ratio` | 90% | 高水位线 |
| `client_live_ttl_sec` | 10s | 客户端存活 TTL |
| `put_start_discard_timeout` | 30s | Put 开始后丢弃超时 |
| `put_start_release_timeout` | 600s | Put 开始后释放超时 |
| `snapshot_interval_seconds` | 600s | 快照间隔 |
| `snapshot_retention_count` | 2 | 快照保留数 |

环境变量：
- `MC_STORE_CLUSTER_ID` — 集群 ID
- `MC_STORE_USE_HUGEPAGE` — 启用 hugepage
- `MC_STORE_MEMCPY` — 启用本地 memcpy 优化
- `MC_STORE_CLIENT_METRIC` — 启用客户端 metrics

---

## 12. Metrics 系统

**文件**: `include/master_metric_manager.h`, `include/client_metric.h`

### Master 端 Metrics
- `key_count` / `soft_pin_key_count` — 对象计数
- `value_size` 分布 — 对象大小直方图
- `memory_cache_total` / `disk_cache_total` — 缓存容量使用
- `put_start_discard_cnt` / `put_start_release_cnt` — 写入丢弃/释放计数
- `promotion_in_flight` / `promotion_cancelled` — 提升任务计数器
- `tenant_quota_*` — 多租户配额指标

### Client 端 Metrics
- 传输操作成功/失败计数
- 缓冲区使用情况
- 热缓存命中率

### KvEvent Publisher
- 可选的对象变更事件发布（存储/删除/驱逐）
- 支持外部系统监听 Store 内的数据变化

---

## 13. 源码文件布局

```
mooncake-store/
├── CMakeLists.txt                    # 顶层 CMake
├── conf/
│   ├── master.json                   # Master 默认配置 (JSON)
│   └── master.yaml                   # Master 默认配置 (YAML)
├── include/                          # 头文件（~80 个头文件）
│   ├── real_client.h                 # 客户端核心实现
│   ├── pyclient.h                    # Python 绑定基类
│   ├── dummy_client.h                # Dummy 客户端（子进程代理）
│   ├── master_service.h              # Master 服务核心接口
│   ├── master_config.h               # Master 配置结构
│   ├── master_client.h               # Master RPC 客户端
│   ├── client_service.h              # Client 服务接口
│   ├── client_buffer.h               # Client 缓冲区管理
│   ├── segment.h                     # 段定义 + SegmentManager
│   ├── replica.h                     # 副本定义与状态机
│   ├── types.h                       # 核心类型+错误码+常量
│   ├── allocator.h                   # 内存分配器
│   ├── allocation_strategy.h         # 分配策略
│   ├── eviction_strategy.h           # 驱逐策略
│   ├── storage_backend.h             # 存储后端接口
│   ├── file_storage.h                # 文件存储
│   ├── metadata_store.h              # Standby 元数据存储接口
│   ├── transfer_task.h               # 传输任务抽象
│   ├── task_manager.h                # 异步任务管理器
│   ├── tenant_quota.h                # 租户配额
│   ├── tenant_quota_sharded.h        # 分片租户配额表
│   ├── rpc_types.h                   # RPC 请求/响应类型
│   ├── rpc_service.h                 # RPC 服务
│   ├── rpc_helper.h                  # RPC 辅助工具
│   ├── http_metadata_server.h        # HTTP 元数据服务器
│   ├── ha/                           # 高可用子系统
│   │   ├── ha_types.h
│   │   ├── standby_controller.h
│   │   ├── standby_state_machine.h
│   │   ├── hot_standby_service.h
│   │   ├── ha_metric_manager.h
│   │   ├── leadership/              # Leader 选举
│   │   │   ├── leader_coordinator.h
│   │   │   ├── leader_coordinator_factory.h
│   │   │   ├── master_service_supervisor.h
│   │   │   └── backends/{etcd,redis,k8s}/
│   │   ├── oplog/                   # OpLog 复制
│   │   │   ├── oplog_manager.h
│   │   │   ├── oplog_store.h
│   │   │   ├── oplog_replicator.h
│   │   │   ├── oplog_applier.h
│   │   │   └── ...
│   │   └── snapshot/                # 快照系统
│   │       ├── master_snapshot_codec.h
│   │       ├── snapshot_provider.h
│   │       ├── catalog/             # 快照目录
│   │       └── object/              # 快照对象存储
│   ├── kv_event/                    # KV 事件发布
│   │   ├── kv_event_publisher.h
│   │   └── kv_event_config.h
│   ├── cachelib_memory_allocator/   # Facebook CacheLib 集成
│   ├── offset_allocator/            # 偏移量分配器
│   ├── device/                      # 加速器设备抽象
│   │   ├── accelerator_device.h
│   │   ├── accelerator_registry.h
│   │   └── runtime_accelerator.h
│   ├── engram/                      # Engram 存储
│   │   └── engram_store.h
│   ├── hf3fs/                       # 3FS 集成
│   │   └── hf3fs.h
│   ├── spdk/                        # SPDK NVMe
│   │   └── spdk_wrapper.h
│   ├── storage/distributed/         # 分布式存储后端
│   │   ├── distributed_storage_backend.h
│   │   ├── fs_adapter.h
│   │   └── hf3fs_adapter.h
│   ├── store_c.h                    # C API
│   └── utils/                       # 工具类
│       ├── file_util.h
│       ├── s3_helper.h
│       ├── zstd_util.h
│       └── base64.h
├── src/                             # 实现文件（镜像 include 结构）
│   ├── real_client.cpp              # 客户端实现
│   ├── master.cpp                   # Master 进程入口
│   ├── master_service.cpp           # Master 服务实现
│   ├── segment.cpp                  # 段管理实现
│   ├── transfer_task.cpp            # 传输任务实现
│   ├── ...
│   └── cachelib_memory_allocator/   # CacheLib 源码
├── go/                              # Go 客户端
│   ├── mooncakestore/store.go
│   ├── examples/basic/main.go
│   └── tests/integration_test.go
├── rust/                            # Rust 客户端
│   ├── src/store.rs
│   ├── src/lib.rs
│   └── examples/
├── tests/                           # 85+ 测试文件
│   ├── master_service_test.cpp
│   ├── client_integration_test.cpp
│   ├── e2e/                         # 端到端测试
│   ├── ha/                          # HA 测试
│   │   ├── leadership/
│   │   ├── oplog/
│   │   ├── snapshot/
│   │   └── standby/
│   ├── stress_workload_test.cpp     # 压力测试
│   └── ...
└── benchmarks/                      # 性能基准
    ├── store_kv_bench.py
    ├── storage_backend_bench.cpp
    ├── allocator_bench.cpp
    └── ...
```

---

## 14. 与 Transfer Engine 的关系

Mooncake Store 依赖 Transfer Engine 提供底层数据传输能力：

```
MooncakeDistributedStore (Python)
        │
        ▼
RealClient (C++)
        │
        ├── 元数据操作 ──► MasterService (RPC)
        │
        └── 数据传输 ────► Transfer Engine (RDMA/TCP)
```

- Store 通过 `register_buffer` 将内存注册到 TE
- Put/Get 操作通过 TE 的 RDMA/TCP 直接传输
- Store 不关心传输协议细节，只需调用 TE API

---

## 15. 设计要点与约束

### 设计亮点

1. **分片元数据** — 1024 个分片设计有效降低锁竞争，支撑高并发
2. **三阶段写入** — PutStart(预留) → Transfer(传输) → PutEnd(确认) 分离控制面和数据面
3. **Soft/Hard Pin** — 灵活的缓存驱逐保护策略
4. **Upsert** — 原地更新避免 KV Cache 场景的反复分配开销
5. **Promotion-on-Hit** — 自动将热点 SSD 数据提升到内存，平衡容量与延迟
6. **Drain Job** — 优雅排空，支持在线段迁移
7. **Multi-Tier Storage** — 内存→SSD→DFS 的自动分层
8. **HA with OpLog** — 不依赖外部数据库，基于 OpLog 的主备复制

### 已知约束

1. **Master 单点**（非 HA 模式）— 默认配置下 Master 是单点，需启用 HA 获得高可用
2. **强一致性** — 元数据操作走 Master，所有 Put/Get 都需要与 Master 通信
3. **内存预分配** — 段（segment）大小在 Mount 时固定，无法动态调整
4. **驱逐触发滞后** — 驱逐线程每 10ms 检查一次，在高写入压力下可能短暂过载

---

## 16. 总结

Mooncake Store 是一个成熟的生产级分布式 KV 缓存存储系统，为 LLM 推理场景中的 KV Cache 共享提供了完整解决方案。其核心价值：

- **高性能** — 零拷贝 RDMA 传输 + Slab 分配器 + 分片元数据
- **数据可靠性** — 多副本 + OpLog 复制 + 快照持久化 + 主备 HA
- **存储分层** — 内存 → SSD → DFS 的自动数据生命周期管理
- **运维友好** — 优雅排空、多租户配额、Prometheus 指标、KV 事件
- **多语言支持** — Python / C++ / C / Go / Rust 五个层级
- **丰富的测试** — 85+ 测试文件覆盖单元、集成、E2E、HA、压力测试
