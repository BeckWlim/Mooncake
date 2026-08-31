# Mooncake Replica 放置与 `NOF_SSD` L4 代码阅读笔记

> 阅读基线：Git HEAD `b97f13cd` 及当前工作区，2026-08-22。
> 本文聚焦 `ReplicateConfig`、`ReplicaWriteMode`、quota、`NOF_SSD` 的前台
> Put/Get 生命周期，以及它与 `LOCAL_DISK` offload 的边界。GDS 的完整分析见
> `mooncake-l4-gds-code-reading.md`。

## 1. 先给结论

1. `ReplicateConfig` 是单次 Put 的 **Replica 放置请求**：
   `replica_num` 表示 MEMORY/DRAM Replica 数量，`nof_replica_num` 表示
   `NOF_SSD` Replica 数量。它们不配置 `DISK` 或 `LOCAL_DISK`。
2. `DetermineReplicaWriteMode` 不决定 Replica 的介质，而是根据两个数量决定
   **本次写入的成功条件和失败回滚范围**。
3. `NOF_SSD` 已是 Store 的一等 Replica：有 extent 分配、PROCESSING/COMPLETE
   状态、前台 Put/Get、PutEnd/PutRevoke、refcount、容量水位和独立淘汰。
4. 如果把 L4 定义为“集群共享 SSD 容量层”，`NOF_SSD` 已满足基本结构闭环；
   但它仍是显式配置的前台 write-through Replica，不是自动
   `MEMORY -> L4 -> MEMORY` 分层策略。
5. `NOF_SSD` 不是 MEMORY 的子对象或只能被动存在的“影子”。同一个
   `ObjectMetadata` 只是并列持有多种 Replica；NoF 可以成为唯一有效副本，也可以
   在 MEMORY 淘汰后继续独立提供读取。
6. `(replica_num=1, nof_replica_num=1)` 是 flexible，而不是严格双写。它只要求
   MEMORY 与 NoF 中至少一侧成功，因此不能用来证明对象一定拥有 L4 副本。
7. `LOCAL_DISK` 的定位不同：当前普通 Put 不直接分配或写入 `LOCAL_DISK`；它主要
   由已完成 MEMORY Replica 经 offload 产生。

一句话口径：**`NOF_SSD` 是显式写入、可独立存活的共享块存储 L4 Replica；它主要
解决 DRAM 之外的共享缓存容量、DRAM 淘汰后的对象存活和计算/存储解耦，而不是自动
冷热迁移或持久化 system of record。**

## 2. `ReplicateConfig` 预定义什么

源码入口：`mooncake-store/include/replica.h`。

```cpp
struct ReplicateConfig {
    size_t replica_num{1};
    size_t nof_replica_num{0};
    // ... pin、preferred segments、data type、host/group routing
};
```

两个数量字段的稳定语义是：

| 字段 | 请求的 Replica | 介质 | 是否计入当前 tenant memory quota |
|------|----------------|------|------------------------------------|
| `replica_num` | `MEMORY` | DRAM/共享内存 Segment | 是 |
| `nof_replica_num` | `NOF_SSD` | SPDK NVMe-oF SSD extent | 否 |

它们是请求，不是提交结果。最终状态还取决于：

- 编译时是否启用 `USE_NOF`；
- 是否存在已挂载且可分配的 Memory/NoF Segment；
- extent 分配是否成功；
- 客户端数据传输是否成功；
- 当前 `ReplicaWriteMode` 是否允许部分成功。

`(0, 0)` 虽然会被 `DetermineReplicaWriteMode` 的最后分支归为
`SINGLE_REPLICA`，但 `PutStart` 参数校验会直接返回 `INVALID_PARAMS`，因此不是合法
的实际写入布局。

`DISK` 和 `LOCAL_DISK` 不由这两个 counter 选择：

- `DISK` 由 Master 的 `root_fs_dir_` 等全局配置启用；
- `LOCAL_DISK` 由 offload/re-registration 流程创建。

## 3. `ReplicaWriteMode` 是写入原子性策略

```cpp
inline ReplicaWriteMode DetermineReplicaWriteMode(
    const ReplicateConfig& config) {
    if (config.replica_num == 1 && config.nof_replica_num == 1) {
        return ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA;
    }
    if (config.replica_num > 1 || config.nof_replica_num > 1) {
        return ReplicaWriteMode::RELIABLE_MULTI_REPLICA;
    }
    return ReplicaWriteMode::SINGLE_REPLICA;
}
```

### 3.1 决策表

| MEMORY 数量 | NoF 数量 | 模式 | 成功条件 |
|-------------|----------|------|----------|
| 1 | 0 | `SINGLE_REPLICA` | MEMORY 分配及传输成功 |
| 0 | 1 | `SINGLE_REPLICA` | NoF 分配及传输成功 |
| 1 | 1 | `FLEXIBLE_DUAL_REPLICA` | 任意一侧成功 |
| >1 | 任意 | `RELIABLE_MULTI_REPLICA` | 满足分配要求且所有已分配传输成功 |
| 任意 | >1 | `RELIABLE_MULTI_REPLICA` | 满足分配要求且所有已分配传输成功 |

当前 `Client::DetermineFinalizeDecision` 对 `SINGLE_REPLICA` 和
`RELIABLE_MULTI_REPLICA` 使用同一个严格分支：所有已分配 Replica 的传输都成功才
执行 `PutEnd(ALL)`；否则执行 `PutRevoke(ALL)`。两种 enum 目前主要区分请求拓扑，
真正具有特殊部分成功语义的是 `FLEXIBLE_DUAL_REPLICA`。

### 3.2 Flexible dual 的四种结果

```text
MEMORY success + NoF success -> PutEnd(ALL)
MEMORY success + NoF fail    -> PutEnd(MEMORY),  PutRevoke(NOF_SSD)
MEMORY fail    + NoF success -> PutEnd(NOF_SSD), PutRevoke(MEMORY)
MEMORY fail    + NoF fail    -> PutRevoke(ALL), Put fails
```

Master 在分配阶段也允许 flexible dual 的一侧分配失败，只要另一侧至少分配出一个
Replica。由此得到一个重要生产口径：

> `(1 MEMORY, 1 NoF)` 表示“尝试双介质写入并允许单侧降级”，不表示“必须同时拥有
> 一份 DRAM 和一份 SSD 副本”。

如果产品语义要求严格的 `1 MEMORY + 1 NoF`，当前 write-mode 分类需要扩展；简单
设置 `(1,1)` 不够。

## 4. quota charge 只计算 DRAM Replica

`MasterService::RequestedMemoryQuotaCharge` 的计算是：

```text
requested memory quota charge = value_length * config.replica_num
```

例如 64 MiB 对象请求两个 MEMORY Replica，PutStart 会预留 128 MiB tenant quota。
`nof_replica_num`、`DISK` 和 `LOCAL_DISK` 不增加这一项 memory quota。

quota 状态流转为：

```text
available
  -> ReserveTenantQuota                 PutStart 预留
  -> reserved
       -> CommitTenantQuota             PutEnd 按完成的 MEMORY Replica 结算
       -> AbortTenantQuota              失败或未使用部分回退
  -> used/committed
       -> ReleaseTenantQuota            Replica 删除时释放
```

`reserved_quota_charge` 是容量会计单位，不是费用。reservation 的价值是避免多个并发
Put 都在完成前观察到相同的剩余 quota，造成 DRAM 超卖。

当前 tenant quota 的边界也说明：NoF 虽有全局容量 metric、水位和 eviction，但还
没有对等的 tenant NoF SSD quota。

## 5. Put 如何创建 MEMORY 与 NoF Replica

核心入口是 `MasterService::AllocateAndInsertMetadata`：

```text
PutStart
  -> RequestedMemoryQuotaCharge
  -> ReserveTenantQuota
  -> DetermineReplicaWriteMode
  -> config.replica_num > 0 ? allocate MEMORY extents
  -> config.nof_replica_num > 0 ? allocate NOF_SSD extents
  -> HasExpectedReplicaAllocation
  -> optional: append descriptor-based DISK Replica
  -> insert ObjectMetadata
       replicas are PROCESSING
       reserved_quota_charge_bytes is recorded
  -> return Replica::Descriptor[] to client
```

客户端随后区分介质：

```text
DISK       -> PutToLocalFile
MEMORY     -> TransferWrite via memcpy / Transfer Engine
NOF_SSD    -> TransferWrite via SPDK NoF worker
LOCAL_DISK -> normal Put does not create or write this type
```

传输完成后，`DetermineFinalizeDecision` 决定调用 `PutEnd` 和/或 `PutRevoke`。
`PutEnd` 只把选中的有效 MEMORY/NoF Replica 从 `PROCESSING` 标记为 `COMPLETE`；
`PutRevoke` 删除失败侧 Replica 并释放其 extent。

## 6. NoF 的地址模型与数据路径

`NoFReplicaData` 和 `MemoryReplicaData` 都持有 `AllocatedBuffer`：

```cpp
struct NoFReplicaData {
    std::unique_ptr<AllocatedBuffer> buffer;
};

struct NoFDescriptor {
    AllocatedBuffer::Descriptor buffer_descriptor;
};
```

但 `AllocatedBuffer::Descriptor` 在 NoF 场景中的字段语义是块设备寻址：

| 字段 | NoF 语义 |
|------|----------|
| `transport_endpoint_` | SPDK NVMe-oF transport string，定位 controller/namespace |
| `buffer_address_` | namespace 内的 byte offset，提交前除以 block size 得到 LBA |
| `size_` | 分配的 extent 长度 |

前台 I/O 链是：

```text
NoFDescriptor
  -> TransferSubmitter::submitSpdkNofOperation
  -> SpdkWrapper::OpenNofSegment(endpoint)
  -> validate offset / pointer / size block alignment
  -> SpdkNofWorkerPool
  -> spdk_nvme_ns_cmd_read/write
  -> completion updates TransferFuture
```

当前约束包括：

- Store 必须以 `USE_NOF=ON` 构建；该选项默认关闭；
- NoF Segment 必须先挂载到 Master；
- 调用方 slices 必须拼成一段连续地址；
- local pointer、NoF offset 和 transfer size 都必须满足 NVMe block 对齐；
- 当前 NoF 分支绕过经典 Transfer Engine/TENT，直接进入 SPDK。

## 7. NoF 不是只能依附 MEMORY 的“影子”

### 7.1 `ObjectMetadata` 中没有父子 Replica

正确模型是：

```text
ObjectMetadata
  ├── MEMORY Replica
  ├── NOF_SSD Replica
  ├── DISK Replica
  └── LOCAL_DISK Replica
```

`ObjectMetadata` 管理同一逻辑对象的并列物理副本；源码没有“MEMORY 是父 Replica，
NoF 是子 Replica”的所有权关系。

### 7.2 NoF-only 是合法布局

```cpp
config.replica_num = 0;
config.nof_replica_num = 1;
```

此时 Put 成功后可以得到：

```text
ObjectMetadata
  └── COMPLETE NOF_SSD Replica
```

quota charge 为 0，读取直接通过 NoF descriptor 进入 SPDK。

### 7.3 MEMORY 淘汰后 NoF 可以继续承载对象

若最初 MEMORY 和 NoF 都成功：

```text
before memory eviction:
ObjectMetadata
  ├── COMPLETE MEMORY
  └── COMPLETE NOF_SSD

after memory eviction:
ObjectMetadata
  └── COMPLETE NOF_SSD
```

`BatchEvict` 可以删除无 refcount 的 COMPLETE MEMORY Replica。只要 NoF 仍有效，
metadata 仍有效，对象可以继续通过 SSD 命中。反过来，`NoFBatchEvict` 只删除 NoF
Replica；若还有 MEMORY/其他 Replica，逻辑对象同样继续存在。

因此 `(1,1)` 下 NoF 在热态可能表现为很少被选择的 backing copy，但在 MEMORY
释放后会成为实际承载对象的唯一 Replica。称为 **write-through backing cache** 比
“影子存储”更准确。

## 8. NoF 与 `LOCAL_DISK` 的本质差异

| 维度 | `NOF_SSD` | `LOCAL_DISK` |
|------|-----------|--------------|
| 创建时机 | PutStart 前台显式分配 | offload 成功或重启扫描后注册 |
| 配置入口 | `nof_replica_num` | Master/FileStorage 全局 offload 配置 |
| 写入来源 | 调用方原始 Put buffer | 已完成 MEMORY Replica |
| 地址模型 | endpoint + block offset + length | holder client + RPC endpoint + size |
| 实际 SSD 定位 | requester/client 可按 LBA 直接提交 SPDK | holder backend 按 scoped key 定位 |
| Replica 初次加入 metadata | `PROCESSING`，PutEnd 后 COMPLETE | SSD 写成功后直接以 COMPLETE 加入 |
| DRAM 下沉 | 不自动发生；Put 时预写 NoF | 主路径就是 MEMORY offload |
| promotion-on-hit | 当前无同等闭环 | 已有 LOCAL_DISK -> MEMORY promotion |
| 重启自扫描 | raw extent 本身不提供对象索引 | backend `ScanMeta` 可重新注册对象 |

`LOCAL_DISK` 的标准路径是：

```text
completed MEMORY
  -> PushOffloadingQueue
  -> holder heartbeat pulls task
  -> FileStorage::OffloadObjects
  -> StorageBackend::BatchOffload
  -> NotifyOffloadSuccess
  -> add COMPLETE LOCAL_DISK Replica
```

因此普通 Put 当前不存在：

```text
allocate LOCAL_DISK -> client directly writes it -> PutEnd(LOCAL_DISK)
```

不要把 `DISK` 的 `PutToLocalFile` 前台路径误认为 `LOCAL_DISK`。

## 9. `NOF_SSD` 是否满足 L4 结构需求

如果 L4 的最低定义是“DRAM 之外、可被 Store 管理和独立读取的共享 SSD cache
tier”，答案是满足：

| L4 结构能力 | 当前 NoF 状态 |
|-------------|--------------|
| SSD 容量介质 | 已满足 |
| 集群共享访问 | 已满足 |
| extent 分配和回收 | 已满足 |
| Replica PROCESSING/COMPLETE 状态 | 已满足 |
| 前台 Put/Get | 已满足 |
| 提交与撤销 | 已满足 |
| lease/refcount | 已满足 |
| 全局容量水位和 eviction | 已满足 |
| 独立于 MEMORY 存活 | 已满足 |
| DRAM 自动下沉 | 未满足 |
| SSD 命中自动 promotion | 未形成与 LOCAL_DISK 对等的闭环 |
| tenant SSD quota | 未满足 |
| 后端无关寻址 | 未满足，descriptor 绑定 SPDK NoF |
| SSD 自描述、CRC、扫描恢复 | 较弱，主要依赖 Master metadata |
| 任意 scatter/gather、非对齐对象 | 未满足 |

所以应区分两种表述：

- 可以说：**Store 已有共享块存储型 L4 Replica 的基本生命周期闭环。**
- 不宜说：**Store 已有通用、自动分层、可独立恢复的完整 L4 后端协议。**

## 10. NoF 在生产形态中主要解决什么

### 10.1 扩展昂贵 DRAM 之外的共享缓存容量

NoF 把远端 NVMe namespace 组织为 Master 可分配的 extent pool，让 KV Cache 可以
在不长期占用 DRAM 的情况下继续存在。它解决的是容量/成本层级问题，而不是简单
增加一个同介质副本。

### 10.2 让对象承受 MEMORY Replica 被回收

当 NoF 已在 Put 阶段写好，后续 DRAM eviction 可以直接释放 MEMORY extent，而无须
临时等待一次 SSD offload。对象随后从 NoF 读取，避免立即回源或重新计算 KV Cache。

### 10.3 降低对单一计算节点内存 holder 的依赖

MEMORY Replica 绑定已挂载内存 Segment；`LOCAL_DISK` 读取依赖 holder RPC。NoF
descriptor 则面向共享 NVMe-oF namespace，只要访问方能连接 endpoint，就可以按
extent 地址提交 I/O。它更接近 disaggregated shared storage pool。

### 10.4 提供跨介质可用性降级

`(1,1)` flexible 的目标是让写入在 MEMORY 或 NoF 单侧不可用时仍尽量成功：

```text
MEMORY unavailable -> NoF may keep the Put successful
NoF unavailable    -> MEMORY may keep the Put successful
both available     -> object has a write-through SSD backing copy
```

这优先解决 availability，不提供严格双副本 durability。若必须保证 L4 copy，不能只
依赖当前 flexible dual 默认语义。

### 10.5 它不是 system of record

当前 NoF Replica 更适合作为可淘汰共享缓存，而不是长期权威存储：

- raw extent 没有 LOCAL_DISK record layout 那样的 key/header/CRC 自描述；
- 对象定位和 extent 所有权主要依赖 Master metadata；
- NoF eviction 可以主动删除过期、无 refcount 的 SSD Replica；
- NoF target/namespace 失效后由 heartbeat 探测并卸载，不等于持久化恢复。

## 11. 为什么 NoF 没有直接成为 Store GDS 首选

这不是 L4 能力不足，而是当前存在两套不同数据面：

```text
Store NOF_SSD
  -> NoFDescriptor(endpoint + LBA extent)
  -> SPDK worker

TENT GDS
  -> file:// FileSegment(path + file offset)
  -> GdsFileContext
  -> cuFile
```

Store 一旦识别到 `NOF_SSD`，就在 `TransferSubmitter` 内直接进入 SPDK，未经过
Transfer Engine/TENT selector。当前路径也没有建立 GPU pointer 探测、GPU memory
向 SPDK 注册、GPU page pin 和 completion 生命周期。因此“Store NoF 已深入实现”
只能证明对象控制面和 Host/SPDK I/O 闭环，不能自动推出端到端 GDS。

若未来坚持 NoF 优先，有两条不同路线：

1. 把 NoF namespace 以 GDS 可接受的 file/raw-device handle 暴露，并将
   NoF extent 适配为 FileSegment；
2. 扩展 SPDK NoF 原生支持 GPU memory registration 和 direct DMA。该路线本质是
   SPDK/GPUDirect NoF，不一定应继续称为复用现有 cuFile GDS。

## 12. 建议统一使用的判断口径

### 12.1 关于配置

> `ReplicateConfig` 描述一次 Put 请求的 MEMORY/NoF Replica 布局；
> `ReplicaWriteMode` 描述这个布局的提交成功条件，而不是新的存储介质。

### 12.2 关于 NoF 与 L4

> `NOF_SSD` 已实现一种共享 NVMe-oF SSD L4 Replica，但不是抽象的通用 L4 协议。

### 12.3 关于“影子存储”

> 在 `(1,1)` 且两侧成功的热态下，NoF 可表现为 MEMORY 的 write-through backing
> copy；但它没有父子语义，可以 NoF-only，也可以在 MEMORY 淘汰后成为唯一副本。

### 12.4 关于生产价值

> NoF 主要提供共享 SSD 容量、DRAM 淘汰后的对象存活、计算/存储解耦和跨介质降级；
> 当前不应把它描述为自动 offload、严格双写或独立可恢复的持久化存储。

### 12.5 关于 `LOCAL_DISK`

> 当前 `LOCAL_DISK` 不由普通 Put 直接创建；其主写入链是 MEMORY offload，读取链是
> holder backend -> holder Host staging -> requester。

## 13. 推荐源码阅读顺序

1. `mooncake-store/include/replica.h`
   - `ReplicateConfig`
   - `ReplicaWriteMode`
   - `NoFReplicaData` / `NoFDescriptor`
2. `mooncake-store/src/master_service.cpp`
   - `RequestedMemoryQuotaCharge`
   - `AllocateAndInsertMetadata`
   - `PutEnd` / `PutRevoke`
   - `BatchEvict` / `NoFBatchEvict`
   - `NotifyOffloadSuccess`
3. `mooncake-store/src/client_service.cpp`
   - `HasExpectedReplicaAllocation`
   - `DetermineFinalizeDecision`
   - Put transfer loop
   - `GetPreferredReplica`
4. `mooncake-store/src/transfer_task.cpp`
   - `TransferSubmitter::submit`
   - `submitSpdkNofOperation`
   - `SpdkNofWorkerPool`
5. `mooncake-store/src/file_storage.cpp`
   - `OffloadObjects`
   - `StorageBackend::BatchOffload`
6. `mooncake-transfer-engine/tent/src/transport/gds/gds_transport.cpp`
   - 仅用于对照 NoF SPDK 与 TENT cuFile 两条独立路径。
