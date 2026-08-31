# Mooncake `LOCAL_DISK` L4 装载路径代码阅读笔记

> 阅读基线：Git HEAD `b97f13cd` 及当前工作区，2026-08-22。
> 本文只讨论 `LOCAL_DISK` 的前台装载（Get/read）路径；写入 L4、
> promotion 和 TENT GDS 仅在影响读路径时说明。

## 1. 先给结论

当前 `LOCAL_DISK` 的“装载”不是 requester 根据磁盘 descriptor 直接访问文件，
而是一个两阶段数据路径：

```text
L4 backend -> holder registered Host buffer -> requester Host/GPU slices
```

两阶段之间分别使用不同机制：

1. requester 根据 `LocalDiskDescriptor.transport_endpoint` 调用 holder 的
   offload RPC；
2. holder 使用自己的 `StorageBackendInterface` 按 key 定位并读取 L4；
3. holder 返回临时 Host buffer 的地址和 Transfer Engine endpoint；
4. requester 通过 TE 把这些 Host bytes 搬入最终 slices；
5. requester 按 `batch_id` 通知 holder 释放临时 buffer，超时 GC 是兜底。

所以 `LOCAL_DISK` 的 descriptor 是 **holder 路由描述符**，不是文件/extent
描述符。底层可以是本地 SSD、Bucket、OffsetAllocator 或 HF3FS，但这些物理寻址
细节都留在 holder 的 backend 内部。

## 2. 关键类型与三种地址

### 2.1 Master 保存和返回的 descriptor

`mooncake-store/include/replica.h`：

```cpp
struct LocalDiskReplicaData {
    UUID client_id;
    uint64_t object_size = 0;
    std::string transport_endpoint;
};

struct LocalDiskDescriptor {
    UUID client_id;
    uint64_t object_size = 0;
    std::string transport_endpoint;
    YLT_REFL(LocalDiskDescriptor, client_id, object_size, transport_endpoint);
};
```

三个字段的作用分别是：

| 字段 | 作用 | 不负责什么 |
|------|------|------------|
| `client_id` | 标识 Replica holder，供 Master 做存活性和清理判断 | 不用于打开文件 |
| `object_size` | 校验目标容量、申请 staging buffer、构造传输长度 | 不描述磁盘 offset |
| `transport_endpoint` | 定位 holder 的 coro_rpc 服务 | 不是 TE endpoint，也不是 SSD URI |

### 2.2 一次装载涉及三种地址

| 地址 | 产生者 | 消费者 | 用途 |
|------|--------|--------|------|
| holder RPC endpoint | holder `RealClient` 启动 offload RPC server 后生成 | requester `ClientRequester` | 请求 holder 执行 `BatchGet` 和释放 buffer |
| holder TE endpoint | holder `Client::GetSegmentEndpoint()` | requester `TransferSubmitter` | 打开 holder 已注册 Host memory segment |
| holder Host pointer | holder `FileStorage::AllocateBatch` | requester TE request | 指向本次 batch 的远端源数据 |

不要把 `LocalDiskDescriptor.transport_endpoint` 与 RPC 响应中的
`transfer_engine_addr` 混为一谈：前者找到控制服务，后者用于实际搬运 payload。

## 3. 装载成立前的状态

在进入 Get 之前，L4 写入完成回调会把 holder 的 `local_rpc_addr_` 写入
`StorageObjectMetadata.transport_endpoint`。`MasterService::NotifyOffloadSuccess`
随后创建：

```text
Replica(
  holder client_id,
  metadata.data_size,
  metadata.transport_endpoint,
  COMPLETE)
```

只有 `COMPLETE` Replica 才会由 `GetReplicaList` 返回。Master 不保存 backend 的
文件路径、bucket ID 或 value offset；装载时 holder 仍以 tenant-scoped key 重新在
本地 backend 中定位对象。

## 4. 端到端时序

```text
requester                 Master                 holder RealClient
    |                        |                           |
    | GetReplicaList(key)    |                           |
    |----------------------->|                           |
    |  COMPLETE LOCAL_DISK   |                           |
    |  + object lease        |                           |
    |<-----------------------|                           |
    |                                                    |
    | batch_get_offload_object(scoped keys, sizes)       |
    |--------------------------------------------------->|
    |                                                    | FileStorage::BatchGet
    |                                                    |   -> AllocateBatch
    |                                                    |   -> BatchLoad
    |                                                    |      L4 -> Host
    | batch_id + pointers + TE endpoint + GC TTL         |
    |<---------------------------------------------------|
    |                                                    |
    | TE READ: holder Host pointers -> requester slices  |
    |--------------------------------------------------->|
    |                                                    |
    | release_offload_buffer(batch_id)                   |
    |--------------------------------------------------->|
```

完整数据流是：

```text
SSD / DFS
  -> StorageBackendInterface::BatchLoad
  -> holder registered Host ClientBuffer
  -> Transfer Engine
  -> requester Host/GPU slices
```

## 5. 第一步：Master 返回可读 Replica

`Client::Query` 调用 `MasterClient::GetReplicaList`。Master：

1. 查找 tenant + key 对应的 `ObjectMetadata`；
2. 只收集 `COMPLETE` Replica descriptor；
3. 没有可读 Replica 时返回 `REPLICA_IS_NOT_READY`；
4. 为对象授予 read lease；
5. 如果没有 MEMORY、只有 LOCAL_DISK 且启用了 promotion-on-hit，则额外尝试
   排队 promotion。

promotion 是旁路异步动作，不会改变本次 Get 已获得的 Replica list。本次请求仍沿
`LOCAL_DISK` 路径完成。

## 6. 第二步：requester 选择并按 holder 聚合

`SelectBestReplica` 的读取优先级是：

```text
local MEMORY
  -> local NOF_SSD
  -> remote MEMORY
  -> remote NOF_SSD
  -> LOCAL_DISK
  -> DISK
```

因此只有没有更高优先级的可用内存或 NoF Replica 时，才进入 `LOCAL_DISK`。
若同时存在 `LOCAL_DISK` 和传统 `DISK`，选择器优先前者。

单对象、批量 buffer 和 `batch_get_into` 等公开入口最终都会构造：

```cpp
std::unordered_map<std::string, std::vector<Slice>> objects;
```

批量路径再按 `transport_endpoint` 分组，使同一个 holder 的多个 key 合成一次 RPC。
发送给 holder 的 key 会先变成 tenant-scoped storage key；每个 size 是该 key 所有
目标 slices 的长度之和。

当前代码即使发现 holder 与 requester 在同一节点，也没有把 `LOCAL_DISK` 自动改成
本进程直接文件读取；它仍按 descriptor 的 RPC endpoint 进入 holder 协议。

## 7. 第三步：holder RPC 执行 L4 读取

holder 启动时为 SSD offload 注册两个 RPC handler：

```text
RealClient::batch_get_offload_object
RealClient::release_offload_buffer
```

`batch_get_offload_object` 不在 coro_rpc I/O 线程中同步执行 SSD I/O，而是把工作投递
到专用线程池，然后调用：

```text
FileStorage::BatchGet(keys, sizes)
  -> AllocateBatch(keys, sizes)
  -> BatchLoad(allocated_batch->slices)
```

### 7.1 staging buffer 分配

`FileStorage` 初始化时创建一个连续的 `AlignedClientBufferAllocator`，并通过
`Client::RegisterLocalMemory` 把整段区域注册给 Transfer Engine。启用 io_uring 时，
同一大块内存还可注册为 fixed buffer。

每个对象实际申请：

```text
align_up(data_size, 4096) + 2 * 4096
```

然后把可见 Slice 起点向上对齐到 4 KiB。额外空间用于 O_DIRECT 的起点对齐和尾部
padding。每个成功 batch 保存：

```text
batch_id
BufferHandle[]
key -> aligned Slice
lease_timeout
remote pointer[]
total_size
```

`BufferHandle` 通过 RAII 把子分配归还给 allocator，但成功返回 RPC 前必须把整个
`AllocatedBatch` 放入 `client_buffer_allocated_batches_`，否则远端 pointer 会立即失效。

### 7.2 backend 读取

`FileStorage::BatchLoad` 只是计时、记录 SSD metrics，然后委托：

```cpp
storage_backend_->BatchLoad(batch_object);
```

物理定位因 backend 而异：

| backend | holder 内部如何定位与加载 |
|---------|----------------------------|
| File-per-key | scoped key -> 文件路径，读取并解析 `KVEntry` |
| Bucket | backend 元数据 -> bucket + offset，处理 O_DIRECT 对齐 |
| OffsetAllocator | 索引 -> data file record/value offset |
| Distributed/HF3FS | scoped key -> hash bucket 文件路径 -> HF3FS adapter |

这一步解释了为什么 `LocalDiskDescriptor` 不需要暴露 `file_path`：当前协议把物理
定位完全委托给 holder 的 `StorageBackendInterface`。

### 7.3 RPC 响应

读取成功后 holder 返回：

```cpp
BatchGetOffloadObjectResponse {
    batch_id,
    pointers,
    transfer_engine_addr,
    gc_ttl_ms
}
```

其中 `pointers[i]` 与请求 key 的顺序对应，指向 holder 已注册 Host 区域；
`transfer_engine_addr` 来自 holder 的 `Client::GetSegmentEndpoint()`。

## 8. 第四步：TE 把 Host staging 搬到最终 slices

requester 调用：

```text
Client::BatchGetOffloadObject
  -> TransferSubmitter::submit_batch_get_offload_object
  -> TransferFuture::get
```

`submit_batch_get_offload_object`：

1. 对同一 holder 的 `transfer_engine_addr` 只打开一次 segment；
2. 以 RPC 返回的 pointer 为每个对象的远端连续起点；
3. 为每个非连续目标 Slice 生成一个 `TransferRequest::READ`；
4. 用累计 offset 把连续源对象 scatter 到 requester slices；
5. 批量提交并等待完成。

目标 Slice 可以是已经注册的 Host 或 GPU memory。因此最后一跳可以是：

```text
holder Host -> requester Host
holder Host -> requester GPU (例如 GPUDirect RDMA)
```

但源端数据已经先经过 `L4 -> holder Host`，所以它不是 SSD/DFS 到 GPU 的端到端
GDS。

## 9. 第五步：临时 buffer 生命周期

TE 等待结束后，requester 立即发起 fire-and-forget：

```text
release_offload_buffer(holder RPC endpoint, batch_id)
```

holder 从 `client_buffer_allocated_batches_` 擦除对应 batch；最后一个 shared pointer
销毁后，`BufferHandle` 把空间归还 allocator。

如果释放 RPC 丢失，后台 GC 会在 `client_buffer_gc_ttl_ms` 到期后擦除 batch。这个
TTL 保护的是 holder 临时 Host pointer，而 Master 的 object read lease 保护的是
Object/Replica 生命周期；两者不是同一个 lease。

需要特别注意：

- TE 传输失败时，只要已经收到 RPC response，requester 仍会尝试释放 batch；
- RPC response 的 pointer 数量不匹配时，当前代码在显式 release 之前返回，只能依赖
  holder GC 回收；
- 完整操作耗时达到或超过 holder 返回的 GC TTL 时，即使 TE 返回成功，requester
  仍返回错误，避免把可能越过 staging 生命周期的操作当作可靠成功；
- holder 掉线后，Master 的 stale-handle 清理会移除该 `client_id` 所属的
  `LOCAL_DISK` Replica，阻止后续请求持续路由到失效 endpoint。

## 10. full read、scatter read 与 range read

### 10.1 full-object read

完整读取可以直接把调用者提供的 slices 作为 TE 最终目标。对于 GPU slices，不需要
requester-side Host bounce：

```text
L4 -> holder Host -> requester GPU
```

### 10.2 scatter/gather

holder 对每个对象提供一段连续 Host 数据；requester 通过多个 TE request 按累计
offset 写入多个非连续 slices。Object 仍只是一份完整连续 value，Slice 只是调用视图。

### 10.3 range read

当前 offload RPC 没有源 offset 参数，只会从对象 offset 0 顺序传输。因此
`LOCAL_DISK` 的部分读取会：

```text
从 L4 加载 [0, src_offset + size)
  -> requester 临时 Host buffer
  -> 截取 [src_offset, src_offset + size)
  -> scatter 到最终 Host/GPU 目标
```

这不是后端原生 range read。`src_offset` 越大，前缀读放大越明显。若后续扩展 L4
协议，range-aware locator/RPC/TE request 是值得优先补齐的接口。

## 11. 失败边界

| 阶段 | 典型失败 | 当前结果 |
|------|----------|----------|
| Master query | object 不存在、无 COMPLETE Replica | Get 失败，不发 holder RPC |
| Replica selection | 没有可用类型 | `INVALID_REPLICA` 或空结果 |
| holder RPC connect | endpoint 失效、连接拒绝 | 本次失败；后续依赖 stale cleanup |
| AllocateBatch | size 非法、临时池不足 | RPC 返回错误；会先尝试回收过期 batch |
| BatchLoad | 文件缺失、校验/读取失败 | RPC 返回 backend 错误，不暴露 pointer |
| pointer mapping | pointer 数量与 key 不一致 | requester 失败，buffer 等待 TTL GC |
| TE open/submit/wait | endpoint 或 transport 失败 | 本次失败，并尝试释放 holder batch |
| release RPC | 网络丢失或 batch 已释放 | 不阻塞结果；holder GC 兜底 |
| 操作超过 GC TTL | pointer 可能失去生命周期保护 | requester 返回错误 |

正确性原则是：任一阶段不确定时返回 miss/error，让上层重算，不能把部分读取或过期
buffer 当成有效 KV。

## 12. 观测与验证入口

当前可以直接观察：

- `offload_rpc_read_count_`：requester 进入 holder RPC 读取的次数；
- `FileStorage::BatchGet` 总耗时日志；
- `ssd_read_ops / ssd_read_bytes / ssd_read_latency_*`；
- TE strategy 与 transfer 完成结果；
- `batch_id`、GC TTL 和 release 日志；
- Master 的 file-cache hit 指标。

优先阅读和测试：

| 目的 | 文件/符号 |
|------|-----------|
| Master 返回 descriptor 与 lease | `master_service.cpp::GetReplicaList` |
| Replica 优先级 | `include/replica_selection.h::SelectBestReplica` |
| requester 聚合与完整生命周期 | `real_client.cpp::batch_get_into_offload_object_internal` |
| holder RPC | `real_client.cpp::batch_get_offload_object` |
| staging 分配、装载和 GC | `file_storage.cpp::BatchGet/AllocateBatch/ReleaseBuffer` |
| TE scatter | `transfer_task.cpp::submit_batch_get_offload_object` |
| backend 基础读测试 | `tests/file_storage_test.cpp` |
| 选择策略 | `tests/replica_selection_test.cpp` |
| Master LOCAL_DISK 状态 | `tests/master_service_ssd_test.cpp` |

当前 focused tests 对 backend `BatchLoad`、指标、Replica 选择和 Master 状态覆盖较多；
跨进程的“holder RPC -> registered Host pointer -> TE -> requester GPU -> release/GC”仍应
作为后续 L4 改造的关键端到端测试面。

## 13. 后续接入更多 L4 协议时的切入点

应保持下面的抽象边界：

```text
Master Replica/lease policy
  -> typed locator or holder route
  -> backend-specific resolve/load
  -> registered staging/direct target
  -> transport completion
```

如果继续沿 `LOCAL_DISK` holder 模型扩展，新的 backend 只需实现
`StorageBackendInterface::BatchLoad` 等接口，但仍会经过 holder Host staging。

如果目标是 descriptor-based DFS 或 GDS，应改变的不是简单地让 holder 解析更多
`file_path`，而是让 Master/requester 获得带类型、受 lease/generation 保护的
`path + offset + length` locator，并提供不经过 holder Host buffer 的新 fast path。

