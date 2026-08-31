# Mooncake L4 与 GDS 代码阅读笔记

> 阅读基线：Git HEAD `b97f13cd` 及 2026-08-22 当前工作区。
> 背景材料：`~/work/tutorial/docs/KVCache-TechnicalReport.md`。
> 本文只核对 Mooncake 仓库中的 Store L4 与 TENT GDS，不展开 SGLang、vLLM
> 和 LMCache 的实现。

## 1. 先给结论

Mooncake 当前同时具备“SSD L4 的控制闭环”和“GPU 与文件直接传输的 GDS
数据面”，但二者尚未形成 Store 的统一默认调用链。

- `L4` 是本文采用的缓存层级口径，不是源码中的类名或公开 API。
- Store 用 `MEMORY / DISK / LOCAL_DISK / NOF_SSD` 表达 Replica 介质；其中
  `LOCAL_DISK` 的 offload、磁盘淘汰、重启扫描和 promotion-on-hit 已形成较完整
  的对象生命周期。
- 当前 `LOCAL_DISK` 写入先把 device Slice D2H 到 pinned Host buffer，再交给
  `StorageBackendInterface::BatchOffload`；读取先在 holder 端把 SSD 数据读入已注册
  ClientBuffer，再经 Transfer Engine 发给请求端。
- 请求端最后一跳可以直接写入 GPU Slice，但这不改变源端已经执行
  `SSD -> holder Host buffer` 的事实，因此它不是端到端 GDS。
- TENT 已实现 `file://` FileSegment、GDS/io_uring 选择、CUDA buffer 注册和
  cuFile Batch I/O；这是一套可独立使用的数据移动能力。
- `MC_USE_TENT` 只让经典 `TransferEngine` 门面把普通 TE 请求转给 TENT，并不会
  自动把 Store 的 `DISK` 或 `LOCAL_DISK` descriptor 改写成 FileSegment。
- 真正缺少的是 Store 与 TENT 之间的语义桥：稳定文件寻址、extent 生命周期、
  GPU page pin、提交/回滚、校验、可见性和 fallback 必须一起接通。

一句话概括：**Store 已经知道“哪个对象该在 SSD 上”，TENT 已经知道“怎样在
文件与 GPU 之间搬字节”，当前代码还没有让两者共享同一份可提交、可回收的
L4 extent 描述。**

## 2. 不要混用的三套概念

| 层次 | 源码抽象 | 回答的问题 |
|------|----------|------------|
| KV 缓存层级 | L1/L2/L3/L4（分析口径） | 数据处于 GPU、Host、共享 DRAM 还是 SSD |
| Store 对象层 | `ObjectMetadata`、`Replica`、lease/refcount | 对象是谁、在哪里、何时可见、何时可删 |
| TENT 数据面 | `SegmentDesc`、`Request`、`Transport` | 一段字节如何从本地地址移动到目标位置 |

`Replica` 不是 TENT `Segment`，Store `Slice` 也不是 GDS 内部的 16 MiB slice。
相同的“块”字眼可能分别表示 KV page、Store object、Replica extent、调用方
scatter/gather Slice 或 transport 内部分片。

## 3. 源码入口地图

### 3.1 Store L4

| 入口 | 重点符号 | 阅读目的 |
|------|----------|----------|
| `mooncake-store/include/allocator.h` | `ReplicaType` | 确认 Store 没有单独的 `L4` 或 `GDS` Replica |
| `mooncake-store/include/replica.h` | `DiskDescriptor`、`LocalDiskDescriptor`、`NoFDescriptor` | 比较三种 SSD 寻址语义 |
| `mooncake-store/include/segment.h` | `LocalDiskSegment` | 看 Master 为 holder 保存的 offload/promotion 队列和 SSD 水位 |
| `mooncake-store/src/master_service.cpp` | `PutEnd`、`BatchEvict`、`NotifyOffloadSuccess`、promotion 系列方法 | 跟控制面状态机 |
| `mooncake-store/src/file_storage.cpp` | `Heartbeat`、`OffloadObjects`、`BatchGet`、`ProcessPromotionTasks` | 跟 holder 端实际数据路径 |
| `mooncake-store/src/real_client.cpp` | `batch_get_offload_object`、`batch_get_into_offload_object_internal` | 跟远端 `LOCAL_DISK` Get |
| `mooncake-store/src/transfer_task.cpp` | `TransferSubmitter::submit*` | 确认不同 Replica 最终选择的执行器 |
| `mooncake-store/include/storage_backend.h`、`src/storage_backend.cpp` | `StorageBackendInterface` 及四种 backend | 看磁盘布局、持久化和淘汰 |

### 3.2 TENT GDS

| 入口 | 重点符号 | 阅读目的 |
|------|----------|----------|
| `mooncake-transfer-engine/tent/include/tent/common/types.h` | `Request`、`IntentType`、`kLocalFileSegmentPrefix` | 理解提交协议 |
| `tent/include/tent/runtime/segment.h` | `FileBufferDesc`、`FileSegmentDesc` | 理解文件寻址 |
| `tent/include/tent/runtime/transport.h` | `Capabilities` | 理解 selector 的能力约束 |
| `tent/src/runtime/segment_manager.cpp` | `makeFileRemote` | 看 `file://` 如何变成 FileSegment |
| `tent/src/runtime/transport_selector.cpp` | `file_storage` 默认 policy | 看 GDS 到 io_uring 的候选顺序 |
| `tent/src/runtime/transfer_engine_impl.cpp` | `getTransportType`、`prepareSubmit`、`commitPreparedSubmit` | 跟选择和提交 |
| `tent/src/runtime/transport_loader.cpp` | `loadTransports` | 区分编译可用与运行时启用 |
| `tent/src/transport/gds/gds_transport.cpp` | `GdsFileContext`、`submitTransferTasks` | 跟实际 cuFile 调用 |

## 4. Store 如何表示 L4

Store 中最接近 L4 的三种 Replica 并不同构：

| 类型 | descriptor 的核心字段 | 实际访问方式 | 生命周期特点 |
|------|------------------------|--------------|--------------|
| `DISK` | `file_path + object_size` | 本进程 `StorageBackend::LoadObject` / file worker | 传统文件路径 |
| `LOCAL_DISK` | `holder client_id + object_size + transport_endpoint` | holder offload RPC + Host ClientBuffer + TE | 与 offload/promotion 闭环结合最深 |
| `NOF_SSD` | `AllocatedBuffer::Descriptor` | SPDK NVMe-oF block I/O | 共享块地址与对齐语义 |

`LOCAL_DISK` descriptor 不保存文件路径、bucket ID 或 value offset。holder 的
`StorageBackend` 知道物理布局，Master 只知道“哪个 client 持有该对象”和“去哪个
RPC endpoint 取”。这正是当前 Store 无法直接构造 TENT FileSegment 的第一处边界。

`StorageObjectMetadata` 虽含 `bucket_id / offset / key_size / data_size /
transport_endpoint`，但 `MasterService::NotifyOffloadSuccess` 最终创建的
`LocalDiskReplicaData` 只保留 client、size 和 endpoint。也就是说，backend 内部
定位信息没有进入 Replica descriptor。

## 5. `LOCAL_DISK` 写路径：Memory 下沉到 SSD

### 5.1 两种入队时机

Master 支持两种 offload 时机：

1. `enable_offload=true, offload_on_evict=false`：`PutEnd` 在 MEMORY Replica
   完成后立即调用 `PushOffloadingQueue`。
2. `enable_offload=true, offload_on_evict=true`：`PutEnd` 不入队，等
   `BatchEvict` 遇到内存压力时才入队。

入队时 Master 会增加源 MEMORY Replica 的 `refcnt`，并写入
`TenantState::offloading_tasks`。这样 SSD 写完成前，承载源字节的内存 extent 不能
被回收。offload-on-evict 模式下，其余未被 pin 的冗余 MEMORY Replica 可以先删，
被选作源的 Replica 要等 offload 完成并释放 refcount 后再由后续驱逐周期回收。

### 5.2 holder 心跳驱动实际 I/O

```text
Master::PushOffloadingQueue
  -> LocalDiskSegment::offloading_objects
  -> FileStorage::Heartbeat
  -> Client::OffloadObjectHeartbeat
  <- vector<OffloadTaskItem>
  -> FileStorage::OffloadObjects
       -> BatchQuerySegmentSlices          locate holder MEMORY Replica
       -> device Slice? D2H to PinnedBufferPool
       -> StorageBackendInterface::BatchOffload(host slices)
       -> Client::NotifyOffloadSuccess
  -> Master::NotifyOffloadSuccess
       -> add COMPLETE LOCAL_DISK Replica
       -> dec source MEMORY refcnt
       -> erase offloading task
```

决定当前路径不是 GDS 的关键代码在 `FileStorage::OffloadObjects`：每个 Slice 先用
accelerator registry 判断是否为 device pointer；若是，就从 `PinnedBufferPool`
取 Host buffer 并执行 `kDeviceToHost` copy，随后 `BatchOffload` 只接收
`host_batch_object`。

发布顺序是合理的：磁盘 backend 成功写入后 complete handler 才调用
`NotifyOffloadSuccess`，Master 随后增加 `COMPLETE LOCAL_DISK` Replica。失败项以
`data_size=-1` 作为 NACK，让 Master 清理 task 并释放源 refcount。

## 6. `LOCAL_DISK` 读路径：SSD 命中如何到达请求端

```text
requester RealClient
  -> Client::Query / Master::GetReplicaList
  <- COMPLETE LOCAL_DISK descriptor + lease
  -> ClientRequester::batch_get_offload_object(holder RPC endpoint)

holder RealClient::batch_get_offload_object
  -> FileStorage::BatchGet
       -> AllocateBatch in registered Host ClientBuffer
       -> StorageBackend::BatchLoad        SSD -> holder Host
  <- batch_id + Host pointers + TE endpoint

requester
  -> Client::BatchGetOffloadObject
  -> TransferSubmitter::submit_batch_get_offload_object
       one TE request per destination Slice
       holder Host -> requester Host/GPU
  -> release holder batch_id
```

这一链路允许 requester 把用户提供的 GPU slices 直接作为最后一跳目的地址，所以
`holder Host -> requester GPU` 可以使用 GPUDirect RDMA；但完整数据流仍是：

```text
SSD -> holder Host ClientBuffer -> network transport -> requester GPU
```

因此这里的“zero-copy”最多描述最后一跳没有 requester-side Host bounce，不能描述
SSD 到 GPU 的端到端 direct path。

`DISK` 走的是另一条分支：`TransferSubmitter::submitFileReadOperation` 把任务交给
`FilereadWorkerPool`，worker 调用 `StorageBackend::LoadObject`。它同样不经过 TENT
FileSegment。`NOF_SSD` 则由 `submitSpdkNofOperation` 进入 SPDK，不应称为 GDS。

## 7. promotion-on-hit：L4 回升到共享内存

当 `GetReplicaList` 发现对象没有 MEMORY Replica、但存在 `LOCAL_DISK` Replica，
并且 `promotion_on_hit` 已启用时，会在释放只读 metadata accessor 后调用
`TryPushPromotionQueue`。入队前依次经过：

- Count-Min Sketch 频率门槛；
- 全局 DRAM high watermark 门槛；
- 已有 MEMORY / 已在途任务去重；
- 集群级 in-flight soft cap；
- 源 `LOCAL_DISK` Replica refcount pin。

holder 后续在 `FileStorage::ProcessPromotionTasks` 中执行：

```text
PromotionObjectHeartbeat
  -> PromotionAllocStart
       Master allocates PROCESSING MEMORY Replica
  -> AllocateBatch + BatchLoad
       SSD -> holder Host staging
  -> PromotionWrite
       holder Host -> target MEMORY Replica via memcpy/TE
  -> NotifyPromotionSuccess
       PROCESSING -> COMPLETE
       release source refcount/quota/task slot
```

任何 `PromotionAllocStart` 之后的失败都会尽力调用 `NotifyPromotionFailure`，立即删除
staged Replica、释放 quota 和 in-flight slot；后台 reaper 是最后保障。这里再次证明
promotion 的当前数据源是 Host staging，而不是 TENT GDS。

还要注意：promotion 为后续请求建立 MEMORY Replica，不会替代触发本次 promotion
的磁盘 Get。本次 Get 已经拿到旧的 Replica list，并沿当前 `LOCAL_DISK` 路径完成。

## 8. 四种磁盘 backend 对 GDS 的意义

| backend | 物理布局 | 当前读写对象 | 对直通的影响 |
|---------|----------|--------------|--------------|
| File-per-key | protobuf `KVEntry`，每 key 一个文件 | 中间 `std::string` | 数据需解析，不能把整个文件直接当 raw value |
| Bucket | 多 key 聚合，metadata 保存 offset/key size/data size | Host iovec / aligned bounce | 可定位 value，但对象边界和对齐需继续核实 |
| OffsetAllocator | 单一 data file，record v3：header + key + padding + value | Host iovec | value 布局显式为未来 DMA/GDS 做 4 KiB 对齐准备 |
| Distributed | 通过 `FileSystemAdapter`，当前可选 HF3FS | backend buffer | 是否能 GDS 取决于文件系统呈现和 adapter 能力 |

`OffsetAllocatorStorageBackend::RecordHeader` 的注释直接说明 v3 布局面向未来 DMA
writer；`ValueOffsetInRecord` 把 value region 对齐到 4 KiB，未带 CRC 的 record 还可
依靠 checkpoint sequence guard 恢复。这个设计是接入 GDS 的重要准备，但还不是
接入完成的证据：当前 `BatchOffload` 仍对 Host slices 计算 CRC，并用
`vector_write` 写入；`BatchLoad` 仍先读 header/key，再用 `vector_read` 写 Host
destination。

若未来让 value 直接 DMA，需要同时决定：header/key 由 CPU 写还是合并写、CRC 如何
生成或禁用、何时 fsync/checkpoint、失败 extent 如何回收，以及 Master 何时发布
Replica。只替换 value 的 `vector_read/write` 不足以保持现有恢复语义。

## 9. TENT GDS 的独立调用链

### 9.1 FileSegment 建立

调用 `openSegment("file:///path/to/data")` 后，`SegmentManager::makeFileRemote` 会：

1. 去掉 `file://` 前缀并拼接可选 base path；
2. 对路径执行 `stat`，只接受当前进程可见的普通文件；
3. 构造 `SegmentType::File`；
4. 保存一个 `FileBufferDesc{path, st_size, 0}`。

因此 `file://` 不是远端 holder URI。NVMe、NFS 或 DFS 必须先以当前进程可访问的
文件路径呈现，才能进入此路径。

### 9.2 选择和启用

构建阶段只有在 `USE_CUDA`、CUDA Toolkit、`libcufile` 和 `cufile.h` 都存在时才
定义 `USE_GDS`。运行阶段还必须在 TENT config 中设置：

```json
{
  "transports": {
    "gds": { "enable": true }
  }
}
```

配置可通过 `MC_TENT_CONF` 指向 JSON 文件或直接传 JSON 字符串。若没有显式配置，
`transports/gds/enable` 默认是 `false`。安装 CUDA/cuFile 不代表 GDS transport
已经被装载。

默认 File policy 的候选顺序是 `GDS -> IOURING`。生产策略还应把
`local_memory` 限定为 `cuda`：selector 的抽象 GPU 类型比当前 NVIDIA cuFile
实现更宽，不能只凭 `gpu_to_file` capability 判断异构设备兼容。

### 9.3 cuFile 执行

```text
local CUDA pointer
  -> registerLocalMemory
  -> GdsTransport::addMemoryBuffer
  -> cuFileBufRegister

Request{READ/WRITE, source=local_ptr,
        target_id=file_segment, target_offset, length}
  -> resolveTransport
  -> GdsTransport::allocateSubBatch
  -> GdsFileContext(open O_RDWR|O_DIRECT + cuFileHandleRegister)
  -> split request into <= 16 MiB CUfileIOParams_t
  -> cuFileBatchIOSubmit
  -> cuFileBatchIOGetStatus
  -> aggregate bytes/status for the public task
```

16 MiB 是 GDS transport 内部 I/O 分片，不是 Store object 或 KV page。每个
SubBatch 使用可复用的 `CUfileBatchHandle_t`；默认 `io_batch_depth` 为 32。

当前文件 context 使用 `O_RDWR | O_DIRECT`，所以只读业务也需要文件具备读写权限。
CUDA buffer 的显式 `cuFileBufRegister` 是稳态快路径与生命周期契约，但 selector
不会把“已注册”当作硬条件；未注册 buffer 即使 API 成功，也需要额外确认是否走了
理想 direct path 或 compatibility path。

## 10. 为什么 `MC_USE_TENT` 仍不会得到 Store GDS

经典 `TransferEngine` 门面检测到 `MC_USE_TENT` 后，会把内存注册、Segment 打开和
`TransferRequest` 提交转发到 `tent::TransferEngine`。这能让 Store 的 MEMORY
Replica 数据面复用 TENT，但磁盘分支在到达门面之前已经被分流：

```text
Replica::MEMORY
  -> TransferSubmitter::submitTransferEngineOperation
  -> TransferEngine facade
  -> classic TE or TENT

Replica::DISK
  -> TransferSubmitter::submitFileReadOperation
  -> FilereadWorkerPool

Replica::LOCAL_DISK
  -> holder FileStorage::BatchGet
  -> Host ClientBuffer
  -> TransferEngine facade only for the second hop

Replica::NOF_SSD
  -> SPDK worker
```

同时，当前 Store 源码不会生成 `file://` endpoint。因此仅设置 `MC_USE_TENT` 和
`transports.gds.enable=true`，不会把已有 SSD offload 变成 GDS。

## 11. Store 与 TENT 之间缺少的桥

一条可生产的 `L4 -> GDS -> GPU` 路径至少要补齐下面六组契约。

### 11.1 寻址

- Replica 必须能稳定导出 `file identity/path + value offset + length`。
- Bucket/OffsetAllocator 的内部 metadata 不能只留在 holder 进程。
- requester 必须能访问同一文件；holder 私有 NVMe 不能直接伪装成本地
  `file://`。

### 11.2 布局与对齐

- file offset、GPU address 和 length 要满足 direct I/O/cuFile 约束；尾部 padding
  不能覆盖相邻 object。
- File-per-key 的序列化包、Bucket 的 key 前缀和 OffsetAllocator 的 record header
  都要求准确定位 raw value，而不是从文件 offset 0 读取整个 record。

### 11.3 所有权

- Get lease 和 Replica refcount 必须覆盖整个 TENT batch。
- GPU page、CUDA registration 和 file handle 在 completion 前不能释放。
- SSD eviction、RemoveAll、extent reuse 和文件 GC 必须等待在途读写结束。

### 11.4 提交与可见性

- 写路径先创建不可见/PROCESSING 的 L4 extent。
- 数据、header、校验与 durability 条件全部满足后，才把 Replica 置为 COMPLETE。
- 任何失败都要能撤销 metadata、释放 extent，或留下可由 recovery 明确识别的记录。

### 11.5 路径选择

- `FOREGROUND_GET` 且 GPU page 已确定：优先 direct `L4 -> L1`。
- `BACKGROUND_PREFETCH`：可能更适合先落 Host L2，避免过早占用 HBM。
- `promotion_on_hit`：目标是 Store MEMORY Replica，不等同于服务本次请求。
- GDS 不可用时保留 io_uring/Host staging/holder RPC/TE fallback。

### 11.6 可观测性

每次请求至少记录：object key/replica、intent、文件 offset/length、源/目标 memory
type、候选和实际 transport、buffer registration、submit/completion、fallback 原因、
各阶段耗时与实际字节数。否则无法区分 direct、cuFile compatibility 和 Host staged
三种路径。

## 12. 推荐的最小落地顺序

1. **先固定正确性基线。** 用现有 `LOCAL_DISK -> Host -> GPU` 路径记录命中、
   字节数、TTFT、CPU copy 和 Host bandwidth。
2. **先选一种物理布局。** 优先验证 OffsetAllocator v3 的单 data file 和 value
   offset，不要一开始同时兼容 File-per-key、Bucket 和 Distributed backend。
3. **扩展 descriptor，但不改变 policy。** 让 Store 能在 lease/refcount 保护下导出
   只读 FileSegment 所需的稳定 extent。
4. **只接前台 read fast path。** 已分配、已注册的 CUDA Slice 走 TENT GDS；其余
   请求继续走旧路径，并逐字节比对结果。
5. **补全失败与回收。** 覆盖 file open/register、batch capacity、submit、poll、
   timeout、Remove/evict 并发和 fallback。
6. **最后接 write 与 promotion。** 写路径涉及 CRC、checkpoint、原子发布和失败
   extent，风险高于只读 fast path。

## 13. 测试与验证入口

现有测试可分为三组：

- Store 控制面：`offload_on_evict_test.cpp`、`promotion_on_hit_test.cpp`、
  `master_service_ssd_test.cpp`。
- holder 数据面：`file_storage_test.cpp`、`file_storage_promotion_test.cpp`、
  `ssd_metrics_test.cpp` 和各 backend 测试。
- TENT 选择：`tent/tests/transport_selector_test.cpp` 覆盖 File Segment 的
  DRAM/GPU capability、policy、hint、intent 和 fallback index。

当前 `tent/tests` 未见独立的 cuFile 硬件 E2E 测试。因此“编译成功”和“selector
选中 GDS”只能证明软件路径可达，不能证明没有 Host bounce。新增集成测试应至少
覆盖：

| 场景 | 必须验证 |
|------|----------|
| 冷读 direct | value 字节正确、目标 GPU page 可直接消费、实际 transport=GDS |
| 未对齐对象 | 明确拒绝或 staged fallback，不能静默越界 |
| GDS 初始化/提交失败 | Replica 仍有效、fallback 可解释、无 batch/registration 泄漏 |
| 读与 SSD eviction 并发 | extent 在 completion 前不复用 |
| RemoveAll 与在途读并发 | 文件/handle 生命周期安全，结果或错误确定 |
| promotion 与普通 Get 并发 | 去重、refcount、PROCESSING/COMPLETE 状态正确 |
| 重启恢复 | checkpoint 前后 record、CRC/seq guard 和 Master ScanMeta 一致 |

硬件验证不能只看吞吐。还应同时观察 CPU 使用率、Host memory bandwidth、NVMe
带宽、GPU/SSD PCIe 流量、batch depth 和 cuFile 日志，确认被测请求确实没有经过
Host data buffer。

## 14. 阅读后的判断清单

看到任何“Mooncake 已支持 L4 GDS”的说法时，逐项追问：

1. 说的是 `DISK`、`LOCAL_DISK`、`NOF_SSD`，还是 TENT FileSegment？
2. 是 Store 对象生命周期，还是单独的文件传输 microbenchmark？
3. SSD 数据是否先进入 holder/requester Host buffer？
4. Store Replica 是否保存了可供 TENT 使用的稳定 file offset？
5. GPU page 和 SSD extent 是否由同一 completion 生命周期保护？
6. 写完成前 Replica 是否保持不可见？
7. GDS 失败能否安全退回旧路径，还是只在 completion failure 时尝试下一 transport？
8. 证据能否区分 direct、compatibility 和 staged path？

只要第 3～6 项尚未闭合，就应把能力准确描述为：**Store 有 SSD L4，TENT 有
GDS 数据面基础，但 Store 尚未形成端到端 GDS L4 默认路径。**
