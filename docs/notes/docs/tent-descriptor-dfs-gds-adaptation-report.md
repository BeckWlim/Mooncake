# TENT descriptor-based DFS 与 GDS 适配报告

> 源码基线：Git HEAD `b97f13cd` 及 2026-08-22 当前工作区。
> 文档范围：Store DFS Replica、TENT FileSegment、文件资源解析和 GDS 数据路径。

## 1. 范围与结论

本报告采用以下研发优先级：

1. 在 TENT 中建立 typed file resource descriptor；
2. 建立 DFS descriptor resolver 和进程内 handle 生命周期；
3. 让 GDS、io_uring 和后续启用的 buffered I/O 共享解析结果；
4. 接入只读 `DFS -> GPU` 数据路径；
5. 接入写入、原子发布和恢复；
6. `LOCAL_DISK` 在后续阶段复用同一基础设施。

当前源码已经包含两部分基础能力：

- Store 管理 `DISK`、`LOCAL_DISK` 等 Replica 的状态、lease 和读取选择；
- TENT 管理 FileSegment，并提供 GDS 与 io_uring transport。

两部分之间的接口仍以路径和 holder endpoint 为主。适配工作的核心是建立以下链路：

```text
Store DFS Replica descriptor
  -> TENT FileSegment descriptor
  -> provider resolver
  -> requester-local owned handle
  -> transport selection
       -> GDS
       -> io_uring
       -> buffered/provider-native I/O
  -> transfer completion
```

本文中的 descriptor 是可序列化的资源描述。Linux fd、`CUfileHandle_t` 和 provider
运行时指针属于进程内 handle，由 resolver 创建和管理。

## 2. 当前源码架构

### 2.1 Store Replica 读取顺序

`mooncake-store/include/replica_selection.h` 声明的基础读取顺序为：

```text
local MEMORY
  -> local NOF_SSD
  -> remote MEMORY
  -> remote NOF_SSD
  -> LOCAL_DISK
  -> DISK
```

`SelectBestReplica` 的实现保证 MEMORY/NoF 位于文件副本之前，并保证
`LOCAL_DISK` 覆盖 `DISK`。local MEMORY 与 local NoF 同时存在时，循环在遇到第一个
本地候选后返回，二者的实际顺序受 Master 返回顺序影响。

该读取策略将文件层置于 MEMORY/NoF 之后。`LOCAL_DISK` 专用 GDS 优化因此列入后续
阶段；当前工作集中于可复用的 DFS descriptor 和 TENT 文件数据面。

### 2.2 `GetReplicaList` 的职责

`MasterService::GetReplicaList` 位于
`mooncake-store/src/master_service.cpp`。该方法执行以下操作：

1. 按 tenant 和 key 定位对象元数据；
2. 筛选 `ReplicaStatus::COMPLETE` 的 Replica；
3. 更新读取指标；
4. 刷新对象或对象组 lease；
5. 在满足条件时提交 promotion-on-hit 任务；
6. 返回 Replica descriptor 和 lease TTL。

实际副本选择和数据传输位于客户端。`DetermineReplicaWriteMode` 服务于 Put 的副本
提交语义，与 `GetReplicaList` 查询路径相互独立。

### 2.3 Store 的两类文件 Replica

`mooncake-store/include/replica.h` 定义：

```cpp
struct DiskDescriptor {
    std::string file_path{};
    uint64_t object_size = 0;
};

struct LocalDiskDescriptor {
    UUID client_id;
    uint64_t object_size = 0;
    std::string transport_endpoint;
};
```

两者的寻址语义不同：

| Replica | 控制面地址 | 当前读取路径 |
|---------|------------|--------------|
| `DISK` | requester 可见的文件路径 | `FilereadWorkerPool` |
| `LOCAL_DISK` | holder client 和 RPC endpoint | holder 读取到 Host buffer，再经 TE 回传 |

`TransferSubmitter::submit` 在
`mooncake-store/src/transfer_task.cpp` 中按 Replica 类型分流。MEMORY 进入 Transfer
Engine，NoF 进入 SPDK 路径，文件 Replica 进入 file-read 路径。

传统 `DISK` 写入由 `Client::PutToLocalFile` 执行。设备 Slice 先复制到 pinned Host
buffer，再由 `StorageBackend::StoreObject` 写入文件。该路径已经具备 requester 侧
文件寻址语义，适合作为 Store 接入 typed DFS descriptor 的第一入口。

### 2.4 DistributedStorageBackend

`mooncake-store/include/storage/distributed/fs_adapter.h` 定义
`FileSystemAdapter`，用于抽象 3FS、CephFS、JuiceFS 等 DFS 的文件操作。当前工厂实现
支持 HF3FS。

`DistributedStorageBackend` 使用以下对象布局：

```text
root_dir / hash_bucket / escaped_key
```

每个文件保存一个对象的 raw value，value 从 offset 0 开始。主要调用关系为：

```text
BatchOffload
  -> FileSystemAdapter::VectorWriteFile

BatchLoad
  -> FileSystemAdapter::ReadFile
```

offload 完成后，`FileStorage` 将 `StorageObjectMetadata` 和 holder RPC endpoint 上报给
Master。`MasterService::NotifyOffloadSuccess` 创建 `LocalDiskReplicaData`。DFS 对象
identity 当前保留在 holder/backend 内部，requester 接收到的是 holder route。

因此，distributed backend 在物理介质上属于 DFS，在 Store 控制面上仍采用
`LOCAL_DISK` 模型。

### 2.5 HF3FS adapter

`mooncake-store/src/storage/distributed/hf3fs_adapter.cpp` 的读取流程为：

```text
open(path)
  -> hf3fs_reg_fd
  -> hf3fs_prep_io
  -> hf3fs_submit_ios
  -> hf3fs_wait_for_ios
  -> copy from USRBIO iov to destination
  -> hf3fs_dereg_fd
  -> close
```

该实现展示了现有 provider 生命周期：path 在客户端进程内解析为 fd，fd 再注册到
HF3FS runtime。当前数据路径使用 Host iov buffer，并在每次文件 API 调用中完成
handle 创建和销毁。

HF3FS fd 与 cuFile 的兼容性属于部署环境能力。该能力决定 HF3FS resolver 是否公布
GDS capability。

### 2.6 TENT FileSegment

`mooncake-transfer-engine/tent/include/tent/runtime/segment.h` 定义：

```cpp
struct FileBufferDesc {
    std::string path;
    uint64_t length;
    uint64_t offset;
};

struct FileSegmentDesc {
    std::vector<FileBufferDesc> buffers;
};
```

`SegmentManager::makeFileRemote` 位于
`mooncake-transfer-engine/tent/src/runtime/segment_manager.cpp`。它处理
`file://` segment name，执行 path 解析、`stat` 和 FileSegment 构造。

`TransferEngineImpl::openSegment` 接受 segment name 字符串。公开接口中的
`exportLocalSegment` 和 `importRemoteSegment` 当前返回 `NotImplemented`。TENT 尚未提供
structured file descriptor 的公开导入接口。

### 2.7 TENT GDS

`GdsTransport` 位于
`mooncake-transfer-engine/tent/src/transport/gds/gds_transport.cpp`。当前流程为：

```text
FileBufferDesc.path
  -> open(path, O_RDWR | O_DIRECT)
  -> CU_FILE_HANDLE_TYPE_OPAQUE_FD
  -> cuFileHandleRegister
  -> cuFileBatchIOSubmit
  -> cuFileBatchIOGetStatus
```

CUDA buffer 通过 `GdsTransport::addMemoryBuffer` 调用 `cuFileBufRegister`。请求按照
16 MiB 上限拆分为 `CUfileIOParams_t`，SubBatch 使用复用的
`CUfileBatchHandle_t`。

默认 File policy 位于
`mooncake-transfer-engine/tent/src/runtime/transport_selector.cpp`，候选顺序为：

```text
GDS -> IOURING
```

这是 FileSegment 内部的 transport 顺序，与 Store Replica 顺序属于两个独立层次。

### 2.8 File transport 的共同约束

GDS、io_uring 和 BufIO 源码都读取 `FileSegmentDesc::buffers[0].path`，并分别创建文件
context。当前 `TransportType` 和 `loadTransports` 注册 GDS 与 io_uring；BufIO 已包含
实现和构建目标，尚未进入 runtime transport registry。由此形成以下实现特征：

- provider 解析逻辑位于各 transport；
- 同一文件可被多个 transport 分别 open；
- file context cache 以 `SegmentID` 为 key；
- transport 只消费第一个 `FileBufferDesc`；
- GDS 使用 `Request::target_offset` 作为 file offset；
- `FileBufferDesc::offset` 尚未参与 physical offset 计算；
- `SegmentDesc::findBuffer` 仅处理 MemorySegment。

当前 FileSegment 的有效模型是“单文件、单 buffer、resource offset 0”。typed DFS
descriptor 需要补充 file range lookup 和逻辑地址到物理地址的转换。

### 2.9 TENT fallback 行为

`TransferEngineImpl::updateTaskStatusAfterPoll` 在已提交任务返回 `FAILED` 后调用
`resubmitTransferTask`，并按下一 transport priority 重提任务。

`commitPreparedSubmit` 对 `submitTransferTasks` 的非 OK 返回执行以下处理：

```cpp
batch->task_list[task_id].type = UNSPEC;
```

`UNSPEC` 任务不会进入上述 completion-time failover。因此 descriptor resolve、context
构造和 GDS batch submit 阶段的错误需要独立的 preflight 或 submit-time fallback
机制。

### 2.10 TENT 当前组件关系

从 TENT 源码可以归纳出以下组件结构：

```text
Public API
  TransferEngine / C API / Python binding
        |
        v
TransferEngineImpl
  +-- SegmentManager -------- SegmentDesc cache / metastore
  +-- TransportSelector ----- policy / memory type / transport priority
  +-- Admission + QoS ------- queue / priority / deadline metadata
  +-- Batch runtime --------- prepare / commit / poll / failover
  +-- Transport registry
        +-- RDMA / TCP / SHM / NVLink / MNNVL
        +-- GDS / io_uring
        +-- BufIO implementation (not registered in runtime)
        +-- accelerator-specific transports
```

FileSegment 复用了 TENT 的 Batch、Request、selector、QoS 和 completion 模型。文件
资源的描述与解析目前直接分布在 SegmentManager 和各 file transport 中。descriptor-
based DFS 的主要架构变化位于 SegmentManager 与 file transport 之间，TransferEngine
公共传输模型保持稳定。

### 2.11 当前能力矩阵

| 能力 | 当前状态 | 源码边界 |
|------|----------|----------|
| MemorySegment metadata | 已实现 | `SegmentDesc`、SegmentRegistry、metastore |
| FileSegment metadata | 已实现，path-only | `FileBufferDesc{path,length,offset}` |
| `file://` 本地文件打开 | 已实现 | `SegmentManager::makeFileRemote` |
| Structured file descriptor import | 接口预留，未实现 | `importRemoteSegment` |
| 多 FileBuffer 描述 | 类型已表达 | transport 只消费 `buffers[0]` |
| File extent range lookup | 未实现 | `findBuffer` 仅支持 MemorySegment |
| GDS batch I/O | 已实现 | `GdsTransport` |
| CUDA buffer registration | 已实现 | `cuFileBufRegister/Deregister` |
| io_uring file I/O | 已实现 | `IOUringTransport` |
| Host-staged CUDA fallback | io_uring 已实现 | CUDA 分支使用 aligned Host buffer |
| BufIO transport | 实现与构建目标已存在，runtime 未注册 | `TransportType` 和 `loadTransports` 无 BufIO |
| Provider resolver | 未实现 | transport 直接 `open(path)` |
| Per-resource capability | 未实现 | selector 只接收 segment/memory 属性 |
| Completion-time failover | 已实现 | `resubmitTransferTask` |
| Submit-time fallback | 未实现 | submit error 将 task 置为 `UNSPEC` |
| Segment descriptor refresh | 已实现 | SegmentManager cache invalidation |
| File context generation refresh | 未实现 | context map 以 `SegmentID` 为 key |
| Store MEMORY -> TENT | 已接通 | TransferEngine facade |
| Store file Replica -> TENT | 未接通 | file Replica 提前分流到 file worker/holder |

该矩阵界定了后续路线：TENT 的调度、Batch 和 GDS 执行能力可以复用；FileSegment
descriptor、resource resolver、capability selection 和 Store file bridge 构成主要新增
模块。

## 3. 适配边界与目标架构

源码现状与目标能力之间包含五个接口边界：

| 边界 | 当前输入 | 目标输入 |
|------|----------|----------|
| Store Replica | path 或 holder endpoint | typed DFS resource locator |
| TENT FileSegment | path、length、offset | versioned resource descriptor 与 extent |
| 文件资源解析 | transport 内 `open(path)` | provider resolver |
| transport selection | SegmentType 和 memory type | resource capability + memory type |
| context cache | SegmentID | resource identity + generation + access mode |

适配设计围绕这些边界展开，Store lease、Replica 状态和 TENT Request/Batch 模型保持
现有职责。

### 3.1 目标分层

```text
Store control plane
  ObjectMetadata / Replica / lease / generation
                 |
                 | typed DFS locator
                 v
TENT segment plane
  FileSegmentDesc / extent mapping / descriptor serialization
                 |
                 v
File resource plane
  ResolverRegistry / ResolvedFileResource / context cache
                 |
                 | capability snapshot
                 v
TENT scheduling plane
  TransportSelector / policy / hint / intent / fallback order
                 |
                 v
Transport data plane
  GDS | io_uring | buffered I/O | provider-native transport
```

各层职责为：

| 层 | 职责 |
|----|------|
| Store control plane | 对象身份、副本状态、lease、删除和发布 |
| TENT segment plane | 可序列化资源描述和 segment 地址空间 |
| File resource plane | provider 解析、handle 所有权、capability 和 cache |
| TENT scheduling plane | transport 排序、QoS、hint 和 fallback |
| Transport data plane | 数据提交、poll、completion 和字节统计 |

### 3.2 控制面与数据面时序

```text
Control path
  Store Master
    -> GetReplicaListResponse
    -> client receives typed DFS locator + lease TTL
    -> TENT imports FileSegmentDesc
    -> resolver publishes resource capability snapshot

Data path
  client slices
    -> TENT READ requests
    -> selector chooses GDS/io_uring/provider transport
    -> resolved handle remains pinned through completion
    -> client validates status and transferred bytes
```

descriptor serialization、resolver handle 和 transfer task 分属三个生命周期：

| 对象 | 生命周期 |
|------|----------|
| Serialized descriptor | Replica/segment metadata 生命周期 |
| Resolved resource | resource generation 和 context cache 生命周期 |
| Transfer reference | submit 到 completion 的任务生命周期 |

## 4. 架构方案评估

### 4.1 方案 A：扩展 URI/path scheme

方案形式：

```text
file:///path
hf3fs://cluster/path
otherdfs://authority/object
```

SegmentManager 按 scheme 解析资源，各 transport 继续持有文件 context。

| 维度 | 评估 |
|------|------|
| 改动范围 | 小 |
| `file://` 兼容 | 直接兼容 |
| provider 扩展 | scheme parser 随 provider 增长 |
| capability 表达 | 需要附加规则或配置 |
| generation | URI 字段扩展后可表达 |
| 凭证 | URI 与本地配置需要额外约定 |
| handle 复用 | transport 间仍分离 |
| 适用阶段 | 原型和单 provider 验证 |

该方案保留 path-oriented 架构，适配成本低。随着 provider、凭证和 transport 组合增长，
解析与生命周期逻辑继续分布在各 transport。

### 4.2 方案 B：Typed descriptor + 共享 resolver

方案形式：

```text
FileResourceLocator
  -> ResolverRegistry
  -> ResolvedFileResource
  -> GDS/io_uring/buffered I/O
```

| 维度 | 评估 |
|------|------|
| 改动范围 | 中等，集中在 Segment、resolver 和 file transport |
| `file://` 兼容 | 通过 POSIX locator 保留 |
| provider 扩展 | 新增 resolver |
| capability 表达 | resolver 直接发布 |
| generation | descriptor 与 cache key 原生表达 |
| 凭证 | credential profile 与本地 resolver 配置分离 |
| handle 复用 | file transport 共享 resolved resource |
| 适用阶段 | 多 provider、生产生命周期、Store 集成 |

该方案与 TENT 当前 Segment/Transport 分层一致。descriptor 负责资源描述，resolver
负责本地资源构造，selector 负责 transport 选择。

### 4.3 方案 C：DFS provider-native transport

为特定 DFS 增加独立 transport：

```text
HF3FS descriptor
  -> HF3FS transport
  -> provider GPU/Host I/O API
```

| 维度 | 评估 |
|------|------|
| 改动范围 | 中等至高 |
| provider 性能能力 | 可完整使用 |
| GDS 复用 | 取决于 provider API |
| selector 集成 | 与现有 transport registry 一致 |
| 多 provider 成本 | 每个 provider 维护 transport |
| handle 生命周期 | transport 或共享 resolver 管理 |
| 适用条件 | provider 提供独立于 POSIX/cuFile 的 GPU direct API |

该方案与方案 B 可以组合：typed descriptor 和 resolver 提供统一控制面，provider-native
transport 提供数据面。若目标 DFS 的 fd 兼容 cuFile，GDS transport 已覆盖主要数据
路径；若 provider 使用专用 GPU I/O API，独立 transport 具有明确价值。

### 4.4 方案 D：Store 内直接实现 DFS/GDS

Store 的 file worker 或 distributed backend 直接调用 cuFile/provider API，TENT 仅处理
MEMORY Replica。

| 维度 | 评估 |
|------|------|
| Store 接入速度 | 单路径实现较快 |
| TENT selector/QoS | 无法直接复用 |
| transport fallback | Store 侧重新实现 |
| CUDA registration | Store 侧管理 |
| 多 provider 复用 | 较低 |
| 适用阶段 | 性能基线或短期实验 |

该方案形成 Store 与 TENT 两套文件传输控制逻辑。它适合作为实验对照，不作为长期
架构主线。

### 4.5 方案 E：`LOCAL_DISK` holder 内 GDS

holder 读取本地 SSD 时直接写入 holder GPU 或使用 GDS staging，再通过网络发送给
requester。

| 维度 | 评估 |
|------|------|
| 影响范围 | `LOCAL_DISK` holder 路径 |
| requester Host bounce | 取决于第二跳目标 |
| 跨节点 direct DFS | 不提供 |
| resolver 复用 | 可复用方案 B |
| Store 优先级 | 文件层低优先级路径 |
| 适用阶段 | 通用 resolver 稳定后的局部优化 |

### 4.6 路线选择

主路线采用方案 B，并为方案 C 保留 transport 扩展点：

```text
Typed descriptor + shared resolver
  + cuFile-compatible resource -> GDS
  + POSIX async resource        -> io_uring
  + provider GPU API            -> provider-native transport
  + Host-only resource          -> buffered/staged path
```

方案 A 用于 Phase 0 原型时，可直接映射到方案 B 的 POSIX/provider locator。方案 D 用作
性能对照。方案 E 位于通用基础设施完成后的优化阶段。

### 4.7 基于 provider 能力的规划分支

| Provider 能力 | 实施路线 |
|---------------|----------|
| fd 可由 cuFile 注册并支持 direct I/O | descriptor + resolver + GDS |
| fd 支持 POSIX/io_uring，不支持 cuFile | descriptor + resolver + io_uring/staging |
| 提供专用 GPU direct API | descriptor + resolver + provider-native transport |
| 仅提供 Host I/O API | descriptor + provider resolver + Host staged path |
| resource identity 仅 holder 可解析 | 保留 holder route，复用 resolver 于 holder 内 |

该分支表将 provider 验证结果映射为确定的实现路线，TENT 上层 descriptor 与 Store
Replica 协议保持一致。

## 5. Descriptor 模型

### 5.1 数据结构

建议将资源 identity 与 extent 映射分离：

```cpp
enum class FileResourceKind {
    POSIX_PATH,
    DFS_OPAQUE,
};

struct PosixPathResource {
    std::string path;
};

struct DfsOpaqueResource {
    std::string provider;
    std::string opaque_locator;
    std::string credential_profile;
};

using FileResourceLocator =
    std::variant<PosixPathResource, DfsOpaqueResource>;

struct FileBufferDesc {
    FileResourceLocator resource;
    uint64_t segment_offset;
    uint64_t resource_offset;
    uint64_t length;
    uint64_t generation;
};
```

字段语义为：

| 字段 | 语义 |
|------|------|
| `resource` | 可序列化的文件或 DFS 对象 identity |
| `segment_offset` | extent 在 TENT segment 地址空间中的起点 |
| `resource_offset` | extent 在底层资源中的起点 |
| `length` | extent 可访问长度 |
| `generation` | 对象版本与 handle cache 失效依据 |

地址转换公式为：

```text
physical_offset =
    resource_offset + (request.target_offset - segment_offset)
```

range lookup 负责验证 request 完整落在一个 extent 内，并处理整数溢出。跨 extent
请求由 runtime 层拆分为多个 transport request。

### 5.2 序列化兼容

`FileBufferDesc` 已进入 TENT JSON metadata。兼容策略包括：

1. descriptor 增加 schema version；
2. `from_json` 将旧 `{path,length,offset}` 映射为 `POSIX_PATH`；
3. 旧 `offset` 映射为 `segment_offset`，`resource_offset` 取 0；
4. 新 writer 在滚动升级窗口内保留 legacy path 字段；
5. 未识别 schema version 返回明确的 metadata error。

Store `DiskDescriptor` 的 RPC 和 snapshot 采用同一原则：保留 legacy `file_path`，新增
optional typed locator，新客户端优先读取 typed locator。

### 5.3 凭证

descriptor 保存 provider、资源 identity 和 credential profile。实际 secret 由
requester 本地 resolver 配置或凭证服务提供。日志使用 provider、resource hash 和
generation 标识资源。

## 6. Resolver 模型

### 6.1 接口职责

resolver registry 按 resource kind 和 provider 选择实现：

```text
FileResourceLocator
  -> FileResourceResolver
  -> ResolvedFileResource
```

`ResolvedFileResource` 表达以下能力：

- owned fd 或 provider handle；
- resource identity 与 generation；
- resolved length；
- read/write access mode；
- direct-I/O alignment；
- GDS capability；
- io_uring capability；
- buffered/provider-native I/O capability；
- RAII owner。

provider-specific handle 通过窄能力接口交给 transport。GDS consumer 获取可构造
`CUfileDescr_t` 的资源；io_uring consumer 获取兼容 fd；provider-native transport
获取对应 provider handle。

### 6.2 生命周期与缓存

context cache key 定义为：

```text
provider + resource identity + generation + access mode
```

Segment close、metadata refresh、generation 更新、凭证过期和 resolver stale-handle
状态触发 context 失效。transport task 在 completion 前持有
`ResolvedFileResource` owner。

线程本地缓存保存与全局 cache generation 对齐的 owning reference 或 weak reference，
避免长期持有旧 SegmentID 对应的 context。

### 6.3 Resolver 类型

第一阶段包含以下 resolver：

| Resolver | 输入 | 输出能力 |
|----------|------|----------|
| POSIX | path locator | fd、io_uring、buffered；环境支持时包含 GDS |
| HF3FS | provider locator | HF3FS-registered handle；GDS capability 由环境 probe 确定 |

后续 DFS provider 通过 resolver registry 扩展，核心 TENT 保持 provider-neutral。

## 7. Transport 适配

### 7.1 Selector

FileSegment 的 selection context 增加 resource capabilities。选择过程为：

```text
local memory type
  + policy
  + transport hint
  + resolved resource capabilities
  -> ordered transport candidates
```

典型结果：

| 本地内存 | 资源能力 | 候选顺序 |
|----------|----------|----------|
| CUDA | GDS + io_uring | GDS -> io_uring |
| CUDA | io_uring only | io_uring |
| Host | io_uring + buffered | io_uring -> buffered |
| 任意 | provider-native only | provider transport |

resolve/preflight 在 segment open/import 或首次 context 构造时完成，并按 generation
缓存结果。

### 7.2 GDS

`GdsFileContext` 改为消费 `ResolvedFileResource`，主要变化包括：

- 从 resolved capability 构造 `CUfileDescr_t`；
- 使用 physical offset；
- 校验 request extent；
- 按 access mode 注册 read-only 或 read-write resource；
- context cache 使用 resource identity 和 generation；
- completion 前保持 file handle 与 CUDA registration 有效；
- 记录 resolve、register、submit 和 completion 指标。

现有 16 MiB I/O 分片和 batch handle pool 可以继续复用。

### 7.3 io_uring 与 buffered I/O

io_uring 和 buffered I/O 使用同一 `ResolvedFileResource`。BufIO 若作为 fallback
进入正式路线，需要先增加 `TransportType`、loader、selector policy 和配置入口。该
结构使各 file transport 复用相同的 resource identity、generation 和 access mode。

io_uring 对 CUDA memory 的现有实现使用 aligned Host buffer 和 platform copy。该路径
标记为 staged I/O；GDS 路径标记为 direct I/O。

### 7.4 Submit-time fallback

descriptor resolve、context register 和 transport submit 属于提交阶段。适配后提供两种
实现方式：

1. 在 commit 前完成 transport preflight，再选择首个可提交候选；
2. 为未产生外部副作用的 submit failure 执行下一候选重提。

GDS read 的首版采用 preflight 方式，覆盖 resolver、fd capability 和 cuFile handle
registration。`cuFileBatchIOSubmit` 的错误进入 submit-time fallback。completion 返回
`FAILED` 时继续使用现有 `resubmitTransferTask`。

## 8. Store 接入

### 8.1 第一入口：`DISK`

requester 可直接解析的 DFS 资源与现有 `DISK` 语义一致。第一阶段扩展：

```text
DiskReplicaData / DiskDescriptor
  + typed FileResourceLocator
  + resource_offset
  + generation
```

客户端读取分流为：

```text
DISK + typed locator + CUDA destination + GDS capability
  -> TENT FileSegment READ

other DISK cases
  -> existing FilereadWorkerPool
```

该方案保留现有 Replica 类型、选择顺序和 legacy path。

### 8.2 DistributedStorageBackend descriptor 导出

`FileSystemAdapter` 增加对象 descriptor 导出能力：

```text
ExportObjectDescriptor(key)
  -> provider
  -> opaque locator
  -> resource offset
  -> length
  -> generation
```

`DistributedStorageBackend::BatchOffload` 在完成回调中返回该 descriptor。
`StorageObjectMetadata` 由 backend 内部 metadata 与 wire-level resource descriptor 两个
类型组成，避免 `bucket_id`、`offset` 和 `transport_endpoint` 继续扩展语义。

Master 将 direct-access DFS 对象保存为 typed `DISK` descriptor。holder RPC 继续服务
legacy client 和 resolver capability 缺失的 client。

当 DFS 在 lease、删除、配额或选择策略上形成独立语义时，可进一步引入专用 `DFS`
Replica type。第一阶段复用 `DISK`，控制 RPC、snapshot 和 switch 分支的改动范围。

### 8.3 Get 生命周期

```text
GetReplicaList grants lease
  -> SelectBestReplica
  -> resolve DFS descriptor
  -> acquire ResolvedFileResource owner
  -> submit TENT READ requests
  -> verify status and transferred bytes
  -> release owner
  -> lease expires according to TTL
```

descriptor generation 与 immutable resource identity负责对象版本一致性；Store lease
负责 Replica 删除和读取生命周期。

### 8.4 Slice 映射

TENT `Request::source` 表示本地 buffer。DFS Get 对每个目标 Slice 构造 READ request：

```text
request.source        = slice.ptr
request.target_id     = file segment
request.target_offset = object_resource_offset + cumulative_slice_offset
request.length        = slice.size
request.opcode        = READ
```

提交前验证所有 Slice 总长度与 `object_size` 一致，并验证每个 request 落在 descriptor
extent 内。

## 9. 实施阶段

### Phase 0：能力基线

- 验证目标 DFS fd 与 cuFile handle registration；
- 验证 read-only、alignment 和 CUDA buffer registration；
- 记录 direct、compatibility 和 staged 路径的可观测信号；
- 固定 descriptor schema 和 generation 语义。

### Phase 1：Typed FileSegment

- 扩展 `FileBufferDesc`；
- 实现 legacy JSON 兼容；
- 实现 file extent lookup 和 physical offset 计算；
- 增加 structured open/import API；
- 添加 descriptor、range 和 serialization 单元测试。

### Phase 2：Resolver

- 实现 resolver registry；
- 实现 POSIX resolver；
- 实现目标 DFS resolver；
- GDS、io_uring 和选定的 buffered I/O 实现使用 resolved resource；
- context cache 加入 identity、generation 和 access mode。

### Phase 3：只读 DFS -> GPU

- 扩展 Store `DISK` descriptor；
- client 构造 TENT FileSegment READ；
- 接入 submit-time 和 completion-time fallback；
- 验证内容、字节数、lease、generation 和并发删除。

### Phase 4：Distributed backend 导出

- `FileSystemAdapter` 导出对象 descriptor；
- offload completion 发布 direct-access DFS Replica；
- requester 直接解析 DFS 资源；
- holder RPC 保留为兼容路径。

### Phase 5：写入与发布

- 创建和预分配 DFS resource；
- GPU -> DFS GDS WRITE；
- 校验、durability 和原子 publish；
- PutRevoke 和失败资源回收；
- restart scan 与 snapshot 恢复。

### Phase 6：`LOCAL_DISK` 复用

- 评估 local backend descriptor 导出；
- 评估 holder 本机 GPU direct path；
- 评估 promotion-on-hit GDS；
- 复用 resolver、context cache 和 transport 实现。

## 10. 首个代码切片

首个变更限定在 TENT 内部：

1. versioned typed `FileBufferDesc`；
2. legacy path serialization compatibility；
3. file extent lookup；
4. POSIX resolver；
5. GDS/io_uring resolved handle 接口；
6. identity + generation context cache；
7. fake resolver 与 selector 测试。

该切片保持 Store API、Replica policy 和 `file://` 行为不变。第二个切片加入目标 DFS
resolver，第三个切片接入 Store `DiskDescriptor`。

## 11. 验证矩阵

### 11.1 软件测试

| 层次 | 场景 |
|------|------|
| descriptor | legacy path、typed path、DFS opaque、未知 version |
| range | 单 extent、多 extent、跨边界、零长度、整数溢出 |
| resolver | provider 匹配、access mode、generation stale、credential profile |
| cache | resolve reuse、close、refresh、generation 更新、并发访问 |
| selector | GDS、io_uring、buffered、provider-native capability |
| fallback | preflight error、submit error、completion error、候选耗尽 |
| Store | legacy DiskDescriptor、typed locator、lease、Replica 选择 |

### 11.2 硬件测试

| 场景 | 验收项 |
|------|--------|
| aligned DFS -> CUDA read | 内容、字节数、GDS transport、Host bounce 指标 |
| 多 Slice read | 累计 file offset 和目标内容 |
| unaligned read | staged fallback 或确定性参数错误 |
| provider GDS capability 缺失 | selector 进入 io_uring/provider fallback |
| generation 更新 | context 刷新并访问新资源 |
| GDS submit error | fallback、batch 回收、handle 回收 |
| GDS completion error | failover、最终状态、实际字节数 |
| Remove/overwrite 并发 | lease 与 generation 一致性 |
| 大对象 | 16 MiB 分片、batch depth 和完整字节数 |
| 滚动升级 | legacy 与 typed descriptor 互操作 |

硬件验收数据包括吞吐、延迟、CPU 使用率、Host memory bandwidth、GPU/SSD PCIe
traffic、cuFile 状态和 TENT transport metrics。

## 12. 外部验证项

源码范围之外需要在目标环境确认：

| 验证项 | 对设计的影响 |
|--------|--------------|
| HF3FS fd 的 cuFile registration 与 direct I/O 能力 | 决定 HF3FS resolver 的 GDS capability |
| DFS object identity 的跨节点稳定性 | 决定 opaque locator schema |
| 同名对象覆盖与 generation 规则 | 决定 context cache 和一致性协议 |
| provider credential 生命周期 | 决定 resolver refresh 与错误分类 |
| direct write durability 语义 | 决定 PutEnd 前的提交条件 |

这些项目影响 provider 实现，不改变 typed descriptor、resolver 和 transport capability
三层结构。

## 13. 可观测性

每次文件传输记录：

- descriptor version、provider、resource identity hash、generation；
- segment offset、resource offset、length；
- source memory type、intent、policy 和 transport hint；
- resolver latency、context cache 命中和 capability；
- transport candidates、selected transport 和 fallback reason；
- CUDA buffer registration；
- submit/completion status、transferred bytes 和 sub-request count；
- direct、compatibility 或 staged path。

credential、capability token 和完整 opaque locator 按敏感信息处理。

## 14. 源码入口

| 主题 | 文件/符号 |
|------|-----------|
| Store Replica 类型 | `mooncake-store/include/replica.h` |
| Store 副本选择 | `mooncake-store/include/replica_selection.h` |
| GetReplicaList | `mooncake-store/src/master_service.cpp` |
| Store file 分流 | `mooncake-store/src/transfer_task.cpp` |
| 传统 DISK 写入 | `mooncake-store/src/client_service.cpp::PutToLocalFile` |
| LOCAL_DISK offload | `mooncake-store/src/file_storage.cpp` |
| distributed backend | `mooncake-store/src/storage/distributed/distributed_storage_backend.cpp` |
| DFS adapter | `mooncake-store/include/storage/distributed/fs_adapter.h` |
| HF3FS adapter | `mooncake-store/src/storage/distributed/hf3fs_adapter.cpp` |
| TENT descriptor | `mooncake-transfer-engine/tent/include/tent/runtime/segment.h` |
| TENT segment manager | `mooncake-transfer-engine/tent/src/runtime/segment_manager.cpp` |
| TENT route/submit/failover | `mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp` |
| TENT selector | `mooncake-transfer-engine/tent/src/runtime/transport_selector.cpp` |
| TENT GDS | `mooncake-transfer-engine/tent/src/transport/gds/gds_transport.cpp` |
| TENT io_uring | `mooncake-transfer-engine/tent/src/transport/io_uring/io_uring_transport.cpp` |
| TENT buffered I/O | `mooncake-transfer-engine/tent/src/transport/bufio/bufio_transport.cpp` |

## 15. 设计决策摘要

| 决策 | 选择 |
|------|------|
| 总体架构 | 方案 B：typed descriptor + shared resolver |
| Provider 特殊数据面 | 满足专用 GPU API 条件时增加方案 C transport |
| 首要适配对象 | TENT FileSegment 与 DFS resolver |
| Store 首个接入类型 | `DISK` |
| descriptor 形式 | versioned typed resource + extent |
| 进程内资源 | resolver-owned handle |
| cache identity | provider + resource identity + generation + access mode |
| 首个数据路径 | read-only DFS -> CUDA |
| fallback | capability preflight + submit/completion failover |
| distributed backend | 导出 direct-access DFS descriptor |
| `LOCAL_DISK` | 后续复用通用基础设施 |
| write path | read path 验收后实施 |
