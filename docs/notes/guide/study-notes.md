# Mooncake 源码阅读地图

> 结构化导航，按模块和层级组织，用于渐进式代码阅读。

---

## 目录

1. [阅读路线总览](#1-阅读路线总览)
2. [分层架构：MasterService / WrappedMasterService / MasterClient](#2-分层架构)
3. [Store 核心流程：Put 全链路](#3-store-核心流程put-全链路)
4. [关键数据结构](#4-关键数据结构)
5. [Transfer Engine 补充](#5-transfer-engine-补充)
6. [HiCache 缓存层级](#6-hicache-缓存层级)
7. [Client 接口层与运行时架构](#7-client-接口层与运行时架构)
8. [从三级缓存瓶颈到 L4：TENT FileSegment 与 GDS](#8-从三级缓存瓶颈到-l4tent-filesegment-与-gds)

---

## 1. 阅读路线总览

### 第一遍：入口与核心抽象（~30 min）

| 优先级 | 文件 | 关注点 |
|--------|------|--------|
| ★★★ | `mooncake-transfer-engine/include/transfer_engine.h` | TransferEngine 公共 API 声明 |
| ★★★ | `mooncake-transfer-engine/include/transfer_metadata.h` | 核心类型：TransferRequest, BatchID, SegmentID |
| ★★☆ | `mooncake-transfer-engine/include/transfer_engine_c.h` | C API（Python FFI 层） |

### 第二遍：Transfer Engine 实现（~1-2 h）

| 优先级 | 文件 | 关注点 |
|--------|------|--------|
| ★★★ | `mooncake-transfer-engine/include/transfer_engine_impl.h` | TransferEngineImpl 类声明 |
| ★★★ | `mooncake-transfer-engine/src/transfer_engine_impl.cpp` | 核心实现：`init()`, `submitTransfer()`, `registerLocalMemory()`（§5） |
| ★★★ | `mooncake-transfer-engine/include/multi_transport.h` | MultiTransport：多协议传输管理（§5.2） |
| ★★☆ | `mooncake-transfer-engine/include/transport/transport.h` | Transport 抽象基类 |
| ★★☆ | `mooncake-transfer-engine/src/transport/rdma_transport/` | RDMA 传输实现（GPUDirect、多网卡聚合） |

### 第三遍：Mooncake Store（按兴趣深入）

| 优先级 | 文件 | 关注点 |
|--------|------|--------|
| ★★★ | `mooncake-store/include/master_service.h` | Master 节点：全局元数据、空间分配、逐出（§2） |
| ★★★ | `mooncake-store/src/master_service.cpp` | `PutStart`（§3.2）、`PutEnd`（§3.4）、`GetReplicaList` |
| ★★★ | `mooncake-store/include/client_service.h` | Client 类：Put/Get 流程编排（§3.1） |
| ★★★ | `mooncake-store/src/client_service.cpp` | `Put()` 完整实现：`PutStart → TransferWrite → PutEnd` |
| ★★☆ | `mooncake-store/src/master_client.cpp` | `MasterClient`：`invoke_rpc` RPC 客户端（§2.3, §2.5） |
| ★★☆ | `mooncake-store/src/rpc_service.cpp` | `WrappedMasterService`：RPC 适配层 + 服务注册（§2.2, §2.5） |
| ★★☆ | `mooncake-store/include/allocator.h` | `AllocatedBuffer::Descriptor` 结构（§4.2） |
| ★★☆ | `mooncake-store/include/replica.h` | `Replica`, `Replica::Descriptor`, `ReplicateConfig`（§4.1） |

### Client 接口层：PyClient → RealClient

建议从 `mooncake-store/include/pyclient.h` 中的 `PyClient` 抽象接口开始，再进入 `real_client.h` 和 `real_client.cpp`。重点是先弄清高层 API、底层 Store Client、SSD Offload 和传输缓冲区之间的边界，详见 §7。

### 第四遍：高层集成

| 优先级 | 文件 | 内容 |
|--------|------|------|
| ★★☆ | `mooncake-ep/include/mooncake_ep_api.cuh` → `src/mooncake_ep_buffer.cpp` | Expert Parallelism API 与 Buffer 实现 |
| ★★☆ | `mooncake-pg/include/mooncake_backend.h` → `src/mooncake_backend.cpp` | torch.distributed Backend 与操作分发 |
| ★☆☆ | `mooncake-integration/store/store_py.cpp` | Python bindings：`RealClient` → 对外暴露 `MooncakeDistributedStore` |

### 核心设计原则

项目核心是数据搬运：Transfer Engine 提供多协议传输层（RDMA/NVLink/TCP），Store/EP/PG 在不同场景下使用此传输能力。建议先理解传输机制（§5），再进入上层模块。

### 术语与章节边界

本文按源码抽象使用以下术语，避免把相邻层次混为一谈：

| 术语 | 本文含义 | 主要章节 |
|------|----------|----------|
| Page / Block | 推理框架的 KVCache 分配与复用单位 | §6 |
| Object / Key | Mooncake Store 的逻辑对象及其名字 | §4、§6 |
| Replica | 一个 Object 的完整物理副本及其介质状态 | §4 |
| Segment / AllocatedBuffer | 可分配存储资源及其中分给 Replica 的区域 | §4 |
| Slice / TransferRequest | 一次调用的数据视图与传输任务 | §3、§4、§5 |
| L1～L4 | 本文采用的 KVCache 软件存储层级，不是 GPU 硬件缓存层级 | §6、§8 |

章节职责遵循“定义只展开一次”：§3 讲 Put 调用链，§4 讲 Store 通用数据模型，§6 讲 SGLang HiCache 的 Page/Hash/命中，§7 讲 RealClient 运行时，§8 讲 TENT FileSegment/GDS。其他章节遇到相同概念时只补充本章特有的边界。

---

## 2. 分层架构

### 架构图

```text
+------------------------------------------------------------------+
| SGLang / Store client process                                    |
| MooncakeDistributedStore -> RealClient -> Client                 |
|                                           +-> MasterClient       |
|                                           +-> TransferEngine     |
+-----------------------------------------------+------------------+
                                                | control-plane RPC
                                                v
+------------------------------------------------------------------+
| mooncake_master process                                          |
| WrappedMasterService -> MasterService                            |
|                         - sharded metadata                       |
|                         - allocation / eviction                  |
|                         - lease / quota / tasks / HA             |
+------------------------------------------------------------------+
```

图中的水平边界是进程边界：`RealClient` 与 `Client` 是同进程函数调用；
`MasterClient` 通过 RPC 访问 Master，而对象字节由 `TransferEngine` 直接搬运。

### 2.1 MasterService — 核心业务逻辑

**位置：** `master_service.h`, `master_service.cpp`

**职责：** 纯业务逻辑，与 RPC/网络无关

| 关注点 | 实现 |
|--------|------|
| 分片元数据 | 1024 个 `MetadataShard`，每个有独立 `SharedMutex` |
| 多级锁 | `AcquireObjectOperationLock`（per-key 互斥）+ `snapshot_mutex_`（shared_lock）+ shard RW lock |
| 空间分配 | `allocation_strategy_->Allocate()` — 支持 random/free_ratio_first/local_first/cxl 等策略 |
| 逐出 | `EvictionThreadFunc()` + `BatchEvict()` — 近似 LRU，尊重 soft_pin/hard_pin/lease |
| 配额 | `ReserveTenantQuota()` / `CommitTenantQuota()` / `AbortTenantQuota()` — 多租户配额管理 |
| 快照 | `MetadataSerializer` + `MasterSnapshotManager` — fork-based COW 快照持久化 |

**关键内部类：**

| 类 | 作用 |
|----|------|
| `ObjectMetadata` | 每个 key 的元数据记录（client_id, size, replicas_, lease_timeout, quota 等） |
| `MetadataShard` | 分片：包含 `tenants → TenantState → metadata map` |
| `MetadataAccessorRW` | RAII 访问器：构造时获取 shard RW 锁 + 自动清理 stale handles |
| `MetadataAccessorRO` | 只读版本：构造时获取 shared_lock |
| `MetadataShardAccessorRW` / `MetadataShardAccessorRO` | 分片级锁访问器 |

### 2.2 WrappedMasterService — RPC 桥梁

**位置：** `mooncake-store/include/rpc_service.h`、`mooncake-store/src/rpc_service.cpp`

**本质：** `MasterService` 的一个薄包装。持有 `master_service_` 成员。

```cpp
class WrappedMasterService {
    MasterService master_service_;  // 持有核心实例
public:
    WrappedMasterService(const WrappedMasterServiceConfig& config, ...);
};

WrappedMasterService::PutStart(...) {
    return execute_rpc("PutStart",
        [&] { return master_service_.PutStart(client_id, key, ...); },
        // ↑ 委托给核心
        [&](auto& timer) { timer.LogRequest(...); },   // 请求日志
        [] { inc_put_start_requests(); },               // 成功计数
        [] { inc_put_start_failures(); }                // 失败计数
    );
}
```

**职责：**
- **Tenant 转换：** 将 RPC 层的字符串 `tenant_id` → 内部 `TenantId` 对象
- **Observability：** `execute_rpc` 模板统一处理耗时日志、请求/失败计数
- **服务注册：** `RegisterRpcService()` 将所有方法注册为 coro_rpc handler
- **部署：** 随 `mooncake_master` 进程启动，监听配置的 RPC 端口

### 2.3 MasterClient — RPC 客户端

**位置：** `master_client.h`, `master_client.cpp`

嵌入在 Client 进程中的 RPC 代理，通过 coro_rpc 协程框架与远端 Master 通信。每个 Client 实例持有一个 `MasterClient` 成员。

**每个 RPC 方法遵循"薄包装 + invoke_rpc"模式**（详见 §2.5.1）：方法声明于 `master_client.h`，实现在 `master_client.cpp`，做少量参数预处理后调用 `invoke_rpc<&WrappedMasterService::MethodName, ReturnType>(args...)`。

**`invoke_rpc` 模板**（`master_client.cpp`）是该层的核心。它封装三个异步层次：连接池管理（`coro_io::client_pools`，负责复用/创建/HA failover）、coro_rpc 协议传输（序列化、TCP 发送、等待响应、反序列化）、`syncAwait` 桥接（将 C++20 协程封装为同步 API）。完整实现分析见 §2.5.2。

### 2.4 三层职责对比

| 层 | 类 | 位置 | 核心关注点 |
|----|-----|------|-----------|
| 核心业务 | `MasterService` | `master_service.cpp` | 正确性：锁、分片、分配策略、逐出、配额 |
| RPC 适配 | `WrappedMasterService` | `rpc_service.cpp` | 服务暴露：tenant 解析、日志、指标、coro_rpc 注册 |
| 远程调用 | `MasterClient` | `master_client.cpp` | 连接管理：连接池、超时、协程异步 I/O、错误转换 |

### 2.5 RPC 通信机制 —— 以 PutStart 为例

**涉及文件：** `master_client.cpp`（PutStart、`invoke_rpc`、`RpcNameTraits`）与 `rpc_service.cpp`（服务注册和 Wrapped 委托）

#### 2.5.1 客户端：MasterClient 方法模式

`MasterClient` 对外暴露的每个 RPC 方法（PutStart、PutEnd、GetReplicaList 等）均遵循同一模式——参数预处理后调用 `invoke_rpc` 模板。

以 `MasterClient::PutStart` 为例：

```cpp
MasterClient::PutStart(const std::string& key,
                       const std::vector<size_t>& slice_lengths,
                       const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::PutStart");
    uint64_t total_slice_length = 0;
    for (const auto& sl : slice_lengths) total_slice_length += sl;

    auto result = invoke_rpc<&WrappedMasterService::PutStart,
                             std::vector<Replica::Descriptor>>(
        client_id_, key, total_slice_length, config, tenant_id_.value());
    return result;
}
```

该方法本身是**普通成员函数**（非内联，非模板），定义在 `.cpp` 文件。其职责仅限于：

1. 参数预处理：将调用方传入的 `vector<size_t> slice_lengths` 汇总为单一 `total_slice_length`。RPC 协议只传输总长度。
2. 调用 `invoke_rpc` 模板，传入成员函数指针 `&WrappedMasterService::PutStart` 作为编译期路由标识，以及指定的返回类型 `vector<Replica::Descriptor>`。
3. 结果透传。

`MasterClient` 的所有 RPC 方法均遵循"薄包装 + invoke_rpc"此一模式，唯一差异在于参数预处理逻辑。

#### 2.5.2 invoke_rpc 模板 — 协程式 RPC 发送

**签名：**

```cpp
template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> MasterClient::invoke_rpc(Args&&... args)
```

三个模板参数均在调用点由编译器推导：`ServiceMethod` 接收成员函数指针 `&WrappedMasterService::PutStart`；`ReturnType` 为业务返回类型 `vector<Replica::Descriptor>`；`Args...` 由传入的实际参数推导。

**函数体分五段**：

**段一：获取连接池。** `auto pool = client_accessor_.GetClientPool()`。`RpcClientAccessor` 内部以读写锁保护 `shared_ptr<coro_io::client_pool<coro_rpc_client>>`。连接池管理到同一 Master 地址的多条 TCP 连接，支持复用和 HA 地址切换。

**段二：syncAwait — 协程到同步的桥。** `return async_simple::coro::syncAwait(lambda())`。`invoke_rpc` 的签名是同步的（返回 `tl::expected`），但内部所有 I/O 均为异步。`syncAwait` 将 lambda 返回的 `Lazy<T>` 协程提交到内部事件循环并阻塞调用线程，直到协程完成。使用此模式的原因为 MasterClient 的调用方（Client::Put）已经是全同步流程——RDMA 传输阶段已阻塞在 `TransferFuture::get()`——在 RPC 层引入异步返回类型只会增加调用链复杂度而无收益。

**段三：两层 co_await。** 这是协程体中唯一一段执行逻辑：

```cpp
auto ret = co_await pool->send_request(lambda);
// ...
auto result = co_await std::move(ret.value());
```

第一层 `co_await pool->send_request(lambda)`：从连接池获取一条到 Master 的 `coro_rpc_client`。若无空闲连接，协程在此处挂起。拿到连接后，lambda 被调用，其内部调用 `client.send_request<ServiceMethod>(args...)`，此调用返回一个惰性协程对象（`Lazy<coro_rpc_result<T>>`）——此时 TCP 请求尚未发出。`ret` 即为这个惰性协程。

第二层 `co_await std::move(ret.value())`：启动上一步的惰性协程。coro_rpc 在此处将 `ServiceMethod` 转换为 function ID，与序列化后的参数组装为 RPC 请求帧，TCP 发送。协程随后挂起等待服务端响应。响应到达后，coro_rpc 反序列化结果。`result` 为已填充的 `coro_rpc_result`。

两层分离的原因：若在 `pool->send_request` 的 lambda 内部直接 `co_await` 发送请求，则整个发送+等待过程占用着连接池内部状态，其他协程无法并发获取连接。分两步执行——先取连接后立即释放池锁，再在外层等待响应——最大化连接池并发度。

**段四：错误转换。** coro_rpc 框架的错误码统一映射为 Mooncake 自身的 `ErrorCode` 枚举：

| 错误来源 | 检测方式 | 映射为 |
|---------|---------|--------|
| 连接池无可用连接 | `!ret.has_value()` | `RPC_FAIL` |
| 请求超时 | `result.error().code == coro_rpc::errc::timed_out` | `RPC_TIMEOUT` |
| 序列化/协议/其他 | 其余 coro_rpc 错误码 | `RPC_FAIL` |

**段五：metrics 记录与返回。** 成功路径上记录延迟直方图：

```cpp
metrics_->rpc_latency.observe({RpcNameTraits<ServiceMethod>::value}, latency.count());
co_return result->result();
```

`RpcNameTraits<ServiceMethod>::value` 在编译期解析为方法名字符串（如 `"PutStart"`），作为 Prometheus histogram 标签，不参与路由。`result->result()` 从 coro_rpc 返回包装中提取业务结果。

#### 2.5.3 服务端：注册与派发

**注册。** mooncake_master 进程启动时，`RegisterRpcService()` 将 WrappedMasterService 的所有方法注册到 coro_rpc_server：

```cpp
server.register_handler<&WrappedMasterService::PutStart>(
    &wrapped_service, &WrappedMasterService::PutStart);
```

coro_rpc 根据成员函数指针生成 function ID（编译期确定），构建 `handler_map_[function_id] → (service_instance, method_pointer)` 映射。

**派发。** 请求到达时，coro_rpc 反序列化请求头中的 function ID，查 handler_map_，调用匹配的 WrappedMasterService 方法。全过程为编译期类型安全的整数查表，不使用字符串匹配。客户端和服务端引用编译期相同的函数指针 `&WrappedMasterService::PutStart`——任何签名不匹配均在编译或注册阶段报错。

**委托。** WrappedMasterService 方法体内部硬编码委托到 MasterService 的同名方法：

```cpp
return execute_rpc("PutStart",
    [&] { return master_service_.PutStart(client_id, key, ...); }, ...);
```

`execute_rpc` 模板负责统一包装耗时日志和请求/失败计数。Wrapped 层本身不做方法查找——它是纯适配器，将 coro_rpc 反序列化后的参数转发给 MasterService。

#### 2.5.4 路由机制汇总

| 组件 | 机制 | 是否参与路由 |
|------|------|------------|
| `client.send_request<ServiceMethod>(args)` | 成员函数指针 → function ID（编译期），写入 RPC header | 是 |
| `server.register_handler<ServiceMethod>(handler)` | function ID → handler 映射注册 | 是 |
| `RpcNameTraits<ServiceMethod>::value` | 字符串常量，注入 Prometheus metrics 标签 | 否 |
| `WrappedMasterService::Method → master_service_.Method()` | 硬编码函数调用，非字符串/ID 查找 | 否 |

#### 2.5.5 完整调用路径

```
Client::Put()
  └→ MasterClient::PutStart(key, lengths, config)          // 参数预处理
       └→ invoke_rpc<&Wrapped::PutStart, vector<Descriptor>>(args)
            ├─ RpcNameTraits → metrics: "PutStart"          // 仅 Prometheus 标签
            ├─ syncAwait(                                    // 协程→同步桥
            │    ├─ co_await pool->send_request              // [挂起1] 等连接
            │    │    └→ client.send_request<&Wrapped::PutStart>(args)
            │    │         → function ID + 序列化参数 → TCP
            │    └─ co_await result                          // [挂起2] 等响应
            │         → coro_rpc 反序列化 → ReturnType
            └─ co_return 业务结果
                 ─ ─ ─ ─ TCP ─ ─ ─ ─
                 ▼
coro_rpc_server: function ID → handler_map_ 查表
  └→ WrappedMasterService::PutStart(args)
       └→ execute_rpc(metrics + 日志)
            └→ master_service_.PutStart(...)
                 └→ vector<Replica::Descriptor>
```

### 2.6 HA：Mooncake Master 高可用

这里的 `HA` 是 **High Availability（高可用）**，不是 Hash Algorithm，也不表示 HiCache 的分页哈希。它解决的问题是：单个 `mooncake_master` 进程失效后，怎样让 Store 控制面恢复服务，而不是怎样匹配 KVCache Page。

非 HA 模式下，Client 直接连接一个 `IP:Port`。这个 Master 保存对象元数据、Replica 位置、租约、分配和逐出状态；它一旦不可用，已有 Replica 的数据字节可能仍在各 Store Segment 中，但新的 `Exist/Get/Put` 无法正常完成元数据查询和编排。

HA 模式启动多个 Master 实例，并通过 HA backend 协调领导权。当前源码定义了 `etcd`、`redis` 和 `k8s` 三种 backend：

```text
                    HA backend
          （leader lease / MasterView）
                 ▲             ▲
                 │             │
        Master A: serving   Master B: standby
                 │             │
                 │      snapshot + OpLog 追赶
                 ▼             ▼
             对外服务        准备接管
```

正常情况下，只有进入 `serving` 状态的 leader 对外处理 Master 请求；其他实例可能依次处于 `starting`、`standby`、`candidate`、`recovering`、`catching_up` 或 `leader_warmup`。leader 失效后，其他实例竞争领导权，候选者完成元数据恢复和最终追赶后再进入 `serving`。这些状态定义在 `mooncake-store/include/ha/ha_types.h`。

Client 在 HA 模式下不把某个 Master 的 `IP:Port` 当成永久地址，而是使用类似下面的入口发现当前 leader：

```text
etcd://host1:2379;host2:2379;host3:2379
redis://host:6379
```

一次故障切换可以概括为：

```text
Client → 当前 leader
             │
             ├── leader 正常：处理 BatchExist/GetReplicaList/PutStart 等请求
             │
             └── leader 故障
                    ↓
              backend 中的 lease/领导权失效
                    ↓
              standby 恢复并追赶元数据
                    ↓
              新 leader 发布 MasterView
                    ↓
              Client 发现新地址并重连
```

必须区分三个不同保障范围：

| 机制 | 主要保护对象 | 不直接保证什么 |
|------|--------------|----------------|
| Master HA | Master 控制面可用性、leader 发现和元数据接管 | 不会自动复制每个 KV Object 的数据 |
| Snapshot + OpLog | Master 元数据基线及增量状态恢复 | 不等价于 Store 数据副本 |
| Object Replica | KV/Object payload 在不同 Segment/介质上的物理副本 | 本身不负责 Master 选主 |

因此，HA 不参与分页 key 的计算，也不改变 Mooncake 的精确 key 查找。它只是让执行 `key → ObjectMetadata → Replica` 查询的 Master 服务在节点故障后能够由另一个实例接管。数据面是否还能读取，还取决于至少一个 `COMPLETE` Replica 是否存活和可达。

---

## 3. Store 核心流程：Put 全链路

### 3.1 总体流程（三阶段）

**位置：** `mooncake-store/src/client_service.cpp` 的 `Client::Put`

Put 操作分为三个串行阶段：

1. **PutStart（RPC）** — 向 Master 申请存储空间。Master 在目标 Segment 上分配 buffer，返回 `Replica::Descriptor` 列表，每个描述符包含发起 RDMA 传输所需的全部信息（`buffer_address_`、`transport_endpoint_`、`protocol_`）。此时对象元数据状态为 `PROCESSING`，配额已暂扣（reserve）。

2. **TransferWrite（本地）** — 对每个分配到的 memory/nof replica，将本地数据推送到目标地址。分两种策略：若目标端点和本地端点相同（同进程），走 `LOCAL_MEMCPY` 捷径；否则走 `TRANSFER_ENGINE`（RDMA/TCP/NVLink）。这是整个 Put 过程中唯一涉及数据搬运的阶段，Master 完全不参与。

3. **PutEnd（RPC）** — 向 Master 确认传输结果。Master 将对象副本状态从 `PROCESSING` 转为 `COMPLETE`，结算配额（commit 成功部分，abort 失败部分），并可能触发 SSD offload。

**关键约束：** PutStart/PutEnd 之间靠 `client_id` 校验——只有 PutStart 的发起者才能 PutEnd。

### 3.2 PutStart — Master 空间分配

**位置：** `mooncake-store/src/master_service.cpp` 的 `PutStart` 与 `AllocateAndInsertMetadata`

**RPC 到达路径：** `Client::Put()` → `MasterClient::PutStart()`（参数预处理）→ `invoke_rpc<&WrappedMasterService::PutStart>(args)`（§2.5 已详述）→ TCP → coro_rpc_server 派发 → `WrappedMasterService::PutStart()` → `execute_rpc`（耗时日志、请求/失败计数）→ `master_service_.PutStart()`。

**WrappedMasterService 的职责：** 通过 `execute_rpc` 模板做统一 observability 包装。业务逻辑完全在 `MasterService::PutStart()` 中。

**核心业务逻辑**（`PutStart` + `AllocateAndInsertMetadata`）：

- **前置校验：** replica_num/nof_replica_num 不能同时为 0，key 非空，slice_length > 0。
- **预备工作：** `ResolveTenantIdForWrite` 规范化 tenant，并在多租户模式下检查其是否注册；随后执行 `UpdateClientHostId`、`AcquireObjectOperationLock`（tenant-scoped key 互斥）并计算 `requested_quota_charge`。
- **`attempt_once()`：** 先在当前路由指向的 lookup shard 中排除已有对象，再在必要时把执行交接到本次 `group_id` 指向的 target shard，最后调用 `AllocateAndInsertMetadata`。
- **分配与插入：** `ReserveTenantQuota` → `allocation_strategy_->Allocate()`（DRAM）→ 可选 NoF SSD 分配 → 可选 Disk replica → 生成 Descriptor → `metadata.emplace()` → `RegisterGroupMember()` → 将 key 标为 `PROCESSING`。
- **外层重试：** 只有 `attempt_once()` 返回 `TENANT_QUOTA_EXCEEDED` 才调用 `EvictTenantMemoryForQuota` 并重试。当前 `kMaxTenantQuotaEvictionRetries == 2`，所以最多调用三次 `attempt_once()`、执行两次 tenant quota eviction；其他错误和成功结果都立即返回。

#### 3.2.1 tenant_id、user_key 与 group_id

三者属于不同定位维度：

| 字段 | 语义层级 | 作用 |
|------|----------|------|
| `tenant_id` | 命名空间层 | 隔离租户对象、配额和 group 路由；单租户模式统一规范化为 `TenantId::Default()` |
| `user_key` | 对象层 | 在某个 tenant 的 `TenantState::metadata` 中索引一个 `ObjectMetadata` |
| `group_id` | 对象集合层 | 可选的生命周期与元数据路由标签，使相关对象共置于同一 metadata shard |

对象的稳定逻辑身份始终是 `(tenant_id, user_key)`；`group_id` 不替代对象主键，也不决定 Replica 的物理 Segment。未分组对象使用 `hash(tenant_id, user_key)` 选择 shard，分组对象使用 `hash(group_id)` 选择 shard，然后仍在目标 shard 内按 `tenant_id → user_key` 查找：

```text
metadata_shards_[route_hash]
└── tenants[tenant_id]
    └── metadata[user_key]
        └── ObjectMetadata
            └── replicas_
```

`ResolveRequestTenantId()` 是请求侧规范化：多租户关闭时忽略传入值并返回进程生命周期内有效的静态 `default` tenant；多租户开启时保留传入 tenant。`MakeObjectIdentityForRequest()` 随后把规范化结果和 `user_key` 复制进按值返回的 `ObjectIdentity`，不存在悬空引用。写入路径使用更严格的 `ResolveTenantIdForWrite()`，额外拒绝未注册 tenant。

#### 3.2.2 已提交路由与本次期望路由

必须区分两个信息源：

```text
object_group_ids_   已经成功创建并提交的 (tenant_id, key) -> group_id 路由
config.group_ids    本次 PutStart 请求期望建立的 group 路由
```

`GetGroupIdForKey(config, 1, 0)` 只负责校验单 key 请求的 `group_ids` 数量并取第 0 项；空字符串表示不分组。`getMetadataShardIndex(tenant_id, key)` 则解析已有路由：

```text
getMetadataShardIndex(tenant_id, key)
├── object_group_ids_ 中无记录
│   └── getShardIndex(tenant_id, key)
└── object_group_ids_ 中有记录
    └── getShardIndex(existing_group_id)
```

因此，路由表无记录时，`getMetadataShardIndex(tenant_id, key)` 与 `getShardIndex(tenant_id, key)` 完全相同。首次 grouped Put 仍可能发生 shard 交接，原因是 lookup 使用当前已提交状态，而 target 使用请求中的待提交状态：

```text
请求：tenant-A / key-1，config.group_id = group-X

lookup_shard_idx = hash(tenant-A, key-1) % 1024   // 尚无已提交路由
target_shard_idx = hash(group-X) % 1024           // 本次期望路由
```

先查 lookup shard 是为了排除默认位置已经存在同一 `(tenant_id, key)` 的未分组对象。只有对象和 Replica 成功创建后，`RegisterGroupMember()` 才登记路由；提前登记会在 quota 或分配失败时留下指向空 metadata 的悬空路由。

同组对象按 `group_id` 共置，使组级 lease 刷新和 eviction 可以在一把 shard 锁及一个 `TenantState` 内遍历 `group_members[group_id]`。否则同组 key 会散落到多个 shard，引入多 shard 锁顺序、死锁和部分处理问题。group 状态仍是 tenant-scoped：相同 group 字符串即使跨 tenant 落到同一 shard，也由 `tenants[tenant_id]` 隔离。

#### 3.2.3 `attempt_once()` 的两阶段 shard 交接

lookup 阶段的关键控制流是：

```text
锁定 lookup shard
        │
        ▼
tenant_state.metadata.find(key)
        │
        ├── 有有效 COMPLETE Replica
        │   └── 返回 OBJECT_ALREADY_EXISTS
        │
        ├── 有尚未超时的 PROCESSING Replica
        │   └── 返回 OBJECT_ALREADY_EXISTS
        │
        ├── 旧记录或 handle 已失效
        │   └── 清理记录，使目标 key 进入“不存在”状态
        │
        └── 原本不存在
            └── 继续计算 target shard
```

`metadata.find(key) == metadata.end()` 只表示目标 key 不存在，不表示整个 `metadata` 容器为空。它是新建对象前的不变量：有效对象不能被普通 `PutStart` 覆盖。真正插入时的 `emplace()` 还会再次检查 `inserted`，形成容器层的最终防线。

如果 target 与 lookup 相同，当前已持有正确 shard 的独占锁，直接返回 `AllocateAndInsertMetadata()`。如果不同，则只把索引带出当前锁作用域：

```cpp
std::optional<size_t> retry_shard_idx;  // 初始为 std::nullopt，不是 0

{
    MetadataShardAccessorRW lookup_shard(this, lookup_shard_idx);
    // ...确认 key 不存在...

    if (target_shard_idx != lookup_shard_idx) {
        retry_shard_idx = target_shard_idx;
    } else {
        return AllocateAndInsertMetadata(lookup_shard, ...);
    }
}  // 先释放 lookup shard 独占锁，再进入 target shard

MetadataShardAccessorRW target_shard(this, retry_shard_idx.value());
```

能运行到 `.value()` 的唯一路径已经执行过赋值；相同 shard 的路径从 lambda 直接返回。不过这个安全性依赖控制流不变量，`optional::value()` 在空状态会抛出 `std::bad_optional_access`。`optional` 在这里既避免把合法 shard 0 当哨兵，也承担跨锁作用域传递目标索引的职责。

`retry_shard_idx` 不是外层 quota retry。它只表示“释放 lookup shard 后，改锁 target shard，继续同一次 `attempt_once()`”。`MetadataShardAccessorRW` 也不创建 shard；1024 个 shard 已随 `MasterService` 存在，它只是引用数组元素并独占锁定其 `mutex`。

lookup 阶段通过 `shard->tenants[tenant_id]` 获取 `TenantState`，而 `operator[]` 在 tenant 不存在时会创建空节点。若需要交接到另一个 shard，代码只在 `tenant_state.Empty()` 时删除 lookup shard 中这个无用占位：

```text
lookup shard
└── tenants[tenant_id]
    └── empty TenantState
        └── 交接前删除

target shard
└── tenants[tenant_id]
    └── ObjectMetadata
        └── 成功插入时创建
```

这不是删除整个 tenant，也不影响该 tenant 在其他 shard 中的数据。非空 `TenantState` 可能仍含其他对象、processing key、复制/卸载/晋升任务或 group 成员，必须保留。

#### 3.2.4 锁作用域与两类“重试”

lookup 和 target 两个阶段都遵循：

```text
snapshot_mutex_ shared lock
└── metadata_shards_[shard_idx].mutex exclusive lock
```

前者是全局快照屏障：普通 metadata 操作可共同持有 shared lock，但会阻塞需要 unique lock 的快照/恢复阶段。后者才实际串行化当前 shard 的结构修改。两把锁彼此独立，按上述顺序获取、按 shard → snapshot 的逆序释放。

首次 grouped Put 在 `attempt == 0` 内即可完成 lookup → target 交接并成功返回。只有 `AllocateAndInsertMetadata()` 中的 `ReserveTenantQuota()` 返回 `TENANT_QUOTA_EXCEEDED`，外层循环才逐出该 tenant 的内存并重新调用整个 `attempt_once()`：

```text
attempt 0 --quota exceeded--> tenant eviction 1
attempt 1 --quota exceeded--> tenant eviction 2
attempt 2 --quota exceeded--> 记录拒绝指标并返回错误
```

`OBJECT_ALREADY_EXISTS`、`NO_AVAILABLE_HANDLE`、`INVALID_PARAMS` 等非 quota 错误不会进入该重试循环。

#### 3.2.5 已有 ObjectMetadata 的状态判定

`TenantState::metadata` 的类型是
`unordered_map<string, ObjectMetadata>`。因此：

```cpp
auto it = tenant_state.metadata.find(key);
```

查询的是当前 metadata shard、当前 tenant 下，是否存在该 `user_key` 的对象元数据，
而不是直接查询是否存在一个正在运行的传输进程：

```cpp
it->first   // user_key
it->second  // ObjectMetadata
```

`ObjectMetadata` 会跨 RPC 保留，既可以表示仍在创建的对象，也可以表示已经完成、可读取
的对象。正常 `PutEnd` 会把相应 Replica 从 `PROCESSING` 标记为 `COMPLETE`，并在对象
完成后把 key 从 `processing_keys` 移除，但不会删除 `metadata[key]`。因此正常写入结束后，
重新执行 `find(key)` 仍应得到 `it != metadata.end()`；只有 Remove、eviction 或失效清理
真正删除对象后，结果才是 `end()`。

`PutStart` 是 create-only 操作。查询到已有记录后，先调用 `CleanupStaleHandles()` 删除
allocator 已失效的已完成内存/NoF Replica，以及 owner client 已离线的已完成 LocalDisk
Replica。清理后若对象已无有效 Replica，函数返回 `true`，旧 metadata 可以整体删除。
否则继续执行：

```cpp
auto& metadata = it->second;
if (metadata.HasReplica(&Replica::fn_is_completed) ||
    metadata.put_start_time + put_start_discard_timeout_sec_ >= now) {
    return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
}
```

这个判据可拆为：

```cpp
const bool has_completed_replica =
    metadata.HasReplica(&Replica::fn_is_completed);

const bool previous_put_may_still_be_active =
    now <= metadata.put_start_time + put_start_discard_timeout_sec_;
```

| 是否有 `COMPLETE` Replica | 是否超过 discard timeout | 结论 |
|---------------------------|----------------------------|------|
| 是 | 否 | 已有合法对象，当前 `PutStart` 返回 `OBJECT_ALREADY_EXISTS` |
| 是 | 是 | 已有合法对象，仍然不允许普通 `PutStart` 覆盖 |
| 否 | 否 | 旧 Put 可能仍正常传输，拒绝当前请求抢占 |
| 否 | 是 | 旧 Put 可能已废弃，隔离旧资源后允许重新创建 |

发现 `COMPLETE` Replica 不表示出现了错误的 metadata 抢占。它表示合法已有对象占用了
`(tenant_id, user_key)`，失败的是当前 create-only 请求。覆盖已有对象应走 Upsert 语义。

发现 `PROCESSING` Replica 也不直接表示旧写入异常。只有“没有任何完成副本”且“从旧
`PutStart` 起已经超过 discard timeout”同时成立时，Master 才允许当前请求接管这个
对象槽位。这是保守的时间判据，不是对客户端传输进程存活状态的精确探测。

同一个 key 会查到旧 `PROCESSING` Replica，是因为对象身份和写入尝试是两个层次：

```text
same object identity: (tenant_id, user_key)
different attempts:   old put attempt -> new put attempt
```

第一次 `PutStart` 已把旧 `ObjectMetadata` 写入 Master，但客户端可能未调用 `PutEnd`。
后续同 key 请求会查到这条跨 RPC 留存的旧记录。旧 Replica 与该 key 相关，只是属于上一次
写入尝试，而不是当前尝试。当前实现没有显式 generation ID，而是用 Replica 状态和
`put_start_time` 判断旧写入是否仍受保护。

一个 `ObjectMetadata` 中的 Replica 在逻辑上都属于同一个对象，并应保存相同内容；但
`PROCESSING` 只表示目标 buffer 正在或等待写入，不保证各副本此刻已经写完或内容一致。
对象也可能暂时同时包含 `COMPLETE` 与 `PROCESSING` Replica，不过只要存在一个
`COMPLETE`，上述路径就会保留整个对象并拒绝新的 `PutStart`。

#### 3.2.6 超时 PROCESSING Replica 的隔离与延迟释放

只有旧记录“没有 `COMPLETE` Replica 且已经超过 discard timeout”时，代码才进入回收
路径：

```cpp
auto replicas =
    metadata.PopReplicas(&Replica::fn_is_processing);

if (!replicas.empty()) {
    std::lock_guard lock(discarded_replicas_mutex_);
    discarded_replicas_.emplace_back(
        std::move(replicas),
        metadata.put_start_time + put_start_release_timeout_sec_);
}
```

`PopReplicas()` 不是销毁 Replica，而是把所有匹配项从旧
`ObjectMetadata::replicas_` 中移出，并把所有权交给返回的 vector。随后 vector 被移动到
`discarded_replicas_`，旧 metadata 才会由 `EraseMetadata()` 删除。

这样做是为了隔离迟到写入。旧 `PutStart` 已把目标 buffer 地址交给客户端；即使 Master
根据超时允许新 Put 接管 key，也无法证明旧客户端已经停止 RDMA 或内存写入。如果直接
析构旧 Replica，其 `AllocatedBuffer` 会把地址归还 allocator；同一地址一旦分给新对象，
迟到的旧写入就可能破坏新数据：

```text
old Replica destroyed
  -> old AllocatedBuffer releases address X
  -> allocator assigns X to a new Replica
  -> delayed old client writes to X
  -> new object is corrupted
```

隔离后的所有权关系是：

```text
metadata[user_key]
└── new ObjectMetadata
    ├── new R3: PROCESSING
    └── new R4: PROCESSING

discarded_replicas_
├── old R1 + old AllocatedBuffer
└── old R2 + old AllocatedBuffer
```

`DiscardedReplicas` 保存 `vector<Replica>`、`ttl` 和内存大小统计。后台
`ReleaseExpiredDiscardedReplicas(now)` 删除 TTL 到期的 list 元素，随后通过
`DiscardedReplicas -> vector<Replica> -> Replica -> AllocatedBuffer` 的析构链真正归还
资源。

默认时间窗口为：

```text
old PutStart
├── +30 seconds: discard timeout，可将旧写入判为废弃并让新 Put 接管
└── +10 minutes: release timeout，可真正释放旧 buffer
```

这是基于超时的 grace-period 隔离，不是严格的传输取消或客户端确认协议。如果旧客户端
在 release timeout 后仍使用旧地址，系统无法继续提供绝对保护。

如果完全保留超时的旧 metadata，新的同 key `emplace()` 无法成功，废弃写入会持续占用
对象槽位、内存和 quota；如果删除 metadata 却不先移出并隔离 `PROCESSING` Replica，
底层地址又可能被过早复用。当前策略同时解决“允许 key 重试”和“推迟旧地址复用”两个
问题。

#### 3.2.7 EraseMetadata(kFull) 的对象级清理边界

超时 Replica 被隔离后，`PutStart` 调用：

```cpp
EraseMetadata(tenant_state, it, object_id.tenant_id,
              QuotaEraseMode::kFull, &shard);
it = tenant_state.metadata.end();
```

其逻辑含义是清理当前 shard、当前 tenant 下，该 `user_key` 对应的全部对象级控制面状态：

```text
(metadata shard, tenant_id, user_key)
```

它不是重置整个 tenant，也不清空整个 shard。`kFull` 路径依次完成：

1. 若有 offloading task，解除源 Replica 引用计数并删除任务；
2. 从 `processing_keys` 删除该 key；
3. 删除该 key 的 replication task；
4. 删除 promotion task 及其关联状态；
5. 更新 LocalDisk 使用量和 memory/disk cache-total 统计；
6. abort `reserved_quota_charge_bytes`；
7. release `committed_quota_charge_bytes` 和
   `pending_replaced_quota_charge_bytes`；
8. 执行 `tenant_state.metadata.erase(it)` 并减少 tenant 对象计数；
9. 通过传入的 shard accessor 更新 `disk_object_count`；
10. 清理 `(tenant_id, key) -> group_id` 路由和 `group_members` 成员关系。

当前路径中已经被 `PopReplicas()` 移出的旧 `PROCESSING` Replica 不再属于
`ObjectMetadata`，所以不会随 `EraseMetadata()` 立即析构，而是继续由
`discarded_replicas_` 持有到 TTL 到期。

删除 map 元素后，原 `it` 已失效。这里把它显式设置为 `metadata.end()`，是将 `end()`
作为“目标 key 已不存在，可以进入创建逻辑”的本地状态标记。`EraseMetadata()` 返回的
下一个 iterator 可能指向 map 中另一个不相关 key，不能表达当前目标 key 已被删除。

清理范围可以表示为：

```text
metadata_shards_[N]
└── tenants[tenant-A]
    ├── metadata[key-1]  <- erase object-scoped state
    ├── metadata[key-2]  <- unchanged
    └── metadata[key-3]  <- unchanged
```

只有清理后整个 `TenantState` 为空，调用方才可能进一步从当前 shard 的 `tenants` map
删除这个空 tenant 节点；这仍不影响该 tenant 在其他 metadata shard 中的局部状态。

**返回值：** `vector<Replica::Descriptor>`，核心字段来自 `AllocatedBuffer::Descriptor`：`{size_, buffer_address_, protocol_, transport_endpoint_}`。Client 端 TE 凭 `transport_endpoint_`（远端传输端点）和 `buffer_address_`（已分配区域的目标地址）即可发起数据传输，不需在数据路径中再次经过 Master。

### 3.3 TransferWrite — TE 数据传输

**位置：** `client_service.cpp`（`TransferWrite`、`TransferData`）与 `transfer_task.cpp`（`submit`、`submitTransferEngineOperation`、`submitTransfer`）

这是 Put 流程中唯一涉及数据搬运的阶段，也是理解 RDMA 如何被唤起的关键。

#### 3.3.1 两级分支总览

数据传输从 `TransferWrite` 入口开始，经两级分支到达具体传输机制。

**第一级：`TransferData`。** 按 replica 类型分流——NOF 副本调用 `GetContiguousSliceRange()` 获取连续内存范围后，将 ptr 和 size 作为额外参数传入 `submit()`；其余类型直接传入 `submit()`。

**第二级：`TransferSubmitter::submit()`。** 按 replica 类型三路分发：

| Replica 类型 | Write 路径 | Read 路径 | 底层机制 |
|-------------|-----------|----------|---------|
| Memory | `selectStrategy()` → LOCAL_MEMCPY 或 TRANSFER_ENGINE | `submitMemoryReadOperation()` → 同左 | memcpy 或 TE（RDMA/TCP/NVLink） |
| NOF | `submitSpdkNofOperation(handle, ptr, size, op)` | 同上 | SPDK 用户态 NVMe-oF |
| Disk / LocalDisk | `PutToLocalFile()`（非 TransferWrite） | `submitFileReadOperation()` | 文件系统 I/O |

**NOF 要求连续内存的原因：** SPDK NVMe-oF 的 I/O 提交需连续的物理内存区域构造 NVMe SQE。非连续 slice 列表无法映射为单个 NVMe 命令，因此 `TransferData` 在第一级用 `GetContiguousSliceRange()` 预先获取连续范围。

**Memory 可处理非连续 slice：** TE 的 `submitTransfer` 接受 `vector<TransferRequest>`，每个请求指向一个独立 slice，TE 内部逐个发起 RDMA WRITE。

#### 3.3.2 Memory 副本：策略选择（LOCAL_MEMCPY vs TRANSFER_ENGINE）

`TransferSubmitter::submit()` 对 memory 副本调用 `selectStrategy()`：

- **`memcpy_enabled_` 为 false**（环境变量 `MC_STORE_MEMCPY` 未设置）→ 直接走 `TRANSFER_ENGINE`
- **`isLocalTransfer(handle)` 为 true**（handle 的 `transport_endpoint_` == 本地 `ip:port`）→ `LOCAL_MEMCPY`：同一进程地址空间，异步 memcpy
- **其他** → `TRANSFER_ENGINE`：RDMA/TCP/NVLink 远程传输

此处 "local" 指同一进程而非同一主机——同主机不同进程共享 IP 但端口不同，虚拟地址空间不互通。

#### 3.3.3 TRANSFER_ENGINE 路径详解

当策略为 `TRANSFER_ENGINE` 时，进入 `submitTransferEngineOperation()`：

**第一步：打开远程段。** 调用 `engine_.openSegment(handle.transport_endpoint_)`，将远程 endpoint 字符串解析为 `SegmentHandle`（整数句柄）。TE 内部会维护 segment 句柄到远程内存注册信息的映射。

**第二步：构造 TransferRequest 列表。** 遍历每个 slice，填充请求结构：

```cpp
TransferRequest request;
request.opcode = TransferRequest::WRITE;       // 写操作
request.source = slice.ptr;                    // 本地数据指针
request.target_id = seg;                       // 远程段句柄
request.target_offset = handle.buffer_address_ + offset; // 远程偏移
request.length = slice.size;
```

关键点：`target_offset` = `buffer_address_` + `offset`。其中 `buffer_address_` 来自 PutStart 返回的 Descriptor，是 Master 为目标 Replica 分配区域的起始地址；`offset` 是多个 Slice 的累积字节偏移。虽然 Transfer Engine 字段名为 `target_offset`，这里的基值来自 `AllocatedBuffer::buffer_ptr_`，不能简单理解成脱离基址的 Segment 内相对偏移。

**第三步：提交传输。** `submitTransfer(requests)`：

```cpp
BatchID batch_id = engine_.allocateBatchID(batch_size);   // 分配批次 ID
Status s = engine_.submitTransfer(batch_id, requests);    // ★ RDMA 在此发起
auto state = std::make_shared<TransferEngineOperationState>(
    engine_, batch_id, batch_size);                        // 封装异步状态
return TransferFuture(state);                             // 返回 Future
```

TE 内部根据 protocol_ 将请求分发到对应 transport（RDMA/TCP/NVLink），由 RDMA transport 发起单边 WRITE——数据经本地 RNIC → 网络 → 远端 RNIC → DMA 写入远端注册内存，远端 CPU 完全不被中断。

#### 3.3.4 结果轮询

`TransferFuture::get()` 调用 `TransferEngineOperationState::wait_for_completion()`，后者调用 `check_task_status()`：

对 batch 中的每个子传输 i，调用 `engine_.getTransferStatus(batch_id, i, status)` 检查状态：`COMPLETED`（跳过）、`FAILED/CANCELED/INVALID`（记录到 failed 列表）、其他（标记仍在传输中，等待下次 poll）。全部终端状态后汇总结果：有失败 → `TRANSFER_FAIL`，全部成功 → `OK`。

**这是主动轮询模型，不是监听模型。** TE 在发起端通过 `getTransferStatus` 主动检查传输是否完成。远端不需要轮询——其内存已预注册，RDMA 网卡自动将数据 DMA 写入。

### 3.4 PutEnd — Master 状态确认

**位置：** `mooncake-store/src/master_service.cpp` 的 `PutEnd`

**权限校验：** MetadataAccessorRW 获取写锁，自动清理 stale handles。检查对象存在（否则 OBJECT_NOT_FOUND）且 client_id 匹配（否则 ILLEGAL_CLIENT）。

**状态转换：** `metadata.VisitReplicas(筛选条件, mark_complete())` — 将 PROCESSING 的副本标记为 COMPLETE。

**配额结算：** CommitTenantQuota（成功部分确认）、AbortTenantQuota（失败部分释放暂扣配额）、ReleaseTenantQuota（被替换旧副本配额释放）。实现"多退少补"语义。

**后续操作：** 若 enable_offload_，PushOffloadingQueue 触发异步 SSD 写回；EraseFromProcessing 从 processing_keys 移除；SyncCacheTotalAccounting；GrantLease(0, soft_pin_ttl) 开放读取；PublishKvStored 发布 KV 事件。

### 3.5 任务分发路径（补充路径）

除上述 Put 内联传输外，还存在一条**异步、Master 驱动的任务分发路径**，用于副本迁移（Move）和副本拷贝（Copy）—— 典型场景是 Segment 下线排水（Drain）、负载均衡（Rebalance）。

#### 3.5.1 Master 侧：任务创建与调度

Master 进程内部的 `JobDispatchThreadFunc()` 以固定间隔（`kJobDispatchThreadSleepMs`）调用 `ProcessDrainJobs()`。该函数遍历所有活跃的 DrainJob，调用 `ScheduleDrainJobTasks()` → `CreateMoveTask()`/`CreateCopyTask()`。

`CreateMoveTask` 流程：校验对象存在（OBJECT_NOT_FOUND）、校验目标 segment 可分配（UNAVAILABLE_IN_CURRENT_STATUS）、校验源 segment 确实持有该对象副本 → 通过 `task_manager_.submit_task_typed<TaskType::REPLICA_MOVE>(client_id, payload)` 创建任务。

任务被绑定到**拥有源副本的 Client**（通过 `SegmentManager.GetClientIdBySegmentName(source_segment)` 查找），payload 包含 `{key, tenant_id, source_segment, target_segment}`。

#### 3.5.2 Client 侧：轮询拉取与执行

Client 启动时创建轮询线程 `TaskPollThreadMain()`，定期调用 `PollAndDispatchTasks()`：`FetchTasks(kTaskBatchSize)` → RPC 从 Master 拉取任务列表 → `SubmitTask(task)` → 入队 `task_thread_pool_` 线程池 → `ExecuteTask(task)`。

`ExecuteTask()` 根据任务类型分发：

- **REPLICA_MOVE：** 解析 `ReplicaMovePayload` → `Move(key, tenant_id, source, target)` → `MoveStart`（RPC）→ `ExecuteReplicaTransfer()` → `MoveEnd`（RPC）。
- **REPLICA_COPY：** 解析 `ReplicaCopyPayload` → `Copy(key, tenant_id, source, targets)` → `CopyStart`（RPC）→ `ExecuteReplicaTransfer()` → `CopyEnd`（RPC）。

执行成功后调用 `MarkTaskToComplete(task_id, SUCCESS)` 通知 Master。若失败且错误为 `NO_AVAILABLE_HANDLE`，则支持指数退避重试（`max_retry_attempts`）。

#### 3.5.3 ExecuteReplicaTransfer — 公共的数据搬运核心

**位置：** `mooncake-store/src/client_service.cpp` 的 `ExecuteReplicaTransfer`

Copy 和 Move 最终都收敛到 `ExecuteReplicaTransfer()`：

1. 校验源 replica 类型为 memory（仅支持从内存源复制）。
2. `IsReplicaOnLocalMemory(source)` 确认源在本进程地址空间。
3. 使用 `split_into_slices(buffer, size)` 将源 replica 的 buffer 拆分为 slices（零拷贝，直接引用已有内存）。
4. 对每个 target，调用 **`TransferWrite(target, slices)`** — 这与 Put 路径完全相同，最终也是走 `TransferSubmitter::submit()` → `engine_.submitTransfer()`。
5. 全部 target 传输成功后，调用 end_fn（MoveEnd/CopyEnd）确认。

**关键结论：无论哪条路径（Put 内联、Move 任务、Copy 任务），数据搬运的最终执行点都是 `TransferWrite()` → `TransferSubmitter::submit()` → `engine_.submitTransfer()`。**

### 3.6 两条路径对比

| 维度 | Put 内联传输 | 任务分发传输（Move/Copy） |
|------|------------|------------------------|
| 触发方 | Client（用户代码调用 Put） | Master（DrainJob 调度器创建任务） |
| 客户端获取方式 | N/A（同步调用） | 轮询 `FetchTasks()` RPC，每秒一次 |
| 执行线程 | 调用线程内同步执行 | `task_thread_pool_` 线程池异步执行 |
| 两阶段协议 | PutStart → TransferWrite → PutEnd | MoveStart/CopyStart → TransferWrite → MoveEnd/CopyEnd |
| 失败处理 | 立即返回错误 | 支持重试（指数退避，仅对 NO_AVAILABLE_HANDLE） |
| 典型场景 | 用户写入 KV Cache | Segment 下线排水、负载均衡、副本修复 |
| TE 唤起点 | `TransferSubmitter::submit()` | 同一 `TransferSubmitter::submit()`（经 `ExecuteReplicaTransfer` → `TransferWrite`） |
| 数据传输方向 | Client → 远程 Node | 本地源 replica buffer → 远程 target replica |

### 3.7 TransferFuture：统一的异步传输句柄

**位置：** `mooncake-store/include/transfer_task.h`、`mooncake-store/src/transfer_task.cpp`

上述两级分支产生了 5 种底层传输策略（LOCAL_MEMCPY、TRANSFER_ENGINE、SPDK_NVMF、FILE_READ、EMPTY），每种完成机制不同——memcpy 走 cv 回调、TE 走状态轮询、SPDK 走 callback。如果每种策略返回不同类型的句柄，调用方 `TransferData` 就需要了解所有这些细节。

`TransferFuture` 通过 **Strategy 模式 + 类型擦除** 解决了这个问题。

#### 3.7.1 设计结构

**第一层：多态基类 `OperationState`。** 抽象基类定义两个纯虚方法：

```cpp
virtual bool is_completed() = 0;         // 非阻塞检查
virtual void wait_for_completion() = 0;  // 阻塞等待
```

五个子类各自实现：

| 子类 | 传输策略 | 完成机制 | 关键上下文 |
|------|---------|---------|-----------|
| `EmptyOperationState` | 空操作 | 立即返回 true | 无 |
| `MemcpyOperationState` | 同进程 memcpy | 工作线程拷贝完后调 `set_completed()` → `cv_.notify_all()` | `mutex_` + `cv_` |
| `TransferEngineOperationState` | RDMA/TCP/NVLink | 轮询 `engine_.getTransferStatus(batch_id_, i, status)` 直到所有子传输 terminal；事件驱动模式则等 `batch_desc.completion_cv` | `engine_` 引用 + `batch_id_` + `batch_size_` |
| `SpdkNofOperationState` | SPDK NVMe-oF | SPDK callback 触发 `set_completed()` | `mutex_` + `cv_` |
| `FilereadOperationState` | 文件 I/O | I/O 线程完成后调 `set_completed()` | `mutex_` + `cv_` |

**第二层：`TransferFuture` 作为统一句柄。** 只持有一个 `shared_ptr<OperationState> state_`，对外暴露三个方法，完全隐藏底层策略类型：

```cpp
class TransferFuture {
    shared_ptr<OperationState> state_;    // 类型擦除，调用方不知底层是什么

    bool isReady() const                  // → state_->is_completed()
    ErrorCode get()                       // → state_->wait_for_completion() + get_result()
    TransferStrategy strategy() const     // 仅透传策略名，用于日志
};
```

不可拷贝，只可移动——确保每个 batch_id 只被一个 Future 持有。`TransferEngineOperationState` 的析构函数调 `engine_.freeBatchID(batch_id_)` 回收 TE 资源，RAII 机制防止泄漏。

#### 3.7.2 两种底层完成模型

**Push 模型（Memcpy / SPDK / Fileread）：** 工作线程执行传输 → 完成后调 `set_completed()` → notify cv → `future->get()` 中阻塞的 `cv_.wait()` 被唤醒。

**Poll 模型（TransferEngine）：** `future->get()` → `wait_for_completion()` → 循环调 `engine_.getTransferStatus(batch_id_, i, status)` 检查每个子传输状态，直到全部 `COMPLETED` 或出现 `FAILED`。

为什么 TE 用 poll 而非 push？RDMA 传输的底层完成事件由 TE 内部的 CQ 轮询循环驱动。TE 层本身就有自己的 CQ polling 线程，`TransferFuture` 层不做 CQ 轮询，而是通过 `getTransferStatus` 查询 TE 维护的状态摘要——TE 内部已经将 CQ 事件转化为状态枚举值，上层只需检查和等待。

#### 3.7.3 调用方视角

在 `TransferData` 中，提交后立即拿到 Future，`get()` 阻塞等待完成。对调用方而言完全透明——不关心底层是 RDMA 轮询还是 memcpy 回调：

```cpp
auto future = transfer_submitter_->submit(replica, slices, op_code);
// 不管底层策略，接口完全一致
return future->get();  // 阻塞等待结果
```

---

## 4. 关键数据结构

### 4.1 Replica::Descriptor

Master 通过 `PutStart`、`GetReplicaList` 等接口把内部 Replica 转换成 Descriptor 返回给 Client。Descriptor 只携带定位和状态，不转移底层资源所有权（`replica.h`）：

```cpp
AllocatedBuffer::Descriptor {        // allocator.h:77-84, 传输层的核心字段
    uint64_t size_;                  // 分配空间大小
    uintptr_t buffer_address_;       // 已分配区域的目标起始地址
    std::string protocol_;           // "rdma" 或 "tcp"
    std::string transport_endpoint_; // "mlx5_0:192.168.1.10:12345"
};

Replica::Descriptor {
    ReplicaID id;
    ReplicaStatus status;            // PROCESSING / COMPLETE / ...
    variant<MemoryDescriptor,        // 含 AllocatedBuffer::Descriptor
            NoFDescriptor,           // 同 Memory，但指向 NVMe-oF SSD 段
            DiskDescriptor,          // 含 file_path + object_size
            LocalDiskDescriptor>     // 含 client_id + endpoint
};
```

Memory Descriptor 中的 protocol、endpoint、address 和 size 共同构成远端传输定位；Client 取得这些信息后可直接发起数据传输，无需让 Master 进入 payload 路径。

### 4.2 AllocatedBuffer::Descriptor 与 Segment 的关系

`AllocatedBuffer` 持有 `weak_ptr<BufferAllocatorBase>` 回指为所在 Segment 服务的 allocator，但不直接持有 `Segment` 对象；该所有权关系也不进入序列化的 Descriptor。

构造 Descriptor 时，`transport_endpoint_` 来自 `alloc->getTransportEndpoint()`，`buffer_address_` 则直接来自 `AllocatedBuffer::buffer_ptr_`。这两个字段对传输定位已完备：endpoint 标识远端传输服务，address 标识已分配区域的目标地址。Segment 名称主要用于分配偏好、管理和监控，不进入常规远端传输定位字段。

### 4.3 ObjectMetadata

Master 侧每个 key 的元数据记录（`mooncake-store/include/master_service.h`）：

```cpp
struct ObjectMetadata {
    UUID client_id;                   // PutStart 发起者（PutEnd 时校验）
    time_point put_start_time;        // 用于 zombie 检测和抢占
    const size_t size;                // 对象总大小
    const ObjectDataType data_type;
    const std::string group_id;       // 分组 ID（关联生命周期）
    const TenantId tenant_id;
    const std::string user_key;

    mutable SpinLock lock;            // 细粒度自旋锁
    mutable time_point lease_timeout;      // 读保护租约
    mutable optional<time_point> soft_pin_timeout; // 软固定过期
    const bool hard_pinned;           // 永不逐出
    bool memory_cache_total_accounted;
    bool disk_cache_total_accounted;
    uint64_t reserved_quota_charge_bytes;    // PutStart 预留
    uint64_t committed_quota_charge_bytes;   // PutEnd 提交
    uint64_t pending_replaced_quota_charge_bytes;

    vector<Replica> replicas_;        // 内部副本列表
};
```

`ObjectMetadata` 是控制面记录，不内嵌用户对象的 payload 字节。其主要负载包括：

- 对象身份和创建者：`tenant_id`、`user_key`、`group_id`、`client_id`；
- 对象属性和写入阶段：`size`、`data_type`、`put_start_time`；
- 生命周期：lease、soft pin、hard pin；
- 配额和统计记账；
- `replicas_`：每个 Replica 的状态及其内存分配句柄、磁盘路径或 LocalDisk owner/endpoint。

真正的数据字节位于 Client 注册的内存 Segment、NoF SSD 区域或磁盘位置。Master 保存的是可以管理这些区域生命周期并返回给 Client 的 Replica/AllocatedBuffer 句柄与描述符。

### 4.4 存储模型汇总

Master 进程内以三级映射管理所有对象：

| 层级 | 结构 | 说明 |
|------|------|------|
| 分片层 | `array<MetadataShard, 1024>` | 未分组对象按 tenant+key、分组对象按 group_id 路由；每分片独立 `SharedMutex` |
| 租户层 | `unordered_map<TenantId, TenantState>` | 每分片内按租户隔离对象、任务和 group 成员；一个 TenantState 只是该租户落入本 shard 的局部状态 |
| 对象层 | `unordered_map<key, ObjectMetadata>` | 每对象一条元数据记录，含 `replicas_: vector<Replica>` |

每个 `ObjectMetadata` 通过 `replicas_` 关联一到多个介质副本；各 Replica 类型和物理位置统一在 §4.5 展开。

同一个 tenant 的 key 可以散落在多个 shard，同一个 shard 也可以保存多个 tenant 的局部状态：

```text
MasterService
└── metadata_shards_[1024]
    └── MetadataShard
        ├── mutex
        ├── disk_object_count
        └── tenants
            ├── tenant-A -> TenantState（tenant-A 在本 shard 的局部状态）
            │   ├── metadata: user_key -> ObjectMetadata
            │   ├── processing_keys
            │   ├── replication/offloading/promotion tasks
            │   └── group_members: group_id -> member keys
            └── tenant-B -> TenantState（tenant-B 在本 shard 的局部状态）
```

tenant quota 不存放在 `TenantState` 中，而由独立的 sharded quota table 管理；`ObjectMetadata` 只保存本对象的 reserved/committed/pending charge。

#### 4.4.1 `MetadataShardAccessorRW` 与 `MetadataAccessorRW`

二者最终锁定同一类 `MetadataShard::mutex`，但抽象层级不同：

| 对比项 | `MetadataShardAccessorRW` | `MetadataAccessorRW` |
|--------|---------------------------|----------------------|
| 输入 | 显式 `shard_index` | `ObjectIdentity{tenant_id, user_key}` |
| 路由 | 不负责，由调用者决定 | 自动调用 `getMetadataShardIndex()` |
| 访问层级 | 整个 shard，调用者自行找 tenant/key | 自动定位 tenant、metadata、processing 和 replication task |
| 行为 | RAII 独占锁 + shard 引用 + disk 计数辅助 | 组合 shard guard，并封装 `Exists/Get/Create/Erase`、失效 memory handle 清理和空 tenant 清理 |
| 典型用途 | PutStart 两阶段路由、批量或分片级操作 | PutEnd、Remove 等路由已经确定的单对象操作 |

组合关系如下：

```text
MetadataAccessorRW
├── ObjectIdentity object_id_
├── size_t shard_idx_
├── MetadataShardAccessorRW shard_guard_
│   ├── MetadataShard& shard_
│   └── SharedMutexLocker lock_       // 独占 shard mutex
├── TenantState* tenant_state_
└── ObjectMetadata iterator it_
```

“对象级 Accessor”描述的是接口语义，不代表锁粒度缩小到单对象；`MetadataAccessorRW` 在其整个生命周期内仍独占对象所在的完整 shard。它也不会自动获取 `snapshot_mutex_` 或 object-operation lock，这些更外层的锁仍由调用者按协议获取。

`MetadataShardAccessorRW` 的 `SharedMutexLocker` 默认调用自定义 `SharedMutex::lock()`，所以是独占模式；RO 版本显式传入 `shared_lock` 标签。在 PutStart 中，构造顺序是先持有 snapshot shared lock，再持有 shard exclusive lock，作用域结束时按相反顺序释放。

### 4.5 Segment、AllocatedBuffer、Replica 与 Slice

这四个概念分属资源、对象副本和传输三个层次：

```text
Segment（节点贡献的大块存储资源）
  └── BufferAllocatorBase
        └── AllocatedBuffer（分配给一个内存型 Replica 的空间）
              ▲
              │ Put/Get 数据传输
              │
          vector<Slice>（一次操作中的本地内存视图）
```

| 概念 | 所属层次 | 所有权/生命周期 | 主要作用 |
|------|----------|-----------------|----------|
| `Segment` | 存储资源 | 从节点挂载到卸载 | 描述一块可分配空间及其协议、端点和宿主信息 |
| `AllocatedBuffer` | 副本存储 | 通常由一个内存型 `Replica` 独占 | 表示 Segment 中实际分配给副本的地址和大小 |
| `Replica` | 对象元数据 | 从副本创建到删除 | 描述一个逻辑对象的一份物理副本、介质和状态 |
| `Slice` | 数据传输 | 单次 Put/Get 操作 | 非拥有地描述一段连续的源或目标内存 |

#### Segment 与 BufferAllocator

在 Master 的普通内存/NoF 路径中，一个物理 Segment（以 UUID 区分）只对应一个 `BufferAllocatorBase` 实例。挂载时根据 `SegmentManager` 配置创建 `CachelibBufferAllocator` 或 `OffsetBufferAllocator`，二者选其一：

```text
一个 Segment UUID
  └── 一个 BufferAllocatorBase
        └── 多个 AllocatedBuffer
```

同一个 allocator 的 `shared_ptr` 会同时进入两个索引：

```text
                         ┌── mounted_segments_[segment.id]
同一个 allocator 实例 ◄──┤
                         └── allocator_manager_[segment.name]
```

这表示多个管理结构共享同一个 allocator，并不表示一个 Segment 创建了多个 allocator。`MountedSegment` 保存按 UUID 定位的 Segment 与唯一 allocator；`AllocatorManager` 则按逻辑名称组织可分配资源：

```text
逻辑 Segment Name
  ├── 物理 Segment A / Allocator A
  ├── 物理 Segment B / Allocator B
  └── 物理 Segment C / Allocator C
```

因此必须区分：

| 观察对象 | 基数关系 |
|----------|----------|
| 普通物理 Segment UUID | 通常 `1 Segment : 1 allocator` |
| 逻辑 Segment Name | 可以 `1 name : N allocators` |
| 单个 allocator | `1 allocator : N AllocatedBuffers` |

CXL 是特殊路径。Master 使用单例 `cxl_global_allocator_` 统一管理 CXL 共享内存，多个 CXL `MountedSegment` 记录可以引用同一个全局 allocator：

```text
CXL Segment Record A ─┐
CXL Segment Record B ─┼──→ cxl_global_allocator_
CXL Segment Record C ─┘
```

所以 CXL 的例外是“多个 Segment 记录共享一个 allocator”，仍然不是“一个 Segment 被多个 allocator 分别管理”。所有权方向上，更准确的说法是 Master 的 `MountedSegment` 和 `AllocatorManager` 持有 allocator；allocator 管理 Segment 地址范围中的分配状态，而不是 allocator 持有完整的 `Segment` 对象。

#### AllocatedBuffer 与 Replica

对于 `MEMORY` 和 `NOF_SSD` 类型，Replica 持有一个 `unique_ptr<AllocatedBuffer>`，通常是一对一关系：

```text
ObjectMetadata[key]
  └── replicas_: vector<Replica>
        ├── Replica A
        │     └── AllocatedBuffer A → Segment A 中的一块空间
        └── Replica B
              └── AllocatedBuffer B → Segment B 中的一块空间
```

`Replica` 回答“这是什么类型的副本、当前是否可读”，`AllocatedBuffer` 回答“该副本的数据存在哪里、占用多大空间”。多个 Replica 保存同一个 value 的独立物理拷贝，各自拥有不同的 AllocatedBuffer。

创建关系为：

```text
AllocationStrategy 选择 Segment
  → Segment 对应的 BufferAllocatorBase::allocate(object_size)
  → AllocatedBuffer
  → 构造状态为 PROCESSING 的 Replica
  → 数据写入完成后将 Replica 置为 COMPLETE
```

Replica 删除时，其 `unique_ptr<AllocatedBuffer>` 随之析构，AllocatedBuffer 再通过关联的 allocator 归还 Segment 空间。RPC 返回的 `AllocatedBuffer::Descriptor` 只提供 §4.1～§4.2 所述的远端定位信息；`DISK` 和 `LOCAL_DISK` Replica 不持有 AllocatedBuffer，分别使用文件路径或本地 SSD holder/endpoint 描述数据位置。

#### Replica 与物理介质

`Replica` 位于 Master 的元数据内存中，它通过 `variant` 保存一种介质相关状态，但不内联保存对象字节：

| Replica 类型 | Master 中保存的定位信息 | 实际数据位置 |
|--------------|-------------------------|--------------|
| `MEMORY` | `unique_ptr<AllocatedBuffer>` | 某个 Store Client 挂载的 DRAM/CXL Segment 区域 |
| `NOF_SSD` | `unique_ptr<AllocatedBuffer>` | NVMe-oF Segment 中分配的块区域 |
| `DISK` | `file_path + object_size` | StorageBackend 管理的文件 |
| `LOCAL_DISK` | `client_id + endpoint + object_size` | 指定 Client 的本地 SSD，由其 FileStorage 管理 |

因此 `Replica::data_` 应理解为“介质特定的资源句柄或位置元数据”，不是对象 payload。Memory Replica 中的 `AllocatedBuffer::buffer_ptr_` 指向实际 Segment 区域；Master 主要管理该地址范围的分配所有权，数据面由 Client 和 Transfer Engine 直接访问。

一个对象的每个 Replica 都保存完整 value：

```text
ObjectMetadata[key]
  ├── Replica A → Node A Segment 中的完整 value
  ├── Replica B → Node B Segment 中的完整 value
  └── Replica C → SSD/文件中的完整 value
```

#### Slice 与副本存储

Slice 只包含 `ptr + size`，不拥有内存，也不知道 key、Segment 或 endpoint。它描述的是一次 Put/Get 的本地 scatter/gather 内存布局，而不是 Replica 的持久化分片。

Put 时，Client 将所有 Slice 长度交给 PutStart；当前 Master 按其总长度为每个目标 Replica 分配一个可容纳完整对象的连续 `AllocatedBuffer`。随后 `TransferSubmitter` 按累计偏移把每个 Slice 映射到同一个 Replica：

```text
本地 Slice 0 ──→ replica_base + 0
本地 Slice 1 ──→ replica_base + size(slice 0)
本地 Slice 2 ──→ replica_base + size(slice 0) + size(slice 1)
```

对于 Memory Replica，每个非空 Slice 通常生成一条 `TransferRequest`：

```cpp
request.source = slice.ptr;
request.target_offset = handle.buffer_address_ + accumulated_offset;
request.length = slice.size;
```

写入多个 Replica 时，同一组 Slice 会分别完整写入每个 Replica，而不是把不同 Slice 分配给不同 Replica。Get 的方向相反：从选中 Replica 的连续地址范围依次填充调用方提供的 Slice。

```text
一个 Object
  └── N 个 Replica，每个保存完整 value

一次 Put/Get
  └── M 个 Slice，按顺序映射到其中一个或每一个 Replica
```

Store 层的 Slice 与 Transfer Engine 内部网络分片也不是同一概念：Store Slice 描述调用方内存，转换成 `TransferRequest` 后，具体 Transport 还可以为多 NIC、对齐或流水线进一步切分请求。NoF 路径则要求 Slice 能合并为连续范围，并按设备块大小执行对齐的 SPDK I/O。

因此，多个 Slice 不代表多个 key 或多个 Replica；Slice 边界也不要求与 allocator 的物理分配粒度或 KVCache Page 边界一致。

### 4.6 KVCache Block 与 Store Object 的映射

Mooncake Store 是通用对象存储，只强制下面的关系：

```text
一个 Store Key
  → 一个 ObjectMetadata / 完整 value
      → 一到多个 Replica
          → 每个 Replica 保存完整 value 的一份物理副本
```

Store 不理解 Token、Layer、K/V Tensor、Page Table 或 KVCache Block 边界。因此“一个 Replica 对应一个 paged KVCache”不是 Store 层的强制语义；KVCache 与 Object 的粒度由上层推理框架决定。

#### 论文中的典型建模

Mooncake 论文将 KVCache 分成固定 Token 数量的 Block，并为每个 Block 生成结合自身内容与 Prefix 关系的哈希 key。若上层按 Block 独立 Put，则形成典型的一对一映射：

```text
一个请求的 Paged KVCache
  ├── KVCache Block 0 → Store Object key_0 → Replica 0A, 0B, ...
  ├── KVCache Block 1 → Store Object key_1 → Replica 1A, 1B, ...
  └── KVCache Block 2 → Store Object key_2 → Replica 2A, 2B, ...
```

对应基数为：

```text
Request KVCache   1 : N   KVCache Blocks
KVCache Block     1 : 1   Store Object       （论文典型建模）
Store Object      1 : M   Replicas
```

此时可以说“一个 Replica 是一个 KVCache Block 的一份完整物理副本”，但不能说“一个 Replica 对应整个请求的 Paged KVCache”。整个请求通常由多个 Block/Object 组成。

#### Store 允许的其他建模

一对一不是 Store 的硬约束，上层也可以选择：

| 建模方式 | Object 内容 | 影响 |
|----------|-------------|------|
| 一个 Block 一个 Object | 单个 KVCache Block | 去重、复制和逐出粒度最细，符合论文典型语义 |
| 多个 Block 聚合为一个 Object | 多个 Block 的连续或序列化字节 | 管理开销较低，但复制、逐出和热点统计粒度变粗 |
| 一个 Block 拆成多个 Object | 按 Layer、K/V 或其他布局拆分 | 可独立管理局部数据，但 key 和元数据数量增加 |

无论上层选择哪种映射，Store 内部始终保持：

```text
Object 是元数据与生命周期单位
Replica 是 Object 的完整物理副本
AllocatedBuffer 是内存型 Replica 的存储区域
Slice 是 Put/Get 的临时传输视图
TE 内部 Chunk 是网络执行粒度
```

热点复制增加的是同一 Object 的 Replica 数量，而不是在一个 Replica 内部嵌套 Replica；Slice 切分也不会改变 Object 或 Replica 的数量。

当前 SGLang HiCache 并非严格采用“一 Page 一 Object”：逻辑 Page 可能按 K/V、rank、stage 或 head shard 展开为多个物理 Object。该映射、链式 Page Hash 和连续前缀命中统一放在 §6 说明；本章只保留 Store 不理解 Page 语义这一通用边界。

### 4.7 从逻辑查找到数据通路

Store 的通用对象访问链路为：

```text
TenantId + Object Key
  → ObjectMetadata
  → replicas_: vector<Replica>
  → 选择一个可用的 COMPLETE Replica
  → 取得介质相关定位信息
  → 选择或复用数据通路
  → 实际对象字节
```

`ObjectMetadata` 是 Master 视角下的逻辑对象索引与生命周期管理单元，不是物理地址；只有上层把 KVCache Block/Page Hash 用作 Store key 时，该 hash 才成为查询入口。

Replica 完成从逻辑对象到一份物理副本的映射。查询后还需要过滤或选择副本，例如检查 `COMPLETE` 状态、介质类型、本地性、endpoint 可用性和拓扑评分。不同介质随后进入不同路径：

| Replica 类型 | 定位描述 | 数据通路 |
|--------------|----------|----------|
| `MEMORY` | `AllocatedBuffer::Descriptor` 中的 endpoint、address、size、protocol | 同进程 memcpy，或 Transfer Engine 的 TCP/RDMA/EFA 等 Transport |
| `NOF_SSD` | NoF `AllocatedBuffer::Descriptor` | SPDK/NVMe-oF I/O |
| `DISK` | `file_path + object_size` | StorageBackend 文件 I/O |
| `LOCAL_DISK` | holder client、RPC endpoint、object size | ClientRequester → 远端 FileStorage → 临时 Buffer → TE |

Memory/NoF 路径中，Master 内部 Replica 持有 `AllocatedBuffer` 的资源所有权，Client 通过 RPC 获得的只是 `AllocatedBuffer::Descriptor`。Descriptor 提供定位信息，不拥有内存，也不保护其生命周期。

`transport_endpoint_` 用于调用 `TransferEngine::openSegment()` 定位远端 Segment/服务端点；`buffer_address_` 用于确定该 Segment 下具体副本区域。TE 通常会打开或复用已有 SegmentHandle、Transport 连接和 endpoint pool，而不是为每个 AllocatedBuffer 单独新建一条 RDMA/TCP 信道：

```text
多个 AllocatedBuffer
  → 可能属于同一个 Segment / endpoint
  → 复用同一套 Transport 连接资源
  → 每次操作提交独立 TransferRequest
```

因此，这条链路的准确表述是：

> `TenantId + Object Key` 定位 ObjectMetadata；ObjectMetadata 关联多个 Replica；选定 Replica 后，根据介质取得 AllocatedBuffer Descriptor、文件路径或 holder endpoint；数据面再选择或复用 memcpy、TE、SPDK 或文件 I/O 路径完成传输。

---

## 5. Transfer Engine 补充

### 5.1 init() 初始化流程

**位置：** `transfer_engine_impl.cpp`

初始化分五个阶段：

1. **setFilesLimit()** — RDMA 需要大量文件描述符，调高进程 fd 上限。
2. **RPC 地址绑定** — 三种模式：Legacy（用户指定端口）、P2P（随机端口 + 本地 daemon，去中心化）、New（自动探测 IP + 随机端口，推荐）。
3. **强制 TCP 检查** — 若 `MC_FORCE_TCP` 环境变量设置，仅安装 TCP transport 并返回（用于调试节点）。
4. **平台协议安装** — 按优先级递减安装：nvlink_intra > nvlink > rdma/barex > tcp（fallback）。编译时 `#ifdef` 可引入 Ascend / UBSHMEM / HIP 等额外协议。
5. **集群注册** — `metadata_->addRpcMetaEntry()` 将本节点注册到集群元数据服务（etcd/HTTP/P2P）。

### 5.2 multi_transports_ — 多协议管理

**位置：** `multi_transport.h`

`MultiTransport` 管理一个 `map<string, shared_ptr<Transport>> transport_map_`，将协议名（`"rdma"`, `"tcp"`, `"nvlink"` 等）映射到对应的 Transport 子类实例。

| 方法 | 功能 |
|------|------|
| `installTransport(proto, topology)` | 按协议名注册 Transport 子类 |
| `selectTransport(request)` | 根据请求的源/目的地址自动选择最优协议 |
| `submitTransfer(batch_id, entries)` | 将 TransferRequest 列表路由到对应 transport 执行 |

该组件让 Store/EP/PG 等上层模块无需关心底层传输协议，只需调用统一接口。

### 5.3 RDMA 与 CXL 的边界

RDMA 是由 RNIC 执行的跨节点数据搬运机制，在条件满足时可连接远端 DRAM 与本地 VRAM；CXL 是 CPU、设备和内存扩展设备之间的内存互连。Mooncake 当前 `CxlTransport` 通过 DAX `mmap` 建立地址映射，以 `cxl_base_addr + offset` 定位空间，并使用同步 `std::memcpy` 读写，因此不能简单视为 RDMA 热路径的替代品。

工程判断应以拓扑和消费端为准：延迟敏感的热 KVCache 优先 VRAM 或 GPUDirect RDMA；CXL 更适合作为容量池化、CPU 可直接消费或可提前预取的数据层。其带宽、故障域、一致性和多主机共享能力取决于具体硬件、Fabric、内核及 DAX 配置；普通 `std::memcpy` 也不能把 CUDA device pointer 当作通用目标，CXL→VRAM 通常仍需额外路径。

---

## 6. HiCache 缓存层级

缓存层级编号属于 SGLang/HiCache 等上层系统的部署语义，不是 Mooncake Store 的固定类型定义。本文统一采用下面的四层口径；阅读其他文档时仍需以其具体定义为准：

| Tier | 典型介质与范围 | 主要管理者 | 典型访问路径 |
|------|----------------|------------|--------------|
| L1 (Hot) | 当前 Pod 的 GPU VRAM | 推理引擎的 GPU KVCache/Page 管理器 | GPU 本地访问 |
| L2 (Warm) | 当前 Pod 专有的 CPU DRAM | 推理引擎/HiCache | H2D/D2H、本机内存访问 |
| L3 (Shared) | Store 节点贡献的共享 DRAM | Mooncake Master + Store Client | 当前 HiCache Mooncake backend：RDMA 到 L2 Host Page |
| L4 (Cold) | SSD、NVMe-oF、DFS 文件 | Mooncake Store/文件后端 | GDS、SPDK、文件 I/O 或带 staging 的回退路径 |

这里的 `VRAM` 描述 GPU 可直接寻址的设备内存，`HBM` 描述其常见物理实现；数据中心推理卡通常以 HBM2e/HBM3/HBM3e 等作为 VRAM，其他 GPU 也可能使用 GDDR。因此推荐写作“L1 位于 GPU VRAM，在主流数据中心计算卡上通常由 HBM 实现”，而不是把 VRAM 与 HBM 当作两个并列层级。

还需避免与 GPU 芯片内部硬件缓存混淆：本文的 L1/L2/L3/L4 是 KVCache 软件存储层级；GPU 内部的 `SM L1 cache → GPU L2 cache → HBM` 是另一套微架构层级。KVCache Page 长期驻留在 HBM 地址空间，kernel 访问时其数据才可能暂时进入硬件 Cache、Shared Memory 或寄存器。

### 6.1 本地索引与分布式对象名

SGLang 内并非所有索引都依赖分页哈希链。需要区分两个层次：

| 层次 | 输入与输出 | 主要结构 |
|------|------------|----------|
| L1/L2 本地前缀索引 | 请求 token prefix → 本地 GPU/Host Page | `HiRadixTree` |
| L3 分布式对象命名 | token pages → 稳定的 Mooncake Object Key | 链式分页哈希 |

HiRadixTree 节点可以同时关联两种本地位置：

```text
HiRadixTree Node
├── key/token prefix
├── value       → L1 GPU Page indices
└── host_value  → L2 Host Page indices
```

L1 Page 被淘汰但 L2 备份仍存在时，同一节点可以保留 `host_value`。L2 也淘汰后，本地树节点可以消失；这不表示 L3 对象无法再发现，因为相同请求仍可从 token pages 确定性地产生相同的 Mooncake key。

Mooncake L3 没有同步一棵全局 RadixTree。这样可以避免多个 SGLang 实例持续同步树节点、页位置和逐出状态；跨实例共享改为使用稳定 key 的精确对象查询。

### 6.2 Page 粒度、链式哈希与物理 Object

`page_size` 定义一个逻辑 Page 覆盖的连续 token 位置数。Page 是 SGLang/HiCache 的缓存分配、命中、预取、回写和逐出单位，不是 Mooncake Store 或 Transfer Engine 的协议单位。固定 Page Pool 避免不定长分配，也能批量合并对象查询和 I/O；Page 越大，元数据开销越低但部分匹配更难复用，Page 越小则相反。

设一个请求按 `page_size` 划分为 `P0、P1、P2`，逻辑 page key 可抽象为：

```text
H0 = Hash(P0)
H1 = Hash(H0, P1)
H2 = Hash(H1, P2)
```

实际输入是 token IDs 和前一页 hash，而不是 KVCache payload 字节。前一页 hash 参与下一页计算，使 key 同时表达“当前 page 内容”和“此前 prefix”：

```text
相同当前 Page + 不同历史 Prefix → 不同 Page Key
相同完整 Prefix + 相同 Page 边界 → 相同 Page Key
```

这也是不同 SGLang 实例能够共享 KVCache 的基础。只要模型/并行布局的命名空间、token 序列、哈希算法和 page 边界一致，就会重新得到同一组逻辑 page keys。

一个逻辑 Page 不一定等于一个 Store Object。当前 SGLang Mooncake backend 会根据模型布局把逻辑 key 展开为物理 keys：

```text
Logical Page H_i
├── 普通 MHA：H_i + model/rank tag + K suffix
├── 普通 MHA：H_i + model/rank tag + V suffix
├── MLA：通常为一个主 Object
└── split-head / TP / PP：可能继续展开为更多 Object
```

因此命中判断按完整逻辑 Page 对齐，而 Mooncake 精确查询和保存的是展开后的物理 Object。Store Slice 可只覆盖 K、V、Layer 或 head shard 等张量分量，但这些分量仍可对应同一组 `page_size` token；TE 后续的字节切片已不再携带 Page 语义。

### 6.3 写入 Mooncake 时谁计算哈希

哈希由 SGLang 根据 token page 计算，Mooncake 不根据传入的 KVCache 数据重新计算：

```text
SGLang
  ├── token pages → 逻辑 page hash
  ├── 逻辑 hash → K/V 等物理 Object Keys
  └── BatchSet(key, KV bytes)
                         │
                         ▼
Mooncake
  ├── 保存 key → ObjectMetadata
  └── 保存 ObjectMetadata → Replicas
```

Mooncake Store 是通用字节对象存储，不知道 token、attention prefix 或 K/V 的语义，因此也没有足够信息自行重建这条哈希链。

“淘汰到 Mooncake”也不一定表示唯一副本在发生同步迁移。根据 HiCache 的 `write_through`、selective 或 `write_back` 策略，L3 备份可以在 L1/L2 淘汰之前建立，并在一段时间内与本地副本共存。淘汰改变的是某层副本是否仍驻留，不改变该 Page 的逻辑 key。

### 6.4 查询与连续前缀命中

假设本地 HiRadixTree 已命中 `P0`，而 `P1/P2/P3` 不在本地：

```text
P0：L1/L2 命中，已有 H0
P1：计算 H1 = Hash(H0, P1)
P2：计算 H2 = Hash(H1, P2)
P3：计算 H3 = Hash(H2, P3)
```

若本地节点保留了最后一个匹配页的 hash 状态，可以从 `H0` 继续计算；实例重启或本地树完全没有命中时，也可以从请求开头重新计算。哈希是确定性的，两种路径得到相同结果。

SGLang 将逻辑 keys 展开为物理 keys 后调用 Mooncake 的批量存在性查询。Mooncake Master 的查询步骤是：

1. 将 `TenantId + Object Key` 映射到 1024 个 `MetadataShard` 之一。
2. 在该 shard 的 `TenantState::metadata` 哈希表中执行精确 `find(key)`。
3. 检查对象是否至少存在一个 `COMPLETE`、可读的 Replica。
4. 返回每个物理 key 的存在状态。

这不是相似度搜索、全表扫描或按访问频率筛选，而是 `computed_key == stored_key` 的精确匹配。平均查询成本近似为：

```text
O(候选逻辑 Page 数 × 每 Page 物理 Object 数)
```

一个逻辑 Page 只有在所需物理对象全部存在时才命中。例如普通 MHA：

```text
H0_K = exist, H0_V = exist  → P0 命中
H1_K = exist, H1_V = exist  → P1 命中
H2_K = exist, H2_V = miss   → P2 不命中
H3_K = exist, H3_V = exist  → 不会跨过 P2 使用 P3
```

HiCache 只接受从请求起点开始的最长连续命中前缀；遇到第一个缺失 Page 即停止。后面的 Object 即使存在，也不能绕过缺口直接复用，因为 Attention 所需 KV prefix 必须连续。

### 6.5 端到端查询与传输

“通过 Transfer Engine 查询 L3”是不准确的。Mooncake 将对象查询和字节传输分开：

```text
控制面 / 对象索引：
Page Key
  → Mooncake Store Client
  → Master RPC: BatchExistKey / BatchGetReplicaList
  → ObjectMetadata
  → Replica Descriptor

数据面：
Replica endpoint + remote address + size
  → Transfer Engine openSegment()
  → RDMA/TCP/本地 memcpy
  → SGLang 已注册的 L2 Host Page
  → H2D
  → L1 GPU Page
```

Master 的 `ObjectMetadata` 记录对象生命周期和 Replica 列表；Replica Descriptor 再提供介质类型、endpoint、远端地址、大小或文件位置。Transfer Engine 不理解 token prefix，也不负责判断 key 是否存在；它在 Client 已取得 Replica Descriptor 后才负责搬运数据，Master 不进入大块 payload 的数据路径。

若命中的是 Mooncake DRAM Replica，当前 HiCache backend 的恢复路径是：

```text
Mooncake L3 DRAM Replica
  → TE/RDMA 直接写入 SGLang 注册的 L2 Host Page
  → cudaMemcpyAsync 或 GPU-assisted I/O kernel
  → SGLang L1 GPU Page
  → 完成事件后更新 Page Table / HiRadixTree
```

这里的“零拷贝”仅指 L3 数据直接进入目标 Host Page，省去 `RealClient 临时 Buffer → SGLang Host Page`；L2→L1 的 H2D 仍然存在。`page_first_direct` 优化 Host 布局和 H2D 聚合，也不表示 L3 直接写 VRAM。

TE 本身可以注册 VRAM，PD 直接交接也可走 `Prefill VRAM → Decode VRAM`，但那是另一条路径，不能用来描述当前 HiCache L3 Get。若对象已 offload 到 SSD，Object Key 仍不变，只是 Replica 类型和读取通路切换为 Local Disk、文件后端或 NoF/SPDK。

### 6.6 匹配索引与缓存生命周期

高命中对象生存期更长确实存在，但它属于逐出、Pin 和晋升策略，不承担匹配：

| 机制 | 作用 | 是否用于 key 匹配 |
|------|------|-------------------|
| HiRadixTree | 本地 token prefix → L1/L2 Page | 是，本地前缀匹配 |
| 链式 page hash | 生成跨实例稳定 Object Key | 是，生成查询 key |
| Master 分片哈希表 | Object Key → ObjectMetadata | 是，精确对象查找 |
| LRU/LFU/SLRU 等 | 选择 SGLang L1/L2 淘汰候选 | 否 |
| lease / soft pin / hard pin | 控制 Mooncake 对象可淘汰时间 | 否 |
| Count-Min Sketch promotion | 判断 SSD 热对象是否晋升 DRAM | 否 |

Mooncake DRAM 淘汰到 SSD 时，通常保持相同 Object Key，只更新或增加 Replica 的介质和位置描述。因此重新查询仍先走相同的哈希表精确匹配；命中后再根据 Replica 类型选择 TE、文件 I/O 或 SPDK 数据路径。

整体关系可以压缩为：

```text
HiRadixTree          请求 prefix → 本地 Page
链式分页哈希          请求 prefix → 分布式 Object Key
Mooncake Master      Object Key → ObjectMetadata → Replica
生命周期策略          决定 Replica 在哪一层、保留多久
Transfer Engine      Replica 地址 → 实际 KV 数据
HA                   保证上述 Master 控制面可被其他实例接管
```

继续核对上游实现时，优先阅读：

- [SGLang HiCache System Design and Optimization](https://github.com/sgl-project/sglang/blob/main/docs_new/docs/advanced_features/hicache_design.mdx)
- [SGLang Mooncake backend](https://github.com/sgl-project/sglang/blob/main/python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py)

---

## 7. Client 接口层与运行时架构

### 7.1 接口层次与阅读路径

**位置：** `mooncake-store/include/pyclient.h`

`PyClient` 是面向上层调用者的抽象接口，定义了 Store Client 对外提供的能力，例如初始化、Put/Get、批量操作、范围读取、对象删除、健康检查和副本任务。它本身不实现实际的数据传输，而是规定高层接口契约。

`RealClient` 继承 `PyClient`，负责把这些高层接口落实到底层 `mooncake::Client`、传输内存、SSD Offload 和 Dummy/Real 共享内存机制上。因此当前推荐阅读路径是：

```text
PyClient（先看对外接口和公共成员）
    ↓
RealClient 声明（看实现范围和内部状态）
    ↓
RealClient::setup_internal（看四个核心组件如何建立）
    ↓
put/get 主路径（看四个组件如何协作）
```

对应文件：

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1 | `mooncake-store/include/pyclient.h` | 高层接口契约，以及四个核心共享对象 |
| 2 | `mooncake-store/include/real_client.h` | `RealClient` 的公开 API、内部辅助方法和状态 |
| 3 | `mooncake-store/src/real_client.cpp` | 初始化、内存读写、SSD Offload、共享内存和清理流程 |
| 4 | `mooncake-store/include/client_service.h` | 底层 `mooncake::Client` 的 Store 编排能力 |

### 7.2 RealClient 与底层 Client 的职责边界

`RealClient` 是 `PyClient` 相对于 DummyClient 的完整实现，不是单纯的 RPC Stub。它负责把上层 API 适配到底层 Store 运行时，并管理进程内资源和辅助服务：

- 创建并持有一个底层 `mooncake::Client`；
- 管理 Segment、本地注册内存和资源清理；
- 适配返回值、错误和 Python/Binding 接口；
- 处理 Dummy/Real 共享内存映射；
- 提供本地 SSD Offload，并请求远端节点的 SSD 数据；
- 启动 IPC、HTTP 和 Offload RPC 服务。

`RealClient` 与成员 `client_` 通常处于同一进程，二者之间是函数调用而非网络跳转：

```text
同一 SGLang / Store Client 进程
  └── Python Binding / Mooncake Connector
        └── RealClient                 高层适配、资源与服务管理
              └── mooncake::Client     Store Put/Get 编排运行时
                    ├── MasterClient   控制面 RPC
                    └── TransferEngine 数据面传输
```

```cpp
RealClient::put(...)
  → put_internal(...)
  → client_->Put(...)
```

真正的网络边界位于：

```text
控制面：mooncake::Client → MasterClient → Master RPC
数据面：mooncake::Client → TransferEngine → 远端 Segment
SSD 特殊路径：RealClient → ClientRequester → 远端 RealClient
```

SGLang 通过 Binding/Connector 主动调用 RealClient；Master 不向推理进程主动推送 KV 数据。Put 三阶段流程见 §3，对象查询和介质数据路径见 §4.7，HiCache L3 Get 见 §6.5。本章后续只解释 RealClient 如何持有这些能力。

三种“调度”需要区分：

| 层次 | 决策者 | 职责 |
|------|--------|------|
| 推理请求调度 | SGLang/Conductor | 选择 Prefill/Decode Pod、Prefix 复用与热点迁移 |
| 副本与空间调度 | Master | 选择 Segment、分配 AllocatedBuffer、管理 Replica/Lease/逐出 |
| 数据传输调度 | Client/TransferEngine | 选择 Replica 和 memcpy/TCP/RDMA/EFA/SPDK 等数据路径 |

普通 Memory Replica 已通过预注册 Segment 暴露给 TE，远端 holder 通常不需要为每次 RDMA 读取额外“就位”；`LOCAL_DISK` 才需要 `ClientRequester` 与 holder 的 Offload RPC 配合。

### 7.3 核心运行时组件

```cpp
std::shared_ptr<mooncake::Client> client_ = nullptr;
std::shared_ptr<mooncake::ClientRequester> client_requester_ = nullptr;
std::shared_ptr<mooncake::FileStorage> file_storage_ = nullptr;
std::shared_ptr<ClientBufferAllocator> client_buffer_allocator_ = nullptr;
```

四者分别覆盖核心 Store、远端 SSD 请求、本地 SSD 服务和传输内存四个层次：

| 成员 | 核心职责 | 典型调用 | 创建条件 |
|------|----------|----------|----------|
| `client_` | Store 控制面、数据面和对象生命周期的核心客户端 | `Query`、`Put`、`Get`、`BatchGet`、Segment 管理 | `setup_internal()` 成功创建底层 Client 后 |
| `client_requester_` | 向其他节点的 RealClient 发起 SSD Offload RPC | `batch_get_offload_object`、`release_offload_buffer` | `setup_internal()` 中创建 |
| `file_storage_` | 管理本节点 SSD Offload、临时 Buffer 和后台任务 | `Init`、本地 SSD 批量读取、`ReleaseBuffer` | 仅启用 `enable_ssd_offload` 时 |
| `client_buffer_allocator_` | 管理默认的本地 CPU 工作内存池，并进行线程安全子分配；它可作为 TE 端点，但不是所有传输的必经端点 | `allocate`、`getBase`、`size` | 初始化本地 Client Buffer 时 |

#### client_：核心 Store Client

`client_` 是常规 Store 操作的核心入口。它连接 Master，查询元数据，编排对象的 Put/Get，并通过 TransferEngine 执行实际的数据传输。

CPU Buffer 返回型/拷贝型 API 的典型写入路径：

```text
RealClient::put
  → put_internal
  → ClientBufferAllocator 分配临时传输内存
  → 将输入数据拷贝到注册内存
  → split_into_slices
  → client_->Put
```

CPU Buffer 返回型 API 的典型读取路径：

```text
RealClient::get_buffer
  → client_->Query
  → SelectBestReplica
  → ClientBufferAllocator 分配目标内存
  → client_->Get
```

这些示例只适用于由 RealClient 自动分配返回空间的便利 API。调用方也可以向 `batch_get_into` 等接口提供已注册的外部 Buffer；底层 `Client` 同时编排 Master 元数据请求和具体介质的数据路径，并非单纯的 RPC 代理。

#### client_requester_：远端 RealClient RPC 请求器

`client_requester_` 的功能比 `client_` 窄，主要服务于远端 `LOCAL_DISK` 副本读取。它维护 coro_rpc 连接池，向保存 SSD 副本的远端 RealClient 发起请求。

```text
当前 RealClient
  → client_requester_->batch_get_offload_object(...)
  → 远端 RealClient 的 Offload RPC Server
  → 远端 file_storage_ 从 SSD 读取
  → 数据传回当前节点
  → client_requester_->release_offload_buffer(...)
```

因此，`client_` 是通用 Store Client，而 `client_requester_` 是节点间 SSD Offload 的专用 RPC Client。

#### file_storage_：本节点 SSD Offload 管理器

`file_storage_` 只在启用 SSD Offload 时创建，负责：

- 初始化本地 `StorageBackend`；
- 从本地 SSD 批量读取对象；
- 分配并保存读取结果所需的临时传输 Buffer；
- 使用 `batch_id` 跟踪临时 Buffer；
- 在远端完成传输后释放 Buffer；
- 运行 Buffer GC、Heartbeat 等后台线程；
- 记录 SSD 指标。

它内部也保存一份 `shared_ptr<Client>`，因为从 SSD 取出数据后仍需要借助底层 Client/TransferEngine 将数据发送到请求节点。

#### client_buffer_allocator_：默认 CPU 工作内存池

`client_buffer_allocator_` 通常从当前 Pod 的 CPU DRAM 中取得一块连续区域，并通过 offset allocator 在其中执行线程安全的子分配。普通协议下该区域会注册给 TransferEngine，因此适合作为 TCP/RDMA 等传输操作的源或目标 Buffer。它在物理上可以属于 Pod 的 L2 DRAM，但其默认职责是 API 工作区或 staging pool，并不天然等同于由 SGLang 索引、长期保留的整个 L2 KVCache。

```text
ClientBufferAllocator 管理的连续内存
┌──────────────────────────────────────┐
│ [Buffer A] [空闲区域] [Buffer B] ... │
└──────────────────────────────────────┘
```

每次 `allocate()` 返回一个 move-only 的 `BufferHandle`。`BufferHandle` 使用 RAII，在析构时自动归还对应的子区域。

外部已注册 Buffer 可以绕过该池：HiCache L3 Get 提供 SGLang 自己的 L2 Host Page，PD 直接交接则可以提供 GPU Slice；两条路径的区别见 §6.5。`client_buffer_allocator_` 主要服务于：

- `get_buffer()` / `batch_get_buffer()` 等由 RealClient 自动分配返回空间的接口；
- GPUDirect RDMA 不可用时的 Host staging；
- 普通 CPU 对象、布局转换和生命周期解耦；
- 不能直接写入 GPU 的文件、SSD 或其他后端回退路径。

源码允许 `local_buffer_size == 0`。只有在调用链完全使用外部提供且已注册的 Buffer、并且不依赖上述 CPU Buffer API 和回退路径时，才适合将默认池设为零或极小；否则内部 `allocate()` 会失败。

### 7.4 共享所有权与资源生命周期

这里的 `shared_ptr` 主要表达跨组件或跨返回值的共享生命周期，而不是表示一个 RealClient 可以同时使用多个底层 Client。

主要的共享关系有两类：

```text
RealClient ──shared_ptr──┐
                        ├──> Client
FileStorage ─shared_ptr──┘
```

`FileStorage` 在后台线程或 SSD 传输路径仍使用 `Client` 时，底层 Client 不能提前析构。

```text
RealClient ───────────────┐
                          ├──> ClientBufferAllocator
存活的 BufferHandle ──────┘
```

即使 `RealClient` 已释放自己的 allocator 引用，只要调用方还持有 `BufferHandle`，allocator 及其底层内存就必须继续存活，避免 `BufferHandle::ptr()` 变成悬空地址。

从概念上也可以使用 `unique_ptr + 非拥有型引用` 实现部分关系，但这样会把析构顺序变成隐含约束。当前实现通过引用计数显式保证异步组件和返回 Buffer 使用期间资源仍然有效。

### 7.5 存储贡献者、请求者与介质边界

#### Master“挂载”Client Segment 的准确含义

Master 不会将 Client DRAM 映射进自己的进程地址空间。提供存储容量的 Client 先在本地分配内存并注册给 Transfer Engine，再将 Segment 元数据登记到 Master：

```text
Client 本地分配 global segment
  → TE registerLocalMemory(remote_accessible=true)
  → 构造 Segment{id, base, size, protocol, host_id, te_endpoint}
  → MasterClient::MountSegment
  → Master 创建 MountedSegment 和 BufferAllocator
```

实际字节仍位于 Client 节点；Master 只持有 Segment 地址范围、owner、endpoint 和分配状态，并在 PutStart 时从中分配 `AllocatedBuffer`。

#### Provider 与 Requester 是角色，不是两种 Client 类型

源码没有独立的 `StorageClient` 和 `RequesterClient` 类型。二者都由同一个 `mooncake::Client` 实现，一个实例可以同时承担两种角色：

| 运行时角色 | 主要行为 |
|------------|----------|
| Storage Provider | 调用 `MountSegment`，贡献可远程访问的全局 Segment，承载 Memory Replica |
| Requester | 调用 `Put/Get/Query`，取得 Descriptor，使用本地 Slice 和 TE 读写副本 |

`RealClient::setup_internal` 中两类内存体现了这一差异：

| 配置/对象 | 注册方式 | 是否进入 Master Store Pool | 用途 |
|-----------|----------|----------------------------|------|
| `local_buffer_size` / `client_buffer_allocator_` | 普通协议下调用 `RegisterLocalMemory(..., remote_accessible=false)` | 否 | 当前 Pod 的默认 CPU 工作区、staging Buffer 和 CPU 返回型 API 内存池 |
| `global_segment_size` / mounted Segment | `MountSegment`，内部注册为 `remote_accessible=true` | 是 | 集群共享容量，保存 Memory Replica |

由此可以形成三种部署角色：

```text
纯请求 Pod：local_buffer_size > 0，global_segment_size = 0
存储贡献 Pod：global_segment_size > 0
混合 Pod：local_buffer_size > 0，global_segment_size > 0
```

这些只是配置形成的角色差异，不是不同的 C++ Client 定义。

此表只描述 RealClient 自建的两类内存。SGLang 提供的外部 GPU KVCache Buffer 是第三类重要端点：它属于推理引擎管理的 L1 VRAM，可以注册到 TE 并直接作为 Put/Get Slice，但不会因此成为 Master Store Pool 中的 Replica 空间。

#### Store 介质类型与 GDS Transport

`L3/L4` 是上层缓存层级，Mooncake Store 的 Replica 类型是 `MEMORY / NOF_SSD / DISK / LOCAL_DISK`；`GDS` 则是 TENT 面向 `FileSegmentDesc` 的 Transport，不是第五种 Replica。测试或文档中的 “L4 GDS” 必须核实它究竟指 FileSegment + GDS、NoF、普通 Disk，还是 LocalDisk Offload。各磁盘路径和 capability 选择统一见 §8，避免在 Client 章节再次展开。

### 7.6 热点复制：论文机制与当前代码实现

热点复制需要分成两个层次理解：FAST '25 论文描述的是由 Conductor 调度触发的自动热点迁移；当前仓库还提供显式 Copy Task，以及面向 `LOCAL_DISK → MEMORY` 的 promotion-on-hit。三者都会增加或产生副本，但触发条件和目标不同。

#### 论文中的 Cache Load Balancing

**来源：** `FAST25-release/Mooncake-FAST25.pdf` §3.2.1、§4.2、§5.3.3；在线版本：[Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving](https://www.usenix.org/system/files/fast25-qin.pdf)。

论文首先定义了 KVCache Block：KVCache 以分页 Block 存入分布式缓存池，典型大小为 16～512 tokens。每个 Block 使用由自身内容及 Prefix 关系共同确定的哈希 key；同一个 key 可以在多个节点拥有 Replica，以降低热点访问延迟。

论文 §4.2 的关键机制不是“Mooncake Store 直接观察某个 Replica 的命中计数，超过阈值后立即复制”，而是 Conductor 进行全局 Cache-aware 调度时触发的启发式热点迁移：

```text
新请求到达 Conductor
        │
        ├── 查找拥有最长 Prefix Match 的实例 best_instance
        │
        ├── 估算所有 Prefill 实例的：
        │     Transfer Time + Queue Time + Prefill Time
        │
        └── 如果选择了另一个负载更合适的实例 p，且：
              best_len / p.prefix_len > balancing threshold
                    │
                    ▼
              从 best_instance 传输 KVCache 到 p
                    │
                    ▼
              p 将 KVCache 保存在本地
                    │
                    ▼
              同一个 Block Key 新增一份本地 Replica
```

也就是说，热点复制是“调度决策 + 主动迁移 + 本地保留”的涌现结果：热门 Prefix 经常被请求，也更频繁地因为负载均衡被传到其他 Prefill 实例；传输后的 KVCache 在目标实例本地保留，于是该 key 的 Replica 数随访问逐渐增加。

论文中的阈值比较的是最佳远端 Prefix Match 与目标实例本地 Prefix Match 的差距，用来判断是否值得迁移；并非直接比较单个 Replica 的命中次数。论文还明确指出该阈值当时由人工调整。

实验 §5.3.3 显示，在 Conversation 和 Tool&Agent 工作负载中，Top 100 热 key 稳定后几乎在每个 Prefill 实例上都有 Replica；热点更分散的 Synthetic 工作负载则只有较少且波动更明显的副本。这说明热点副本数是访问模式和全局调度共同作用的结果。

#### 当前代码一：显式 CreateCopyTask

当前 Store API 提供 `CreateCopyTask`，调用者可以指定目标 Segment，为已有对象创建新的副本：

```text
调用者 / 上层调度器
    │ create_copy_task(key, target_segments)
    ▼
Master 创建异步 Copy Task
    │
    ▼
ClientTaskManager 执行数据复制
    │
    ▼
目标 Segment 上产生新的 Replica
```

这是一种程序化、目标明确的副本扩展能力，但触发决策来自调用方，不等同于论文中 Conductor 根据请求调度自然形成热点副本的完整策略。

#### 当前代码二：promotion-on-hit

当前 Master 还实现了可选的 `promotion_on_hit`，但它的范围更具体：当正常 Get 路径观察到某个对象只有 `LOCAL_DISK` Replica 时，使用 Master 侧 `CountMinSketch` 估计访问频率；达到 admission threshold 后，将对象从本地 SSD 提升到新的 Memory Replica。

```text
GetReplicaList(key)
    │
    ├── 对象只有 LOCAL_DISK Replica
    ├── promotion_on_hit 已启用
    └── CountMinSketch 估计频率达到阈值
            │
            ▼
      TryPushPromotionQueue
            │
            ▼
      LOCAL_DISK holder 通过 Heartbeat 拉取任务
            │
            ▼
      PromotionAllocStart
      分配 PROCESSING Memory Replica
            │
            ▼
      FileStorage 从本地 SSD 读取并写入新 Buffer
            │
            ▼
      NotifyPromotionSuccess
      Replica 状态变为 COMPLETE
```

其关键限制是：

- 默认关闭，需要配置启用；
- 只针对仅有 `LOCAL_DISK` 副本的对象；
- 目标是 `LOCAL_DISK → MEMORY` 分层提升；
- 若对象已经存在 Memory Replica，则不会通过这条路径继续横向复制更多 Memory Replica；
- 正常 `GetReplicaList` 会更新频率并可能触发 promotion，Admin 只读查询不会。

因此，当前 `promotion_on_hit` 可以描述为“命中频率驱动的存储层级提升”，不能直接等同于论文 §4.2 的“Prefill 节点间热点扩散”。

#### 三种机制对比

| 机制 | 决策者 | 触发依据 | 数据方向 | 主要目标 |
|------|--------|----------|----------|----------|
| 论文 Cache Load Balancing | Conductor | Prefix Match、实例负载、传输时间和 Prefill 时间 | Prefill 实例之间迁移并本地保留 | 分散热点访问和网络负载 |
| `CreateCopyTask` | API 调用者/上层系统 | 调用方显式指定 key 和目标 Segment | 已有 Replica → 指定 Segment | 可控地增加对象副本 |
| `promotion_on_hit` | Master + FileStorage | `LOCAL_DISK`-only 对象的频率估计达到阈值 | 本地 SSD → Memory Segment | 将重新变热的对象提升回内存 |

### 7.7 源码阅读顺序

完成接口层、运行时组件和部署角色的区分后，按以下顺序进入实现：

1. `PyClient`：建立上层 API 契约和公共运行时成员的整体认识。
2. `RealClient::create`、构造函数和析构函数：理解外层实例生命周期。
3. `RealClient::setup_internal`：观察底层 Client、Local Buffer、Global Segment、IPC/HTTP/Offload 服务的初始化顺序。
4. `Client::MountSegmentAndGetId`：区分 TE 内存注册与 Master Segment 登记。
5. `RealClient::put_internal` 和 `Client::Put`：跟踪 `PutStart → TransferWrite → PutEnd`。
6. `RealClient::get_buffer_internal` 和 `Client::Get`：跟踪 Query、副本选择、Buffer 分配和读取。
7. `RealClient::batch_get_buffer_internal`：观察批量路径如何保留输入顺序并区分 Memory、NoF、Disk 与 LocalDisk。
8. `FileStorage`、`ClientRequester`：理解 LocalDisk Offload 与 promotion-on-hit。
9. TENT `SegmentManager`、`TransportSelector`、`GdsTransport`：仅在研究 File Segment/GDS 路径时进入，避免与 Store Memory Segment 混读。
10. `RealClient::tearDownAll_internal`：反向检查服务停止、内存注销、Segment 释放和 shared ownership。

---

## 8. 从三级缓存瓶颈到 L4：TENT FileSegment 与 GDS

本章沿用本笔记的缓存层级：L1 表示 GPU VRAM/HBM，L2 表示 Pod 内 Host DRAM，L3 表示 Mooncake 分布式共享 DRAM，L4 表示 SSD/NVMe、共享文件系统或更冷的持久化存储。该命名是一个分析模型；SGLang、Dynamo 和不同版本的 Mooncake 文档也可能把外部存储称为 L3、G3 或 Disk Pool。

理解 L4 的关键，不是先记住 `FileSegmentDesc` 或 cuFile API，而是先回答两个系统问题：为什么分布式 DRAM 之后还需要一个更慢的层级，以及这个层级为什么不能只靠“增加一种磁盘传输协议”完成。带着这两个问题再阅读源码，Store、TENT 和 GDS 的边界会清楚得多。

本章将沿以下逻辑展开：

```text
长上下文与跨请求复用扩大 KVCache 工作集
  → L1/L2/L3 的容量与成本压力上升
  → 需要用 SSD/共享存储承接更冷但仍有复用价值的数据
  → 现有磁盘路径存在 Host staging、描述符割裂和生命周期断点
  → GDS 提供 File ↔ GPU 的直接快路径
  → TENT 负责路径选择与失败编排，Store 仍负责对象和缓存语义
```

因此，本章的核心结论不是“L4 等于 GDS”，而是：

```text
L4 是一个存储层级和缓存策略问题；
GDS 是 NVIDIA 环境下访问该层级的一条高性能数据路径；
TENT 是在 GDS、io_uring 及其他路径之间进行选择和编排的数据移动层。
```

### 8.1 为什么三级缓存之后仍需要 L4

L1～L3 的共同问题是成本随容量快速上升。L1 最接近计算，却受 GPU HBM 容量约束；L2 可以使用更大的 Host DRAM，但每个实例私有时容易重复保存相同前缀；L3 通过 Mooncake 汇聚分布式 DRAM 并支持跨实例复用，但它仍然是昂贵且易失的内存资源。当长上下文、多轮对话和 Agent 工作流让同一批前缀在较长时间窗口内重复出现时，直接删除冷却后的 KVCache 会把存储压力转化为重复 Prefill 计算，继续保留在 L3 又会挤占更热对象的空间。

L4 的价值正是在两者之间提供一个容量更大、单位成本更低的缓冲区。它不要求所有对象都永久保存，而是让“暂时不值得占用 DRAM、但重新计算仍然昂贵”的对象继续存活。当 L3 出现内存压力时，冷对象可以异步下沉而非立即丢弃；后续请求再次命中时，系统再从 L4 恢复或晋升，从而以较低的存储成本换取 Prefill 计算的减少。本地 NVMe 适合承接单机冷数据，共享文件系统或 NVMe-oF 则进一步扩大容量和共享范围；对于模型权重、Checkpoint 等非 KVCache 数据，文件层也比内存副本更接近其自然的持久化形式。

从成本模型看，L4 准入的依据并不是对象是否“冷”，而是其预期复用收益能否覆盖下沉和恢复开销。若用 `p_reuse` 表示再次命中的概率，`C_recompute`、`C_restore` 和 `C_offload` 分别表示重算、恢复与写入成本，则一个简化的判断条件可以写为 `p_reuse × (C_recompute - C_restore) > C_offload + C_capacity`，其中 `C_capacity` 表示占用 L4 空间带来的机会成本。这不是当前代码中的显式公式，却揭示了 L4 调度的本质：介质容量只是前提，准入、晋升和淘汰策略决定了容量能否转化为有效命中。

这同时说明 L4 不能只被理解为一块 SSD。一个真正可用的 L4 至少要回答：对象放在哪里、何时下沉、何时晋升、满了淘汰谁、写到一半是否可见、设备失败后如何重试，以及读取时应该直接进入 L1 还是先落入 L2。介质只解决容量，Store 的控制面与数据面的共同编排才构成缓存层级。

### 8.2 工程现状与体系边界

Mooncake 并非从零开始建设 L4。当前代码已经存在本地 SSD Offload、传统文件副本、NVMe-oF SSD Pool 和 TENT FileSegment 四条路径。问题在于，它们分别从不同阶段和需求生长出来，使用不同的寻址方式与生命周期，而不是一个统一的 `L4Replica` 抽象：

| 路径 | Store/TE 表示 | Master 持有的寻址信息 | 当前数据路径 |
|------|--------------|-----------------------|--------------|
| RealClient 本地 SSD offload | `LOCAL_DISK` | `client_id + object_size + transport_endpoint` | SSD → holder ClientBuffer → TE/RDMA/TCP → 请求方 DRAM/VRAM |
| 共享 DFS/传统文件副本 | `DISK` | `file_path + object_size` | 文件读取 → CPU 临时 Buffer → scatter/copy 到目标 |
| 共享 NVMe-oF SSD Pool | `NOF_SSD` | `AllocatedBuffer::Descriptor`，即 namespace 中的 offset/length | SPDK NVMe-oF block I/O |
| TENT 文件数据面 | `FileSegmentDesc` | `path + offset + length` | GDS 直达 GPU；失败/不可用时可选 io_uring staging |

四条路径都能回答“数据如何落到磁盘或从磁盘读出”的一部分问题，却没有共同回答“一个 Store Object 如何在 L3 与 L4 之间稳定流转”。`LOCAL_DISK` 通过 RealClient RPC 定位 SSD holder，虽然可以把 holder 的 ClientBuffer 直接 RDMA 到请求方 GPU，但源端 SSD 读取仍然经过 Host Buffer；`DISK` 使用文件路径语义，当前通用读取同样依赖 CPU 临时 Buffer；`NOF_SSD` 则采用 namespace offset 的块地址语义。与三者不同，FileSegment 只是 TENT 的传输目标类型，并非 Store `ReplicaType`，创建它不会自动建立对象、租约和淘汰关系。因此，“Store 已支持 SSD offload”与“Store 已通过 GDS 从 SSD 直达 KVCache”是两个不同命题。

当前缺口首先表现为元数据模型的分裂：holder endpoint、文件路径、namespace offset 与 FileSegment path/offset/length 分别服务于不同路径，上层尚不能用一种 Replica 描述同时完成查询、传输和回收。其次，传输优化与远端可达性尚未统一。LocalDisk 路径以 Host staging 换取跨节点访问，而 TENT 的 `file://` 又要求文件对当前进程可见；远端 SSD、3FS 或 NVMe-oF 必须先由底层存储栈呈现本地文件语义，GDS 本身并不能直接访问 S3 或任意远端磁盘。

更深层的问题是对象生命周期与传输生命周期之间仍有断点。TENT 可以报告字节传输是否完成，却不知道 Replica 何时可发布、KVCache Page 是否仍被 pin、失败 extent 如何回滚，以及 CRC 与版本应在何时提交。同样，读取目标层级也不能被固定为单一路径：前台且即将消费的数据适合直接进入 L1，而复用时间不确定、L1 紧张或可取消的预取更适合先进入 L2。由此可见，L4 的工程核心不是增加协议，而是统一对象语义、可达性和层级决策。

这些问题把下一步工程目标限定得很清楚：不是再增加一条孤立的磁盘读取函数，而是保留 Store 的对象管理能力，同时让 TENT 把不同传输实现组织成可选择、可回退的执行路径。

上述差异表明，L4 的主要矛盾并非磁盘接口缺失，而是对象语义与传输语义尚未统一。在这一背景下，GDS 的价值需要被限定在数据面的范围内。

在 NVIDIA GPU、文件对计算节点可见、文件系统和拓扑满足 GDS 条件时，FileSegment + GDS 是当前代码中最明确的 L4→L1 直接路径。它能够把文件范围与 CUDA Buffer 放入同一个异步 Batch I/O 请求，避免显式的 SSD→Host Buffer→GPU 两段搬运。因此，当目标 KVCache 已经确定会马上进入计算，且 GPU Page 已经分配并能在 I/O 期间保持有效时，GDS 很适合承担前台恢复快路径。

但 GDS 只解决“字节怎样移动”，不解决“字节为什么移动、移动到哪里后应该保留多久”。当 L4 是 S3 等对象存储、数据需要先驻留 L2、Store 需要跨节点选择 holder 或维护多副本时，仍需其他控制面和数据面组件。写入过程中的 checksum、原子可见、崩溃恢复与失败 extent 回收也不属于 GDS；非 NVIDIA 平台则需要 io_uring、SPDK 或厂商特定的 Direct I/O。

因此较可行的演进不是用 GDS 替换整个 SSD Offload，而是把它接入 Store 已有的 L4 管理框架：GPU 目标满足条件时选择 GDS direct；Host 目标、GDS 条件不满足或前台路径降级时选择 io_uring/L2 staging；远端 holder 仍可保留 ClientBuffer + RDMA 路径。这样，GDS 是可优化的执行器，而不是新的数据孤岛。

因此，讨论可行路径时应先确立 Store 与 TENT 的职责分界。一条完整的 L4 路径可以分成五层：

```text
推理框架 / HiCache
  Page 分配、命中判断、计算依赖、L1/L2 准入
        ↓
Mooncake Store
  Object / Replica / lease / eviction / promotion / file layout
        ↓
TENT
  FileSegment / Request / intent / policy / batch / failover
        ↓
GDS / io_uring / SPDK / RDMA
  存储 I/O、staging 与 DMA 编排
        ↓
NVMe / NVMe-oF / filesystem / NIC / PCIe / GPU
  实际设备和传输机制
```

在这个分工中，Store 的写路径应先分配 L4 extent，等待数据写入和校验完成，再发布磁盘 Replica；读路径应先查询 Replica 和目标层级，再交给 TENT 生成 direct 或 staged 执行。TENT 不决定对象是否热门，也不直接修改 Master 元数据；它负责根据请求意图、Buffer 类型、Segment 类型、拓扑和 transport capability 完成一次可靠的数据移动。

这个边界也给出一条现实的接入顺序：先复用 `FileStorage/StorageBackend` 的索引、容量和淘汰，把 TENT 作为 `BatchLoad/BatchOffload` 下方的 executor；随后再逐步把 foreground get、background prefetch、promotion 和 checkpoint 等意图传入 TENT。后面的源码分析，就是在确认这条接入路径已经具备哪些基础、还缺少哪些桥梁。

### 8.3 FileSegment 与 GDS 的执行机制

经典 TE 的核心抽象是已注册 Memory Segment，调用方较早确定 RDMA、TCP、NVLink 等协议。TENT 把异构数据位置统一为：

```cpp
enum class SegmentType { Memory, File };

struct SegmentDesc {
    std::string name;
    SegmentType type;
    std::string machine_id;
    std::string rpc_server_addr;
    std::variant<MemorySegmentDesc, FileSegmentDesc> detail;
};
```

位置：`mooncake-transfer-engine/tent/include/tent/runtime/segment.h`。

由此形成两类寻址模型：

```text
Memory Segment
  → virtual address + length + memory location
  → rkey/CUDA IPC/NVLink 等 transport-specific descriptor

File Segment
  → file path + file offset + length
  → cuFile/io_uring file handle
```

TENT 的价值不只是增加一个 GDS backend，而是将请求描述与具体 transport 解耦：调用方提交 `Request`，运行时再根据 SegmentType、本地指针类型、策略和 capability 选择执行路径。TENT 后续的 slice spraying、QoS、failover 和 staging 也围绕这一统一 Request/Segment 模型展开。

在统一 Segment 模型中，FileSegment 将文件位置压缩为一个很窄的物理描述：

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

当前最直接的入口是：

```cpp
SegmentID file_segment;
engine.openSegment(file_segment, "file:///path/to/kv_cache.data");
```

源码调用链：

```text
TransferEngineImpl::openSegment
  → SegmentManager::openRemote
  → 记录 segment_name ↔ SegmentID
  → 首次查询 Descriptor
  → SegmentManager::getRemote
  → 识别 "file://" 前缀
  → SegmentManager::makeFileRemote
  → stat(path)，要求 S_ISREG
  → FileSegmentDesc{FileBufferDesc{path, st_size, 0}}
```

位置：`mooncake-transfer-engine/tent/src/runtime/segment_manager.cpp`。

该入口的语义边界十分严格。`file://` 只接受当前进程能够 `stat` 的普通文件，而不是远端文件 RPC URI；`makeFileRemote` 将 `machine_id` 设为本机，选择阶段也把 File Segment 视为同机资源。远端 NVMe、NFS、3FS 或其他 DFS 只有先被底层存储栈呈现为本机可见文件，才能进入该路径。当前自动构造的 FileSegment 只包含一个 base offset 为 0 的 `FileBufferDesc`，对象在大文件中的位置由 `Request::target_offset` 补充。由于描述符不含 Store key、KVCache Page、Replica 状态、CRC、租约或淘汰策略，FileSegment 应被理解为数据面寻址单元，而非完整的 L4 缓存元数据模型。

FileSegment 解决寻址之后，数据方向由 TENT Request 表达。其核心字段为：

```cpp
struct Request {
    OpCode opcode;
    void* source;          // 本地内存地址
    SegmentID target_id;   // FileSegment handle
    uint64_t target_offset;// 文件 offset
    uint64_t length;
    int priority;
    TransportType transport_hint;
};
```

`source` 这个名字容易造成误解：它始终是当前进程的本地 Buffer，但数据方向由 opcode 决定。

| opcode | 文件路径语义 |
|--------|--------------|
| `READ` | FileSegment `[target_offset, target_offset+length)` → 本地 `source` Buffer |
| `WRITE` | 本地 `source` Buffer → FileSegment `[target_offset, target_offset+length)` |

一个面向 GPU KVCache 的读取请求可以表示为：

```cpp
Request req{};
req.opcode = Request::READ;
req.source = gpu_kv_buffer;
req.target_id = file_segment;
req.target_offset = object_file_offset;
req.length = object_size;
```

这里的 `length` 和 `target_offset` 是文件字节范围，不带 token、Page 或 Block 语义。SGLang/vLLM 或 Store 必须先把 KVCache 对象映射为确定的文件范围，再生成请求。

#### 路径选择与能力约束

`TransferEngineImpl::prepareSubmit` 会先解析和合并 Request，再通过：

```text
resolveTransport(request, transport_index=0)
  → getTransportType
  → 取得目标 SegmentDesc
  → Platform::getMemoryType(request.source)
  → 构造 SelectionContext
  → TransportSelector::select
```

对 File Segment，SelectionContext 固定为：

```cpp
ctx.segment_type = SegmentType::File;
ctx.same_machine = true;
ctx.local_memory_type = getMemoryType(request.source);
ctx.remote_memory_type = MTYPE_CPU;
ctx.buffer_transports = nullptr;
```

选择器不会只根据 transport 名称判断，而会检查 capability：

```text
本地 CPU Buffer → 要求 transport.caps.dram_to_file
本地 CUDA/ROCm/TPU Buffer → 要求 transport.caps.gpu_to_file
```

当前 capability 实现为：

| Transport | `dram_to_file` | `gpu_to_file` | 结果 |
|-----------|----------------|---------------|------|
| `GdsTransport` | true | true | 可用于 File Segment |
| `IOUringTransport` | true | 非 CPU Platform 时为 true | 可直接或经 Host staging 执行 |
| `RdmaTransport` | false | false | 即使策略中列出 `rdma` 也会被过滤 |

`isGpuType` 在选择器中包含 CUDA、ROCm 和 TPU，但当前 `GdsTransport` 的实际接口是 NVIDIA cuFile，Buffer 预注册也只接受 `cuda:*` location。异构环境中应使用 `local_memory: "cuda"` 等 policy 约束 GDS，不应只依赖宽泛的 `gpu_to_file` capability。

源码中的内建 File policy 是：

```cpp
{GDS, IOURING}
```

仓库示例 `transfer-engine.json` 写成 `gds → io_uring → rdma`，但这不代表当前 RDMA transport 能直接读写 FileSegment。后续若实现支持远端 File 的 RDMA backend，才可能通过 capability 模型真正进入该分支。

路径被选择之前，GDS 还必须同时满足构建期与运行期条件。

构建期，`tent/src/CMakeLists.txt` 检查：

```text
USE_CUDA
  + CUDAToolkit_FOUND
  + libcufile
  + cufile.h
  → 定义 USE_GDS
  → 编译并链接 tent_xport_gds
```

运行期，`TransferEngineImpl::loadTransports` 还要求：

```json
{
  "transports": {
    "gds": { "enable": true }
  }
}
```

如果程序没有显式配置该项，loader 中的读取默认值是 `false`。所以“系统安装了 CUDA/cuFile”不等于 GDS transport 一定被装载。

`GdsTransport` 构造时通过 `std::once_flag` 在进程内调用一次：

```cpp
cuFileDriverOpen();
```

`install()` 随后保存 ControlService、Topology 和 Config，并设置：

```cpp
io_batch_depth_ = conf_->get("transports/gds/io_batch_depth", 32);
caps.dram_to_file = true;
caps.gpu_to_file = true;
```

当前构造函数没有检查 `cuFileDriverOpen()` 的返回值；实际不可用通常会在后续 `cuFileHandleRegister`、`cuFileBufRegister` 或 Batch API 中暴露。

#### 内存与文件注册

应用通过高级注册接口显式提供 CUDA location 时：

```text
MemoryOptions options{.location = "cuda:0"}
TransferEngine::registerLocalMemory(gpu_ptr, size, options)
  → 构造 BufferDesc
  → 遍历已装载 transports
  → GdsTransport::addMemoryBuffer
  → cuFileBufRegister(gpu_ptr, size, 0)
  → BufferDesc.transports.push_back(GDS)
```

对应实现：

```cpp
Status GdsTransport::addMemoryBuffer(BufferDesc& desc,
                                     const MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() != "cuda") return Status::OK();
    auto result = cuFileBufRegister((void*)desc.addr, desc.length, 0);
    ...
    desc.transports.push_back(GDS);
}
```

`GdsTransport::addMemoryBuffer` 当前解析的是调用方传入的 `options.location`，不是自动探测后写入的 `desc.location`。如果使用默认 `registerLocalMemory(gpu_ptr, size)`，`MemoryOptions.location` 保持 `"*"`，该函数会跳过 `cuFileBufRegister`；要走显式预注册路径，需要使用高级接口并提供类似 `"cuda:0"` 的 location。

注销已登记的 GDS Buffer 时调用 `cuFileBufDeregister`。这里还需要区分“推荐的注册快路径”和“接口是否强制注册”：当前 FileSegment 选择逻辑只根据 pointer type 与 transport capability 选 GDS，并不会检查该地址是否已经出现在 `BufferDesc.transports` 中；cuFile 本身也可以处理未显式注册的 Buffer，但可能发生动态注册、内部缓存或 compatibility/staging 行为。因此，`cuFileBufRegister` 是当前 TENT 提供的稳态优化和资源生命周期契约，不应被表述为所有 GDS API 调用的绝对语义前提。

要使该路径稳定地表现为预期的直接数据面，地址必须在操作完成前持续有效，GPU Buffer 应优先成功向 cuFile 预注册，CUDA context、驱动、GPU BAR/PCIe 拓扑和文件系统也必须满足 GDS 条件。若使用未注册 Buffer，则需要单独验证实际路径与性能。上述条件最终都指向同一生命周期约束：上层不得在传输期间回收或重新映射对应的 KVCache Page。

TENT 的 Buffer 注册只建立可访问性，不负责 GPU KVCache Page 的所有权。Page 的分配、引用计数和何时可被 attention kernel 使用仍由推理引擎负责。

与 CUDA Buffer 注册相对应，GDS 在第一次访问 FileSegment 时创建并缓存文件上下文：

```text
SegmentID
  → ControlService/SegmentManager 查 FileSegmentDesc
  → 取当前实现中的 buffers[0].path
  → open(path, O_RDWR | O_DIRECT)
  → CU_FILE_HANDLE_TYPE_OPAQUE_FD
  → cuFileHandleRegister
  → 缓存 CUfileHandle_t
```

当前代码使用 `O_RDWR`，即使调用方只发起 READ，也要求该文件能够以读写方式打开。这是当前实现选择，不是 GDS 协议本身要求所有读路径都必须拥有写权限。

FileContext 使用一个进程级 map 保存 `SegmentID → shared_ptr<GdsFileContext>`，同时为线程维护快照，避免每次请求重新打开并注册文件。析构时执行：

```text
cuFileHandleDeregister
close(fd)
```

这一缓存减少了稳态文件注册成本，但也意味着文件替换、truncate、权限变更或挂载变化需要与 Segment/FileContext 生命周期协调；TENT 不会自动理解 Store 文件版本。

#### 批量执行与完成语义

每个 GDS SubBatch 使用 `CUfileBatchHandle_t`。由于 `cuFileBatchIOSetUp` 成本较高，当前实现维护 handle pool，在 SubBatch 释放后复用 handle。

默认参数：

```text
io_batch_depth = 32
kMaxSliceSize = 16 MiB
```

提交一条 Request 时，GDS transport 按字节把它继续切分：

```text
Request{gpu_ptr, file_offset, length}
  ├── [0, 16 MiB)
  ├── [16 MiB, 32 MiB)
  └── ...
```

每个子请求生成一项 `CUfileIOParams_t`：

```cpp
params.mode = CUFILE_BATCH;
params.opcode = READ ? CUFILE_READ : CUFILE_WRITE;
params.u.batch.devPtr_base = request.source;
params.u.batch.devPtr_offset = slice_offset;
params.u.batch.file_offset = request.target_offset + slice_offset;
params.u.batch.size = slice_length;
params.fh = file_context->getHandle();
```

最后调用：

```cpp
cuFileBatchIOSubmit(batch_handle, num_params, params, 0);
```

这里的 16 MiB 切片是 TENT GDS transport 的内部 I/O 切片，不是 KVCache Page，也不是 Store Object/Replica 的拆分。它仅为满足 Batch API、控制单次 I/O 大小和并行度。

所有展开后的 `CUfileIOParams_t` 数量必须不超过 `io_batch_depth`。默认 depth=32 时，一条大于约 512 MiB 的 Request 会仅因内部子项数超过 batch capacity 而在提交阶段返回 `TooManyRequests`，除非上层预先拆分请求或调大配置。

完成阶段通过 `cuFileBatchIOGetStatus` 拉取 `CUfileIOEvents_t`。每个内部 I/O 的 cookie 保存其所属的 TENT task id，多个内部 slice 被汇总为：

```text
IOParamRange
  ├── count
  ├── complete_count
  ├── transferred_bytes
  └── status
```

只有 `complete_count == count` 时，对应 TENT task 才进入 `COMPLETED`。

TENT 的跨 transport failover 当前主要处理 completion-stage failure。GDS I/O 已提交且 completion 返回失败时，运行时可以尝试下一候选 transport；但 `cuFileBatchIOSubmit`、文件注册或 batch capacity 在提交阶段直接报错时，通用 failover 尚不能安全地自动重试。因此，配置中的 `GDS → IOURING` 表示候选顺序，而不意味着所有初始化和提交错误都能透明降级。

该限制见 `docs/source/design/tent/failover.md` 的 Known Gaps。生产使用时仍需对 GDS enablement、对齐、文件权限和请求大小进行启动前验证。

### 8.4 可靠性、降级与 Store 集成

`IOUringTransport` 同样通过 FileSegment 找到文件，并优先以 `O_DIRECT` 打开；失败后会退回普通 `O_RDWR` buffered I/O。

对于已对齐的 Host Buffer：

```text
File ── io_uring read/write ── Host Buffer
```

对于 CUDA pointer 或未按 4 KiB 对齐的 Host pointer：

```text
READ:
File ── io_uring ── aligned Host temp
                     └── Platform::copy ──→ GPU/原始 Buffer

WRITE:
GPU/原始 Buffer ── Platform::copy ──→ aligned Host temp
                                         └── io_uring ──→ File
```

所以 `IOUringTransport::caps.gpu_to_file=true` 表示它能完成 GPU 与文件之间的逻辑请求，不表示文件 DMA 直接进入 GPU。当前源码在检测到 CUDA pointer 时显式执行 Host staging。

GDS 与 io_uring 的差异应表述为：

| 维度 | GDS | io_uring GPU 路径 |
|------|-----|-------------------|
| 文件 API | cuFile Batch API | Linux io_uring |
| GPU Buffer | 可通过 `cuFileBufRegister` 预注册，并作为 DMA 目标/源 | 先读写对齐 Host temp，再执行 device copy |
| CPU DRAM bounce | 理想直达路径可避免 | 当前实现明确存在 |
| 依赖 | NVIDIA CUDA、cuFile、受支持的文件系统和拓扑 | Linux kernel、liburing；GPU copy 另依赖平台 runtime |
| 适用性 | GPU 最先/最后消费、I/O 构成瓶颈 | 通用回退、Host 消费、GDS 条件不满足 |

从端到端视角看，L4 数据面可以归纳为 direct 与 staged 两种基本形态。前者将文件数据直接送入 GPU，后者以 Host Buffer 为中间层。读写方向虽相反，但都要求上层在传输完成前维持目标或源 Page 的所有权。

**L4 → L1（恢复 KVCache）**

```text
推理引擎分配目标 KVCache Page/Block（GPU）
  → 推荐预注册 GPU Buffer 到 TENT/cuFile
  → Store/上层元数据得到 file offset + length
  → openSegment("file://...")
  → Request{READ, gpu_ptr, file_segment, offset, length}
  → TransportSelector 选择 GDS
  → cuFileBatchIOSubmit(CUFILE_READ)
  → NVMe/NIC DMA 写入 GPU Buffer
  → TENT task COMPLETED
  → 推理引擎完成同步后把 Page 交给 attention kernel
```

**L1 → L4（持久化 KVCache）**

```text
推理引擎确认 GPU KVCache 数据生产完成
  → 保持 Page/Block pin，不允许复用
  → Request{WRITE, gpu_ptr, file_segment, offset, length}
  → cuFileBatchIOSubmit(CUFILE_WRITE)
  → GPU Buffer 写入 SSD/DFS 文件
  → 所有内部 I/O COMPLETED
  → 上层提交对象元数据/Replica 状态
  → 解除 GPU Page pin
```

最后两步不属于 `GdsTransport`。TENT 只报告字节传输状态；对象何时变为可见、失败时如何回滚文件空间、CRC 如何提交，以及旧 Replica 何时删除，仍需要 Store 或推理框架提供事务边界。

#### Store 集成所需的语义桥梁

当前 Store `LOCAL_DISK` 读取链路是：

```text
Master Query
  → LocalDiskReplicaData{holder endpoint}
  → ClientRequester RPC 到 holder
  → holder FileStorage::BatchGet
  → SSD 读入 holder ClientBuffer
  → TE 把 ClientBuffer 直接写入请求方 Slice（可为 GPU）
```

TENT FileSegment + GDS 链路则是：

```text
本机可见 file path + offset
  → FileSegment
  → 本地 GPU Buffer
  → cuFile 直接执行文件 I/O
```

前者若要演进为后者，需要一个同时覆盖寻址、可达性和生命周期的语义桥梁。首先，`ObjectMetadata/Replica` 应能够稳定给出 `file path + offset + length`，而不只记录 holder RPC endpoint；请求节点也必须能够看到同一文件，或通过 GDS-compatible DFS/NVMe-oF mount 获得等价的本地文件语义。其次，Store 的对象布局需要满足 GDS 对 file offset 和 size 的约束。`OffsetAllocatorStorageBackend` 已将 value 起始位置对齐到 4 KiB，这是一项必要准备，但本身不能证明 GDS 已经接入。

生命周期衔接同样不可省略。GPU Slice 无论采用预注册还是 cuFile 未注册 Buffer 路径，其地址都必须在整个 I/O 期间有效；Store 的 Replica refcount、租约、淘汰与 FileStorage GC 必须等待 TENT Batch 结束。若 GDS 不可用，系统还要在 io_uring staging、holder ClientBuffer 和 Memory Replica promotion 之间给出明确选择。只有这些约束被统一表达，FileSegment 才可能成为 Store L4 的数据面，而不只是独立的文件传输接口。

当前能力与尚未打通的部分集中汇总在 §8.5。

实现这一桥梁时还必须保持协议层次清晰。前面的调用链最终都会落到 §8.2 所示的分层栈，但源码中的类名容易让不同层次再次混在一起。例如，一次“远端 SSD 直达 GPU”的操作可能同时出现 NVMe-oF、RDMA、GDS 和 TENT：它们描述的是同一数据流的不同部分，不是四种可以互相替换的同级方案。

几个容易混淆的术语：

| 名称 | 准确定位 |
|------|----------|
| SSD/NAND | L4 的物理非易失介质 |
| NVMe | 主机与 NVMe device/subsystem 的存储命令协议，常运行于 PCIe |
| NVMe-oF | 把 NVMe 命令映射到 RDMA、TCP 或 FC fabric，使远端 SSD 呈现为 block device/namespace |
| RDMA | NIC 执行的远端内存数据传输机制，也可能成为远端 GDS/NVMe-oF 的底层网络能力 |
| GDS | 文件/块存储与 NVIDIA GPU memory 之间的直接 DMA 数据路径和 cuFile API |
| SPDK | 用户态 NVMe/NVMe-oF I/O 框架，Mooncake `NOF_SSD` 使用该路线 |
| io_uring | Linux 异步 I/O 提交/完成接口；当前 TENT GPU 路径仍使用 Host staging |
| TENT | 在上述机制上提供 Segment、Request、transport selection、batch 和可靠性编排 |

GDS 不会替代 NVMe、文件系统或 RDMA。对本地 NVMe，DMA engine 通常位于 NVMe controller；对 GDS-compatible 远端存储，数据面可能由 NIC/RDMA 完成。Mooncake/TENT 不实现这些硬件协议，只使用其驱动和用户态 API。

### 8.5 演进方向与实现判断

当前 TENT 已经能够根据 File Segment 和 Buffer capability 在 GDS、io_uring 等候选中选择 transport，也已经在 `Request` 中保留 `priority`、`policy_name`、`transport_hint`、`deadline_ns` 和 `intent_type`。这解决了“调用方不必把某一种协议写死在业务代码中”的第一步，但还没有完全回答 L4 调度最重要的问题：同一个 L4 命中在不同业务上下文中应该走哪一种路径。

更完整的目标是让上层提交数据移动意图，而不是提交具体协议。例如：

| 上层意图 | 更可能的执行计划 | 原因 |
|----------|------------------|------|
| `foreground_get`，数据马上参与 attention | `Direct: L4 → GDS → L1` | 缩短关键路径，避免额外 Host bounce |
| `background_prefetch`，命中和消费时间不确定 | `Staged: L4 → io_uring/GDS → L2 → L1` | 保护稀缺 L1，允许取消、复用和计算重叠 |
| `promotion_on_hit`，SSD 对象重新变热 | `L4 → L3 Memory Replica` | 这是 Store 侧决策，可映射为 migration 或后续专用 intent |
| `checkpoint` / `weight_loading` | 依据吞吐和 deadline 选择 direct、staged 或 degraded | 更关注大吞吐、隔离和后台流量控制 |

沿着这一方向，TENT 的演进并非若干互不相关的功能叠加，而是一条从意图表达走向闭环调度的连续路径。首先，foreground get、background prefetch、migration、checkpoint 和 weight loading 等意图需要通过稳定的 C/Python API 进入运行时，并与 Store、SGLang、vLLM 对接。此后，调度结果才能从简单的 transport 候选列表提升为 execution plan：运行时结合 Segment、内存类型、拓扑、健康状态和 policy，生成可解释的 `Direct`、`Staged`、`Fallback` 或 `Degraded` 路径。

当路径模型稳定后，SLO 与资源竞争才有条件进入选择过程。带宽、队列深度、QP 压力和 receiver credit 可以共同参与准入、优先级、backpressure 与 QoS 隔离，避免后台 L4 预取拖慢前台 KV 传输。最终，tracing 和指标需要记录请求为何选择某条路径、在哪一 stage 降级以及实际耗时，并将这些观测反馈给路径成本估计和故障排除，由此形成可验证的调度闭环。

这一演进仍然遵守 Store/TENT 的职责边界：TENT 可以决定本次搬运采用 direct 还是 staged，却不应该独立决定对象是否晋升、Replica 是否可见或哪个 key 应被淘汰。换句话说，未来的理想形态不是“TENT 接管 L4”，而是 Store 把缓存决策表达为 intent，TENT 把 intent 翻译为可执行、可降级的数据移动计划。

从工程落地顺序看，可以先保留当前 ClientBuffer + io_uring/RDMA 路径作为正确性基线；随后让对齐的 OffsetAllocator extent 和已注册 CUDA Slice 进入 GDS fast path；最后再引入基于 intent、deadline 和负载的自动选择。这样每一步都能与上一阶段做结果和性能对照，不需要在一次改造中同时重写 Store 元数据、磁盘布局和传输调度。

#### 当前实现边界

截至当前源码版本，可以将状态分为：

| 能力 | 状态判断 | 依据 |
|------|----------|------|
| FileSegment 数据结构与 `file://` 解析 | 已实现 | `segment.h`、`segment_manager.cpp` |
| GDS transport 构建与动态装载 | 已实现、依赖环境 | CMake 的 cuFile 检测、`transport_loader.cpp` |
| CUDA Buffer 与文件 handle 注册 | 已实现 | `cuFileBufRegister`、`cuFileHandleRegister` |
| cuFile Batch READ/WRITE 与状态聚合 | 已实现 | `gds_transport.cpp` |
| GDS → io_uring capability-based selection | 已实现 | `transport_selector.cpp` |
| Store LOCAL_DISK SSD offload | 已实现，但当前是 Host ClientBuffer 路径 | `file_storage.cpp`、`real_client.cpp` |
| Store Object/Replica 自动映射 FileSegment GDS | 尚未形成统一默认路径 | Store 与 TENT 仍有不同描述符和生命周期 |
| 提交阶段透明 fallback | 当前不完整 | TENT failover Known Gaps |
| GDS 硬件端到端测试覆盖 | 当前 `tent/tests` 主要覆盖 selector，未见独立 GDS hardware E2E | 测试目录检索结果 |

当前实现因而应按适用域而非统一开关来判断。热 KVCache 的跨节点转移仍应优先使用 GPUDirect RDMA Memory Segment；`LOCAL_DISK` 命中可以将 holder Host Buffer 直接传入目标 VRAM，但这一链路不属于 GDS。只有当目标是 NVIDIA GPU、文件对请求节点可见、驱动与文件系统及拓扑均已验证，而且数据将立即由 GPU 消费时，FileSegment + GDS 才构成优先候选。io_uring 或既有 ClientBuffer 路径仍需作为显式降级方案，因为当前 TENT 尚不能覆盖全部提交阶段错误。对于共享 block SSD Pool，则应按 `NOF_SSD + SPDK/NVMe-oF` 的块存储语义分析，而不宜强行套用 FileSegment。

把这些判断串起来，当前最值得推进的不是宣布一条全新的“GDS L4”，而是在现有 Store SSD Offload 的正确性边界内增加 GDS 快路径：Store 继续掌握对象、空间和可见性，TENT 逐步掌握 direct/staged/fallback 的执行选择。只有这两部分接上以后，L4 才从“能够读写 SSD”变成真正可调度、可降级、可观测的缓存层级。

#### 源码阅读路径

源码阅读可以按“抽象—执行—集成”三层推进。抽象层先阅读 `tent/include/tent/transfer_engine.h`、`tent/include/tent/runtime/segment.h` 和 `tent/src/runtime/segment_manager.cpp`，建立 Request、FileSegment 与 `file://` 解析之间的关系。执行层再进入 `transfer_engine_impl.cpp`、`transport_selector.cpp` 和 `transport_loader.cpp`，理解请求如何形成候选路径；随后对照 `gds_transport.cpp` 与 `io_uring_transport.cpp`，比较 cuFile 直接路径和 Host staging 路径。最后回到 Store 的 `replica.h`、`real_client.cpp`、`file_storage.cpp` 与 `transfer_task.cpp`，观察 `DISK/LOCAL_DISK/NOF_SSD` 的对象语义为何尚未与 FileSegment 完全统一。

外部接口约束可继续参照：

- [NVIDIA GPUDirect Storage 文档](https://docs.nvidia.com/gpudirect-storage/index.html)
- [Mooncake Transfer Engine NEXT Roadmap](https://github.com/kvcache-ai/Mooncake/issues/1058)
- [TENT: A Declarative Slice Spraying Engine for Performant and Resilient Data Movement in Disaggregated LLM Serving](https://arxiv.org/abs/2604.00368)
