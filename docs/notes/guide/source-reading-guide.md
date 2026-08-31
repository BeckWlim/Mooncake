# Mooncake 源码阅读指引

> 对齐 Git HEAD `b97f13cd` 及当前工作区。目标是先建立稳定的职责边界，再沿一条真实调用链下钻。

## 1. 先记住这张图

```text
+------------------------------------------------------------------+
| Python / C++ callers                                             |
| wheel, pybind modules, Store/TE public APIs                      |
+-------------------------------+----------------------------------+
                                |
                 +--------------+--------------+
                 |                             |
                 v                             v
+--------------------------------+  +-------------------------------+
| Mooncake Store                 |  | Transfer Engine               |
| object/replica/lease/placement |  | segment/request/transport     |
|                                |  |                               |
| control: Client -> Master RPC  |  | classic TE and TENT coexist   |
| data:    Client -> TE          |  | behind the build-time switch  |
+----------------+---------------+  +---------------+---------------+
                 |                                  |
                 v                                  v
+--------------------------------+  +-------------------------------+
| DRAM / SSD / NoF / DFS         |  | RDMA / TCP / NVLink / GDS ... |
+--------------------------------+  +-------------------------------+
```

核心边界：Master 管元数据和决策，Client 编排 Put/Get，Transfer Engine
搬运对象字节。Master 不进入每次对象传输的数据路径。

## 2. 仓库逻辑分层

| 层次 | 入口 | 负责什么 | 不负责什么 |
|------|------|----------|------------|
| Python 包 | `mooncake-wheel/mooncake/` | CLI、配置、vLLM connector、结构化对象封装、EP/PG 包装 | Store/TE 核心算法 |
| Python 绑定 | `mooncake-integration/store/store_py.cpp`、`transfer_engine/transfer_engine_py.cpp` | C++ 类型和方法的 pybind11 暴露 | 业务状态机 |
| Store API 适配 | `mooncake-store/include/pyclient.h`、`real_client.h` | 对外 API、进程内 Buffer、IPC、SSD offload 服务 | 全局元数据决策 |
| Store 编排 | `mooncake-store/include/client_service.h` | Query、Put/Get、Replica 选择、TE 提交 | 全局空间所有权 |
| Store 控制面 | `master_client.h`、`rpc_service.h`、`master_service.h` | RPC、元数据、Segment/Replica、租约、配额、驱逐、任务、HA | 对象字节搬运 |
| Classic TE | `mooncake-transfer-engine/include/transfer_engine.h` | 稳定公共 API、多协议管理、现有 Store 数据面 | Store 对象语义 |
| TENT | `mooncake-transfer-engine/tent/include/tent/transfer_engine.h` | 策略选择、准入、QoS、staging、异构后端 | Replica 生命周期和缓存策略 |
| EP / PG | `mooncake-ep/`、`mooncake-pg/` | MoE dispatch/combine、torch.distributed backend | Store 元数据 |

构建时先区分两个开关：根项目用 `WITH_STORE` / `WITH_TE`；TENT 在
`mooncake-transfer-engine/CMakeLists.txt` 中由 `USE_TENT` 选择。不要把
`WITH_STORE` 写成不存在的 `USE_STORE`。

## 3. 第一条主线：Store Put

```text
MooncakeDistributedStore.put
  -> RealClient::put / put_internal
  -> ClientBufferAllocator + split_into_slices
  -> Client::Put
       -> MasterClient::PutStart
       -> WrappedMasterService::PutStart
       -> MasterService::PutStart
            allocate Replica descriptors
       <- descriptors
       -> Client::TransferWrite
       -> TransferEngine::submitTransfer
       -> MasterClient::PutEnd or PutRevoke
       -> MasterService commits or rolls back replica state
```

建议按以下顺序打开文件：

1. `mooncake-store/src/real_client.cpp`：`put_internal`，观察输入如何进入已注册 Buffer。
2. `mooncake-store/src/client_service.cpp`：`Client::Put`，看三阶段编排和不同 Replica 类型。
3. `mooncake-store/src/master_client.cpp`：`PutStart` / `PutEnd` / `PutRevoke`，看 RPC 参数边界。
4. `mooncake-store/src/rpc_service.cpp`：`WrappedMasterService`，看租户上下文、日志和指标包装。
5. `mooncake-store/src/master_service.cpp`：`PutStart` / `PutEnd`，看锁、配额、分配和状态提交。
6. 回到 `Client::TransferWrite`，再进入 Classic TE 或 TENT 的提交路径。

阅读时记录三种状态，不要混在一起：对象元数据是否存在、Replica 是否
`COMPLETE`、底层传输任务是否完成。

## 4. 第二条主线：Store Get

```text
RealClient get API
  -> Client::Query
  -> MasterClient::GetReplicaList
  -> MasterService::GetReplicaList
       returns COMPLETE replica descriptors + lease TTL
  -> Client selects a usable replica
  -> Client::TransferRead
       MEMORY/NOF: Transfer Engine path
       DISK:       storage backend path
       LOCAL_DISK: holder RealClient offload RPC path
  -> caller-provided or ClientBufferAllocator-owned destination
```

优先阅读 `Client::Query`、`Client::Get`、`TransferReadInternal`，之后再按
Replica 类型分支进入 `file_storage.cpp`、`storage_backend.cpp`、SPDK/NoF
或 Transfer Engine。这样不会把 `LOCAL_DISK` holder RPC 误认为普通 RDMA
Memory Replica 的必经路径。

## 5. 第三条主线：Classic TE

```text
TransferEngine public facade
  -> TransferEngineImpl
  -> MultiTransport
       -> select transport from protocol/topology/locality
       -> allocate batch and submit requests
       -> backend Transport
       -> poll task/batch status
```

阅读顺序：

1. `mooncake-transfer-engine/include/transfer_engine.h`：公共 API 和核心类型。
2. `include/transfer_engine_impl.h` + `src/transfer_engine_impl.cpp`：初始化、内存注册、Segment 和提交。
3. `include/multi_transport.h` + `src/multi_transport.cpp`：协议安装、选择、Batch 状态聚合。
4. `include/transport/transport.h`：后端契约。
5. 只选一个后端深入：TCP 最适合理解正确性；RDMA 适合理解 endpoint、QP、CQ 和多 NIC。

## 6. 第四条主线：TENT

```text
tent::TransferEngine
  -> TransferEngineImpl::submitTransfer
  -> resolveTransport / TransportSelector
  -> optional LocalTransferAdmissionQueue
  -> direct backend or ProxyManager staging
  -> ProgressWorker / caller polling
  -> completion, cancellation, or failover handling
```

阅读顺序：

1. `tent/include/tent/common/types.h`：`Request`、`IntentType`、`TransportType`、状态。
2. `tent/include/tent/runtime/segment.h`：Memory/File Segment 描述符。
3. `tent/include/tent/runtime/transport.h`：能力矩阵和后端接口。
4. `tent/src/runtime/transfer_engine_impl.cpp`：初始化、选择、提交、状态推进。
5. `transport_selector.cpp`、`admission_queue.cpp`、`proxy_manager.cpp`：策略、准入和 staging。
6. `transport/tcp/` 或 `transport/shm/` 建立最小模型，再进入 `transport/rdma/`。
7. 研究文件路径时再读 `transport/gds/` 与 `transport/io_uring/`。

Classic TE 与 TENT 有同名 `TransferEngine` / `TransferEngineImpl`。搜索符号时始终
检查命名空间和路径；不要把一个实现的 Batch、Segment 或 Transport 类型套到另一个实现上。

## 7. 专题分支

| 主题 | 最短入口 |
|------|----------|
| 元数据与并发 | `master_service.h` 的锁顺序说明 → `MetadataAccessor*` → Put/Get 实现 |
| Segment 与分配 | `segment.h` → `allocator.h` → `allocation_strategy.h` |
| Replica 与驱逐 | `replica.h` → `eviction_strategy.h` → `MasterService::BatchEvict` |
| 多租户 | `tenant_id.h` → `tenant_quota*.h` → Master 的 Reserve/Commit/Abort 路径 |
| SSD 分层 | `file_storage.h` → `storage_backend.h` → `transfer_task.cpp` |
| HA | `ha/leadership/` → `ha/oplog/` → `ha/snapshot/` → `standby_controller.cpp` |
| Python 对外 API | `store_py.cpp` → `PyClient` → `RealClient` |
| vLLM 集成 | `mooncake-wheel/mooncake/mooncake_connector_v1.py` |
| 结构化对象 | `mooncake-wheel/mooncake/structured_object_store.py` |
| EP | `mooncake_ep_api.cuh` → `mooncake_ep_buffer.cpp` → CUDA kernels |
| PG | `mooncake_backend.h` → `mooncake_backend.cpp` → connection poller / P2P proxy → workers |

## 8. 用测试反向确认理解

不要只顺读实现。每完成一个主题，找同名测试确认边界条件：

- Store：`mooncake-store/tests/master_service_test.cpp`、`client_integration_test.cpp`、`tests/ha/`。
- Classic TE：`mooncake-transfer-engine/tests/` 中对应 transport 或 failover 测试。
- TENT：先看 `transport_selector_test.cpp`、`segment_manager_test.cpp`、
  `runtime_queue_dispatch_test.cpp`，再看 RDMA/failover 测试。
- EP/PG：`mooncake-ep/tests/`、`mooncake-pg/tests/`。

推荐每次回答四个问题后再进入下一层：谁拥有状态、谁做决策、谁搬数据、
失败时由谁提交/回滚/重试。能用这四个问题解释 Put 和 Get，就已经建立了
后续阅读所需的主干模型。
