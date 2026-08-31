# TENT (Transfer Engine NEXT) — 总体调研报告

> 调研日期：2026-08-04
> 源码路径：`mooncake-transfer-engine/tent/`
> 编译开关：`-DUSE_TENT=ON`

---

## 1. 概述

TENT（Transfer Engine NEXT）是 Mooncake 项目中的**新一代点对点数据传输运行时**，也是经典 Transfer Engine（TE）的后继者。它的核心目标是在异构 AI 集群中**高效、可靠地移动数据，同时让应用层无需关心传输层的具体细节**。

TENT 的设计应对了现代 AI 集群的三个现实问题：
1. **异构互联** — 同一集群内可能同时存在 NVLink、RDMA、共享内存等多种互联方式
2. **动态拓扑** — 链路质量随时间变化（拥塞、硬件复位、瞬态故障）
3. **部分故障** — 大规模集群中局部链路故障是常态，不应让应用层感知

TENT 将**传输选择、调度和故障处理全部移入运行时**，应用层只需声明"要移什么数据"，而不是"怎么移"。

---

## 2. 核心设计原则

### 2.1 动态传输选择

应用不直接选择传输后端，而是提交描述**数据内容**的传输请求。TENT 运行时根据源和目标之间的可用路径，在运行时决定使用哪个传输后端。如果直连路径不可用，TENT 会自动构造分阶段传输（例如通过主机内存中转）。

### 2.2 细粒度调度 + 遥测（Slice Spraying）

当多个路径/网卡可用时，TENT 不使用静态条带化（striping），而是将大传输切分成小切片（slice），每个切片独立调度。运行时基于简单的遥测数据（完成时间、队列深度）决定每个切片发往哪里——慢的路径自然分到更少的切片，快的路径分到更多切片。

### 2.3 运行时故障处理

局部故障不向上层应用暴露。路径变慢/不可用时，运行时暂停向该路径调度切片，继续使用其他可用路径。整个后端不可用时，自动切换到另一个后端。恢复后的路径会自动重新加入调度。

---

## 3. 架构总览

```text
+--------------------------------------------------------------+
| C / C++ API | Python binding | Rust binding                  |
+--------------------------------------------------------------+
| TransferEngine facade -> TransferEngineImpl                  |
+----------------+----------------+----------------+-----------+
| Config/Policy  | Segment/Meta   | Platform       | Runtime   |
| selector       | control plane  | CUDA/ROCm/...  | queue/QoS |
+----------------+----------------+----------------+-----------+
| Transport backends                                           |
| RDMA | MNNVL | SHM | NVLink | GDS | io_uring | TCP | TPU ... |
+--------------------------------------------------------------+
| Metastore: p2p / etcd / Redis / HTTP | control-plane RPC     |
+--------------------------------------------------------------+
```

---

## 4. 核心组件详解

### 4.1 TransferEngine 门面类

**文件**: `include/tent/transfer_engine.h`

- `mooncake::tent::TransferEngine` — 面向使用者的 C++ 门面
- 持有 `std::unique_ptr<TransferEngineImpl>`
- 提供完整的生命周期管理：`openSegment`、`closeSegment`、`allocateBatch`、`submitTransfer`、`getTransferStatus` 等
- 同时暴露 C API（`tent_engine_t` 等），支持 C/C++/Python/Rust 多语言调用

### 4.2 TransferEngineImpl 核心引擎

**文件**: `include/tent/runtime/transfer_engine_impl.h`, `src/runtime/transfer_engine_impl.cpp`

这是整个 TENT 的心脏，包含：

| 成员 | 说明 |
|------|------|
| `config_` | 共享配置 (`Config`) |
| `metadata_` | 元数据服务 (`ControlService`) |
| `topology_` | 拓扑信息 (`Topology`) |
| `transport_selector_` | 传输策略选择器 |
| `transport_list_[11]` | 所有传输后端的数组（按 `TransportType` 枚举索引） |
| `local_segment_tracker_` | 本地段追踪器 |
| `batch_set_` | 活跃 batch + 空闲链表 |
| `staging_proxy_` | 分阶段传输代理 (`ProxyManager`) |
| `progress_worker_` | 可选的异步进度推进线程 |
| `runtime_queue_` | 运行时准入队列（限流） |

关键内部方法：
- `resolveTransport()` — 针对一个请求选出最佳传输后端
- `resubmitTransferTask()` — 跨传输故障恢复的单一入口
- `prepareSubmit()` / `commitPreparedSubmit()` — 请求提交的两阶段流程（含 staging 检测）
- `pollTaskStatus()` / `updateTaskStatusAfterPoll()` — 任务状态推进与故障恢复

### 4.3 Segment 抽象

**文件**: `include/tent/runtime/segment.h`

Segment（段）表示数据的逻辑位置：

- **MemorySegmentDesc**: 包含拓扑信息、BufferDesc 列表、DeviceDesc 列表
- **FileSegmentDesc**: 文件路径 + 偏移 + 长度
- **BufferDesc**: 一个可传输的内存区域，包含地址、长度、location、支持的后端列表、NUMA region 信息
- **SegmentDesc**: 统一的段描述符（variant: Memory 或 File），支持 JSON 序列化/反序列化

段的发现机制：本地段导出为字符串 handle → 远程段通过 handle 导入。

### 4.4 Transport 抽象层

**文件**: `include/tent/runtime/transport.h`

所有传输后端实现 `mooncake::tent::Transport` 接口：

```cpp
class Transport {
    virtual Status install(...);           // 初始化传输层
    virtual Capabilities capabilities();   // 能力查询
    virtual Status submitTransferTasks(SubBatchRef, requests);  // 提交传输
    virtual Status getTransferStatus(SubBatchRef, task_id, status); // 查询状态
    virtual Status cancelTransferTask(SubBatchRef, task_id);    // 取消（best effort）
    virtual Status addMemoryBuffer(...);   // 注册内存
    // ... etc
};
```

`Capabilities` 结构体声明了该后端支持的数据路径：`dram_to_dram`、`dram_to_gpu`、`gpu_to_dram`、`gpu_to_gpu`、`dram_to_file`、`gpu_to_file`。

### 4.5 TransportSelector 策略引擎

**文件**: `include/tent/runtime/transport_selector.h`

基于 JSON 配置的策略驱动的传输选择。策略按序评估，包含以下匹配维度：

- `segment_type` — memory / file
- `intent_type` — foreground_get / background_prefetch / migration / checkpoint / weight_loading / staging_internal
- `priority` — high / medium / low
- `same_machine` — 本地 vs 远程
- `local_memory` / `remote_memory` — cuda / cpu / hip / npu
- `min_size` / `max_size` — 传输大小范围
- `devices` — 允许的设备白名单（转成 64-bit 位掩码）
- `transports` — 后端偏好列表（按序尝试）

### 4.6 Platform 抽象

**文件**: `include/tent/runtime/platform.h`、`include/tent/platform/{cuda,rocm,ascend,cpu,tpu,sunrise}.h`

Platform 层封装了硬件特定的操作：

- 内存分配/释放（GPU/CPU/NPU）
- 内存类型探测（`getMemoryType`）
- NUMA 位置探测（`getLocation`）
- 设备属性查询
- 流管理（CUDA stream pool、ROCm stream pool）

支持平台：CUDA、ROCm/HIP、Ascend NPU、Sunrise、TPU（PJRT）、CPU

---

## 5. 传输后端

| 后端 | 枚举值 | 用途 | 状态 |
|------|--------|------|------|
| **RDMA** | `RDMA` | InfiniBand/RoCE 高速网络传输 | 主要后端，功能最完整 |
| **NVLink** | `NVLINK` | GPU 间直接互联 | 同机 GPU-GPU |
| **SHM** | `SHM` | 共享内存（同主机进程间） | 同机传输 |
| **TCP** | `TCP` | TCP 网络回退 | RDMA 不可用时的 fallback |
| **GDS** | `GDS` | NVIDIA GPUDirect Storage | GPU ↔ 文件 |
| **io_uring** | `IOURING` | Linux io_uring 文件 I/O | 文件传输 |
| **MNNVL** | `MNNVL` | 多节点 NVLink | 跨机 GPU |
| **AscendDirect** | `AscendDirect` | 华为 Ascend NPU 直连 | Ascend 集群 |
| **SunriseLink** | `SUNRISE_LINK` | 曙光 Sunrise 互联 | 曙光平台 |
| **TPU** | `TPU` | Google TPU 互连（PJRT） | TPU 平台 |

### 5.1 RDMA 传输（最复杂的后端）

RDMA 后端位于 `transport/rdma/`，包含以下子组件：

| 组件 | 文件 | 说明 |
|------|------|------|
| `RdmaContext` | `context.h/cpp` | 管理 `ibv_context`、保护域、设备发现 |
| `RdmaEndpoint` | `endpoint.h/cpp` | 管理 QP 连接、地址解析 |
| `EndpointStore` | `endpoint_store.h/cpp` | QP 连接缓存池（LRU） |
| `RdmaBufferManager` | `buffers.h/cpp` | MR 注册/注销管理 |
| `CompletionQueue` | `cq.h/cpp` | CQ 轮询与完成处理 |
| `Workers` | `workers.h/cpp` | 多 worker 线程 + 优先级队列 |
| `Slice` | `slice.h` | 大数据切分逻辑 |
| `RailMonitor` | `rail_monitor.h/cpp` | 单 rail 故障检测与指数退避恢复 |
| `Quota/SharedQuota` | `quota.h/cpp` | 单进程/跨进程带宽配额（共享内存） |
| `BwArbitration` | `bw_arbitration.h` | 带宽仲裁 |
| `PromotionPolicy` | `promotion_policy.h` | 优先级提升策略（防饿死） |
| `Params` | `params.h` | RDMA 配置参数（QP 属性等） |
| `ibv_loader` | `ibv_loader.h/cpp` | 动态加载 libibverbs（dlopen） |

---

## 6. 关键特性

### 6.1 动态传输选择（Transport Selection）

- **策略驱动**: 基于 JSON 配置文件中的 policy 规则匹配
- **多级 fallback**: `transports: ["nvlink", "rdma", "shm"]` → 按序尝试
- **per-request override**: `Request::transport_hint` 可强制指定传输后端
- **Intent-based 匹配**: 支持按业务意图（foreground_get、migration 等）选择策略

### 6.2 Slice Spraying（切片喷洒）

- 大传输被切分成切片（slice），每个切片独立调度到不同网卡
- **Baseline 模式**: 简单 round-robin（同 TE 经典行为）
- **Smart 模式**: EWMA 带宽估计 + NUMA 感知 + 动态负载均衡
  - 预测完成时间 = (inflight_bytes + slice_bytes) / ewma_bandwidth
  - NUMA 惩罚因子：Rank 0 (1.0x)、Rank 1 (5.0x)、Rank 2 (10.0x)
  - 1% 概率探测所有设备以保证 EWMA 不被饿死
- 每个切片独立计算得分（score = predicted_time × numa_penalty + jitter）

### 6.3 服务质量（QoS）

三层 QoS 体系：

1. **Per-worker 优先级队列**: HIGH → MEDIUM → LOW 严格优先级调度
2. **全局时间片协调**: 共享内存中的 3 槽位轮转（每槽 2ms），跨进程协调
3. **优先级提升**: 低优先级请求等待超时（默认 10ms）后自动提升，防止饿死

### 6.4 故障恢复（Failover）

两层故障恢复：

1. **跨传输 failover**（`resubmitTransferTask`）：
   - 当传输后端返回 FAILED，引擎提升 `xport_priority` 并选择下一个后端
   - 默认最多 3 次 failover 尝试（`max_failover_attempts`）
   - 达到上限后任务标记为 FAILED 并上报应用层

2. **RDMA rail 级恢复**（`RailMonitor`）：
   - 追踪每个 (local_nic, remote_nic) 的错误计数
   - 达到阈值（默认 3）后暂停该 rail，指数退避（初始 30s，倍增到 300s）
   - 一次成功传输即可提前恢复（不等待 cooldown 到期）

**故障注入测试**: `FaultProxyTransport` 包装任意 `Transport`，注入 submit 失败、status 损坏等故障，用于 E2E 验证。

### 6.5 Metrics 系统

- 基于 yalantinglibs，兼容 Prometheus
- **编译时开关**: `-DTENT_METRICS_ENABLED=ON`（默认 OFF，零开销）
- **运行时开关**: `TentMetrics::setEnabled(false)` 关闭
- HTTP 端点:
  - `/metrics` — Prometheus 格式
  - `/metrics/json` — JSON 格式
  - `/metrics/summary` — 人类可读摘要
  - `/health` — 健康检查
- 核心指标: 读写字节数/请求数/失败数、延迟直方图、传输大小直方图、failover 事件计数、deadline MLU、因果链分阶段延迟

### 6.6 运行时准入队列（Runtime Admission Queue）

- 可选的限流机制，按 owner（batch）维度控制并发
- 支持最大并发 owner 数和最大并发字节数限制
- 通过 `runtime_queue` 配置段启用

### 6.7 Progress Worker

- 可选的异步进度推进线程（`enable_progress_worker`）
- 由传输完成回调触发（`notifyBatchMaybeReady`）
- 减少应用层轮询开销

### 6.8 Intent Type（传输意图）

`Request::intent_type` 提供语义标记，不改变传输行为本身，但驱动策略选择：

| Intent | 值 | 用途 |
|--------|-----|------|
| `INTENT_UNSPEC` | 0 | 默认（兼容旧行为） |
| `FOREGROUND_GET` | 1 | 前台 KV 缓存读取 |
| `BACKGROUND_PREFETCH` | 2 | 后台预取 |
| `MIGRATION` | 3 | 数据迁移 |
| `CHECKPOINT` | 4 | 检查点保存 |
| `WEIGHT_LOADING` | 5 | 模型权重加载 |
| `STAGING_INTERNAL` | 6 | 内部分阶段传输 |

---

## 7. API 层级

### 7.1 C API

**文件**: `include/tent/transfer_engine.h` (extern "C" 区域)、`src/transfer_engine_c.cpp`

- 面向 C 语言和 FFI 调用
- 所有类型以 `tent_` 前缀命名（`tent_engine_t`、`tent_request_t`、`tent_segment_id_t` 等）
- 所有状态码为预定义宏（`OPCODE_READ=0`、`STATUS_COMPLETED=4` 等）

### 7.2 C++ API

**文件**: `include/tent/transfer_engine.h` (mooncake::tent 命名空间)

- `mooncake::tent::TransferEngine` 类
- 更丰富的类型系统（`Status`、`Request`、`SegmentInfo`、`MemoryOptions` 等）
- 支持批量操作和条件编译的 metrics 集成

### 7.3 Python 绑定

**文件**: `src/python/pybind.cpp`

- 基于 pybind11
- 自定义异常层次结构（`TentException` → `InvalidArgumentError`、`RdmaError` 等）
- 自动将 C++ `Status` 映射为 Python 异常
- 支持 `Request`、`MemoryOptions`、`SegmentInfo` 等 Python 类型

### 7.4 Rust 绑定

**文件**: `rust/src/transfer_engine.rs`、`rust/src/memory_pool.rs`

- Rust FFI 封装
- 使用 cargo build system（`build.rs` 调用 CMake）
- 提供 `TransferEngine` 和 `MemoryPool` 两个核心结构体

---

## 8. 配置系统

**默认配置文件**: `tent/config/transfer-engine.json`

配置支持三层来源（优先级从高到低）：
1. 配置文件（`transfer-engine.json`）
2. 环境变量（前缀 `MC_` 和 `TENT_`）
3. 默认值

核心配置项：

| 配置路径 | 默认值 | 说明 |
|----------|--------|------|
| `local_segment_name` | `""` | 本地段名称 |
| `metadata_type` | `"p2p"` | 元数据服务类型 (p2p/etcd/redis/http) |
| `metadata_servers` | `"127.0.0.1:2379"` | 元数据服务器地址 |
| `log_level` | `"warning"` | 日志级别 |
| `max_failover_attempts` | `3` | 最大 failover 次数（0=禁用） |
| `enable_auto_failover_on_poll` | `true` | poll 时自动 failover |
| `enable_progress_worker` | `false` | 启用异步进度线程 |
| `metrics.enabled` | `true` | 运行时 metrics 开关 |
| `metrics.http_port` | `9100` | Prometheus HTTP 端口 |

传输层配置按后端单独配置在 `transports.*` 下，策略配置在 `policy` 数组中。

---

## 9. 构建系统

### 顶层 CMake

```
mooncake-transfer-engine/CMakeLists.txt
  └── if (USE_TENT)
        add_subdirectory(tent)
```

### TENT 子项目

```
tent/CMakeLists.txt
  ├── add_subdirectory(src)          # 核心库 + 传输后端 + 各模块
  ├── add_subdirectory(plugins)      # CUDA/ROCm 设备插件
  └── if (BUILD_UNIT_TESTS)
        add_subdirectory(tests)      # 40+ 单元测试
```

编译开关：
- `-DUSE_TENT=ON` — 启用 TENT（默认 OFF）
- `-DTENT_METRICS_ENABLED=ON` — 启用 metrics（默认 OFF）
- `-DBUILD_UNIT_TESTS=ON` — 构建测试
- `-DUSE_CUDA=ON/OFF` — CUDA 支持

---

## 10. 测试

TENT 拥有 **40+ 个单元测试和集成测试**，覆盖：

| 测试文件 | 覆盖范围 |
|----------|----------|
| `rdma_transport_test.cpp` | RDMA 传输核心功能 |
| `tcp_transport_test.cpp` | TCP 传输 |
| `shm_transport_test.cpp` | 共享内存传输 |
| `nvlink_transport_test.cpp` | NVLink 传输 |
| `failover_test.cpp` | 跨传输 failover |
| `engine_failover_e2e_test.cpp` | 故障恢复端到端测试 |
| `fault_proxy_test.cpp` | 故障注入代理 |
| `rail_monitor_test.cpp` | RDMA rail 监控 |
| `qos_contract_test.cpp` | QoS 合约 |
| `transport_selector_test.cpp` | 传输选择策略 |
| `segment_manager_test.cpp` | 段管理 |
| `admission_queue_test.cpp` | 运行时准入队列 |
| `metrics_*_test.cpp` | Metrics 系统 |
| `receiver_credit_test.cpp` | 接收端信用管理 |
| `endpoint_store_test.cpp` | RDMA endpoint 缓存 |
| ... | 等 40+ 测试 |

**注意**: TENT 测试目前**不在 CI 中运行**（上游 workflow 使用 `USE_TENT=OFF`），需要本地手动执行。

---

## 11. 与经典 TE 的关系

TENT 和经典 Transfer Engine 共存于 `mooncake-transfer-engine/` 下：

| | 经典 TE | TENT |
|------|---------|------|
| 传输选择 | 静态绑定单一后端 | 运行时策略驱动 |
| 多 rail | 静态条带化 | 动态切片喷洒 (EWMA) |
| 故障处理 | 上报应用层 | 运行时自动恢复 |
| 配置 | 编译时 + 简单配置 | 完整 JSON 配置 + policy |
| QoS | 无 | 三层 QoS 体系 |
| Metrics | 无内置 | Prometheus 兼容 |
| 平台支持 | CUDA/CPU | CUDA/ROCm/Ascend/TPU/Sunrise/CPU |
| 编译开关 | 默认 ON | `-DUSE_TENT=ON` |

TENT 不是经典 TE 的替代，而是它的演进。两者共用 `mooncake-transfer-engine` 基础设施（metastore、RPC、platform 等），但 TENT 通过 `TransportSelector`、`DeviceSelector` 和 `ProxyManager` 等新增组件实现了更高的自动化水平。

---

## 12. 源码文件布局

```
mooncake-transfer-engine/tent/
├── CMakeLists.txt                    # 顶层 CMake（子目录导航）
├── config/
│   ├── transfer-engine.json          # 默认配置文件
│   └── cluster-topology.json         # 集群拓扑配置
├── include/tent/
│   ├── transfer_engine.h             # 主 API（C + C++）
│   ├── device_plugin.h               # 设备插件接口
│   ├── common/
│   │   ├── config.h                  # JSON 配置管理器
│   │   ├── types.h                   # 核心类型定义
│   │   ├── status.h                  # Status 错误传播
│   │   ├── qos_metrics.h             # QoS 指标
│   │   ├── concurrent/               # 并发原语（rw_spinlock, thread_pool, ticket_lock, mpsc_queue, thread_local_storage）
│   │   └── utils/                    # 工具（ip, os, prefault, random, string_builder）
│   ├── metastore/                    # 元数据存储后端
│   │   ├── etcd.h                    # etcd 存储
│   │   ├── http.h                    # HTTP 存储
│   │   └── redis.h                   # Redis 存储
│   ├── metrics/                      # Metrics 系统
│   │   ├── tent_metrics.h            # 指标定义与记录
│   │   └── config_loader.h           # 配置加载
│   ├── platform/                     # 硬件平台抽象
│   │   ├── cuda.h                    # NVIDIA CUDA
│   │   ├── rocm.h                    # AMD ROCm
│   │   ├── ascend.h                  # Huawei Ascend
│   │   ├── tpu.h / tpu_pjrt_abi.h / tpu_pjrt_shim.h  # Google TPU (PJRT)
│   │   ├── sunrise.h                 # 曙光 Sunrise
│   │   └── cpu.h                     # CPU/DRAM
│   ├── rpc/
│   │   └── rpc.h                     # RPC 控制层
│   ├── runtime/                      # 核心运行时
│   │   ├── transfer_engine_impl.h    # 引擎核心实现
│   │   ├── segment.h                 # 段数据模型
│   │   ├── segment_manager.h         # 段管理器
│   │   ├── segment_registry.h        # 段注册表
│   │   ├── segment_tracker.h         # 段追踪
│   │   ├── slab.h                    # 内存池
│   │   ├── transport.h               # 传输抽象接口
│   │   ├── transport_selector.h      # 传输策略选择
│   │   ├── control_plane.h           # 控制面
│   │   ├── platform.h                # 平台抽象
│   │   ├── topology.h                # 拓扑模型
│   │   ├── metastore.h               # 元数据存储抽象
│   │   ├── admission_queue.h         # 准入队列
│   │   ├── progress_worker.h         # 异步进度工作器
│   │   ├── proxy_manager.h           # 分阶段传输代理管理器
│   │   ├── receiver_credit.h         # 接收端信用管理
│   │   ├── memory_prober.h           # 内存探测
│   │   └── qos_contract.h            # QoS 合约
│   └── transport/                    # 传输后端
│       ├── rdma/                     # RDMA 后端（10+ 头文件）
│       ├── nvlink/                   # NVLink 后端
│       ├── shm/                      # 共享内存后端
│       ├── tcp/                      # TCP 后端
│       ├── gds/                      # GPUDirect Storage 后端
│       ├── io_uring/                 # io_uring 后端
│       ├── mnnvl/                    # 多节点 NVLink 后端
│       ├── ascend/                   # Ascend 直连后端
│       ├── sunrise_link/             # Sunrise 互联后端
│       ├── tpu/                      # TPU 后端
│       ├── bufio/                    # 缓冲 I/O 后端
│       └── fault_proxy/              # 故障注入代理
├── plugins/                          # 设备插件（CUDA/ROCm）
├── src/                              # 实现文件（镜像 include 结构）
│   ├── transfer_engine.cpp           # C++ API 实现
│   ├── transfer_engine_c.cpp         # C API 实现
│   ├── common/                       # 配置、IP 工具、QoS 指标、Status
│   ├── metastore/                    # etcd、HTTP、Redis 存储实现
│   ├── metrics/                      # Metrics 系统实现
│   ├── platform/{cuda,rocm,ascend,cpu,sunrise,tpu}/
│   ├── rpc/                          # RPC 实现
│   ├── runtime/                      # 核心运行时实现（20+ .cpp 文件）
│   ├── transport/{rdma,nvlink,shm,tcp,gds,io_uring,mnnvl,ascend,sunrise_link,tpu,bufio}/
│   └── python/
│       └── pybind.cpp                # Python 绑定
└── tests/                            # 40+ 测试文件
    ├── CMakeLists.txt
    ├── tpu/                          # TPU 测试 mock
    └── *.cpp                         # 各类单元/集成测试
```

---

## 13. 设计约束与已知局限

### 已知局限（来自 failover.md）

1. **Submit 阶段故障不触发 failover** — 当 `submitTransferTasks` 返回非 OK 时，该调用中的所有任务被标记为 FAILED。原因是：
   - `merge_requests` 会导致重复提交（合并后的任务和原始别名）
   - 部分后端（SHM、NVLink）可能部分入队成功，无法安全重试

2. **Rail 恢复时完全清除指数退避记忆** — 一条 rail 在第 N 次恢复后可能保持相同的初始 cooldown，不累积

3. **缺少基于延迟的 failover** — failover 纯粹依赖返回状态，不支持"这个传输太慢，换一个"的延迟感知切换

4. **TENT 测试不在 CI 中** — 需要手动构建和运行

### 设计约束

- 传输后端保持小且聚焦于数据移动；策略决策集中在运行时
- 指标在编译时默认关闭（零开销原则）
- 热路径无锁（MPSC 队列、ticket lock、rw_spinlock）
- CPU PCIe 亲和性感知（通过 NUMA probing + `RangeLocation`）

---

## 14. 总结

TENT 是一个设计精良的高性能数据传输运行时，核心价值在于：

- **声明式 API** — 应用只描述"传输什么"，运行时决定"怎么传"
- **自动适应异构环境** — 同时支持 10 种传输后端 + 6 种硬件平台
- **韧性设计** — 双层故障恢复（传输级 + rail 级），应用无需感知局部故障
- **QoS 感知** — 3 级优先级 + 全局时间片 + 防饿死机制
- **可观测性** — 内置 Prometheus 兼容的 metrics 系统
- **多语言支持** — C/C++/Python/Rust 四个层级的 API

TENT 是 Mooncake 从"经典 TE"到"智能传输运行时"的关键进化，集中体现了项目对生产级分布式 AI 推理系统需求的深刻理解。
