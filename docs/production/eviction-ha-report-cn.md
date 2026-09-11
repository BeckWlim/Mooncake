# Mooncake Store 生产架构与社区进展报告：Eviction 与 HA

> 证据快照：Eviction 截至 2026-09-07，HA 截至 2026-09-08，生产观测截至 2026-09-11
>
> 适用范围：Mooncake Store Master 的分布式 DRAM 淘汰、主备选举、etcd Batch OpLog、快照恢复、主备切换及 Store
> 客户端重连。

## 1. 报告目的与证据边界

### 1.1 综合结论

> **当前结论：`v0.3.13.post1` 已包含主要 Eviction 正确性与性能优化。在高压压测区间内，
> 现有数据没有显示 `BatchEvict` 与 KV 传输时延或 TTFT 存在明显相关性，也没有建立二者的
> 因果关系。**
>
> 同 shard 锁竞争作为局部机制继续监控，当前生产策略与版本基线可以保持。

| 议题 | 结论 | 关键成本证据 | 当前版本判断 |
|---|---|---|---|
| 生产状态 | 高压压测区间内，没有证据表明 `BatchEvict` 与 KV-transfer 或 TTFT 呈现明显相关或因果关系。 | 同一高压区间的 1,201 个样本中，六项零位移相关系数满足 `abs(r) ≤ 0.035`，对应的 95% 置信区间均覆盖零值。 | 保持 `v0.3.13.post1` 和当前 Eviction 策略；使用 request-shard 计时量化局部锁等待。 |
| 前台锁竞争：#2405 / #2508 | 优化目标是缩短 latency-critical lookup 与 eviction 的竞争窗口。#2508 将 `BatchGetReplicaList` 的锁 acquisition 从 O(`K_batch`) 收敛到 O(`S_occupied`)。 | #2405 benchmark 的 average/p99 从 991.5/2,910.7 ms 降至 106.4/296.1 ms，分别降低 89.3%/89.8%；#2508 的 p99/max 分别降低 31.1%/30.9%。 | 两项均已进入生产基线；同 shard 读写串行化继续作为正确性边界。 |
| Issue #2560 | Issue 确认 O(N) candidate scan、整周期 `snapshot_mutex_` 共享锁和高比例场景的 Phase 2 成本。它描述结构性规模边界，不构成生产 TTFT 回归证据。 | 初始 `约 1.0 s` 与后续 `约 1.5 s` 来自不同硬件、commit 和 instrumentation，不能作为 A/B；后续 profile 中 Phase 1 为 2.3%，Phase 2 为 82.2%。 | 高比例场景的后续优化重点是 Phase 2 和快照锁周期；生产优先级由实测 `T_shard_wait(s)` 决定。 |
| PR #2286 | 这是淘汰语义修正与 Phase 1 并行化的正向优化。跨环境的 `1.0 s`/`1.5 s` 不用于评价该 PR。 | SSD workload 的 target population 从 85,622,691 收敛到 3,776,944，5% victim target 从 4,310,991 降到约 190,165；同 workload 的 `T_eviction_cycle` 从约 80 s 降到 2.3 s，降低 97.1%。 | 已进入生产基线；避免 SSD 场景过度淘汰，并显著降低对应周期成本。 |
| PR #3118 | 低比例路径以第二次 scan 换取更小的完整 candidate 集合，收益集中在低 target。 | 1% target：288.1 → 88.3 ms，降低 69.9%；10%：529.7 → 285.1 ms，降低 47.1%；30% 位于等价区间。 | 已进入生产基线；低比例场景直接受益，O(N) census 和整周期快照共享锁仍是保留边界。 |

Eviction 相关的必要 PR 已进入 `v0.3.13.post1`，当前工作重点是生产计时和 SLO 验收。
HA 的 post-release 能力按启用范围选择 containing commit，与上述 Eviction 生产结论分别管理。

### 1.2 当前生产状态与结论

当前生产基线为 `v0.3.13.post1`。以下结果参考
[Monitor 生产分析](monitor/analysis.md)，观测窗口为 2026-09-10 12:00 至 2026-09-11 06:00。
生产指标以 30 s 为采样周期，`BatchEvict` 事件由 key 数和已分配内存同时下降推断。

> **生产结论：在高压压测区间内，现有数据没有显示 `BatchEvict` 与 KV 传输时延或 TTFT
> 存在明显相关性，也没有建立二者的因果关系。**
>
> 同 shard 锁竞争是成立的实现机制，但该机制在当前版本和生产负载下没有形成稳定、可辨识的
> 聚合时延信号。`v0.3.13.post1` 可继续作为生产运行与精细监控的基线。

SGLang 的 Mooncake 读取包括 metadata 查询、replica 选择和 payload 传输；完成的 Host prefix
随后进入 GPU 恢复、prefill 和首 token 生成。因而，metadata 锁等待只占 KV 传输指标的一部分，
KV 传输又只占 TTFT 的一部分。

![SGLang–Mooncake 请求路径与 BatchEvict 生命周期](monitor/figures/request_chain_and_contention.png)

#### 1.2.1 全时段状态

下图覆盖共享 Master 与独立 Master 两个阶段。红色标记表示推断的 `BatchEvict` 事件。

![全时段 Master 状态与客户端时延](monitor/figures/production_timeline.png)

| 指标 | Shared master | Independent master | 相对变化 |
|---|---:|---:|---:|
| 推断事件数 / 事件率 | 48 / 13.002 h⁻¹ | 315 / 22.288 h⁻¹ | +71.4% |
| 每次事件中位 evicted keys | 111,608.5 | 73,159 | -34.5% |
| 每次事件中位 released memory | 44.5 GiB | 59.0 GiB | +32.6% |
| KV-transfer P50 | 38 ms | 33 ms | -13.2% |
| KV-transfer P95 | 466 ms | 91 ms | -80.5% |
| KV-transfer P99 | 493 ms | 222 ms | -55.0% |
| TTFT average | 2,525 ms | 1,610 ms | -36.2% |
| TTFT P95 | 5,000 ms | 5,600 ms | +12.0% |
| TTFT P99 | 5,800 ms | 7,430 ms | +28.1% |

Shared master 与 Independent master 阶段具有不同的负载、缓存增长和 Master 共享状态。
本表用于描述两个生产阶段的运行范围，不用于推断 `BatchEvict` 与时延之间的相关或因果关系。
该关系由下一节在同一高压区间内进行事件样本分析；PR 性能收益则以同负载 A/B 测量为准。

#### 1.2.2 高压片段与事件关联

03:00–03:30 的高压片段中，KV-transfer P50 保持稳定，P95/P99 呈间歇峰值。峰值分布在事件
bucket 与普通 bucket 中，`BatchEvict` 标记与 KV-transfer 或 TTFT 峰值之间没有形成固定对齐。

![高压 30 分钟生产片段](monitor/figures/high_pressure_30min_fragment.png)

20:00–06:00 的高压压测区间包含 1,201 个样本，其中事件样本 298 个。各指标相对 30 分钟
局部中位数进行调整，并使用 30 分钟 block bootstrap 计算 95% 置信区间。

![BatchEvict 与时延的事件关联](monitor/figures/event_process_correlation.png)

| 指标 | Event/control 原始均值差 | 零位移相关系数 | 95% 置信区间 |
|---|---:|---:|---:|
| KV-transfer P50 | +0.06% | -0.0227 | [-0.0732, +0.0213] |
| KV-transfer P95 | +5.41% | +0.0347 | [-0.0032, +0.0727] |
| KV-transfer P99 | +6.18% | +0.0269 | [-0.0207, +0.0689] |
| TTFT average | +1.18% | +0.0104 | [-0.0354, +0.0544] |
| TTFT P95 | +0.72% | -0.0085 | [-0.0390, +0.0172] |
| TTFT P99 | +1.96% | +0.0068 | [-0.0347, +0.0451] |

事件样本的原始均值略高，其中 KV-transfer P95/P99 的差值为 5.41%/6.18%，TTFT 的差值为
0.72%–1.96%。消除局部运行水平后，所有相关系数的绝对值均不超过 0.035，且 95% 置信区间
均覆盖零值。该结果没有显示 `BatchEvict` 与 KV-transfer 或 TTFT 存在明显相关性；当前数据
也没有建立 `BatchEvict` 对两项时延指标的因果影响。

综合生产状态、局部片段与统计区间，可形成以下判断：

1. 当前版本下，高压压测数据没有显示 `BatchEvict` 与 KV 传输时延或 TTFT 存在明显相关性。
2. 当前观测粒度没有建立 `BatchEvict` 对 KV 传输时延或 TTFT 的因果关系。
3. 同 shard 的读写锁竞争属于局部实现机制，其实际影响由 request-shard 级 `T_shard_wait(s)` 量化。
4. 当前生产策略可以保持；后续精细计时用于补充局部效应量，不改变现有生产结论。

### 1.3 生命周期与时间成本口径

本报告将“生命周期”定义为一个操作从开始边界到完成边界的全部区间，将“时间成本”
定义为该区间的 wall time 或其中明确的子区间。后续 issue 与 PR 的性能分析统一使用
以下符号和包含关系：

| 符号 | 生命周期或时间成本 | 开始与完成边界 | 包含关系 |
|---|---|---|---|
| `T_storage` | SGLang 观测的 Mooncake storage-read 时间 | `RealClient::batch_get_into_multi_buffers` 外层计时开始，到整批结果返回 | 包含 metadata RPC、shard 读锁等待、replica 选择、传输提交与完成、checksum 和 lease 校验。 |
| `T_metadata` | Mooncake metadata 查询时间 | `BatchQuery` 开始，到 `BatchGetReplicaList` 返回 descriptor | 包含同 shard 写锁导致的 `T_shard_wait`；区间结束于对象字节传输开始之前。 |
| `T_payload` | `Client::BatchGet` 字节传输子区间 | replica 选择后的传输准备开始，到传输完成和 lease 校验结束 | 区间始于 `BatchQuery` 和 Master shard 锁等待完成之后。 |
| `T_eviction_cycle` | 一轮 `BatchEvict` 生命周期 | 进入 `BatchEvict`，到该轮清理完成并返回 | `snapshot_mutex_` 共享锁几乎覆盖整轮；`T_phase1`、`T_phase2` 和收缩等均为内部子区间。 |
| `T_phase1` | 并行候选 census 与物化成本 | Phase 1 census 开始，到 target 计算和 candidate vector 准备完成 | 包含 worker scan/join；#3118 选择性路径还包含 deadline rank 与 frontier scan。 |
| `T_phase2` | 串行候选应用成本 | candidate selection 开始，到 first/lower-bound pass 完成 | 包含 candidate `nth_element`、逐对象 lookup、锁内 revalidation、evict/offload/OpLog 提交和可能的 refill。 |
| `T_shard_wait(s)` | 前台查询在 shard `s` 上的读锁等待 | 请求 shard 共享锁，到锁获授 | 只在查询与 `BatchEvict` 的同 shard 独占区间重叠时产生，是 `T_metadata` 的子区间。 |
| `T_snapshot_unique_wait` | 外部独占 `snapshot_mutex_` waiter 的等待 | 请求快照独占锁，到共享锁持有者退出 | 与 `T_eviction_cycle` 重叠，作为外部等待区间单独分析。 |
| `T_evict_logical` / `T_evict_physical` | HA 模式的逻辑淘汰与物理回收生命周期 | 锁内标记 `REMOVED` 与提交 OpLog；到 durable callback 锁内重新校验并释放内存 | `T_evict_physical` 可晚于该轮的逻辑目标完成点，差值受 writer、etcd 和 callback backlog 影响。 |

```text
T_storage = T_metadata + T_replica_and_slice + T_payload + T_result
T_metadata 包含 T_shard_wait(s)
TTFT 只在 storage read 位于关键路径时包含 T_storage 的变化
```

phase 占比仅在同一计时周期内解释。嵌套子区间使用包含关系表达；例如 #2560 中的 candidate
vector collection 是 metadata traversal 的子区间。不同 commit、target、对象分布或 instrumentation 方式的
绝对时间分别描述各自证据代际；PR 优化收益使用同 workload、同计时边界的对照组。

### 1.4 MasterService 视角的普通读取生命周期与潜在竞态

```mermaid
sequenceDiagram
    participant S as SGLang
    participant C as RealClient / Client
    participant M as MasterService
    participant L as metadata_shard[s].mutex
    participant E as BatchEvict

    S->>C: batch_get(keys, host destinations)
    Note over C: T_storage 开始
    par 前台普通读取链
        C->>M: Client::BatchQuery → MasterClient/RPC → BatchGetReplicaList
        M->>M: 按 shard 聚合 keys；获取 S(snapshot)
        M->>L: 请求 R(s)
    and 后台 BatchEvict 链
        E->>E: 高水位/分配失败 → BatchEvict；整轮持有 S(snapshot)
        E->>L: census / apply 在 shard s 请求 W(s)
    end
    Note over M,E: 两个 S(snapshot) 相容；锁获授顺序由同 shard 的 R(s) / W(s) 决定
    alt W(s) 先获授
        L-->>E: W(s)
        E->>E: 完成当前 shard 独占临界区
        E->>L: 释放 W(s)
        L-->>M: R(s)，T_shard_wait(s) 结束
        M->>M: 查找 metadata，拷贝当前 descriptor 并续租
        M->>L: 释放 R(s)
    else R(s) 先获授
        L-->>M: R(s)
        M->>M: 查找 metadata，拷贝 descriptor 并续租
        M->>L: 释放 R(s)
        L-->>E: W(s)
        E->>E: census 观测新 lease，或 apply 重新校验后跳过候选
        E->>L: 释放 W(s)
    end
    par 前台读取链继续
        M-->>C: 释放 S(snapshot)；返回 QueryResult
        C->>C: SelectBestReplica / FilterQueryResult
        C->>C: Client::BatchGet → local-copy / TCP / RDMA → completion 与校验
        Note over C: 数据路径使用已返回 descriptor；T_storage 结束
        C-->>S: batch_get 结果
    and 后台淘汰链继续
        E->>E: 候选选择与其余 shard 处理
        E->>E: sparse-map shrink 也逐 shard 获取 W → cleanup
        E->>E: 释放 S(snapshot) → BatchEvict 结束
    end
```

从 MasterService 的视角看，SGLang 的一次 `batch_get` 在前半段通过 `Client::BatchQuery`
进入 `MasterService::BatchGetReplicaList`，在后半段通过 `Client::BatchGet` 移动对象字节。
`BatchEvict` 通过同 shard 的独占 metadata 锁直接增加前半段的 `T_metadata`。
该 shard 锁交互在 descriptor 返回前结束；后半段的 local-copy、TCP 或 RDMA 使用已返回的 descriptor。

图中的“竞态”是读写锁获授顺序决定请求观测到哪个合法状态的语义竞态。
metadata shard mutex 为每个结果提供串行化保护。如果 `BatchEvict` 在普查中先记录候选，但 `BatchGetReplicaList` 在候选应用前先完成，
该读取会续租，后续锁内重新校验会跳过已续租候选。一个 batch 覆盖多个 shard 时，
每个 occupied shard 都可能遇到一次这种串行化；#2508 减少了同 shard 的重复加锁，
同 shard 读写串行化作为正确性边界继续保留。sparse-map shrink 的 W(s) 也会产生同类读锁等待，
其语义效果限于锁顺序和 `T_shard_wait(s)`。

#2508 对该前台生命周期的成本对比如下。令 `K_batch` 表示 batch 内的 key 数，
`S_occupied` 表示这些 key 覆盖的 metadata shard 数。

| 成本维度 | #2508 前 | #2508 后 | 时间成本边界 |
|---|---|---|---|
| shard 共享锁 acquisition | 每 key 获取一次，O(`K_batch`) | 每 occupied shard 获取一次，O(`S_occupied`) | acquisition 数由 `K_batch` 收敛到 `S_occupied`，其中 `S_occupied ≤ min(K_batch, 1,024)`。 |
| 同 shard 内的 metadata 查找 | 多个短读锁区间 | 一个聚合读锁区间处理该 shard 的全部 batch keys | 减少 lock/unlock 成本；单个读锁 hold time 随该 shard 的 batch key 数变化。 |
| `T_metadata` | 包含 per-key acquisition 与可能重复的同 shard 等待 | 包含 per-shard acquisition 和每 shard 最多一次聚合等待 | 优化作用于 metadata RPC 子区间；收益量由 batch size、shard fan-out 和 W(s) 重叠分布决定。 |
| benchmark `BatchGetReplicaList` p99 | 353.019 ms | 243.121 ms | 减少 31.1%；计时从 `MasterClient::BatchGetReplicaList` 调用到返回，属于 `T_metadata` 的 RPC 子区间。 |
| benchmark `BatchGetReplicaList` max | 2,319.008 ms | 1,602.496 ms | 减少 30.9%；结果来自 #2508 在 #2405 基础上的 controlled benchmark。 |
| `T_payload` | 使用返回的 descriptor 执行数据传输 | 使用返回的 descriptor 执行数据传输 | 生命周期和直接成本保持一致。 |

### 1.5 Request-shard 因果验证设计

本节将生产验证项映射到第 1.3 节的生命周期。分析单元是 request-shard lookup：treatment
是前台 lookup 与
`BatchEvict` 在同一 shard 的独占锁区间重叠；concurrent control 是同一 cycle 内另一个 shard
的 lookup；temporal control 是 cycle 前后的匹配请求。匹配变量包括 payload bytes、hit length、
prompt tokens、transport、source node、concurrency 和 queue depth。

| Instrumented interval | 维度 | 对应生命周期与估计量 |
|---|---|---|
| `BatchEvict` cycle 与 phase timer | master, cycle, census/frontier/apply/shrink | 精确定位 `T_eviction_cycle`、`T_phase1`、`T_phase2` 及 treatment phase。 |
| Exclusive shard-lock timer | master, shard, cycle, phase | 记录 eviction-side wait 与 hold time，定义同 shard 独占占用。 |
| Shared lookup-lock timer | master, shard, request, batch | 直接测量 `T_shard_wait(s)`。 |
| Metadata RPC timer | client, request, batch | 测量包含锁等待的 `T_metadata`。 |
| Payload timer | client, request, bytes, transport, source | 将 RDMA/TCP 子区间 `T_payload` 与 Master 等待分离。 |
| SGLang request timeline | request, hit pages, prompt tokens, concurrency | 将 `T_storage` 与同一请求的 TTFT 关键路径对齐。 |

直接 storage effect 和 mediator consistency check 定义为：

```text
Delta T_storage = E[T_storage | same-shard exclusive overlap, X]
                - E[T_storage | concurrent other-shard lookup, X]

Delta T_storage ~= Delta T_shard_wait(s)
```

其中 `X` 表示匹配后的 workload、transport 和 scheduling 变量。因果验收同时对齐三项记录：
eviction 的独占 shard 占用、同 shard 前台共享锁等待、包围该等待的 storage-read 区间。
同一 request 的 TTFT timeline 用于估计下游传播。

## 2. 解决方案总览

建议采用“配置确定性、淘汰可伸缩性、恢复有界性、联合验证”四条主线。当前已合入能力作为部署基线，
社区中的完整设计作为演进目标，各阶段均设置可量化门禁。以下性能分析均使用第 1.3 节的生命周期与时间成本口径。

### 2.1 生产基线、merge timeline 与升级必要性

当前生产基线为 [`v0.3.13.post1`](https://github.com/kvcache-ai/Mooncake/releases/tag/v0.3.13.post1)，
tag commit 为 `719735896c86`，日期为 2026-08-31。
包含关系通过 PR merge commit 与该 tag 的 Git ancestry 确认，merge date 用于表达社区时间线。

Eviction 与前台查询路径的必要 PR 均已进入当前生产基线：

| PR | merge date | `v0.3.13.post1` | 必要能力 | 升级必要性与当前动作 |
|---|---:|---:|---|---|
| #2405 | 2026-06-17 | 已包含 | `BatchExistKey` 按 shard 聚合，并缩短销毁对 shard 锁的占用 | 当前版本已满足；验收 batch-size 分布和 eviction-window p95/p99。 |
| #2508 | 2026-06-18 | 已包含 | `BatchGetReplicaList` 按 shard 聚合 | 当前版本已满足；验收 `T_metadata`、`S_occupied` 和 `T_shard_wait(s)`。 |
| #2286 | 2026-06-24 | 已包含 | 可淘汰 DRAM 分母与 Phase 1 并行 census | 当前版本已满足；复核 configured/computed/actual ratio 和 `T_phase1`。 |
| #3154 | 2026-07-28 | 已包含 | MasterService teardown 期间的 segment 容量记账 | 当前版本已满足；验收 HA term 变化前后的 capacity 指标。 |
| #3168 | 2026-07-29 | 已包含 | serving MasterService 的 capacity release 作用域 | 当前版本已满足；与 #3154 作为同一容量门禁验收。 |
| #3118 | 2026-08-03 | 已包含 | 低比例候选选择性物化 | 当前版本已满足；验收 target 分布、`T_eviction_cycle` 和临时内存。 |
| #3576 | 2026-08-25 | 已包含 | 淘汰后稀疏 metadata map 收缩 | 当前版本已满足；验收 Master RSS/PSS 与 shrink 的 shard 锁 hold time。 |

HA 完整恢复链的若干必要 PR 已合入 `main`，其 merge date 与 `v0.3.13.post1` 的发布日期存在交叉；
升级决策使用 containing commit：

| PR | merge date | `v0.3.13.post1` | 必要能力 | 升级必要性 |
|---|---:|---:|---|---|
| #3497 | 2026-08-28 | post-release `main` | restore 失败时保持 non-serving 并释放 leadership | HA promotion 正确性环境使用包含该 PR 的 pinned build。 |
| #3642 | 2026-08-28 | post-release `main` | `latest`/`fallback` bootstrap 与 suffix replay | batch-snapshot bootstrap 环境使用包含该 PR 的 pinned build。 |
| #3640 | 2026-08-31 | post-release `main` | maintenance lease 与 fenced snapshot publication | 快照发布环境将该 PR 与 #3642 组合升级。 |
| #3794 | 2026-09-01 | post-release `main` | 周期 batch-OpLog snapshot coordinator | 周期快照环境升级到 containing commit，并同时完成 N08 生产接线。 |
| #3810 / #3811 | 2026-09-03 | post-release `main` | production writer fencing / restore 保留 `ReplicaID` | OpLog HA 生产环境升级到同时包含两项的 pinned commit。 |
| #3841 | 2026-09-07 | post-release `main` | bounded promotion install | 大 metadata state 的 HA promotion 环境升级到 containing commit。 |
| #3860 | 2026-09-07 | post-release `main` | OpLog 队列压力下的淘汰停止条件与 leadership keep-alive 隔离 | 同时启用 Eviction 和 OpLog HA 的环境升级到 containing commit。 |

表中“post-release `main`”表示当前 tag 的后续能力。生产升级以同时包含所需 PR 的单一
commit 为目标，并执行第 10 节的联合验收。当前 `v0.3.13.post1` 在 Eviction 路径上保持为生产基线；
HA 升级范围由已启用的恢复能力决定。

### 2.2 方案主线

```mermaid
flowchart LR
    Baseline[固定版本、配置与观测基线]

    Baseline --> Question[性能问题模型<br/>#2560 全量扫描与整周期快照锁]
    Question --> Phase1[Phase 1 规模化<br/>#2286 修正分母并并行普查]
    Phase1 --> Select[候选规模化<br/>#3118 选择性物化]
    Select --> EvictNext[增量索引、有界批次<br/>segment 压力感知]

    Baseline --> HAQuestion[恢复问题模型<br/>#3561 四项核心议题]
    HAQuestion --> Snapshot[standby 周期快照<br/>latest/fallback + suffix replay]
    Snapshot --> Retention[compaction floor<br/>OpLog 与 snapshot GC]
    Retention --> HAGate[fencing、promotion、客户端 RTO 门禁]

    Phase1 --> Joint[Eviction + HA 联合压测]
    Select --> Joint
    HAGate --> Joint
    Joint --> Production[固定 commit 的生产能力清单]
```

| 优先级 | 方案 | 交付结果 |
|---|---|---|
| P0 | 固定 `v0.3.13.post1` 的 commit、镜像和配置指纹；记录 #2286、#2405、#2508、#3118、#3154、#3168、#3576 的已验证包含关系 | 建立可复现、可审计的当前生产基线。 |
| P1 | 以 #2560 为性能问题模型；使用 #2286 的 Phase 1 并行普查和正确分母，结合 #3118 的低比例候选物化 | 分阶段控制扫描、候选构造、CPU、临时内存和锁持有时间。 |
| P1 | 以 #3561 为问题定义，接通 fenced writer、standby snapshot、suffix replay 和 promotion 门禁 | 建立可验证的 metadata 恢复链。 |
| P2 | 发布 reader-safe compaction floor，并实施 OpLog 与 snapshot GC | 建立有界冷启动与有界 etcd 空间模型。 |
| P2 | 建立 segment 级压力感知、增量候选索引和有界淘汰批次 | 改善大对象基数与异构 segment 的尾延迟。 |
| P3 | 将 etcd 压力、durable callback、failover 和客户端重连加入统一场景 | 用端到端 SLO、RTO 和字节一致性完成生产验收。 |

> **重点议题｜Issue #2560：从全量扫描识别 `BatchEvict` 的规模边界。**
> #2560 的初始测量定位了串行 metadata scan；#2286 随后并行化 Phase 1，并使高比例场景的主要耗时转移到
> Phase 2；#3118 进一步压缩低比例场景的完整候选集合。三者共同展示了瓶颈随实现版本和淘汰比例迁移的过程。

> **重点议题｜Issue #3561：HA 应按完整恢复链评估。**
> #3561 将 cold bootstrap、OpLog GC、snapshot 更新和 snapshot/OpLog 协同归纳为同一恢复闭环。
> 对应方案是 standby 生成快照、指针化发布、后缀回放、reader-safe retention 和真实 etcd 端到端门禁。

## 3. 架构与结论概览

### 3.1 实现事实

1. Eviction 是周期性批处理机制。Master 每 10 ms 检查全局内存使用率，超过高水位或分配失败后执行
   `BatchEvict`。周期性内存下降来自该批处理模型，前台 SLO 由锁竞争、批次规模、命中率和下沉流量共同决定。
2. `BatchEvict` 在整个周期持有 `snapshot_mutex_` 共享锁，并以最多 16 个工作线程逐分片获取元数据写锁。
   候选发现与实际删除是两个独立临界区，实际删除前会按 `(shard, tenant, key)` 重新查找和校验。
3. 非 HA 模式可在释放元数据分片锁后销毁被淘汰副本；HA 模式先记录逻辑删除，再等待 OpLog 持久化回调完成物理回收。
   因此，HA 模式分别记录“已接受淘汰”和“内存可重新分配”两个时间点。
4. HA 由领导权、元数据 OpLog 复制和快照引导三个相互独立的能力层组成。
   etcd、Redis 和 Kubernetes Lease 均可用于选主；连续 OpLog 复制的当前后端是 etcd。
5. 领导权获取、Master RPC 就绪、Store 客户端切换和数据面恢复是不同阶段。
   恢复完成判据由上述四个阶段共同构成。

### 3.2 社区状态

1. 当前生产基线 `v0.3.13.post1` 已包含 Eviction 的主要已合入修复：SSD 淘汰比例修正、前台批量查询按分片聚合、
   低比例候选压缩、HA 容量记账修正和稀疏元数据表收缩。
2. Eviction 的下一阶段聚焦两个结构性边界：每轮 O(N) 元数据普查，以及覆盖整个淘汰周期的
   `snapshot_mutex_` 共享锁。异构 segment 下，全局水位也可能晚于局部分配失败。
3. `v0.3.13.post1` 提供了 HA writer、快照格式、分块捕获、恢复校验和 fencing 的基础组件。
   有界冷启动、有界 OpLog 保留和滚动替换使用独立的部署门禁进行确认。
4. 截至 HA 证据日期，standby 生成快照、`latest`/`fallback` 引导和后缀回放的组件已逐步进入 `main`，
   生产配置接线、真实 etcd 端到端门禁、compaction floor、OpLog 清理和快照 GC 由 #3808 继续跟踪。

### 3.3 综合判断

Eviction 与 HA 采用联合生产验收。HA 会把淘汰的物理内存释放延迟到持久化回调；
OpLog 队列饱和或 etcd 抖动会延长这段时间，并可能放大分配失败、重复淘汰和选主稳定性问题。
生产验收需要在同时启用 HA、真实 etcd、目标对象规模和目标淘汰比例的条件下进行。

## 4. Eviction 主架构

### 4.1 触发与批次目标

Master 的后台线程根据全局内存使用率和异步分配失败标志触发淘汰：

```text
SegmentManager 全局使用率 ----+
                              +-> EvictionThreadFunc -> BatchEvict
PutStart 分配失败 ------------+
             设置 need_mem_eviction_
```

目标比例为：

```text
target = max(configured_eviction_ratio,
             used_ratio - high_watermark + configured_eviction_ratio)

lower_bound = max(target / 2,
                  used_ratio - high_watermark)
```

比例的分母定义为“存在可淘汰内存副本且未被 hard pin 的对象数”；总元数据对象数和字节数属于独立统计量。
因此，对象大小、内存副本数、group 和 SSD-only 对象分布都会使实际释放字节比例偏离对象淘汰比例。

### 4.2 核心执行链

```mermaid
flowchart TD
    Trigger[高水位或分配失败] --> Barrier[获取 snapshot_mutex_ 共享锁]
    Barrier --> Census[最多 16 个线程遍历元数据分片]
    Census --> Select[候选排序或选择性物化]
    Select --> Lookup[按 shard、tenant、key 重新查找]
    Lookup --> Validate[重新校验 deadline、pin 和副本状态]
    Validate --> NonHA[非 HA：删除或下沉]
    Validate --> HA[HA：标记 REMOVED 并提交 OpLog]
    HA --> Durable[持久化回调完成物理释放]
    NonHA --> Cleanup[清理过期副本并收缩稀疏表]
    Durable --> Cleanup
    Cleanup --> Unlock[释放 snapshot_mutex_]
```

每个 census worker 在访问当前分片时持有该分片写锁，并在进入下一分片前释放。
census 形成逐分片观察结果；候选身份的重新查找和最终状态校验为并发变化提供正确性边界。

### 4.3 锁与并发边界

| 阶段 | 锁范围 | 对前台路径的影响 |
|---|---|---|
| 整个 `BatchEvict` | `snapshot_mutex_` 共享锁 | 普通共享锁操作可以并发；需要独占快照锁的管理、快照或卸载操作等待整轮结束。 |
| 元数据普查 | 单个 shard 写锁 | 当前 shard 上的前台查找或修改等待；其他 shard 仍可访问。 |
| 候选选择 | 无 shard 锁 | 不直接阻塞分片访问，但仍持有外层快照共享锁。 |
| 实际淘汰 | 候选所在 shard 写锁 | 重新查找、资格校验和逻辑状态变更均在锁内完成。 |
| group 淘汰 | 先复制 group 成员，再按 shard 顺序逐个加写锁 | 同一时刻最多持有一个成员 shard 锁。 |
| HA 持久化回调 | 快照共享锁，再获取对象 shard 写锁 | 删除 `REMOVED` 副本并完成记账；当前实现中的 buffer 销毁发生在 shard 解锁前。 |

### 4.4 非 HA、SSD 下沉与 HA 回收差异

| 模式 | 逻辑状态变化 | 物理内存释放 | 主要失败边界 |
|---|---|---|---|
| 非 HA | 在 shard 锁内移除副本和更新元数据 | 被选副本延迟到 shard 解锁后销毁 | 重新校验失败时跳过候选。 |
| SSD 下沉 | 增加引用计数并登记 offload task | 下沉成功或允许 force evict 后释放 | 队列失败时默认保留对象，本轮可能没有释放容量。 |
| HA OpLog | 标记副本为 `REMOVED`，提交有序 OpLog | durable callback 重新校验并销毁副本 | reservation、commit、后端持久化或 callback backlog 都会延迟可用容量。 |

## 5. Eviction 社区议题与解决方向

### 5.1 社区议题全景

| 议题 | 机制 | 社区处理 | 生产含义 |
|---|---|---|---|
| [Issue #2560](https://github.com/kvcache-ai/Mooncake/issues/2560)：全量扫描与整周期快照锁 | 每轮遍历 metadata population，并在整个 `BatchEvict` 周期持有快照共享锁 | #2286 并行化 Phase 1；#3118 压缩低比例场景的完整候选集合；#3124 记录保留的 O(N) 边界 | 按实现版本和淘汰比例分别定位 Phase 1、Phase 2 与锁等待。 |
| #2243：SSD 场景一次淘汰接近全部 DRAM 对象 | 早期分母包含 disk-only 对象，这些对象不释放 DRAM | #2286 改用可淘汰内存对象作为分母，并行化第一阶段 | 优先确认版本包含 #2286，再调整水位和比例。 |
| #2405：`BatchExistKey` 在淘汰期间尾延迟升高 | 大批请求逐 key 获取锁，与 census 写锁竞争 | #2405 按 shard 聚合查询并延迟副本销毁 | 直接缓解 eviction 窗口内的查询锁竞争。 |
| #2508：`BatchGetReplicaList` 存在相同访问模式 | 批量副本查询重复获取 shard 锁 | #2508 扩展按 shard 聚合 | 应与 #2405 一并核对。 |
| #3452：淘汰时 Master RSS 峰值或持续偏高 | 大量完整候选对象及淘汰后稀疏 hash map | #3118 减少低比例完整候选；#3576 收缩稀疏 map | `v0.3.13.post1` 同时覆盖两项。 |
| HA term 变化后容量统计异常 | segment 容量跨 term 重复计算或被临时 snapshot reader 释放 | #3154、#3168 修正生命周期记账 | 防止全局使用率偏低并抑制主动淘汰。 |

### 5.2 重点议题：#2560 的归一化时间成本

> **热点｜#2560 的价值是建立可分阶段复测的问题模型。**
> 它把总周期拆分为 metadata traversal、candidate construction、`nth_element`、实际淘汰和
> `snapshot_mutex_` 独占等待，并明确记录测试 commit、对象规模、淘汰比例和 workload 约束。

[Issue #2560](https://github.com/kvcache-ai/Mooncake/issues/2560) 于 2026-06-22 基于
commit `ef0312f8` 报告初始测量。该测量早于 #2286 合入，使用单 tenant、全部对象过期、pin 数量为零、
每个对象一个可淘汰内存副本的合成 workload；第一轮已达到淘汰目标，执行在 lower-bound pass 之前完成。

计时结果按证据代际和计时方式分别归一化。每个计时序列各自令 `T_eviction_cycle = 100%`；
只有同一序列中的同级区间可以相加，嵌套区间仅用于解释父区间的成本组成。

**Pre-#2286：串行 Phase 1**

| 计时序列 | 层级 | 计时区间 | 结果 | 归一化关系 |
|---|---:|---|---:|---|
| Non-instrumented | 0 | `T_eviction_cycle` | 中位约 0.95–1.0 s；5 次范围 0.93–1.2 s | 该序列的 100%；用于报告实际 wall time。 |
| Instrumented | 0 | `T_eviction_cycle` | 100% | 独立归一化基准；绝对时间受 instrumentation overhead 影响。 |
| Instrumented | 1 | metadata traversal | 73%–75% | 周期内部的父区间。 |
| Instrumented | 2 | candidate vector collection | 28%–31% | 已包含在 metadata traversal 中。 |
| Instrumented | 1 | 全部 `nth_element` | 约 0.1% | 周期内部的同级区间。 |
| Instrumented | 1 | `try_evict_group_or_object` | <0.5% | 周期内部的同级区间。 |

两套计时序列分别用于 wall time 和成本 profile。归一化 profile 将主要成本定位在 metadata
traversal；candidate collection 是其中的嵌套成本，不参与同级求和。

**Post-#2286：并行 Phase 1、高比例 target**

| 层级 | 计时区间 | 绝对时间 | 归一化占比 | 计入 100% 的方式 |
|---|---|---:|---:|---|
| 0 | `T_eviction_cycle` | 约 1.5 s | 100% | 整轮基准。 |
| 1 | `T_phase1`：并行 census | 约 35 ms | 2.3% | 周期内部的同级区间。 |
| 1 | `T_phase2`：串行候选应用 | 约 1.24 s | 82.2% | 周期内部的同级区间。 |
| 1 | 其余管理与清理区间 | 约 0.23 s | 15.5% | 由 100% 减去已报告的两个 phase，作为归一化余量。 |

归一化结果显示，#2286 之后该高比例 workload 的主导成本由 Phase 1 转移到 Phase 2。Pre/post
绝对时间对应不同 commit 和测量设置，用于定位各自的瓶颈；#2286 的 A/B 收益由第 5.3 节的
同 workload 对照给出。

`T_snapshot_unique_wait` 是 `T_eviction_cycle` 外部的等待区间，单独报告：

| 证据代际 | P50 | P95 | Max | 解释 |
|---|---:|---:|---:|---|
| Pre-#2286 独立 probe | 约 1.00 s | 约 1.21 s | 约 1.36 s | waiter 与整轮快照共享锁重叠。 |
| Post-#2286 独立 probe | 约 1.44 s | — | — | P50 接近对应的整轮共享锁持有期。 |

Issue 同时提出 per-shard lease-timeout index 或 coarse time buckets，将候选发现从每轮全量扫描
转换为有界提取。当前优化顺序由归一化 profile 决定：高比例场景优先处理 Phase 2，低比例场景
由 #3118 控制完整候选物化量。

#### Phase 1 候选发现方向

#2286 已使 Phase 1 成为较小的周期子区间；#3118 进一步控制低比例场景的完整候选物化量。
per-shard lease-timeout index、coarse time buckets 或 lazy generation 仍可将 O(N)
census 转换为有界候选提取，同时把索引维护加入 lease refresh、pin、replica state 和 erase 路径。
该方向适合作为规模复杂度优化，由更大对象基数和低淘汰比例的 profile 确定实施优先级。

索引方案继续使用稳定 identity 传递候选，并以 shard 锁内 revalidation 作为最终决策边界。评估指标集中于
census wall time、索引内存、前台更新延迟、stale entry 比例和 refill 次数。

#### Phase 2 串行执行方向

post-#2286 的归一化 profile 将 Phase 2 定位为主要周期成本。当前候选循环逐个调用
`try_evict_group_or_object`；每次调用获取候选 shard 写锁，完成 key lookup、资格复核和淘汰。
单个 shard 锁最大持有时间约为 2–3 ms，表明不同 shard 之间具备并行执行空间。

一个可行方向是按 shard 组织候选，使位于不同 shard 的普通对象并行执行，同一 shard 内保持顺序处理。
该结构可以利用现有分片锁的独立性，并保留 key lookup 和锁内 revalidation。group 淘汰跨越多个 shard，
适合采用独立调度域和单一 group 执行权，以保持共享 deadline 与成员淘汰语义。

另一个方向是将 Phase 2 划分为有界批次，并在批次边界调整 `snapshot_mutex_` 的持有范围，从而缩短
独占 waiter 的连续等待时间。HA OpLog、durable callback 和 SSD offload 的处理能力共同构成并行度上限。
这两个方向分别作用于 Phase 2 wall time 和外层锁等待周期，也可以组合评估。

### 5.3 关联 PR：#2286 的 Phase 1 并行重写

> **热点｜#2286 同时修正工作量定义与执行结构。**
> 语义层把目标分母收敛到可释放 DRAM 的对象；执行层把全量 shard census 分配给最多 16 个 worker。

#2286 的直接问题背景是 SSD offload 场景中的目标膨胀。它同时重写了 Phase 1 扫描方式，
因而改变了 #2560 所描述的性能分布：全量 census 继续存在，wall time 从串行遍历转为分片并行遍历。

```mermaid
flowchart TD
    Meta[全部 metadata shards] --> Split[按连续 shard 区间分配]
    Split --> W1[Worker 1<br/>局部计数与候选]
    Split --> W2[Worker 2<br/>局部计数与候选]
    Split --> WN[Worker N，N ≤ 16<br/>局部计数与候选]
    W1 --> Merge[合并 eviction base 与候选]
    W2 --> Merge
    WN --> Merge
    Merge --> Target[按可淘汰内存对象计算 target]
    Target --> Apply[排序、重新校验与回收]
```

社区材料记录的 SSD workload 中，总 metadata 对象为 85,622,691，可淘汰内存对象为 3,776,944。
该 PR 的成本对比如下：

| 成本维度 | #2286 前 | #2286 后 | 对比结果 | 归因边界 |
|---|---:|---:|---:|---|
| target 分母 `B` | 85,622,691 个全部 metadata 对象 | 3,776,944 个可淘汰 DRAM 对象 | 减少约 95.6% | 语义修正：分母改为能够释放目标资源的 population。 |
| 5% target 对应的 victim 数 `K` | 4,310,991 | 约 190,165 | 减少约 95.6% | 主要降低 `T_phase2` 的候选应用次数；原始实现接近淘汰全部可淘汰 DRAM 对象。 |
| Phase 1 census 执行结构 | 串行扫描 | 最多 16 个 worker 按 shard 区间并行 | 可用结果为合并后的 `T_eviction_cycle` | 降低 `T_phase1` wall time，同时保留 O(N) 全量 census。 |
| `T_eviction_cycle` | 约 80 s | 约 2.3 s | 减少约 97.1% | 同一 SSD workload 下的整轮结果；收益由分母修正、`K` 大幅缩小和 Phase 1 并行化共同构成，并行部分的独立贡献需要 Phase 1 隔离计时。 |
| 实际淘汰对象比例 | 接近全部可淘汰 DRAM 对象 | 约 5% | 回到配置目标 | 对象比例与字节比例作为两个独立统计量。 |

这些数字描述对应社区基准；生产结果还受对象大小、group、副本数、SSD queue 和 shard 分布影响。
优化前后的 `BatchEvict` 生命周期边界保持一致：全量 census、候选应用和整轮
`snapshot_mutex_` 共享锁继续覆盖整个周期。改变的是 `B`、`K` 以及 Phase 1 在生命周期内的执行方式。

#2286 的核心价值包括：

- 目标对象集合与可释放资源建立一致语义；
- Phase 1 通过分片并行降低 census wall time；
- worker-local 统计减少共享聚合路径上的同步；
- 后续优化可以在同一正确分母上分别处理候选内存和 Phase 2 时延。

### 5.4 关联 PR：#3118 的低比例候选优化

> **热点｜#3118 优化“复制多少完整候选”，#2286 优化“如何完成普查”。**
> #3118 的 cutoff 与 `collect_candidates` 位于 Phase 1；它缩小完整候选的物化量和后续工作集，
> 同时保留全量 census、目标数 `K` 和逐 shard 加锁。

```mermaid
flowchart TD
    Start[BatchEvict target ratio] --> Ratio{target ≥ 5/22?}
    Ratio -->|是| Full[一次扫描并物化完整候选集]
    Ratio -->|否| Deadlines[第一次扫描只收集 deadline]
    Deadlines --> Rank[nth_element 选择 target + reserve frontier]
    Rank --> Compact{frontier ≤ limit?}
    Compact -->|是| Recollect[第二次扫描仅物化 frontier]
    Compact -->|否| Full
    Recollect --> Revalidate[按身份重新查找并校验]
    Full --> Revalidate
```

时间对比使用 PR head `22f7b6d0` 与 merge-base `6041a609`、byte-identical benchmark
harness 以及相同的编译和运行协议。表中每个时间均表示完整 `T_eviction_cycle`，因此构成可直接比较的 A/B 结果。

| target | baseline `T_eviction_cycle` | #3118 `T_eviction_cycle` | median reduction | conservative reduction / resolution | 完整候选数：baseline → #3118 |
|---:|---:|---:|---:|---:|---:|
| 1% | 288.1 ms | 88.3 ms | 69.9% | 61.5% | 1,000,000 → 11,024 |
| 10% | 529.7 ms | 285.1 ms | 47.1% | 36.0% | 1,000,000 → 110,000 |
| 30% | 898.9 ms | 895.0 ms | 0.4% | 测量分辨率 ±15.2%，结果位于等价区间 | 1,000,000 → 1,000,000 |

优化前后的成本结构对比如下：

| 成本维度 | baseline 完整物化 | #3118 低比例选择性路径 | 成本效果 |
|---|---|---|---|
| metadata traversal | 1 次 O(N) census | 1 次 O(N) deadline census + 1 次 O(N) frontier scan | 增加一次固定 1,024-shard scan，用于缩小完整 identity 工作集。 |
| shard accessor acquisitions | `1,024 + K` | `2 × 1,024 + K` | 增加 1,024 次；在 1M/1% 场景占 9.3%，在 5M/10% 场景占 0.2%。 |
| 完整 `{shard, tenant, key, deadline}` identity | O(M) | O(F)，`F = K + reserve` | 1M/1% 和 1M/10% 场景的物化量分别减少 98.90% 和 89.00%。 |
| 轻量 deadline 存储 | 完整 candidate 内的 deadline | O(M) timestamp vector | 1M 场景 peak 约 16.4 MB，作为选择性路径的新成本。 |
| 净临时内存 | 完整 candidate 与 identity 存储 | 减去 timestamp vector 后的选择性存储 | 1M/1% 和 1M/10% 场景分别节省约 97.2 MB 和 85.9 MB。 |
| `snapshot_mutex_` 生命周期 | 整轮持有共享锁 | census、frontier scan、候选应用和 cleanup 期间持有共享锁 | 整轮锁生命周期保持一致；`T_eviction_cycle` 的降低会同步缩短典型独占 waiter 的重叠窗口。 |

选择性路径的执行结构包含全量 census，并可能通过第二次扫描收集完整身份；它重点控制候选构造和临时内存。
高比例路径直接物化完整集合，保持一次扫描的成本模型。详细决策公式见
[PR #3118 专题](eviction/pr/%233118.md)。

令 `N` 为 metadata 对象总数，`M` 为过期且满足初步资格的非 soft-pin 对象数，`K` 为目标数，
`F` 为 `K + reserve` 形成的 frontier。选择性路径的主要成本为：

```text
O(N) 初始 census，逐 shard 获取写锁，保存 O(M) 个 deadline
-> 平均 O(M) 的 nth_element，计算 reserve_cutoff
-> O(N) frontier rescan，再次逐 shard 获取写锁，仅物化 O(F) 个完整身份
-> 平均 O(F) 的 candidate nth_element
-> Phase 2 串行 lookup、锁内 revalidation 与淘汰
```

`reserve_cutoff` 的 `nth_element` 引入平均线性时间和 deadline vector 内存；执行期间继续持有外层
`snapshot_mutex_` 共享锁。`collect_candidates` 会再次扫描全部 metadata shard 并获取
`MetadataShardAccessorRW`。因此，该优化的收益项是完整 `{shard, tenant, key, deadline}` identity
的构造、字符串存储和后续 candidate 工作集；metadata traversal 和 shard lock acquisition
则作为保留成本，并在选择性路径增加一次 frontier scan。

高比例 pre-bypass 使用 `1,024 + K` 次 accessor acquisition，与 baseline 一致。因此，
额外 frontier scan 的成本和完整 identity materialization 的收益都集中在低淘汰比例。

### 5.5 保留的结构性边界与相关议题

#### #3124：O(N) 普查和整周期快照锁

#2286 和 #3118 分别降低 Phase 1 wall time 与低比例候选物化成本；后续设计空间包括：

- 每轮遍历全部元数据；
- 选择性路径可能进行第二次扫描；
- 高淘汰比例回退到全候选物化；
- 第二阶段串行处理候选；
- `snapshot_mutex_` 覆盖完整批次。

**建议方向**：将长期优化目标定义为“增量或索引化候选发现 + 有界批次 + 缩小快照锁生命周期”。
实现继续保留身份化候选和锁内重新校验，使跨临界区数据只携带稳定身份。

#### #2430：异构 segment 的局部压力不可见

全局使用率聚合全部 segment；segment 级指标负责表达小 segment 的碎片和局部容量压力。
结果可能是 `PutStart` 先返回 `NO_AVAILABLE_HANDLE`，随后才由失败标志触发全局批量淘汰。

**短期方向**：在固定构建上比较等大小逻辑 segment、降低高水位和减小单轮比例三种策略；
同时采集每个 segment 的空闲字节和最大连续可分配区间。

**长期方向**：使触发器感知 segment 级容量、碎片和目标分配大小，并将回收目标从纯对象数扩展到可释放字节或受压 segment。
该方向属于设计建议，其交付状态以未来 containing commit 为准。

#### #2506：配置防御性修正

[PR #2506](https://github.com/kvcache-ai/Mooncake/pull/2506) 处理一个配置文件边界场景：JSON
显式使用字符串 `"false"` 时，Python `bool(non_empty_string)` 会将其解释为 `true`。
配置项省略时的默认关闭行为和 JSON 原生布尔值行为保持正常。

### 5.6 Eviction 方案讨论

- 单纯降低 `eviction_ratio` 会减小单轮峰值，但可能提高 O(N) census 的执行频率。
- 单纯降低 `eviction_high_watermark_ratio` 可提前回收，但会减少有效缓存容量并影响命中率。
- 等大小 segment 是明确的部署缓解措施，并增加 segment 数量；分配器和触发器的长期改进继续处理局部压力语义。
- 对高对象基数场景，优化目标同时覆盖平均周期耗时、shard 写锁等待、独占快照锁等待、
  Master RSS 峰值和每轮释放字节的偏差。

## 6. HA 主架构

### 6.1 三层能力模型

| 能力层 | 作用 | 当前边界 |
|---|---|---|
| Leadership | 选主、租约续期和领导权丢失检测 | 支持 etcd、Redis、Kubernetes Lease；只解决唯一主视图。 |
| OpLog replication | 将有序 Master 元数据变更复制到 standby | 当前依赖 etcd；不复制对象字节。 |
| Snapshot bootstrap | 从基线恢复元数据，再回放 OpLog 后缀 | batch-snapshot 组件已进入分阶段集成，生产接线由 #3808 继续跟踪。 |

选主成功只说明候选者拥有有效 lease。新主还必须完成 standby 最终追平、状态导出、
`MasterService` 恢复、领导权 preflight 和 RPC 启动，之后客户端才能观察新 view 并重连。

### 6.2 主备生命周期

```mermaid
stateDiagram-v2
    [*] --> Standby
    Standby --> Candidate: 当前无有效 leader
    Candidate --> Standby: 竞争失败或发现其他 leader
    Candidate --> CatchUp: 获取 leadership session
    CatchUp --> Restore: 最终回放 durable prefix 并导出状态
    Restore --> Serving: 恢复成功、续租预检成功、RPC 启动
    Restore --> Standby: 恢复失败并释放 leadership
    Serving --> Standby: leadership 丢失，先撤销服务再停止 RPC
```

服务恢复时间由以下区间共同组成：

```text
旧主停止续租
-> 新 view 获取
-> standby 最终追平
-> promotion context 恢复
-> RPC 可达并发布服务状态
-> Store 客户端观察新 view、重连并重新注册
-> 应用首次成功读取并完成字节校验
```

### 6.3 OpLog 写入与回放

Primary 使用 `OrderedOpLogWriter` 预留容量、分配全局序号并按批次写入 etcd。
批次记录和 durable prefix 在同一个 compare-and-put 事务中推进：

```text
/oplog/<cluster>/batches/<batch_id> = OpLogBatchRecord
/oplog/<cluster>/durable_prefix     = {batch_id, last_seq}
```

Standby 只消费 durable prefix 覆盖的连续批次。缺失批次、prefix 回退、批次或 entry 序号断裂、
checksum 失败和 apply 失败都会阻止完整追平；结构性错误会使 standby 进入 `FAILED`。

Primary 的元数据变更分为两类：

- **持久化前可见**：先改变 live metadata，再将记录送入 writer。
- **持久化后完成**：先标记逻辑状态，等 durable callback 后释放物理资源并完成记账；淘汰属于此类。

`Commit` 成功表示 writer 已接受 entry；durable prefix 的推进是后续持久化检查点。

### 6.4 快照与提升路径

目标架构为：

```text
primary:  fenced ordered batches -----------------------------+
                                                             |
standby:  恢复 latest/fallback snapshot -> 回放 suffix -> 追平
                    |                              |
                    +-> 周期生成并发布校验后的 snapshot
                                      |
new standby:        从 snapshot 启动 -> 回放 suffix
                                      |
retention:          发布 compaction floor -> 安全清理旧批次
```

当前代码包含 batch snapshot descriptor、分块捕获、artifact writer、维护 lease、
`latest`/`fallback` 发布与 provider 等组件。证据快照中的生产构造路径使用
`CatalogBackedSnapshotProvider`；coordinator、batch provider、公开生产开关与保留策略按 #3808 的阶段计划接入。

## 7. HA 社区议题与解决方向

### 7.1 重点议题：#3561 定义的 HA 恢复闭环

> **热点｜Issue #3561 提供了 HA 架构评估的四问题模型。**
> 四项问题分别对应引导起点、历史保留、基线新鲜度和两种恢复机制的协同关系。
> #3808 将相关实现组织为生产接线、故障门禁和 retention 里程碑。

```mermaid
flowchart TD
    Q1[#3561-1<br/>新 standby 的回放起点] --> Bootstrap[verified snapshot bootstrap]
    Q3[#3561-3<br/>snapshot 基线新鲜度] --> Capture[standby 周期捕获与发布]
    Q4[#3561-4<br/>snapshot + OpLog 协同] --> Suffix[从 snapshot cursor 回放 suffix]
    Bootstrap --> Suffix
    Capture --> Pointer[latest / fallback 指针]
    Pointer --> Bootstrap
    Suffix --> CaughtUp[standby 追平 durable prefix]
    CaughtUp --> Floor[发布 reader-safe compaction floor]
    Q2[#3561-2<br/>etcd OpLog 生命周期] --> Floor
    Floor --> GC[批次删除、snapshot GC、配额与告警]
    CaughtUp --> Promote[promotion + restore + serving 门禁]
```

[Issue #3561](https://github.com/kvcache-ai/Mooncake/issues/3561) 的四项原始关切与解决链映射如下：

| #3561 议题 | 已有进展 | 目标闭环 |
|---|---|---|
| 新 standby 从 sequence 1 回放历史成本过高 | #3642 支持 `latest`、`fallback` 和 suffix replay；#3794 增加周期 coordinator | N08 生产接线、公开配置和真实 etcd E2E 门禁。 |
| etcd OpLog 持续增长 | 已定义 compaction floor 等协议基础 | floor-aware rebootstrap、安全批次删除、snapshot GC、floor 发布和实际 pruning。 |
| snapshot 在 standby 初始化时提供一次基线 | #3326、#3447、#3640、#3794 形成捕获、写入、fenced 发布和调度组件 | 周期 pointer 推进、capture 指标和对象存储故障门禁。 |
| legacy primary snapshot 与 OpLog 使用独立运行模式 | 目标架构采用 standby 生成 batch-OpLog snapshot | 生产中持续验证 pointer 推进、新 standby 引导和 suffix 连续性。 |

**建议方向**：以完整恢复链为交付单位。最小闭环同时包含：

1. fenced writer；
2. standby 周期快照和校验后发布；
3. `latest` 损坏时回退 `fallback`；
4. snapshot cursor 后缀回放；
5. reader-safe compaction floor；
6. OpLog 和 snapshot 对象 GC；
7. 真实 etcd 故障注入和容量上限测试。

这一问题模型的价值在于把“恢复速度”和“历史清理”放入同一安全协议：snapshot cursor
定义回放起点，所有活跃 reader 的可恢复边界共同定义 compaction floor，GC 只处理 floor 之前的历史。
因此，bootstrap、promotion 和 retention 使用同一组序号、checksum、pointer identity 和 fencing 规则。

### 7.2 提升恢复的失败原子性：#3497 / #3760 / #3806

#3497 使 restore 失败的候选者保持 non-serving 并释放领导权，建立了正确的 fail-closed 门禁。
#3760 表明单个重叠 descriptor 可导致整次恢复失败。#3806 的方向是丢弃有歧义的副本，同时保留独立有效副本。

**讨论**：fail-closed 建立数据正确性边界，同时引入可用性取舍。容错恢复为每个被丢弃副本记录
tenant、key、segment、offset、原因和恢复决策，并通过对象字节校验确认保留副本可用。
serving 门禁只接收通过恢复校验且具有可信基线的 promotion context。

### 7.3 客户端重连：#3740 / #3743

#3740 观察到新主索引完整，但 Store 客户端仍在旧 endpoint 上重试约两分钟。
解决方向是为运行期连接建立有界超时、及时消费新 MasterView，并把 endpoint switch、remount
和首次成功业务读取共同纳入 RTO，etcd 选主时间作为其中一个分量。

### 7.4 滚动升级和 promotability：#3774

`v0.3.13` 的生产材料尚未形成可保持索引的 Master 滚动升级证据。新 standby 的 `lag=0`
表示当前观测点没有待回放记录；promotability 进一步验证非空、完整且与 primary 对齐的基线。

**建议方向**：增加显式 promotability/readiness 判据，至少联合检查 applied cursor、durable prefix、
key count、segment count、snapshot identity、restore validation 状态和应用字节探针。

### 7.5 物理分配所有权：#3826 / #3858

恢复后的 NoF descriptor 可能引用被新 allocator 再次分配的 offset，形成错误数据读取风险。
当前建议方向是隔离所有权待确认的区间，以受控可用性换取数据正确性；通过 allocator
import/reservation 协议或等价机制恢复物理所有权后，再发布 `COMPLETE` descriptor。

### 7.6 Writer fencing、持久化门禁与 fail-stop

- #3810 将 producer view claim 和每批事务 fencing 接入生产 writer。
- #3885 防止 `PutEnd` 在 reservation 或 admission 失败时暴露 `COMPLETE`，但 accepted entry
  与后端持久化属于两个检查点。
- #3923 曾提出 writer terminal failure 后由 supervisor fail-stop；证据日期的状态存在页面与搜索结果不一致，
  必须通过 containing merge commit 或替代 PR 验证实际行为。
- #3860 缓解 OpLog/eviction 压力下的重复淘汰，并隔离 leadership keep-alive 与通用 etcd client reset；
  完整持久化门禁由 writer admission、durability 和 supervisor lifecycle 共同组成。

### 7.7 其他社区议题

报告保持对相关社区工作的广覆盖，重点议题之外还包括：

| 记录 | 讨论范围 | 建议验证 |
|---|---|---|
| #3496 | etcd 命名测试与生产 etcd 路径的覆盖一致性 | 使用外部真实 etcd 执行 bootstrap、poll、promotion 和 backend retry。 |
| #3761 | legacy 非 OpLog snapshot 恢复后的 lease 语义 | 以持久化时间、恢复时间和对象可见性验证 lease 重建策略。 |
| #3638 | P2P HA promotion 的 OpLog sequence 所有权 | 比较本地 applied sequence、存储 latest sequence 和新 writer 起始值。 |
| #3808 | batch-snapshot 生产接线、retention、身份和可观测性总路线 | 按里程碑和 containing commit 维护能力矩阵。 |
| #2584 | `BatchEvict` 可重复规模与锁等待 benchmark | 使用相同对象数和比例进行版本对比，再以生产分布复核。 |
| #3124 | 选择性物化后的 O(N) 边界 | 将 census、materialization、Phase 2 和独占锁等待分别计时。 |

## 8. Eviction 与 HA 的交叉讨论

### 8.1 淘汰完成具有两个时间点

HA 开启后，淘汰存在“逻辑删除已提交”和“持久化回调已释放内存”两个时间点。
如果运维只观察 `evicted_count`，可能误判当前已有足够可分配容量。建议同时观察：

- attempted/successful eviction 数量与字节；
- OpLog pending entries、committed queue 和 callback queue；
- durable sequence 与 applied sequence；
- allocator active bytes 与实际分配失败；
- 从提交淘汰到物理容量可用的延迟。

### 8.2 OpLog 背压会放大内存压力

etcd 延迟、writer queue 饱和或 callback backlog 会延迟 `REMOVED` 副本回收。
此时继续按逻辑淘汰计数推进批次，可能出现“指标显示已淘汰，但 `PutStart` 仍失败”的窗口。
解决方向包括：

1. 将 durable callback 进度纳入淘汰触发和完成指标；
2. writer terminal 或长期饱和时停止无效重复扫描；
3. 对回调中的 buffer 销毁锁范围进行基准测试，评估是否可安全移到 shard 解锁之后；
4. 将 etcd 故障、队列压力、Eviction SLO 和 HA 功能组合为同一测试场景。

第 3 项是基于现有锁结构的设计建议，需要在保持 replica ID 复核、quota 记账和对象生命周期安全的前提下实现。

### 8.3 容量记账会影响淘汰触发

HA term 切换、snapshot reader 和 segment 生命周期中的容量重复计算或错误释放会使全局使用率失真。
#3154 和 #3168 是 Eviction 与 HA 的共同前置修复。验收必须比较 term 切换前后的 mounted capacity、
`master_total_capacity_bytes`、allocator 实际容量和 leader 切换结果。

### 8.4 快照锁竞争与恢复操作

`BatchEvict` 持有整周期快照共享锁，需要独占快照锁的管理和生命周期操作会等待。
在大对象基数或高淘汰比例下，这会直接影响 snapshot、unmount 或其他恢复边界的时延。
因此，未来缩小快照锁范围需要与 snapshot capture 一致性协议共同设计，并在 Eviction 与 snapshot
两条路径上使用统一的状态边界。

## 9. 实施路线细化

以下内容细化第 2 节的方案。社区交付状态与报告建议分别记录。

### P0：建立可复现基线

1. 将 Master 和 Store client 的当前生产基线固定为 `v0.3.13.post1`，记录镜像摘要、tag commit
   `719735896c86` 和 etcd 版本。
2. 将第 2.1 节中已包含的 Eviction PR 作为当前能力清单；HA 功能升级逐项核对所需
   post-release PR 的 containing commit。
3. 在同一时间轴采集 Eviction、OpLog、领导权、客户端切换、allocator 和应用 SLO 指标。
4. 使用真实 etcd 和生产对象分布重复执行 fill、evict、failover、refill 流程。

### P1：建立正确性门禁

1. 对当前基线中的 #2286、#2405、#2508、#3118、#3154、#3168、#3576 执行第 2.1 节所列行为验收。
2. HA 构建必须具备 restore fail-closed、producer-view fencing、ReplicaID 保留和有界 promotion restore。
3. 对 `PutEnd`、remove 和 eviction 分别验证 admission 失败、持久化失败与 callback 延迟行为。
4. 为 NoF、LOCAL_DISK、DFS 分别验证 descriptor 与物理分配所有权。

### P2：完成有界恢复闭环

1. 接通 standby 周期 snapshot、`latest`/`fallback` 发布和新 standby 引导。
2. 定义 reader-safe compaction floor，并在 rebootstrap 成功后清理旧 batch。
3. 为 snapshot artifact、pointer 和 OpLog 分别建立 GC、配额、告警和灾难恢复流程。
4. 将 etcd `NOSPACE`、损坏 `latest`、对象存储故障和网络分区纳入门禁。

### P3：降低规模相关抖动

1. 设计增量或索引化 eviction candidate 数据结构，减少每轮 O(N) 普查。
2. 将淘汰工作拆为有界批次，评估缩小 `snapshot_mutex_` 生命周期。
3. 增加 segment 级压力和碎片感知，避免全局水位晚于局部分配失败。
4. 评估 HA durable callback 中 allocator deallocation 的锁外执行方案。

## 10. 联合验收标准

候选构建只有在重复测试中同时满足以下条件，才能进入生产：

- 正常水位触发早于分配失败，周期性 `NO_AVAILABLE_HANDLE` 计数保持为零；
- 每轮实际对象淘汰比例和释放字节量处于预期范围；
- eviction 窗口内查询、写入和应用 p99 满足 SLO；
- OpLog 队列压力下，逻辑淘汰与物理内存释放延迟有明确上限；
- standby 从有效 snapshot 恢复非空基线，并只回放必要后缀；
- promotion 使用可信 cursor，restore 或 fencing 错误使候选保持 non-serving；
- 已确认写入的对象在旧主失效后仍存在且字节一致；
- Store 客户端在定义的 RTO 内完成 endpoint 切换、remount 和首次成功读取；
- term 切换前后 ReplicaID、segment allocation、容量和 key count 保持一致；
- etcd 增长、snapshot 大小、capture pause、bootstrap 时间、promotion RSS 和 replay 时间均有上限；
- 冷启动、滚动替换、进程终止、网络中断、etcd 重启、对象存储失败、snapshot fallback
  和 `NOSPACE` 注入均通过相同正确性标准。

## 11. 版本选择结论

当前生产基线为 `v0.3.13.post1` / `719735896c86`。该基线已覆盖第 2.1 节的 Eviction
和前台查询必要 PR。现有生产观测未显示 `BatchEvict` 对 KV 传输时延或 TTFT 产生统计上明确的
显著影响，因此该版本继续作为 Eviction 生产基线，并保留 request-shard 锁等待监控。
HA 升级使用已启用能力作为范围：
promotion 正确性、snapshot publication/bootstrap、production writer fencing、bounded promotion 和
OpLog/eviction 压力管理分别选择包含相应 post-release PR 的单一 pinned commit。

完整 HA 生产能力通过 batch snapshot 生产接线、有界 retention、真实 etcd 端到端门禁和部署级能力清单共同确认。
生产环境使用固定 commit，并持续运行经过验证的 standby；升级或回滚后重新执行 Eviction 与 HA 联合验收。
