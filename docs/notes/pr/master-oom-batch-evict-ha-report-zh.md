# Mooncake Master OOM 与 BatchEvict 锁竞争分析报告

> - 报告日期：2026-09-03
> - 主要范围：Mooncake Store Master、HA、分布式 DRAM 淘汰
> - 生产问题基线：`v0.3.12.post1`
> - 建议验证基线：`v0.3.13.post1`

## 1. 执行摘要

近期讨论集中在两个相互关联、但机制不同的问题上。

| 问题 | 直接表现 | 主要原因 | 修复归属 | 当前判断 |
|---|---|---|---|---|
| Master RSS 持续增长并触发 OOM | 2 至 3 小时增长约 7 GiB，流量停止后 RSS 仍处于高位 | 旧 BatchEvict 为每个 eligible entry 复制完整 Candidate；千万级对象形成多 GiB 临时分配，allocator arena 保持高水位 | 主修复 #3118；HA 放大项 #3154/#3168；补充收缩 #3576 | #3452 已按 `main` 具备修复关闭；原生产环境尚未完成同配置复现 |
| BatchEvict 扫描与锁竞争 | 百万对象下单轮接近秒级，独占 `snapshot_mutex_` 的控制面操作同步等待 | 每轮执行 O(N) 元数据 census，并在整轮期间持有全局 `snapshot_mutex_` 共享锁 | [PR #3118](https://github.com/kvcache-ai/Mooncake/pull/3118) | 已显著减少低淘汰比例的候选构造和执行时间；O(N) census 与全程共享锁仍保留 |

两个问题的交点是 BatchEvict：它既在内存压力发生时分配临时候选数据，又把扫描和执行时间放入全局共享锁的持锁区间。
PR #3118 缩小了临时候选集合，同时降低临时内存和低比例淘汰耗时。
PR #3576 是后续审计发现的补充优化，负责回收淘汰后稀疏元数据表的桶容量。

截至 2026-09-03，[issue #3452](https://github.com/kvcache-ai/Mooncake/issues/3452)
已按“`main` 已修复”关闭。
[关闭说明](https://github.com/kvcache-ai/Mooncake/issues/3452#issuecomment-5408686978)
将 #3118 认定为主修复，将 #3154/#3168 认定为 HA 条件放大项修复。
[Issue #2560](https://github.com/kvcache-ai/Mooncake/issues/2560) 和
[RFC #3124](https://github.com/kvcache-ai/Mooncake/issues/3124) 仍为开放状态，
对应 BatchEvict 的 O(N) census 和全局共享锁结构性边界。

## 2. Master、HA 与存储层架构

Master 保存对象、位置、副本、租约和后台任务的元数据。
分布式 DRAM 中的KV payload 位于 Store Worker 或客户端挂载的内存段中，通常不驻留在 Master。
因此 Master OOM 的主要分析对象是元数据规模、临时工作集、容器容量和后台队列。

```text
SGLang / Store Client
        |
        | PutStart / PutEnd / GetReplicaList / ExistKey
        v
+---------------------- serving authority ----------------------+
| HA serving Master                                             |
|                                                               |
| snapshot_mutex_                                               |
|   shared: 普通 RPC、BatchEvict、元数据修改                    |
|   unique: 需要全局一致视图的快照、恢复或重挂载阶段            |
|        |                                                      |
|        +--> metadata shard[0..1023]                           |
|        |       `--> tenant -> key -> ObjectMetadata           |
|        |              |-> replicas / lease / pin / group      |
|        |              `-> offload / replication task state    |
|        |                                                      |
|        `--> EvictionThreadFunc                                |
|                `--> BatchEvict                                |
|                                                               |
| metadata mutation --> ordered OpLog --> durable backend       |
+-------------------------------------------|-------------------+
                                            v
                                  standby apply / snapshot
                                            |
                                            v
                                   promotion after failover

Store Worker / holder
    |-> distributed DRAM payload
    |-> optional LOCAL_DISK payload
    `-> allocation, offload and removal execution
```

### 2.1 元数据分片的职责与并发模型

`MasterService` 固定包含 1,024 个 `MetadataShard`。
这些分片是单个 Master 进程内的逻辑元数据分区和锁分区。
每个分片包含 tenant map；每个 `TenantState` 再包含对象元数据、processing key，
以及 replication、offload、promotion 和 dynamic-replication 状态。
Replica descriptor 记录 payload 的位置，分布式 DRAM 和 LOCAL_DISK payload 由 Store Worker 持有。

当前 `main` 的对象路由规则为：

```text
default tenant:  hash(key) % 1024
named tenant:    hash_combine(hash(tenant_id), key) % 1024
```

1,024 个数组元素随 `MasterService` 一起创建。
每个 `MetadataShard` 持有一个独立的 `SharedMutex`，实际对象只进入 hash 选中的分片。锁访问规则为：

- `MetadataShardAccessorRO` 获取目标 shard 的共享锁；
- `MetadataShardAccessorRW` 获取目标 shard 的独占锁；
- 同一 shard 的读操作可以并发，写操作与该 shard 的其他访问串行化；
- 不同 shard 使用不同锁，因此可以并行处理。

Shard 提供数据和锁边界；并行执行由 RPC runtime 或显式 worker thread 提供。
普通单 key 调用链如下：

```text
RPC worker
  -> getShardIndex(tenant_id, key)
  -> MetadataShardAccessorRO/RW(shard[index])
  -> shard[index].mutex
  -> tenants[tenant_id] -> metadata[key]
```

批量 API 可以先按 shard 聚合 key。
例如 `BatchRemove` 对每个涉及的 shard只取一次独占锁，然后在该锁内处理属于该 shard 的全部 key。
该聚合减少重复加锁；函数本身按 shard 顺序处理，线程并行度仍由调用路径决定。

`BatchEvict` 显式创建 16 个 census worker。
每个 worker 分配连续且互不重叠的 64 个 shard，并且一次只持有一个 shard 的独占锁。
census 同时清理过期的processing replica，因此采用 RW accessor。
worker 将计数、lease deadline和候选写入各自的局部容器；调用线程等待全部 worker 完成后合并结果。
候选执行阶段为串行流程，并为每个候选重新获取其 shard 锁，完成查找和状态复核。

```text
Eviction thread -> BatchEvict
  -> acquire snapshot_mutex_ shared
  -> start 16 census workers
       worker 0  -> shard 0   -> ... -> shard 63
       worker 1  -> shard 64  -> ... -> shard 127
       ...
       worker 15 -> shard 960 -> ... -> shard 1023
  -> join workers -> merge counts and candidate frontier
  -> serial candidate execution -> reacquire victim shard
```

Group 路由存在版本边界：

- `v0.3.13` 的
  [`getMetadataShardIndex`](https://github.com/kvcache-ai/Mooncake/blob/b04c6a4b6a32e98cf17756a4dc747c950669c1ea/mooncake-store/src/master_service.cpp#L1325-L1333)
  对已注册成员使用 `hash(group_id)`，同组成员通常位于同一 shard；
- 当前 `main` 按 tenant 和 member key 路由对象，并使用独立的 group domain
  保存成员关系。group eviction 按 shard 划分成员，再按 shard index 升序访问。

两个版本都在 group 范围判断 lease eligibility，并把选中的 group 展开为成员。
随后仍逐个复核 member 和 replica，因此受 pin、replica 状态或 persistence
状态保护的 member 或 replica 可以保留。

Shard index 的作用域位于单个 Master 进程内。HA failover 提升完整的 serving
authority 及其全部 metadata keyspace，分片编号本身不形成跨 Master 的所有权分配。

源码证据：

- [`MetadataShard` 和 1,024 元素数组](../../../mooncake-store/include/master_service.h#L1656)
- [RW/RO shard accessor](../../../mooncake-store/include/master_service.h#L1790)
- [tenant 与 key 的 shard 路由](../../../mooncake-store/include/master_service.h#L1905)
- [`BatchRemove` 按 shard 聚合](../../../mooncake-store/src/master_service.cpp#L6815)
- [`BatchEvict` 的 16-worker census](../../../mooncake-store/src/master_service.cpp#L10410)
- [跨 shard 的 group eviction 顺序](../../../mooncake-store/src/master_service.cpp#L1669)

### 2.2 锁层级与竞争传播

BatchEvict 涉及三类主要同步边界：

1. `snapshot_mutex_` 是 Master 级全局边界。BatchEvict 在候选 census 前获取
   共享锁，并在本轮结束前保持该锁。
2. 每个 `metadata shard` 的独立锁保护该分片内的 tenant 和对象元数据。
   census 由 16 个 worker 划分全部 shard，每个 worker 逐个取得目标 shard
   的独占锁；执行阶段重新定位候选并取得对应分片写访问。
3. `ObjectMetadata` 内部锁保护 lease、soft pin 和副本状态等对象级字段。

这里的性能问题可描述为“全局共享锁作用域过宽”，而不是互斥锁调用次数本身。
BatchEvict 的长扫描允许其他共享锁读者并发，但会延迟需要 `snapshot_mutex_` 独占锁的快照、恢复和控制面操作；
分片扫描与修改还会在访问对应 shard 时和前台请求竞争。

当前源码锚点：

- [`EvictionThreadFunc`](../../../mooncake-store/src/master_service.cpp#L9114)
  采样 DRAM 使用率、计算 target/lower bound 并调用 BatchEvict。
- [`BatchEvict`](../../../mooncake-store/src/master_service.cpp#L10056)
  定义候选选择、状态复核、淘汰和 offload 路径。
- [BatchEvict 的全局共享锁和 census](../../../mooncake-store/src/master_service.cpp#L10408)
  展示 `snapshot_mutex_` 作用域和 metadata shard 遍历。
- [`ShrinkBucketsIfSparse`](../../../mooncake-store/include/master_service.h#L125)
  定义 PR #3576 引入的稀疏容器收缩条件。

### 2.3 HA 对淘汰的约束

HA 模式需要使 serving Master 和恢复状态对同一淘汰结果达成一致。
当前 `main` 路径将淘汰表示为有序 OpLog 元数据变更；
可读副本先进入逻辑删除状态，durable 完成回调再执行物理资源和元数据清理。
若 OpLog 预留或提交失败，本轮跳过相应候选或停止扫描，并保留可恢复的一致状态。

[PR #3422](https://github.com/kvcache-ai/Mooncake/pull/3422) 是 #3118 之后最新合入的 BatchEvict 专项正确性修复之一。
它区分 OpLog 预留、提交、fence等错误并恢复被拒绝的提交。
该 PR 处理 HA 错误传播和回滚，作用域独立于#3118 的 scan/lock 性能优化。

这使 BatchEvict 的成本不仅包括候选选择，还可能包括：

- OpLog 预留、提交和 durable finalize；
- 元数据 shard 的重新定位与状态复核；
- group member 扩展；
- LOCAL_DISK offload 排队和源副本引用计数；
- 淘汰事件和指标更新。

因此，大比例单轮淘汰会同时放大 Master CPU、持锁时间、OpLog 变更量和下游SSD/网络工作量。

## 3. 问题一：Master RSS 增长与 OOM

### 3.1 生产现象

[Issue #3452](https://github.com/kvcache-ai/Mooncake/issues/3452) 报告的环境为
Mooncake Master `v0.3.12.post1`、HA 开启、SGLang PD 分离和 RDMA。
压测使用固定 40,000-token 随机 prompt、20,000 个请求。
Master RSS 在 2 至 3 小时内增长约 7 GiB，并在 8 GiB Pod 限额处被 OOM Kill；请求结束后 RSS 仍保持高位。

当 SGLang page size 为 64 tokens 时，一个请求约包含：

```text
40,000 / 64 = 625 个逻辑 KV page
```

Mooncake backend 会把逻辑 page 展开为一个或多个带布局、rank 等后缀的对象键。
随机 prompt 的跨请求前缀复用率较低，因此该流量可以形成数百万级不同对象。
这首先构成高基数 live metadata 工作负载；它还不能单独证明不可达对象泄漏。

### 3.2 RSS 的组成与判别

| 内存来源 | 生命周期 | 对 issue #3452 的意义 |
|---|---|---|
| 活跃对象和副本元数据 | 随 live object count 增长，淘汰或删除后释放 | 高基数 workload 的预期成本 |
| BatchEvict 完整候选 identity | 单轮 BatchEvict 临时分配 | issue 关闭说明认定的主要代码级根因 |
| `TenantState::metadata` bucket array | map 扩容后保持历史峰值 | 后续结构审计确认的补充 RSS 放大项 |
| offload、replication、promotion 等任务状态 | 成功、失败或超时后清理 | 需要通过队列 cardinality 排除生命周期缺陷 |
| snapshot、restore 和 OpLog buffer | 快照、恢复或复制阶段 | HA 模式下的临时峰值来源 |
| allocator retained/fragmentation | 逻辑对象释放后仍可能保留物理页 | 解释临时 Candidate 销毁后 RSS 保持峰值 |

关键判别关系是：

```text
live object count 持续增长
    -> 缓存仍在扩张，或淘汰未形成稳定平衡

live object count 已稳定，bucket count 仍保持历史峰值
    -> 稀疏 unordered_map 容量滞留

live object count 与 bucket count 均下降，allocator active 下降而 RSS 不降
    -> allocator retained、碎片化或 OS 回收策略
```

### 3.3 Issue 关闭结论与 PR #3118

[Issue #3452 的关闭说明](https://github.com/kvcache-ai/Mooncake/issues/3452#issuecomment-5408686978)
将旧 BatchEvict 的完整 Candidate 构造认定为根因。
旧实现为每个 eligible metadata entry 构造包含 `tenant_id` 和 key 字符串副本的 Candidate，
再合并所有 worker 的候选向量，最后才选择真正 victims。

在数千万对象规模下，该路径产生多 GiB 临时分配。
Candidate 销毁会结束这些 C++ 对象的生命周期，而 glibc/jemalloc arena 可以继续保留已申请的 pages，
从而形成“工作负载停止后 RSS 仍处于高位”的现象。
关闭说明据此将 [PR #3118](https://github.com/kvcache-ai/Mooncake/pull/3118) 作为主修复：
先执行 轻量 lease timestamp census，再为 target frontier 和 bounded reserve 物化完整 identity。

同一关闭说明把 [PR #3154](https://github.com/kvcache-ai/Mooncake/pull/3154) 
和 [PR #3168](https://github.com/kvcache-ai/Mooncake/pull/3168) 列为 HA 条件 放大项修复。
旧版本在 leadership term 切换后可能重复累计 segment capacity，使全局使用率偏低并抑制主动淘汰。
该条件需要发生 leadership transition。

三项修复分别于 2026-07-28、2026-07-30 和 2026-08-03 合入，均早于 issue 在 2026-08-15 创建。
维护者未在原始 H100/GLM-5.2 环境复现 OOM；
issue 依据代码机制匹配和 `main` 已包含修复，于 2026-08-26 关闭。
升级后的同类部署若再次出现 OOM，关闭说明要求使用明确的 `main` SHA 重新开启 issue，
并附带 jemalloc `prof` 或 `/proc/PID/smaps` 等 heap/RSS 证据。

### 3.4 PR #3576 的补充修复

[PR #3576](https://github.com/kvcache-ai/Mooncake/pull/3576) 于 2026-08-25 合入，来源于 issue 讨论中的补充 metadata lifecycle 审计。
其源代码依据是：`std::unordered_map::erase()` 删除节点时不会主动缩小 bucket array。
曾经容纳数百万对象的 shard 即使已淘汰大部分对象，也会保留历史高水位的桶内存。

该 PR 增加 `ShrinkBucketsIfSparse()`，条件为：

```text
bucket_count > 1024
and
live size < bucket_count / 4
```

满足条件后执行：

```text
rehash(live size * 2)
```

BatchEvict 记录本轮真正发生淘汰的 shard，并在执行末尾取得 shard 锁，对这些 shard 中的 `TenantState::metadata` 进行条件收缩。
两倍 live size 的目标为后续插入保留空间，四分之一阈值和 1024 bucket 下限用于控制 rehash 频率。

PR 中包含两个区分性测试：阈值单元测试，
以及同一 shard 写入 2048 个对象后执行 BatchEvict、验证 bucket count 降至原高水位一半以下的服务级测试。

### 3.5 相关修复集合

OOM 风险还受到以下已合入修改影响：

- [PR #3118](https://github.com/kvcache-ai/Mooncake/pull/3118)：低淘汰比例下
  仅物化 target frontier 和 bounded reserve 的完整候选 identity。
- [PR #3154](https://github.com/kvcache-ai/Mooncake/pull/3154) 与
  [PR #3168](https://github.com/kvcache-ai/Mooncake/pull/3168)：修正 HA
  leadership term 相关的容量指标所有权，避免容量分母虚高间接抑制淘汰。
- [PR #3160](https://github.com/kvcache-ai/Mooncake/pull/3160)：对象元数据删除时
  清理 LOCAL_DISK segment 的 `offloading_objects` 镜像项。
- [PR #3576](https://github.com/kvcache-ai/Mooncake/pull/3576)：回收淘汰后
  稀疏 metadata map 的 bucket array。

这些修改覆盖 issue 关闭根因、容器容量、HA 指标生命周期和 SSD offload 生命周期。
#3576 扩充了 `main` 的 RSS 防护范围，但不替代关闭说明中对 #3118 和 #3154/#3168 的根因归属。
生产验证仍需区分 live state、bucket capacity、allocator active 和 RSS/PSS。

## 4. Mooncake DRAM 淘汰策略与调用逻辑

### 4.1 触发条件

`EvictionThreadFunc` 周期性采样全局 distributed-DRAM 使用率 `U`。
以下任一条件触发 `BatchEvict`：

```text
U > H
or
need_mem_eviction == true and R > 0
```

其中 `H` 是高水位，`R` 是配置的基础淘汰比例。
前台 `PutStart` 分配失败会设置 `need_mem_eviction`，后台线程在下一次 wake 处理。默认检查周期约为 10 ms。

### 4.2 target 与 lower bound

每轮在调用 BatchEvict 前计算：

```text
target ratio = max(R, U - H + R)
lower bound  = max(target ratio / 2, U - H)
```

`target ratio` 表示期望形成的额外 headroom。
若对象数量和字节分布近似成比例，完整 target 会把使用率从 `U` 拉到约 `H - R`。
`lower bound` 是第一轮实际淘汰不足时的第二轮目标，至少覆盖高水位以上的超额部分。

例如 `U=93%`、`H=90%`、`R=5%`：

```text
target = 8%
lower bound = 4%
```

比例来自字节使用率，随后应用到“可淘汰对象数”。因此策略控制单位是对象数，
实际回收单位是 DRAM replica buffer。
对象大小差异较大时，完成对象数目标并不等价于完成字节目标。

### 4.3 单轮 BatchEvict 生命周期

```text
Foreground PutStart              Eviction thread
        |                               |
        |-- allocation failure -------->| set need_mem_eviction
        |                               |
        |                        [periodic wake]
        |                               |--? sample U and flag
        |                               |--? compute target/lower
        |                               `--> BatchEvict(T, L)
        |                                      |
        |                                      -> capture one `now`
        |                                      -> acquire snapshot shared lock
        |                                      -> parallel metadata census
        |                                         (16 workers, one shard lock each)
        |                                      -> compute eviction_base
        |                                      -> select lease cutoff/frontier
        |                                      -> first pass toward target
        |                                      -> release expired discards
        |                                      -> second pass to lower bound
        |                                      -> shrink affected maps
        |                                      -> metrics / need flag update
        |                               <------ return
        |                               |
        |                        [next periodic wake]
        |                               `--? resample U and flag
```

候选和执行规则如下：

- census 访问每个 `ObjectMetadata`，检查 hard pin 和可淘汰 DRAM replica。
- 具有至少一个 eligible DRAM replica 的对象为 `eviction_base` 贡献一个计数。
- 过期 lease deadline 用于近似 LRU 排序；候选在执行前重新查找并复核状态。
- 普通对象被选中后，移除当前所有 complete、readable 且 `refcnt == 0` 的
  DRAM replicas；SSD、DFS 和正在使用的 replicas 保留。
- grouped object 在候选执行时展开当前成员。
  组级 lease 条件通过后，各成员继续接受 pin、replica、offload 和 HA persistence 检查。
- 配置 `offload_on_evict` 且具备 LOCAL_DISK 时，DRAM-only 对象可以先排入异步 offload；
  已有 disk replica 的对象可以直接回收 eligible DRAM replica。

### 4.4 HA 下的淘汰提交

简化的 HA 调用链为：

```text
candidate revalidation
    -> build post-eviction replica descriptors
    -> reserve ordered OpLog slot
    -> mark selected replicas logically REMOVED
    -> commit OpLog entry
        -> durable callback
            -> finalize replica removal
            -> release quota / buffer / metadata resources
```

这一顺序保证 standby 或恢复后的 Master 能看到一致的副本拓扑。
OpLog 错误会降低本轮可达的 target/lower-bound 数量，同时增加 shard 持锁与重试路径成本。

## 5. 问题二：BatchEvict 扫描和锁竞争

### 5.1 Issue #2560 的测量

[Issue #2560](https://github.com/kvcache-ai/Mooncake/issues/2560) 使用真实
`MasterService::BatchEvict` 路径，在单 tenant、lease 全过期、无 pin、每对象一个
可淘汰 DRAM replica 的合成 workload 上得到：

| 对象数 | BatchEvict 时间 |
|---:|---:|
| 10K | 约 8.7 ms |
| 100K | 约 109 ms |
| 1M | 中位约 0.95 至 1.0 s |

百万对象的分阶段结果为：

| 阶段 | 周期占比 |
|---|---:|
| metadata scan/traversal | 约 73% 至 75% |
| candidate vector collection | 约 28% 至 31% |
| `std::nth_element` | 约 0.1% |
| 实际对象淘汰 | 小于 0.5% |

同一实验中，等待 `snapshot_mutex_` 独占锁的线程得到约 1.00 s p50、1.21 s p95 和 1.36 s 最大等待时间。
主导成本是元数据遍历和候选构造，而不是候选排序或物理淘汰。

[PR #2584](https://github.com/kvcache-ai/Mooncake/pull/2584) 随后把规模测试和单独的 `snapshot_mutex_` waiter probe 合入 benchmark，使该问题可以重复测量；
该 PR 本身不修改生产行为。

### 5.2 PR #3118 的修复范围

[RFC #3124](https://github.com/kvcache-ai/Mooncake/issues/3124) 和
[PR #3118](https://github.com/kvcache-ai/Mooncake/pull/3118) 将候选构造改为：

```text
lightweight timestamp census over N objects
    -> find requested lease-deadline frontier
    -> materialize full {shard, tenant, key, deadline} only for K + reserve
    -> revalidate during execution
    -> refill beyond cutoff when reserve is exhausted
```

PR head 在一百万 eligible objects 下报告：

| 淘汰比例 | 基线完整候选数 | PR #3118 完整候选数 | 中位运行时间变化 |
|---:|---:|---:|---:|
| 1% | 1,000,000 | 11,024 | -69.9% |
| 10% | 1,000,000 | 110,000 | -47.1% |
| 30% | 1,000,000 | 1,000,000 | 未检测到差异 |

一百万对象、1% 比例时，报告的净临时存储节省约 97.2 MB；五百万对象、1%
比例时约 465.4 MB。该优化保留 lease oldest-first、soft-pin fallback、group、
replica、target/lower-bound 和 offload 语义。

### 5.3 剩余结构性问题

PR #3118 保留了以下行为：

- 每轮至少执行一次 O(N) timestamp census；
- 低比例 selective path 可能执行第二次 frontier scan；
- `snapshot_mutex_` 共享锁覆盖整轮 BatchEvict；
- 高淘汰比例直接回到完整候选物化路径；
- reserve 耗尽或 lower-bound fallback 可以增加额外扫描；
- 大批 HA OpLog、offload 和 metadata mutation 仍可能形成突发负载。

因此，PR #3118 是显著的已合入缓解措施，尚未把候选发现改为增量或索引化。
Issue #2560 与 RFC #3124 保持开放，与这一边界一致。

## 6. 近期 issue、RFC、PR 与版本时间线

| 日期 | 项目 | 状态与作用 |
|---|---|---|
| 2026-06-22 | [Issue #2560](https://github.com/kvcache-ai/Mooncake/issues/2560) | 开放；量化 O(N) scan 和 `snapshot_mutex_` 独占 waiter 延迟 |
| 2026-07-07 | [PR #2584](https://github.com/kvcache-ai/Mooncake/pull/2584) | 已合入；加入真实 BatchEvict benchmark 和 lock probe |
| 2026-07-26 | [RFC #3124](https://github.com/kvcache-ai/Mooncake/issues/3124) | 开放；定义 selective materialization、reserve 和 refill |
| 2026-08-03 | [PR #3118](https://github.com/kvcache-ai/Mooncake/pull/3118) | 已合入；显著降低低比例候选内存和执行时间 |
| 2026-08-15 至 08-26 | [Issue #3452](https://github.com/kvcache-ai/Mooncake/issues/3452) | 已关闭；#3118 为主修复，#3154/#3168 修复 HA 放大项 |
| 2026-08-25 | [PR #3576](https://github.com/kvcache-ai/Mooncake/pull/3576) | 已合入；淘汰后条件收缩稀疏 metadata maps |
| 2026-08-27 | [PR #3422](https://github.com/kvcache-ai/Mooncake/pull/3422) | 已合入 `main`；修复 BatchEvict 的 HA OpLog 错误分类和回滚 |
| 2026-08-31 | [`v0.3.13.post1`](https://github.com/kvcache-ai/Mooncake/releases/tag/v0.3.13.post1) | GitHub 标记为 Latest；包含 #3118 和 #3576 |

版本覆盖关系：

| 版本 | #3118 候选优化 | #3154/#3168 HA 容量修复 | #3160 offload 清理 | #3576 map 收缩 |
|---|---:|---:|---:|---:|
| `v0.3.12.post1` | 否 | 否 | 否 | 否 |
| `v0.3.13` | 是 | 是 | 是 | 否 |
| `v0.3.13.post1` | 是 | 是 | 是 | 是 |

`v0.3.13` 已包含 #3452 关闭说明列出的 #3118、#3154 和 #3168，因此覆盖该 issue 认定的主因和 HA 放大项。
它仍缺少 #3576 的 bucket 回收。
新部署应优先使用 `v0.3.13.post1` 或等价完整 backport，以同时获得后续 metadata map 收缩修复。
PR #3422 在 `v0.3.13.post1` 分支点之后合入 `main`；HA 部署应结合其 OpLog 模式评估回移该正确性修复。

## 7. 最终解决方案与上线验收

### 7.1 立即措施

1. 最低根因修复基线为包含 #3118、#3154 和 #3168 的 `v0.3.13`。
   新部署优先选择 `v0.3.13.post1`，或者完整 backport #3118、#3154/#3168、
   #3160 和 #3576。
2. HA BatchEvict 使用 batch-record OpLog 时，评估同步 backport #3422，
   并执行 reservation failure、writer fence 和 commit failure 测试。
3. 保持基础淘汰比例处于低到中等范围，通过实际对象大小分布确定数值。
   高比例路径会恢复完整候选物化，并放大 OpLog/offload burst。
4. 为 Master 设置足够的容器 headroom，使一次 census、frontier、rehash 和
   snapshot 峰值能够同时存在。

### 7.2 生产验证

在同一时间轴记录：

- `U`、`H`、触发原因、target、lower bound；
- live object/replica count、eviction base、实际淘汰对象数和字节数；
- metadata `size` 与 `bucket_count`；
- BatchEvict census、frontier、execution 和总耗时；
- `snapshot_mutex_` 独占 waiter 以及 shard lock wait；
- Master RSS/PSS、allocator allocated/active/retained；
- OpLog queue/latency、offload queue depth/age、SSD 和网络吞吐；
- Put/Get RPC p50、p95、p99 与 SGLang TTFT。

验收标准为：

```text
重复 fill -> evict -> idle 周期
    -> live object count 达到稳定范围
    -> sparse metadata bucket_count 在淘汰后下降
    -> allocator active bytes 在 idle 后下降
    -> RSS/PSS 形成有界平台期
    -> BatchEvict 期间控制面和前台 RPC 延迟满足 SLO
```

### 7.3 后续结构优化

若 #3118 后的 O(N) census 仍影响服务 SLO，下一阶段应把淘汰总目标和单 tick
执行预算分离：

1. 使用增量 shard/key cursor，限制每个 tick 的扫描对象数和持锁时间；
2. 同时设置 selected objects、reclaimed bytes 和 elapsed time 预算；
3. 将 `snapshot_mutex_` 的持锁范围从整轮缩小到需要一致性的局部阶段；
4. 评估 lease-deadline ordered index 或 per-shard expiration structure，以
   `O(K log N)` 候选发现替代每轮 O(N) census；
5. 使用 OpLog、offload queue depth 和 queue age 对下一批淘汰进行反馈控制；
6. 用字节低水位作为总目标，以小批次渐进完成，控制 SSD、网络和恢复流量。

索引化会把维护成本转移到 Put、lease refresh 和 Remove 路径，还会增加 HAsnapshot/recovery 和锁顺序复杂度。
应先比较 lease 更新频率与 eviction round 频率，再选择增量 cursor 或 ordered index。

## 8. 结论

Master OOM 和 BatchEvict 性能问题共享同一压力入口，但需要分别处理：

- Issue #3452 已按 `main` 具备修复关闭。其关闭结论将旧 BatchEvict 的全量
  Candidate 临时分配和 allocator arena 保留认定为根因，由 PR #3118 修复；
  PR #3154/#3168 修复 HA leadership term 下的条件放大项。
- `v0.3.13.post1` 进一步包含 PR #3576，负责回收淘汰后 metadata map 的
  稀疏 bucket array，覆盖后续审计发现的长期 RSS 放大项。
- PR #3118 显著缩短低比例 BatchEvict，但每轮 O(N) census 和整轮
  `snapshot_mutex_` 共享锁仍然存在，属于待继续优化的结构性性能边界。
- 最终生产闭环由完整修复版本、按字节和时间限额的小批次淘汰，以及
  fill/evict/idle 回放中的 RSS 平台期和 RPC SLO 共同确认。
