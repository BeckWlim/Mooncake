# Periodic `BatchEvict` SLO spikes

> Evidence snapshot: 2026-09-07
>
> Upstream: [kvcache-ai/Mooncake](https://github.com/kvcache-ai/Mooncake)
>
> Scope: Mooncake Store distributed-DRAM eviction, including optional SSD
> offload and HA amplification

## Summary

Periodic SLO degradation aligned with object eviction has several upstream matches. 
The most direct match for foreground lookup latency is [PR #2405](https://github.com/kvcache-ai/Mooncake/pull/2405): 
large `BatchExistKey` requests repeatedly contended with the metadata-shard write locks used by `BatchEvict`. 
The follow-up [PR #2508](https://github.com/kvcache-ai/Mooncake/pull/2508) applied the same shard-grouping approach to `BatchGetReplicaList`.

Two other mechanisms can make the periodic peak substantially worse:

- With SSD offload enabled, [issue #2243](https://github.com/kvcache-ai/Mooncake/issues/2243)
  identified an inflated eviction target that could remove most DRAM objects
  in one cycle. [PR #2286](https://github.com/kvcache-ai/Mooncake/pull/2286)
  fixed the denominator and parallelized candidate collection.
- At high object cardinality, `BatchEvict` still performs an O(N) metadata
  census and holds `snapshot_mutex_` in shared mode for the whole cycle.
  [PR #3118](https://github.com/kvcache-ai/Mooncake/pull/3118) substantially
  reduces full candidate construction for low eviction ratios, but
  [issue #2560](https://github.com/kvcache-ai/Mooncake/issues/2560) and
  [RFC #3124](https://github.com/kvcache-ai/Mooncake/issues/3124) remain open
  because the census and outer lock scope remain.

The preferred production baseline is `v0.3.13.post1` or a later release. 
It contains the foreground-contention fixes, the SSD over-eviction fix, selective candidate materialization, 
the HA capacity-accounting fixes, and metadata-map shrinking. 
Configuration tuning should follow the version upgrade 
because a lower watermark on an older build can invoke the expensive old path more often.

## Observed upstream mechanisms

### Cyclic behavior is expected from the batch trigger

The eviction thread checks memory pressure every 10 ms. 
A cycle starts when global utilization is above the high watermark or when a failed allocation has set `need_mem_eviction_`. 
The current implementation calculates the target and lower bound as follows:

```cpp
if (used_ratio > eviction_high_watermark_ratio_ ||
    (need_mem_eviction_ && eviction_ratio_ > 0.0)) {
    double evict_ratio_target = std::max(
        eviction_ratio_,
        used_ratio - eviction_high_watermark_ratio_ + eviction_ratio_);
    double evict_ratio_lowerbound = std::max(
        evict_ratio_target * 0.5,
        used_ratio - eviction_high_watermark_ratio_);
    BatchEvict(evict_ratio_target, evict_ratio_lowerbound);
}
```

Source: [`EvictionThreadFunc`](../../../mooncake-store/src/master_service.cpp#L9114)
and the [10 ms interval](../../../mooncake-store/include/master_service.h#L2228).

For `used_ratio=0.93`, `high_watermark=0.90`, and `eviction_ratio=0.05`, the target is 8% and the lower bound is 4%. 
These ratios are applied to the evictable object population, not directly to allocated bytes. 
Mixed object sizes, replica counts, and grouped objects can therefore make the byte drop differ from the object target. 
The intended state transition is:

```text
fill -> cross watermark or fail allocation -> batch reclaim -> refill
     -> cross watermark or fail allocation -> batch reclaim -> ...
```

This explains the periodic shape. 
The SLO impact depends on the size and duration of each batch, the foreground RPCs competing for the same shards, 
and whether eviction causes a hit-rate or offload-traffic discontinuity.

### Foreground lookup and eviction contend on metadata shards

`BatchEvict` holds an outer shared snapshot lock and uses up to 16 census workers. 
Each worker obtains a write accessor while it visits a metadata shard. 
Condensed from the current implementation:

```cpp
std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
int num_threads = std::min((int)kNumShards, 16);

// In each census worker:
size_t s_start = worker_index * shards_per_thread;
size_t s_end = std::min(s_start + shards_per_thread, kNumShards);
for (size_t s = s_start; s < s_end; ++s) {
    MetadataShardAccessorRW shard(this, s);
    for (const auto& [tenant_id, tenant_state] : shard->tenants) {
        for (const auto& [key, metadata] : tenant_state.metadata) {
            // Count and select evictable objects.
        }
    }
}
```

Source: [`BatchEvict` census](../../../mooncake-store/src/master_service.cpp#L10407).

Before #2405, a large `BatchExistKey` effectively performed a service lookup and lock acquisition per key. 
Under an eviction-active, production-like vLLM benchmark with 20 million objects, 56 lookup threads, and 30,000 keys per request, 
#2405 reported the following results, including refill/put overlap:

| Version | Average | p90 | p99 | Maximum |
|---|---:|---:|---:|---:|
| Baseline | 991.5 ms | 1828.1 ms | 2910.7 ms | 4208.5 ms |
| #2405 | 106.4 ms | 150.1 ms | 296.1 ms | 854.8 ms |

#2405 grouped `BatchExistKey` keys by shard and moved replica/buffer destruction outside the shard write lock. 
The current lookup path now acquires one shard accessor for all keys mapped to that shard:

```cpp
for (size_t scanned = 0; scanned < kNumShards; ++scanned) {
    const size_t shard_idx =
        (start_shard + kNumShards - scanned) % kNumShards;
    const auto& key_indices = indices_by_shard[shard_idx];
    if (key_indices.empty()) continue;

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataShardAccessorRO shard(this, shard_idx);
    for (const size_t key_index : key_indices) {
        // Resolve the key while this shard is held once.
    }
}
```

Source: [`BatchExistKey`](../../../mooncake-store/src/master_service.cpp#L2923).

#2508 extended shard grouping to `BatchGetReplicaList`. 
Its benchmark was measured on top of #2405 and reported p99 reduction from 353.019 ms to 243.121 ms and maximum reduction from 2319.008 ms to 1602.496 ms. 
These are controlled benchmark results rather than guarantees for a specific production topology, 
but the lock-conflict mechanism directly matches eviction-aligned lookup tail latency.

### SSD offload could inflate one batch from 5% to almost the whole DRAM tier

Issue #2243 applies when Mooncake SSD offload is enabled. 
The old code computed the victim target from all metadata entries, including disk-only objects that could not release DRAM. 
In the reported workload:

| Measurement | Value |
|---|---:|
| All metadata objects | 85,622,691 |
| Objects with evictable memory replicas | 3,776,944 |
| Inflated target | 4,310,991 |
| Correct target | 190,165 |
| Configured target ratio | 5% |
| Actual memory-object eviction ratio | 92% |

PR #2286 changed the denominator to the evictable population. 
The current coded erives both target and lower-bound work from `total_eviction_base`:

```cpp
const long ideal_evict_num =
    std::ceil(total_eviction_base * evict_ratio_target);

long target_evict_num =
    std::ceil(total_eviction_base * evict_ratio_lowerbound) -
    evicted_count - released_discarded_cnt;
```

Source: [`BatchEvict` target calculation](../../../mooncake-store/src/master_service.cpp#L10527).

The PR reported a reduction from approximately 80 seconds to 2.3 seconds for
one cycle and restored the actual ratio to approximately 5%. A production
trace showing a much larger DRAM usage drop than `eviction_ratio`, especially
with a large disk-only population, should therefore be checked first for
#2286 inclusion.

### Candidate preparation is improved, but a whole-cycle limit remains

Issue #2560 has two measurement generations that must not be combined:

1. Before #2286, a single-threaded 1-million-object run took approximately
   0.95-1.0 seconds; metadata traversal accounted for 73-75%.
2. After #2286 parallelized Phase 1, a 1-million-object, 50%-target run took
   approximately 1.5 seconds; parallel Phase 1 was approximately 35 ms (2.3%)
   and serial Phase 2 was approximately 1.24 seconds (82.2%). A unique
   `snapshot_mutex_` waiter still waited approximately 1.44 seconds at p50.

The second result supersedes the first for post-#2286 code at that high target
ratio. It shows that candidate scanning is not always the dominant phase, while
the whole-cycle outer lock and serial victim processing remain relevant.

PR #3118 optimized the common low-ratio case by collecting lightweight lease
timestamps first and materializing full `{shard, tenant, key, deadline}`
identities only for the selected frontier plus a bounded reserve. Its
1-million-object measurements were:

| Target ratio | Baseline | #3118 | Median change | Full candidates after #3118 |
|---:|---:|---:|---:|---:|
| 1% | 288.1 ms | 88.3 ms | -69.9% | 11,024 |
| 10% | 529.7 ms | 285.1 ms | -47.1% | 110,000 |
| 30% | 898.9 ms | 895.0 ms | No detected difference | 1,000,000 |

The limitation is visible in the current implementation: the census still
visits all metadata, the selective path can require a second scan, high target
ratios use full materialization, and Phase 2 processes selected candidates
serially. `snapshot_mutex_` remains held across these phases. #3118 reduces
work and temporary memory; it does not convert candidate discovery to an
incremental or indexed algorithm.

### Eviction previously retained sparse-map bucket memory

[Issue #3452](https://github.com/kvcache-ai/Mooncake/issues/3452) also
identified a separate retained-memory mechanism after an eviction cycle.
Removing entries from a tenant's `std::unordered_map` reduced
the map's live size but did not shrink its bucket array. A metadata shard that
had reached a high key count could therefore retain that peak bucket capacity
after most of its objects were evicted. This retained capacity is distinct
from the temporary candidate vectors addressed by #3118: candidate memory is
released after the batch, whereas the bucket array can remain allocated for
the lifetime of the map.

[PR #3576](https://github.com/kvcache-ai/Mooncake/pull/3576) added selective
post-eviction shrinking. `BatchEvict` records shards that report removed
objects and, during final cleanup, evaluates their tenant metadata maps under
the exclusive shard lock. Maps with more than 1,024 buckets and less than
25% occupancy are rehashed toward twice their live size. This targets
materially large, sparse maps while retaining growth headroom and avoiding
rehash churn for small maps. The change addresses persistent bucket-array
capacity; it does not remove the O(N) census cost or shorten the outer shared
snapshot-lock scope.

### Heterogeneous segments delay the trigger until writes fail

Issue #2430 reports a distinct trigger problem on `v0.3.9`: eight 400 GiB segments and two 1000 GiB segments used the default random allocator. 
The smaller segments reached a practical fragmentation ceiling while larger segments remained less utilized. 
The global watermark could not represent the pressured segments, 
so `PutStart` allocation failure triggered eviction rather than the proactive watermark path.

The reported outcome was a client-visible `NO_AVAILABLE_HANDLE` window, followed by bulk eviction of approximately 5% of all objects, then refill. 
This produced a sawtooth in write success and cache hit rate. 
The issue remains open and has no merged upstream eviction-side fix. 
Equal-sized segments or a lower watermark can mitigate the trigger problem, but each has a tradeoff:

- Splitting large memory regions into equal-sized segments is a deployment
  workaround and increases segment count.
- Lowering `eviction_high_watermark_ratio` reserves more headroom and starts
  eviction earlier, reducing effective cache capacity.
- Reducing `eviction_ratio` makes each drop smaller but may increase cycle
  frequency.
- `FreeRatioFirstAllocationStrategy` balances utilization, but the issue
  reports that a newly mounted empty segment can temporarily receive most
  writes.

## Related upstream records

Statuses below were verified from the official repository on 2026-09-07.

| Record | Status | Operational relevance |
|---|---|---|
| #2243 | Closed | SSD disk-only objects inflated the DRAM eviction target. |
| #2286 | Merged 2026-06-24 | Corrected the denominator and parallelized Phase 1. |
| #2405 | Merged 2026-06-17 | Reduced `BatchExistKey`/eviction contention and deferred replica destruction. |
| #2508 | Merged 2026-06-18 | Grouped `BatchGetReplicaList` metadata access by shard. |
| #2430 | Open | Documents heterogeneous-segment write-failure and bulk-eviction sawtooth. |
| #2560 | Open | Measures cycle phases and full-cycle snapshot-lock blocking. |
| #2584 | Merged 2026-07-07 | Adds a reproducible `BatchEvict` scale and lock-wait benchmark; no production behavior change. |
| #3124 | Open | Specifies selective materialization and its remaining O(N) boundary. |
| #3118 | Merged 2026-08-03 | Reduces low-ratio candidate construction, time, and temporary memory. |
| #3452 | Closed 2026-08-26 | Attributes an old-version Master RSS/OOM peak primarily to pre-#3118 candidates. |
| #3576 | Merged 2026-08-25 | Shrinks sparse metadata maps after eviction cycles. |

For HA deployments, #3452 also identifies
[PR #3154](https://github.com/kvcache-ai/Mooncake/pull/3154) and
[PR #3168](https://github.com/kvcache-ai/Mooncake/pull/3168). 
They prevent segment capacity from being counted across leadership terms or released by a temporary snapshot reader. 
Incorrect capacity accounting can make calculated utilization too low and suppress proactive eviction, 
but it applies only when the corresponding service/snapshot lifecycle occurs.

## Release coverage

The following inclusion relationships were verified against official tag commits rather than inferred only from publication dates.

| Release | #2286 | #2405 | #2508 | #3118 | #3154/#3168 | #3576 |
|---|---:|---:|---:|---:|---:|---:|
| `v0.3.12.post1` | Yes | Yes | Yes | No | No | No |
| `v0.3.13` | Yes | Yes | Yes | Yes | Yes | No |
| `v0.3.13.post1` | Yes | Yes | Yes | Yes | Yes | Yes |

[`v0.3.13.post1`](https://github.com/kvcache-ai/Mooncake/releases/tag/v0.3.13.post1) was the latest official release at the evidence date. 
It is the preferred baseline for this incident investigation. 
It mitigates known causes but does not close issue #2560 or #2430.

## Production diagnosis

Correlate at least three complete fill/evict/refill cycles on a single time axis. 
A single latency peak cannot distinguish shard contention from cache misses, allocation failure, offload traffic, or Master memory pressure.

### Required signals

Collect these Mooncake metrics where the deployed version exposes them:

```text
master_allocated_bytes
master_total_capacity_bytes
segment_allocated_bytes{segment=...}
segment_total_capacity_bytes{segment=...}
master_key_count
master_put_start_alloc_failures_total
master_attempted_evictions_mem
master_successful_evictions_mem
master_evicted_key_count_mem
master_evicted_size_bytes_mem
```

Also collect:

- `BatchExistKey` and `BatchGetReplicaList` batch sizes and p50/p95/p99;
- application TTFT, cache-hit rate, write success, and the affected SLO;
- Master CPU, RSS/PSS, allocator active/retained bytes, and thread count;
- per-segment free bytes and largest allocatable extent if available;
- SSD offload queue depth/age and disk/network throughput when offload is on;
- HA leadership changes and `master_total_capacity_bytes` before and after
  each term change.

Current builds provide useful log boundaries and results:

```text
[EVICT-TRIGGER] memory_ratio=... high_watermark=... need_mem_eviction=...
[EVICT-DONE] BatchEvict execution completed.
[EVICT-RESULT] evicted_count=... eviction_base=...
               actual_evict_ratio=... target_evict_ratio=...
[EVICT-DIAG] object_count=... eviction_base=... disk_ratio=...
```

Use log timestamps to derive cycle duration. The current metrics expose
cumulative eviction counts and bytes, but not a dedicated `BatchEvict`
duration histogram.

### Symptom classification

| Correlated observation | Most likely upstream match | Next verification |
|---|---|---|
| Lookup/get p99 rises only inside eviction windows | #2405/#2508 shard contention | Confirm release includes both; inspect request key counts. |
| DRAM falls far more than configured ratio with SSD offload | #2243/#2286 | Compare `actual_evict_ratio` with target and confirm #2286 inclusion. |
| `NO_AVAILABLE_HANDLE` rises before each eviction | #2430 or fragmentation-triggered eviction | Compare global and per-segment utilization and allocation failures. |
| Management, snapshot, or unmount operations wait for the whole cycle | #2560 outer snapshot-lock scope | Measure cycle duration and unique-lock wait together. |
| Master RSS jumps at eviction and remains high | #3452 and pre-#3118 candidates; possibly sparse maps | Compare RSS with `master_key_count`; confirm #3118 and #3576 inclusion. |
| High-ratio cycles remain long after upgrade | #3118 high-ratio bypass and serial Phase 2 | Measure configured/computed target and selected object count. |

## Mitigation and acceptance criteria

Apply changes in this order:

1. Record the exact production image tag and commit SHA. Upgrade to
   `v0.3.13.post1` or later before using tuning as the primary fix.
2. If a full upgrade is not immediately possible, backport according to the
   observed path: #2405/#2508 for lookup SLO, #2286 for SSD over-eviction,
   #3118 for low-ratio candidate cost, #3154/#3168 for HA capacity accounting,
   and #3576 for retained sparse-map capacity.
3. On the fixed build, reduce `eviction_ratio` incrementally to limit one-cycle
   work. Validate total CPU and cycle frequency because smaller batches trade
   peak size for more frequent scans.
4. Set `eviction_high_watermark_ratio` below the measured allocation or
   fragmentation failure point, with explicit capacity headroom. Validate
   cache-hit-rate impact.
5. For heterogeneous segments, test equal-sized logical segments as a scoped
   deployment mitigation. Do not assume a global utilization value represents
   the most pressured segment.

The change is accepted only when a representative fill/evict/refill test shows:

- no allocation-failure window before normal watermark-triggered eviction;
- actual eviction ratio and byte drop remain within the expected batch range;
- lookup/get and application p99 remain within SLO during every measured
  eviction window;
- hit rate and offload queue depth do not form an unacceptable sawtooth;
- `master_key_count`, allocator active bytes, and Master RSS reach bounded
  steady-state ranges;
- HA term changes do not increase total capacity without matching mounted
  capacity; and
- results remain stable for repeated cycles rather than only the first cycle.

## Reproducing the upstream cycle benchmark

PR #2584 added a standalone benchmark that executes the real `BatchEvict`
path. On a build with benchmarks enabled:

```bash
cmake --build build --target batch_evict_bench -j"$(nproc)"

./build/mooncake-store/benchmarks/batch_evict_bench \
  --num_objects=1000000 \
  --evict_ratio_target=0.05 \
  --evict_ratio_lowerbound=0.025
```

The unique-lock probe is opt-in:

```bash
MOONCAKE_EVICT_BENCH_LOCK_PROBE=1 \
MOONCAKE_EVICT_BENCH_LOCK_OBJECTS=1000000 \
MOONCAKE_EVICT_BENCH_LOCK_TRIALS=30 \
./build/mooncake-store/benchmarks/batch_evict_bench \
  --num_objects=1000000 \
  --evict_ratio_target=0.05 \
  --evict_ratio_lowerbound=0.025
```

The benchmark uses a synthetic single-tenant workload with expired leases, no
pins, and one memory replica per object. Use it for version-to-version control
plane comparison, then validate the same change with the production object-size
distribution, batch sizes, pin state, SSD mode, and HA configuration.
