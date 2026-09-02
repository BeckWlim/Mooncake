# Starting Notes: Master OOM and Batch-Eviction Pressure

> - Analysis date: 2026-09-02
> - Production baseline discussed here: Mooncake `v0.3.12.post1`
> - Local workspace baseline: Git `HEAD` at `d0eb775a`
> - Inputs: [Master OOM and SGLang L3 pressure reading plan](master-oom-sglang-l3-pressure-reading-plan.md),
>   [issue #952](https://github.com/kvcache-ai/Mooncake/issues/952), and
>   the current and upstream `BatchEvict` implementations
> - Status: source-backed starting hypotheses; production causality still
>   requires correlated runtime measurements

This note keeps two primary engineering principles separate:

1. A serving Master that exceeds its sustainable metadata or control-plane
   budget requires bounded state first and, when one process remains the
   limiting ownership domain, active multi-master partitioning.
2. Batch eviction must be paced against downstream capacity. A large eviction
   ratio reduces cycle frequency by creating more headroom, but it increases
   peak work and can transfer memory pressure into request, metadata, network,
   and SSD pressure.

The two principles interact, but they address different resource dimensions.

## 1. Master OOM and the Multi-Master Direction

### Observed source facts

[Issue #3452](https://github.com/kvcache-ai/Mooncake/issues/3452) reports that a
`v0.3.12.post1` Master gained approximately 7 GiB of RSS within two to three
hours under a long-context SGLang PD-disaggregated workload. The report states
that RSS did not fall after the request workload completed.

### HA process, leadership-term, and service lifetimes

The phrase "a `MasterService` owns a leadership term" is inaccurate. The HA
supervisor owns and validates the `LeadershipSession`. A `MasterService` is a
term-scoped serving runtime created only after the supervisor has acquired and
warmed up that session.

The relevant ownership hierarchy is:

```text
mooncake_master process                                      process lifetime
├── MasterMetricManager::instance()                         process lifetime
├── MasterAdminServer                                       process lifetime
└── MasterServiceSupervisor loop                            process lifetime
    ├── LeaderCoordinator <-> etcd/Redis/Kubernetes Lease   supervisor attempt
    ├── StandbyController / HotStandbyService               process lifetime
    └── one successful leadership term                      term lifetime
        ├── LeadershipSession {leader view, version, lease}
        ├── coro_rpc server
        └── shared_ptr<WrappedMasterService>
            └── MasterService member
                ├── metadata shards and task state
                ├── SegmentManager and mounted-segment records
                └── eviction, client-monitor, cleanup, and snapshot threads

Store clients and their memory/SSD segments                  external lifetime
```

`WrappedMasterService` contains `MasterService` by value.
Consequently, destroying the last wrapper reference also runs
`MasterService::~MasterService` and destroys its `SegmentManager`. The external
Store clients and their physical segments can remain alive across that
destruction;
they discover the new leader and remount their segments into the next serving instance.

The HA serving sequence is:

```text
standby/candidate
  -> read leader view from the HA backend
  -> acquire a new LeadershipSession
  -> promote/catch up standby state
  -> renew leadership through a warmup interval
  -> construct a fresh RPC server and WrappedMasterService
  -> pass the term's view_version into MasterService
  -> perform a final lease-renewal preflight
  -> publish the service delegate and accept client RPCs
  -> monitor the leadership lease while serving

leadership loss
  -> reject new service traffic and clear the leader label
  -> stop the term's RPC server
  -> remove the admin service delegate
  -> release or observe expiration of the LeadershipSession
  -> return to standby mode
  -> destroy the term-scoped WrappedMasterService/MasterService
```

The leadership monitor, not `MasterService`, decides when the term is no longer
valid. By the time `MasterService` is destroyed, the term may already have
expired or been released. "Term-scoped service" therefore means that the
service lifetime is bounded by the term; it does not mean that the service owns
the lease.

In non-HA mode, `main` constructs one RPC server and one
`WrappedMasterService`, registers the wrapper's methods, and keeps both alive
until process shutdown. There is no in-process destroy/recreate cycle. In HA
mode, the same ordinary request implementation is used after promotion, but
the supervisor may repeat service construction and teardown without restarting
the process:

```text
client
  -> direct Master address, or HA-backend leader discovery/watch
  -> WrappedMasterService RPC boundary
  -> MasterService operation
  -> metadata shard / SegmentManager / allocator / task state
  -> process-wide MasterMetricManager update
```

etcd is outside the service object. With the etcd HA backend it coordinates the
leader lease and versioned Master view, and it can also hold the ordered OpLog
that a standby follows. It does not keep the `MasterService` C++ object alive
and does not store the KV payload. Redis or Kubernetes Lease can replace etcd
for leadership coordination; the process-versus-term lifetime distinction is
the same.

### etcd, snapshot, and OpLog recovery model

These mechanisms have separate roles and should not be treated as three names
for the same replication layer:

| Mechanism | Role in HA | Boundary |
|---|---|---|
| etcd | Shared, strongly consistent coordination and key-value service | Publishes the leased Master view and, for the current etcd hot-standby path, stores and notifies changes to the OpLog. Its internal Raft consensus is separate from Mooncake's Master leadership protocol. |
| Snapshot | Point-in-time checkpoint with a last-included OpLog sequence | Provides a bulk recovery baseline and avoids replaying all historical operations. Snapshot payloads reside in the configured snapshot object store; a catalog publishes their descriptors. |
| OpLog | Mooncake's ordered application-level journal | Records the supported metadata changes after the snapshot boundary. It contains metadata and replica descriptors, not object payload bytes. |

The intended standby reconstruction rule is:

```text
leader coordination:
  leader supervisor -- leased Master view --> etcd <-- watch -- standby

recoverable metadata:
  leader MasterService -- checkpoint at sequence S --> snapshot store/catalog
  leader MasterService -- operations S+1 ... N -----> etcd OpLog

  standby -- load snapshot S --> StandbyMetadataStore
          -- apply OpLog S+1 ... N --> current standby projection
```

Snapshot and OpLog are complementary. A snapshot without following deltas can
be stale as soon as the leader accepts another mutation. An OpLog without a
snapshot can reconstruct state only while all required history remains
available. The sequence ID joins the two: a snapshot whose
`last_included_seq = S` must be followed from `S + 1`.

The current `HotStandbyService` does not maintain a complete clone of
`MasterService`. Its `StandbyMetadataStore` retains this per-object projection:

- object identity/key;
- owner client UUID;
- object size;
- replica descriptors that identify the memory or SSD locations; and
- the last OpLog sequence ID applied to the key.

During bootstrap, the standby snapshot provider downloads the segment and
metadata payloads. It uses a temporary `SegmentManager` to decode replica
locations, filters zero-size or expired objects, and retains only objects with
complete, valid replicas. The temporary segment state is not the standby's
serving `SegmentManager`.

After the snapshot boundary, the standby applies only the currently supported
OpLog mutation set:

| OpLog operation | Current standby effect |
|---|---|
| `PUT_END` | Insert or replace completed metadata from `MetadataPayload`: client UUID, size, and replica descriptors. |
| `PUT_REVOKE` | Remove the entire key from the standby projection. |
| `REMOVE` | Remove the key. |
| `LEASE_RENEW` | Intentionally not recorded or applied in the current etcd hot-standby design. |

Lease and soft-pin timestamps from a full snapshot are used to reject expired
objects during bootstrap, but they are not retained in
`StandbyObjectMetadata`. The standby does not run eviction, and a promoted
primary is expected to grant fresh leases. The live projection also excludes
object payload bytes, RPC connections, worker threads, request queues,
in-flight operations, metrics, eviction timers, and other process-local state.
The actual bytes remain in the external memory or SSD segments named by the
replica descriptors.

The full snapshot producer serializes richer state, including Master metadata,
segment-manager state, and task-manager state. The hot-standby snapshot provider
consumes only the metadata and segment payloads and reduces them to the smaller
standby projection described above.

The current supervisor calls `StandbyController::PromoteStandby()` to stop
following and perform bounded final OpLog catch-up, then constructs a fresh
`WrappedMasterService`. In the inspected path, it does not pass
`HotStandbyService::ExportMetadataSnapshot()` into that new service. The fresh
`MasterService` performs its separately configured full-snapshot restore.
Therefore, the following should be treated as a validation requirement rather
than an established property:

> The serving `MasterService` created after promotion contains every mutation
> accepted after the restored snapshot boundary and before leadership transfer.

The OpLog also has differentiated durability. `PUT_END` uses an asynchronous,
lag-tolerant persistence path, while removal operations that can free and reuse
memory are persisted before returning. Gap recovery and final promotion
catch-up are bounded and can proceed after warnings. Production HA acceptance
criteria should consequently specify maximum permitted sequence lag, treatment
of unresolved gaps, snapshot age, and whether promotion must fail closed when
the exact recovery boundary cannot be established.

### Capacity-accounting leak across leadership terms

Mounted memory-segment capacity crosses two lifetime domains:

- `SegmentManager::mounted_segments_` is term-scoped state owned by the
  current `MasterService`.
- `MasterMetricManager::mem_total_capacity_` is a gauge in a function-local
  static singleton and survives for the lifetime of the process.

Mounting a segment adds its size to both domains. An ordinary committed
unmount removes the record and decrements the gauge. Before PR #3154, service
teardown had no corresponding decrement for segments that were still mounted.
Leadership loss normally stops the old RPC service before every client can
complete an ordinary unmount, so destroying the old `SegmentManager` removed
its records without removing their singleton contributions.

For a fleet whose mounted capacity is `S`, the erroneous sequence was:

```text
process starts:                         metric capacity = 0

term 1 clients mount:                   metric capacity = S
term 1 loses leadership:
  MasterService and mount records die   metric capacity = S       (stale)

term 2 creates a fresh MasterService
term 2 clients remount the same fleet:  metric capacity = 2S

term 2 loses leadership and term 3 remounts:
                                         metric capacity = 3S
```

The stale gauge is accounting state, not retained segment payload. It does not
itself allocate another fleet of memory. Its operational effect is still
material because the eviction trigger reads:

```text
global memory used ratio = allocated bytes / total capacity bytes
```

An inflated denominator makes usage appear lower after every re-election. The
ratio may then remain below `eviction_high_watermark_ratio`, suppressing
proactive eviction even while actual mounted segments are under pressure. This
mechanism can amplify allocation failures and request pressure, but it must not
be reported as direct evidence that the singleton gauge itself caused the
Master RSS growth in issue #3452.

[PR #3154](https://github.com/kvcache-ai/Mooncake/pull/3154) first fixed the
leak by releasing the capacity of all still-mounted segments when
`SegmentManager` was destroyed. [PR #3168](https://github.com/kvcache-ai/Mooncake/pull/3168)
then narrowed the ownership rule: temporary `SegmentManager` instances used to
deserialize snapshots can contain mounted-segment records that never incremented
the singleton gauge. Releasing capacity from every `SegmentManager` destructor
would therefore subtract unowned contributions and corrupt the gauge downward.

The corrected invariant is:

> Only the serving `MasterService` that accounted a mounted segment may release
> that segment's capacity contribution.

The final fix calls `SegmentManager::releaseCapacityMetrics()` explicitly from
`MasterService::~MasterService`. It releases the remaining mounted, non-CXL
segments at the serving-instance boundary. An ordinary unmount has already
removed its segment record, so it is not decremented twice; a temporary
snapshot reader is not a serving `MasterService`, so its destruction does not
touch the gauge.

This yields two metric-lifetime rules for future HA changes:

- Monotonic request and failure counters may intentionally accumulate across
  leadership terms when their documented scope is the process.
- Gauges describing current serving-instance resources must be rebuilt or
  reconciled at promotion and released at demotion. Their ownership cannot be
  inferred solely from a container containing deserialized records.

### BatchEvict transient amplification

At this production baseline, `MasterService::BatchEvict` performs a complete
metadata census and constructs a full candidate containing the tenant, key,
shard, and lease timestamp for every eligible object before selecting the
requested subset. The temporary candidate population is therefore
`O(eligible object count)`, even when the configured eviction ratio is small.
This creates a transient Master-memory and allocation peak at the same time as
the system is already under capacity pressure.

Merged [PR #3118](https://github.com/kvcache-ai/Mooncake/pull/3118), which is
newer than the production baseline, reduces identity materialization for low
eviction ratios by first collecting lease timestamps and then materializing a
bounded eviction frontier. Its published measurements report the following
results for one million eligible objects:

| Eviction ratio | Baseline candidates | PR #3118 candidates | Reported runtime effect |
|---:|---:|---:|---:|
| 1% | 1,000,000 | 11,024 | approximately 70% lower median runtime |
| 10% | 1,000,000 | 110,000 | approximately 47% lower median runtime |
| 30% | 1,000,000 | 1,000,000 | no detected improvement |

The 30% case deliberately uses the full-materialization path because most of
the eligible population is near the requested frontier. This result directly
connects eviction granularity to the Master peak: a high eviction ratio removes
the benefit of selective candidate materialization.

The upstream fixes narrow several concrete OOM amplifiers:

- PR #3118 limits full candidate identity materialization at low ratios.
- [PR #3154](https://github.com/kvcache-ai/Mooncake/pull/3154) and
  [PR #3168](https://github.com/kvcache-ai/Mooncake/pull/3168) correct HA
  capacity accounting.
- [PR #3576](https://github.com/kvcache-ai/Mooncake/pull/3576) shrinks sparse
  metadata-map bucket arrays after eviction.

These changes address distinct mechanisms. They do not by themselves prove
whether the production RSS trajectory is retained live metadata, temporary
allocation retained by the allocator, an HA accounting amplifier, sparse
container capacity, or a lifecycle leak.

### Derived conclusion

Multi-master is the structural solution only when measurements show that the
irreducible live metadata or control-plane work exceeds a single serving
Master's budget after bounded-state and transient-allocation fixes are applied.
It is not the first fix for a temporary `BatchEvict` allocation peak.

The repository's 1,024 metadata shards divide locking inside one Master
process. HA leader/standby support still provides one serving authority. An
active multi-master design requires a separate ownership layer:

```text
router
  -> stable hash of tenant + model namespace + object/group key
  -> serving Master partition
       -> owned metadata shards
       -> leases and eviction/offload/promotion state
       -> OpLog and snapshot
       -> dedicated HA standby
```

This partition reduces object cardinality, metadata memory, scan work, and RPC
load per serving Master. It does not reduce aggregate SSD bytes, network bytes,
or the number of cache objects that an eviction policy selects. A batch that
crosses partitions must also be split and merged by the client or router.

### Decision gate

Advance a multi-master proposal when production measurements demonstrate all
of the following:

- Master memory remains proportional to live metadata after quiescence and
  after the upstream OOM fixes are applied.
- Per-key Master CPU, lock time, RPC rate, or metadata memory remains the
  limiting resource under a controlled workload.
- Static partitioning produces a near-linear reduction in memory and
  control-plane work per Master.
- Routing preserves the ownership boundary of object groups and provides
  fencing, ownership epochs, per-partition snapshot/OpLog recovery, and
  idempotent stale-route retries.

## 2. Batch Eviction as a Peak-Pressure Generator

### What issue #952 proposes

[Issue #952](https://github.com/kvcache-ai/Mooncake/issues/952) is an RFC for
DFS/3FS file cleanup. It proposes storage monitoring plus a lease-based
approximation of LRU. Because SSD monitoring and eviction are slower than DRAM
operations, it suggests running eviction less frequently and reclaiming a
larger amount per cycle, with 30% as an example.

The 30% value is a design example, not an established production default or a
validated optimum. The issue also concerns secondary-storage file eviction,
whereas the production pressure path discussed in the Master OOM note begins
with distributed-memory `BatchEvict`. The two paths share the same batching
tradeoff but do not have identical ownership, latency, or work units.

There is also a unit mismatch that prevents direct transfer of the proposal:

- issue #952 describes clearing a fraction of target **space**;
- the current Master `eviction_ratio` selects a fraction of the evictable
  **object count**.

When object sizes vary, evicting 30% of objects does not imply reclaiming 30%
of bytes. A count ratio can under-reclaim large-capacity pressure or
over-evict many small, reusable KV objects.

### Current memory-eviction mechanism

The production baseline uses these defaults:

- high-watermark trigger: 90% global distributed-memory usage;
- base eviction ratio: 5% of evictable objects;
- eviction-thread check interval: 10 ms.

For observed usage `U`, high watermark `H`, and configured ratio `R`, the
current cycle computes approximately:

```text
target object ratio = max(R, U - H + R)
lower bound         = max(target / 2, U - H)
```

A 93% usage ratio with `H = 90%` and `R = 5%` therefore requests an 8% target
over the evictable object population. Each cycle scans all metadata in
parallel, collects candidates, selects the oldest eligible leases, and applies
the selected mutations serially. If offload-on-evict is active, selection can
also create offload tasks and pin source replicas rather than immediately
freeing their memory.

At the production baseline, the default offload queue limit is 50,000 objects
per local-disk segment and the default per-cycle cap is half of that limit,
or 25,000 objects. `OffloadObjectHeartbeat` returns and clears the holder's
current pending map as one result. These boundaries limit retained queue state,
but they can still deliver a large unit of work to a holder in one heartbeat.

### Pressure-amplification hypothesis

The issue #952 proposal correctly identifies the average-frequency benefit of
a large reclamation cycle: more headroom delays the next trigger. For the
current production problem, however, the same choice increases instantaneous
work along the complete eviction path:

```text
large eviction target
  -> larger candidate frontier and temporary Master allocations
  -> more serial metadata removals and lease/group revalidation
  -> more HA OpLog and event mutations when enabled
  -> more offload tasks and pinned source replicas
  -> larger heartbeat delivery to storage holders
  -> burst of reads, network transfers, staging allocations, and SSD writes
  -> simultaneous cache misses or restores after memory replicas disappear
  -> higher foreground request latency and recomputation pressure
```

The relationship is therefore a throughput-versus-peak tradeoff:

- A larger ratio can reduce the number of eviction cycles and repeated full
  scans.
- A larger ratio raises per-cycle memory, CPU, lock, metadata-mutation, and
  downstream I/O pressure.
- A large drop in resident cache capacity can synchronize subsequent misses,
  promotion attempts, or recomputation with the offload wave.
- If a cycle queues work faster than holders drain it, the low-frequency
  policy shifts pressure into queue age, source pinning, and request-tail
  latency instead of eliminating it.

PR #3118 supplies mechanism-level evidence for the first two points: low ratios
benefit from bounded candidate materialization, while its 30% benchmark takes
the full-candidate path and shows no scan-time improvement. Production request
pressure is still a hypothesis because the PR benchmark does not include
SGLang traffic, SSD offload, HA replication, or foreground latency.

### Direct solution direction

The eviction path should control bytes and elapsed work, not only the number of
selected keys. A production-oriented scheduler should provide:

1. high/low watermark hysteresis expressed in bytes;
2. per-tick budgets for scanned keys, selected keys, reclaim bytes, execution
   time, and per-holder offload bytes;
3. an incremental shard/key cursor so one tick does not materialize or lock the
   full metadata population;
4. queue-depth and queue-age feedback that pauses selection when holders,
   OpLog replication, or event consumers are behind;
5. bounded heartbeat delivery so a holder consumes offload work in controlled
   chunks;
6. admission throttling while reclaim is in progress, preventing new writes
   from immediately consuming the reclaimed headroom; and
7. separate QoS for foreground restores and background offload writes.

A 30% total reclamation goal can remain valid when it is treated as a gradual
low-watermark destination. It should not imply selecting and executing 30% of
the object population in one cycle.

## Combined Priority

The two principles produce the following dependency order:

1. Apply or backport the concrete Master-memory fixes and distinguish live
   state, temporary peak, and allocator-retained memory.
2. Pace eviction and offload so the system approaches a byte-based low
   watermark through bounded increments.
3. Measure the remaining per-Master metadata and control-plane limit.
4. Introduce active multi-master partitioning only if one serving ownership
   domain remains the bottleneck.

Multi-master lowers the `N` seen by each Master and can reduce each partition's
scan and mutation peak. It cannot substitute for pacing: simultaneous 30%
eviction cycles on several partitions can preserve or increase aggregate SSD,
network, and request pressure.

## Validation Plan

Correlate the following measurements on one timeline for every eviction cycle:

- trigger reason, high/low watermark, configured target, eligible objects,
  selected objects, and reclaimed bytes;
- scan duration, execution duration, candidate count, temporary candidate
  bytes, Master RSS/PSS, allocator active bytes, and allocator retained bytes;
- shard-lock wait, metadata mutations, OpLog/event volume, and RPC latency;
- offload queue depth and age, tasks and bytes delivered per heartbeat, pinned
  bytes, holder staging memory, SSD IOPS/throughput, and network throughput;
- foreground request rate, cache-hit ratio, restore/promote rate, TTFT, and
  request latency percentiles.

Compare at least 1%, 5%, 10%, and 30% reclamation targets while holding payload
throughput constant and varying object count independently from total bytes.
Then compare the same total reclamation goal executed as one batch and as
byte/time-bounded incremental ticks. The pressure hypothesis is supported if
the incremental form preserves reclaimed bytes while reducing Master peak RSS,
queue age, SSD burst amplitude, and foreground tail latency.
