# Starting Notes: Master OOM and Batch-Eviction Pressure

> - Analysis date: 2026-09-03
> - Production baseline discussed here: Mooncake `v0.3.12.post1`
> - Local workspace baseline: Git `HEAD` at `427a3a5a`
> - Inputs: [Master OOM and SGLang L3 pressure reading plan](master-oom-sglang-l3-pressure-reading-plan.md),
>   [issue #3452](https://github.com/kvcache-ai/Mooncake/issues/3452),
>   [issue #952](https://github.com/kvcache-ai/Mooncake/issues/952), the
>   SGLang Mooncake backend, and the current and upstream `BatchEvict`
>   implementations
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

[Issue #3452](https://github.com/kvcache-ai/Mooncake/issues/3452) reports that a `v0.3.12.post1` Master gained approximately 7 GiB of RSS
within two to three hours under a long-context SGLang PD-disaggregated workload.
The report states that RSS did not fall after the request workload completed.

### SGLang cache pages and Mooncake objects

The reported SGLang configuration uses a 64-token cache page and a fixed
40,000-token prompt. One request therefore contains approximately 625 logical
cache pages:

```text
40,000 tokens / 64 tokens per page = 625 logical pages
```

SGLang's Mooncake
[`batch_set_v1`](https://github.com/sgl-project/sglang/blob/main/python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py#L1016-L1068)
path receives logical page keys and host-cache indices. It expands each logical
key into the component keys required by the model's cache layout, resolves the
corresponding registered host-memory pointers, checks the component keys with
Mooncake `batch_is_exist`, and submits only missing components through the
zero-copy `batch_put_from` path:

```text
logical cache page
  -> content-derived logical page key
  -> one or more layout/rank-specific component keys
  -> batch_is_exist(component keys)
  -> batch_put_from(missing keys, host pointers, sizes)
  -> Mooncake object metadata plus payload placement
```

A Mooncake object is a distributed-store entry identified by an object key. It
is not a Python or C++ object in the SGLang process and it is not one complete
request. The logical-page-to-object multiplier depends on the model layout:

- an MLA page commonly maps to one Mooncake component object;
- an MHA page commonly maps to separate K and V component objects; and
- split-head and hybrid layouts can map one page to additional component
  objects.

The precise source-backed statement is therefore:

> `batch_set_v1` writes one or more Mooncake component objects for every
> logical cache page whose derived component keys do not already exist.

Random long prompts have limited cross-request prefix reuse, so most derived
page keys can be new. Request completion does not delete the resulting L3
cache objects. They remain live until explicit removal or eviction. At 625
logical pages per request, the reported 20,000-request upper bound can create
millions of distinct objects after accounting for layout and pipeline/rank
suffixes.

This mechanism establishes a high-cardinality metadata workload but does not
alone establish a leak. The discriminating measurement is whether Master RSS
continues increasing after live object count, allocated payload bytes, and
eviction throughput have stabilized. Continued object-count growth indicates
cache population or ineffective eviction; stable live state with increasing
RSS provides stronger evidence for retained metadata, sparse container
capacity, allocator retention, or a lifecycle defect.

### HA process, leadership-term, and service lifetimes

The HA supervisor owns and validates the `LeadershipSession`.
A `MasterService` is a term-scoped serving runtime created only after the supervisor has acquired
and warmed up that session.

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
Consequently, destroying the last wrapper reference also runs `MasterService::~MasterService` and destroys its `SegmentManager`.
The external Store clients and their physical segments can remain alive across that destruction;
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

The leadership monitor, not `MasterService`, decides when the term is no longer valid.
By the time `MasterService` is destroyed, the term may already have expired or been released.
"Term-scoped service" therefore means that the service lifetime is bounded by the term;
it does not mean that the service owns the lease.

In non-HA mode, `main` constructs one RPC server and one `WrappedMasterService`,
registers the wrapper's methods, and keeps both alive until process shutdown.
There is no in-process destroy/recreate cycle.
In HA mode, the same ordinary request implementation is used after promotion,
but the supervisor may repeat service construction and teardown without restarting the process:

```text
client
  -> direct Master address, or HA-backend leader discovery/watch
  -> WrappedMasterService RPC boundary
  -> MasterService operation
  -> metadata shard / SegmentManager / allocator / task state
  -> process-wide MasterMetricManager update
```

etcd is outside the service object.
With the etcd HA backend it coordinates the leader lease and versioned Master view,
and it can also hold the ordered OpLog that a standby follows.
It does not keep the `MasterService` C++ object alive and does not store the KV payload.
Redis or Kubernetes Lease can replace etcd for leadership coordination;
the process-versus-term lifetime distinction is the same.

### etcd, snapshot, and OpLog recovery model

HA coordination, durable recovery, materialized metadata, and physical replica liveness are separate state domains:

| Domain | Role | Boundary |
|---|---|---|
| Leadership | Grants one Master authority for a versioned term | The HA backend publishes a leased Master view. Its consensus protocol is separate from Mooncake's leadership state machine. |
| Snapshot | Supplies a point-in-time metadata baseline at OpLog sequence `S` | Payloads reside in the configured object store; catalog and control records identify valid artifacts. |
| OpLog | Records ordered application-level metadata mutations after `S` | It contains metadata and replica descriptors, not object payload bytes. |
| `StandbyMetadataStore` | Materializes snapshot plus OpLog as the current standby projection | It is in-memory, process-local state used for catch-up and promotion export. |
| Physical storage | Owns memory and SSD payload bytes | Registration, heartbeat, probing, and cleanup report liveness independently from metadata replay. |

The Store does not call the etcd v3 C++ API directly. Its production call path has four layers:

```text
Store HA component
  -> EtcdHaKvBackend or EtcdHelper
  -> C ABI exported by libetcd_wrapper.so
  -> Go etcd client/v3
  -> etcd cluster
```

`EtcdHaKvBackend` is the backend-neutral adapter used by batch OpLog and snapshot-control code.
It exposes `Get`, `Put`, ordered `Range`, and a compare-and-put transaction.
Leadership also uses `EtcdHelper` directly for lease grant/revoke, leased create, keepalive,
and prefix-watch operations that are outside `HaKvBackend`.
The Go wrapper owns three process-wide clients with separate configurations:

| Go client | Consumer | Relevant behavior |
|---|---|---|
| `globalClient` | Transfer Engine metadata discovery | Reference-counted and independent from Store HA. |
| `storeClient` | Store leadership, OpLog, snapshot control, and quota policy | One client per process; ordinary calls use five- or ten-second RPC deadlines. Reset replaces this client and cancels all Store keepalives, maintenance sessions, and watches. |
| `snapshotClient` | Separate large-value etcd interface; no current C++ caller was found in this inspection | Allows messages up to 2 GB and uses a 60-second timeout. The current catalog-backed snapshot path keeps payloads in its configured object store. |

During normal initialization, all Store users of `EtcdHelper` must resolve to the same endpoint string within one process.
`ConnectToEtcdStoreClient()` is idempotent for that string and returns `INVALID_PARAMS` for a different string after initialization.
The reset path replaces the endpoint set and cancels all Store keepalives, maintenance sessions, and watches.
Leadership, batch OpLog, snapshot coordination, and etcd-backed tenant quota policy
therefore share one Store etcd client even though their higher-level interfaces are separate.

The principal etcd key spaces are:

| Purpose | Key form | Lifetime/consistency mechanism |
|---|---|---|
| Master view | `mooncake-store/<cluster>/master_view` | Attached to the elected leader's lease; its etcd create revision is the `view_version`. |
| Batch records | `/oplog/<cluster>/batches/<20-digit-batch-id>` | Immutable ordered records created in batches. |
| Durable OpLog cursor | `/oplog/<cluster>/durable_prefix` | Transactionally advanced with a new batch. |
| OpLog producer view | `/oplog/<cluster>/producer_view` | Fences the writer against the leadership view. |
| Snapshot control | `/oplog/<cluster>/snapshot/{maintenance,latest,fallback,compaction_floor}` | Coordinates snapshot publication, fallback, and compaction. Snapshot artifacts themselves use the configured object-store root. |

#### Leadership acquisition and OpLog authority

The etcd backend gives Mooncake two distinct but connected authorities:

1. The leased Master-view key identifies the only process authorized to own
   the current serving term.
2. The producer view and durable OpLog prefix identify the only accepted writer
   and the contiguous mutation history that standby recovery may apply.

Every Master process initially operates as a standby or candidate. The active
leader keeps the lease attached to the Master-view key alive. After the leader
fails or loses connectivity long enough for that lease to expire, etcd removes
the key. Candidates then race to create the absent key with their own lease:

```text
old leader stops renewing its lease
  -> etcd expires the lease and removes the Master-view key
  -> candidates observe the missing view
  -> each calls TryAcquireLeadership(local address)
  -> etcd atomically permits one create-with-lease transaction
  -> one candidate owns the new LeadershipSession and view_version
  -> all contending candidates remain standbys
```

Mooncake Master processes do not vote for one another. The winner is the
candidate whose conditional etcd transaction succeeds, rather than the
candidate with the lowest replication lag. etcd's own members use Raft to
provide the consensus behind that transaction, but the internal etcd leader is
not the Mooncake Master leader.

The `view_version` is the etcd create revision of the leased Master view and
identifies the leadership term. OpLog publication transactionally compares the
producer-view value and the expected durable prefix before it creates a batch
and advances the prefix. These comparisons fence an obsolete Master and
prevent two producers from independently extending the authoritative history.

Leadership acquisition precedes standby promotion. It does not immediately
authorize client serving. The winning process must stop ordinary following,
read the current durable prefix, apply every missing batch, export the stable
standby projection, restore a fresh `MasterService`, complete warmup and a
lease-renewal preflight, and only then publish the RPC service. Promotion fails
with `INCOMPLETE_OPLOG_CATCH_UP` when it cannot prove a complete prefix.

#### Why standby positions differ

All healthy standbys consume the same authoritative OpLog from etcd. They can
temporarily materialize different prefixes of that log:

```text
authoritative durable prefix in etcd: sequence 10,000
standby A applied prefix:             sequence 10,000
standby B applied prefix:             sequence  9,970
standby C applied prefix:             sequence  9,100
```

Different startup times, replay throughput, CPU pressure, snapshot work,
network interruptions, watch disruption, and process restarts account for the
different local positions. This is replication lag, not divergent OpLog
authority. Each standby records its own applied position and reconstructs a
process-local `StandbyMetadataStore`; etcd retains the common durable batches
and prefix.

Operational leader identification must distinguish election from serving
readiness:

| Signal | Meaning |
|---|---|
| Master-view key and `view_version` | Candidate that currently owns the etcd leadership lease and its term. |
| `applied_seq_id` | Last sequence materialized by that standby. |
| `primary_seq_id` and `lag_entries` | Best-effort durable boundary and the standby's distance from it. |
| Runtime state `candidate` or `catching_up` | Election or recovery is still in progress. |
| Runtime state `leader_warmup` | State restoration completed, but serving publication is not complete. |
| Runtime state `serving` | The elected node completed catch-up, restoration, warmup, and the final lease check. |

The leased Master-view key determines the election winner. The node reporting
`serving` is the production-ready leader. A candidate can win the lease while
lagging, but it cannot safely serve until final catch-up reaches the durable
prefix.

The following diagrams use these symbols:

```text
H  = HA backend and LeaderCoordinator
L  = serving leader MasterService
O  = durable batch OpLog
S  = snapshot artifacts and catalog
M  = StandbyMetadataStore plus StandbySegmentRegistry
P  = physical memory/SSD storage and its liveness signals
q0 = snapshot boundary; qN = current durable OpLog boundary
S[q], M[q] = snapshot or materialized state through sequence q
O(q0,qN] = contiguous OpLog suffix after q0 through qN
--> = call or data flow;  [lock] = synchronization boundary
```

The standby lifecycle is:

```text
time
 |
 v
RunSupervisorLoop()
 `-- EnterStandbyMode()
      `-- StandbyController::StartStandby()
           `-- HotStandbyService::Start()
                +-- PrepareBootstrapBaselineLocked()
                |    +-- BatchOpLogSnapshotProvider::RestoreBaseline(): S[q0] -> M[q0]
                |    `-- RestoreCompleteOpLog(): O(0,q0] -> M[q0] (fallback)
                |
                `-- StartOplogFollowingLocked(q0)
                     `-- ReplicationLoop()
                          `-- repeat:
                               OpLogBatchStandbyReader::PollOnce(): O(q0,qN]
                               -> OpLogApplier::Apply()
                               -> [metadata write] M[qN]

L -- ordered metadata mutations --------------------------> O
L -- allocation/write/mount/unmount ----------------------> P
P -- registration/heartbeat/probe ------------------------> L

Recovery invariant: S[q0] + O(q0,qN] = M[qN].
```

A snapshot provides the bulk baseline; the OpLog supplies later mutations.
`M` materializes their result so promotion does not replay all recovery history
during the outage. It retains object identity, owner, size, replica descriptors,
and applied sequence, but not payload bytes or the complete `MasterService`
runtime. Payloads remain in `P`.

After the snapshot boundary, the standby applies only the currently supported
OpLog mutation set:

| OpLog operation | Current standby effect |
|---|---|
| `PUT_END` | Insert or replace completed metadata from `MetadataPayload`: client UUID, size, and replica descriptors. |
| `PUT_REVOKE` | Remove the entire key from the standby projection. |
| `REMOVE` | Remove the key. |
| `LEASE_RENEW` | Intentionally not recorded or applied in the current etcd hot-standby design. |

Lease and soft-pin timestamps can reject expired snapshot objects but are not
retained in `StandbyObjectMetadata`. The projection also excludes RPC state,
workers, queues, in-flight operations, metrics, and eviction timers.

Promotion transfers the stable projection into a fresh serving service:

```text
old L                 H                  candidate M
  X-- lease lost ---->|                       |
                      |<-- TryAcquireLeadership()
                      |--- LeadershipSession ->|
                      |                        |
                      |   PromoteStandbyAndExport()
                      |   `-- [service mutex]
                      |        +-- StopReplicationLoop(): stop + join
                      |        +-- FinalCatchUpForPromotionLocked()
                      |        |    `-- PollOnce() -> Apply(): O -> M[qN]
                      |        `-- PromoteAndExportSnapshot()
                      |             `-- PromotionContext{objects, segments, qN}
                      |                        |
                      |   WarmupLeadership()   |
                      |   StartLeadershipMonitor()
                      |                        |
                      |   construct WrappedMasterService
                      |   `-- RestoreFromStandby(...)
                      |        `-- [snapshot_mutex_]
                      |             RestoreFromStandbySnapshot()
                      |             - validate descriptors and ranges
                      |             - build metadata shards
                      |             - mark missing endpoints invalid
                      |                        |
                      |   RegisterRpcService() + final renewal preflight
                      |                        `--> new L accepts RPCs
```

The essential consistency boundaries are:

| Boundary | Mechanism | Guarantee |
|---|---|---|
| `L -> O` publication | Producer-view and durable-prefix transaction | Fenced writer and contiguous durable prefix. |
| `O -> M` replay/export | Stop/join replay thread and service mutex | Stable `{objects, segments, sequence}` handoff. |
| `M -> L` restore | `snapshot_mutex_` and shard synchronization | Validated metadata installed before RPC exposure. |
| `P -> L` liveness | Registration, heartbeat, probe, and cleanup | Current physical replica usability. |

No mutex or transaction spans logical metadata and physical payload storage. A
replica descriptor may be consistent with durable OpLog sequence `N` while its
storage endpoint is unavailable. Promotion establishes logical consistency;
post-restore liveness mechanisms establish current physical usability.

The OpLog has differentiated durability: `PUT_END` is asynchronous and
lag-tolerant, while removals that permit memory reuse are persisted before
return. Acceptance tests must correlate the old leader's last acknowledged
mutation with the promoted sequence, object set, and usable replica set.

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
