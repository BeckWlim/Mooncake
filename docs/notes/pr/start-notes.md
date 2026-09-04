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

Random long prompts have limited cross-request prefix reuse, so most derived page keys can be new.
Request completion does not delete the resulting L3 cache objects.
They remain live until explicit removal or eviction.
At 625 logical pages per request,
the reported 20,000-request upper bound can create millions of distinct objects
after accounting for layout and pipeline/rank suffixes.

This mechanism establishes a high-cardinality metadata workload but does not alone establish a leak.
The discriminating measurement is whether Master RSS continues increasing after live object count,
allocated payload bytes, and eviction throughput have stabilized.
Continued object-count growth indicates cache population or ineffective eviction;
stable live state with increasing RSS provides stronger evidence for retained metadata,
sparse container capacity, allocator retention, or a lifecycle defect.

### Metadata-shard role and concurrency

`MasterService` contains a fixed array of 1,024 `MetadataShard` instances.
Each shard stores a tenant map; each `TenantState` then stores object metadata,
processing keys, and replication, offload, promotion, and dynamic-replication state. 
This is an in-process logical partition of Master control-plane state.
Replica descriptors identify payload placement, while Store Workers own the distributed DRAM and LOCAL_DISK payload bytes.

Current `main` computes the shard index as follows:

```text
default tenant:  hash(key) % 1024
named tenant:    hash_combine(hash(tenant_id), key) % 1024
```

Every array entry exists for the lifetime of its `MasterService`. 
Each entry owns a distinct `SharedMutex`; 
populated objects occupy only the shards selected by the hash. The accessors express the lock mode:

- `MetadataShardAccessorRO` acquires the selected shard lock in shared mode;
- `MetadataShardAccessorRW` acquires it in exclusive mode;
- readers of one shard can run together, while a writer serializes access to that shard; and
- operations on different shards use independent locks.

The shard supplies the data and lock boundary. 
Parallel execution comes from the RPC runtime or from an operation that explicitly starts worker threads:

```text
RPC worker
  -> getShardIndex(tenant_id, key)
  -> MetadataShardAccessorRO/RW(shard[index])
  -> shard[index].mutex
  -> tenants[tenant_id] -> metadata[key]

BatchRemove(keys)
  -> group keys by shard
  -> visit each selected shard once
  -> process that shard's keys under one exclusive shard lock
```

`BatchEvict` supplies its own parallel census. 
It creates 16 workers, assigns each worker a disjoint contiguous range of 64 shards, 
and has each worker lock one shard at a time. 
The census uses exclusive shard access because it also discards expired processing replicas. 
Each worker writes to thread-local counts and candidate vectors; 
the calling thread joins the workers and merges their results. 
Candidate execution is then serial and reacquires the selected object's shard lock for lookup and revalidation.

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

Group routing is version-dependent. In `v0.3.13`,
[`getMetadataShardIndex`](https://github.com/kvcache-ai/Mooncake/blob/b04c6a4b6a32e98cf17756a4dc747c950669c1ea/mooncake-store/src/master_service.cpp#L1325-L1333)
routes registered members by `hash(group_id)`, which normally co-locates one group. 

Current `main` routes each member by tenant and key, stores membership in a separate group domain, 
partitions group members by shard during eviction, and visits those shards in ascending order. 

Both versions evaluate lease eligibility at group scope and expand the selected group into members. 
Each member and replica is then revalidated, so protected members or replicas can remain.

The shard index has process-local meaning. 
HA failover promotes the complete serving authority 
and its full metadata keyspace rather than assigning individual indices to different serving Masters.

Source anchors:

- [`MetadataShard` and the 1,024-entry array](../../../mooncake-store/include/master_service.h#L1656)
- [read-write and read-only shard accessors](../../../mooncake-store/include/master_service.h#L1790)
- [tenant-and-key shard routing](../../../mooncake-store/include/master_service.h#L1905)
- [`BatchRemove` grouping by shard](../../../mooncake-store/src/master_service.cpp#L6815)
- [the 16-worker `BatchEvict` census](../../../mooncake-store/src/master_service.cpp#L10410)
- [cross-shard group execution order](../../../mooncake-store/src/master_service.cpp#L1669)

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


The etcd backend gives Mooncake two distinct but connected authorities:

1. The leased Master-view key identifies the only process authorized to own
   the current serving term.
2. The producer view and durable OpLog prefix identify the only accepted writer
   and the contiguous mutation history that standby recovery may apply.

Every Master process initially operates as a standby or candidate.
The active leader keeps the lease attached to the Master-view key alive.
After the leader fails or loses connectivity long enough for that lease to expire, etcd removes the key.
Candidates then race to create the absent key with their own lease:

```text
old leader stops renewing its lease
  -> etcd expires the lease and removes the Master-view key
  -> candidates observe the missing view
  -> each calls TryAcquireLeadership(local address)
  -> etcd atomically permits one create-with-lease transaction
  -> one candidate owns the new LeadershipSession and view_version
  -> all contending candidates remain standbys
```

Mooncake Master processes do not vote for one another.
The winner is the candidate whose conditional etcd transaction succeeds,
rather than the candidate with the lowest replication lag.
etcd's own members use Raft to provide the consensus behind that transaction,
but the internal etcd leader is not the Mooncake Master leader.

The `view_version` is the etcd create revision of the leased Master view and identifies the leadership term. 
OpLog publication transactionally compares the producer-view value 
and the expected durable prefix before it creates a batch and advances the prefix. 
These comparisons fence an obsolete Master and prevent two producers from independently extending the authoritative history.

Leadership acquisition precedes standby promotion. 
It does not immediately authorize client serving. 
The winning process must stop ordinary following, read the current durable prefix, 
apply every missing batch, export the stable standby projection, restore a fresh `MasterService`, 
complete warmup and a lease-renewal preflight, and only then publish the RPC service. 
Promotion fails with `INCOMPLETE_OPLOG_CATCH_UP` when it cannot prove a complete prefix.

All healthy standbys consume the same authoritative OpLog from etcd. 
They can temporarily materialize different prefixes of that log:

```text
authoritative durable prefix in etcd: sequence 10,000
standby A applied prefix:             sequence 10,000
standby B applied prefix:             sequence  9,970
standby C applied prefix:             sequence  9,100
```

Different startup times, replay throughput, CPU pressure, snapshot work, network interruptions,
watch disruption, and process restarts account for the different local positions.

This is replication lag, not divergent OpLog authority.
Each standby records its own applied position and reconstructs a process-local `StandbyMetadataStore`;
etcd retains the common durable batches and prefix.

Operational leader identification must distinguish election from serving readiness:

| Signal | Meaning |
|---|---|
| Master-view key and `view_version` | Candidate that currently owns the etcd leadership lease and its term. |
| `applied_seq_id` | Last sequence materialized by that standby. |
| `primary_seq_id` and `lag_entries` | Best-effort durable boundary and the standby's distance from it. |
| Runtime state `candidate` or `catching_up` | Election or recovery is still in progress. |
| Runtime state `leader_warmup` | State restoration completed, but serving publication is not complete. |
| Runtime state `serving` | The elected node completed catch-up, restoration, warmup, and the final lease check. |

The leased Master-view key determines the election winner. 
The node reporting `serving` is the production-ready leader. 
A candidate can win the lease while lagging, but it cannot safely serve until final catch-up reaches the durable prefix.

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
`M` materializes their result so promotion does not replay all recovery history during the outage.
It retains object identity, owner, size, replica descriptors, and applied sequence,
but not payload bytes or the complete `MasterService` runtime.
Payloads remain in `P`.

After the snapshot boundary, the standby applies only the currently supported OpLog mutation set:

| OpLog operation | Current standby effect |
|---|---|
| `PUT_END` | Insert or replace completed metadata from `MetadataPayload`: client UUID, size, and replica descriptors. |
| `PUT_REVOKE` | Remove the entire key from the standby projection. |
| `REMOVE` | Remove the key. |
| `LEASE_RENEW` | Intentionally not recorded or applied in the current etcd hot-standby design. |

Lease and soft-pin timestamps can reject expired snapshot objects but are not retained in `StandbyObjectMetadata`.
The projection also excludes RPC state, workers, queues, in-flight operations, metrics, and eviction timers.

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

No mutex or transaction spans logical metadata and physical payload storage.
A replica descriptor may be consistent with durable OpLog sequence `N` while its storage endpoint is unavailable.
Promotion establishes logical consistency; post-restore liveness mechanisms establish current physical usability.

The OpLog has differentiated durability: `PUT_END` is asynchronous and lag-tolerant,
while removals that permit memory reuse are persisted before return.
Acceptance tests must correlate the old leader's last acknowledged mutation
with the promoted sequence, object set, and usable replica set.

### Capacity-accounting leak across leadership terms

Mounted memory-segment capacity crosses two lifetime domains:

- `SegmentManager::mounted_segments_` is term-scoped state owned by the current `MasterService`.
- `MasterMetricManager::mem_total_capacity_` is a gauge in a function-local static singleton and survives for the lifetime of the process.

Mounting a segment adds its size to both domains. 
An ordinary committed unmount removes the record and decrements the gauge. 

Before PR #3154, service teardown had no corresponding decrement for segments that were still mounted.
Leadership loss normally stops the old RPC service before every client can complete an ordinary unmount, 
so destroying the old `SegmentManager` removed its records without removing their singleton contributions.

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
- [PR #3160](https://github.com/kvcache-ai/Mooncake/pull/3160) removes stale
  entries from each local-disk segment's `offloading_objects` mirror when
  object metadata is erased. Without that cleanup, a later heartbeat can
  resubmit a task-less key and recreate `LOCAL_DISK`-only metadata or an
  orphan SSD bucket.
- [PR #3576](https://github.com/kvcache-ai/Mooncake/pull/3576) shrinks sparse
  metadata-map bucket arrays after eviction.

These changes address distinct mechanisms. They do not by themselves prove
whether the production RSS trajectory is retained live metadata, temporary
allocation retained by the allocator, an HA accounting amplifier, sparse
container capacity, or a lifecycle leak.

### RSS contributors and issue resolution

The evidence does not support treating the reported RSS growth as one generic
memory leak. The relevant mechanisms have different ownership and
observability:

| Mechanism | Effect on Master RSS or eviction | Evidence for issue #3452 | Resolution |
|---|---|---|---|
| Live metadata for millions of distinct object keys | Persistent memory proportional to the live object and replica count | The random long-context workload directly creates this state; this is cache population, not unreachable memory | Bound object cardinality through eviction and admission; partition the serving Master only if the bounded live set remains too large |
| Full `BatchEvict` candidate identities | A transient allocation proportional to all eligible objects, followed by possible allocator retention | Present in `v0.3.12.post1`; it can raise the peak but does not alone explain continued logical growth | PR #3118 materializes only the low-ratio eviction frontier plus a bounded reserve |
| Sparse `TenantState::metadata` bucket arrays | `erase()` removes nodes but leaves the hash table at its peak bucket count, so RSS can remain high after eviction | PR #3576 explicitly identifies this as part of the RSS findings in issue #3452 and is the closing change associated with that issue | PR #3576 conditionally rehashes large maps after an eviction cycle when live size falls below one quarter of bucket count |
| Allocator-retained freed pages or fragmentation | Freed candidate, node, or bucket allocations can remain in process RSS even though they are no longer live | Plausible from RSS alone; no allocator active/retained measurements were supplied in the issue | Measure allocator active and retained bytes before selecting allocator-specific decay or release controls |
| HA capacity gauge retained across leadership terms | Inflates total capacity and can prevent the high-watermark eviction trigger from firing after remount | An indirect amplifier only; it does not allocate the reported RSS by itself and requires a leadership-term transition | PR #3154 plus the ownership correction in PR #3168 release the serving term's remaining capacity contribution at `MasterService` teardown |
| Stale SSD-offload mirror entries | Retains queued identity and can resurrect removed metadata or pin the offload lifecycle | A real lifecycle defect on the offload path, but the available issue evidence does not attribute the 7 GiB trajectory to it | PR #3160 erases the mirrored key from all mounted local-disk segment queues when metadata is erased |
| Snapshot, recovery, and bounded task queues | Can add full-image transient buffers or bounded backlog | Source-level candidates only; the issue contains no correlated snapshot, failover, or queue measurements | Validate separately with queue cardinalities, snapshot peaks, and leadership events |

The reported `v0.3.12.post1` baseline predates all four directly relevant fix
areas covered by PRs #3118, #3154/#3168, #3160, and #3576. PR #3576 supplies
the repository-level remedy for the specific post-eviction RSS symptom. A safe
deployment uses a release containing the complete fix set or a backport of that
set. Production evidence for the post-quiescence RSS plateau remains pending.

Release status checked on 2026-09-03: the
[GitHub release](https://github.com/kvcache-ai/Mooncake/releases/tag/v0.3.13.post1)
marks `v0.3.13.post1` as latest, and
[PyPI](https://pypi.org/project/mooncake-transfer-engine/) lists
`0.3.13.post1` as the current package release. Plain `v0.3.13` is the preceding
base release.

Acceptance requires replaying the reported workload and showing that live
object count becomes stable, metadata bucket counts contract after eviction,
allocator active bytes fall after quiescence, and RSS/PSS reaches a bounded
plateau across repeated fill-and-evict cycles. If memory remains proportional
to a bounded but irreducibly large live metadata set after these fixes, active
multi-master partitioning becomes the capacity solution. PR #3576 remains the
sparse-container retention remedy.

### Derived conclusion

Multi-master is the structural solution only when measurements show that the
irreducible live metadata or control-plane work exceeds a single serving
Master's budget after bounded-state and transient-allocation fixes are applied.
It is not the first fix for a temporary `BatchEvict` allocation peak.

The metadata shards above remain local concurrency units within one Master.
HA leader/standby support provides one serving authority. An active
multi-master design requires a separate ownership layer:

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

[Issue #952](https://github.com/kvcache-ai/Mooncake/issues/952) proposes an RFC
for DFS/3FS file cleanup. It combines storage monitoring with a lease-based
approximation of LRU. The slower SSD control path motivates less frequent
cycles and larger reclamation amounts, with 30% as an example.

The 30% value has design-example status; its production default and optimum
remain unvalidated.
The issue also concerns secondary-storage file eviction,
whereas the production pressure path discussed in the Master OOM note begins
with distributed-memory `BatchEvict`.
The two paths share the batching tradeoff and differ in ownership, latency, and
work units.

The proposal and the current Master use different units:

- issue #952 describes clearing a fraction of target **space**;
- the current Master `eviction_ratio` selects a fraction of the eviction-base
  **object count**.

Variable object sizes let a count ratio under-reclaim byte pressure or
over-evict many small, reusable KV objects.

### Current memory-eviction mechanism

Distributed-DRAM eviction has its own policy path. The Master invokes
`BatchEvict` when global distributed-memory utilization crosses the high
watermark or a memory allocation failure sets `need_mem_eviction_`. The
production baseline uses these defaults:

- high-watermark trigger: 90% global distributed-memory usage;
- base eviction ratio: 5% of the eviction-base object count;
- eviction-thread check interval: 10 ms.

For observed usage `U`, high watermark `H`, and configured ratio `R`, one cycle
computes:

```text
target object ratio = max(R, U - H + R)
lower bound         = max(target / 2, U - H)
```

The target combines the watermark excess with configured headroom. Under a
proportional relationship between object count and allocated bytes, the full
target moves usage from `U` to approximately `H - R`. An allocation-failure
cycle below the watermark uses `R` as its target. The lower bound supplies the
second-pass objective: at least half the target and at least the watermark
excess.

For `U = 93%`, `H = 90%`, and `R = 5%`:

```text
target      = 93% - 90% + 5% = 8%
lower bound = max(4%, 3%)     = 4%
```

`BatchEvict` builds thresholds, candidates, group expansion, and final mutation
decisions incrementally. The two lifelines show the asynchronous
foreground/thread boundary; synchronous internals use an ordinary call chain.

```text
Legend:  ----> call/action    --?-> check    <---- result

Foreground request       Eviction thread
        |                        |
        |-- allocation fails     |
        |   set need_mem=true -> |
        |                        |
        |                 [periodic wake]
        |                        |--? sample U and need
        |                        |    false -> wait for next wake
        |                        |    true  -> compute T and L
        |                        |----> BatchEvict(T,L)
        |                        |      |
        |                        |      -> capture now
        |                        |      -> parallel_census(16 workers)
        |                        |         ?-> hard pin
        |                        |         ?-> DRAM state and refcnt
        |                        |         ?-> lease and soft pin
        |                        |         <- base count and deadlines
        |                        |      -> build_candidate_frontier()
        |                        |      -> first_pass(target_count)
        |                        |         -> for each candidate
        |                        |            ?-> lookup and revalidate
        |                        |            -> ordinary object
        |                        |               -> persist or offload
        |                        |               -> evict DRAM replicas
        |                        |            -> grouped object
        |                        |               -> get current members
        |                        |               -> for each member
        |                        |                  ?-> revalidate
        |                        |                  -> persist/offload/evict
        |                        |      -> release_expired_discarded()
        |                        |      ?-> result below lower bound
        |                        |          -> second_pass(remaining_count)
        |                        |      -> cleanup and record metrics
        |                        |<---- BatchEvict result
        |                        |
        |                 [next periodic wake]
        |                        |--? sample new U and need
        |                        |    true  -> start another cycle
        |                        |    false -> wait for next wake
        |                        |
                   time proceeds downward
```

`EvictionThreadFunc` calculates `T` and `L` immediately before the synchronous call.
A foreground allocation failure sets `need_mem_eviction_` and returns;
the background thread observes the flag at its next wake.

At entry, `BatchEvict` captures one `now` value.
The census visits each `ObjectMetadata` once and inspects its replicas.
An object contributes one unit to `eviction_base` when its hard-pin check passes
and it has at least one complete, readable DRAM replica with `refcnt == 0`.
Lease expiry and soft-pin policy then determine candidate membership.
An object with several reclaimable replicas still contributes one unit.

For low target ratios in `v0.3.13` and later, Mooncake materializes full tenant/key identities
for a bounded frontier around the oldest deadline cutoff.
The frontier is an ephemeral work list whose lifetime matches the current invocation.
Each candidate receives a fresh lookup and revalidation immediately before mutation.
A concurrent read can extend its lease beyond the captured `now` and move it out of the current execution set.
A lease that expires after the captured value enters eligibility in a later invocation.

Successful `ExistKey` and `GetReplicaList` calls extend the object lease.
Ordering expired candidates by lease deadline implements approximate LRU at the object level.
The object owns the eviction lease; each replica owns its completion state and reference count.
Ordinary-object execution removes every DRAM replica that is complete, readable, and has `refcnt == 0`.
Busy DRAM replicas and SSD or DFS replicas remain valid members of the object.

A grouped candidate expands into its current member list at execution time.
Mooncake visits member shards in a fixed order and applies serial, best-effort revalidation and mutation.
Eligible members release their DRAM replicas; members protected by lease, pin, replica state, or persistence state remain.
One group expansion can reclaim several members and overshoot the object-count target.

In `v0.3.13`, each member stores its own `lease_timeout`, and group execution requires every member lease to be expired.
The newer implementation assigns one shared `Lease` to the group, so a read of any member refreshes the group deadline.
Both implementations place eviction leases at object or group scope;
replicas retain independent state and reference counts.
Dynamic-replication leases serve operation control.

After the first pass and expired-discard cleanup, Mooncake calculates:

```text
remaining lower-bound count =
    ceil(eviction_base * lower_bound)
    - evicted_object_count
    - released_discarded_count
```

A positive remainder starts the second pass over remaining expired ordinary
objects and, when configured, soft-pinned objects. Candidate availability,
concurrent changes, OpLog results, and deferred SSD offload determine the
attainable count. Completion records the actual object count and reclaimed
bytes through the existing metrics.

DRAM utilization is sampled at cycle boundaries. A large-object removal can
move utilization below `H` while the current invocation continues toward its
count objective; small-object removals can satisfy that objective while byte
pressure remains. The next wake applies these rules:

- `U > H`: start another cycle;
- `U <= H` with `need_mem_eviction_` cleared: wait for the next wake;
- `U <= H` with `need_mem_eviction_` retained: start another cycle.

Successful removal or accepted offload deferral normally clears the
allocation-failure flag, including results below the lower bound. A
zero-progress cycle can retain the flag while the eviction base remains
populated.

### Pressure-amplification hypothesis

The issue #952 proposal correctly identifies the average-frequency benefit of
a large reclamation cycle: more headroom delays the next trigger.
For the current production problem, however,
the same choice increases instantaneous work along the complete eviction path:

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
- If a cycle queues work faster than holders drain it,
  the low-frequency policy shifts pressure into queue age, source pinning,
  and request-tail latency instead of eliminating it.

PR #3118 supplies mechanism-level evidence for the first two points: low ratios
benefit from bounded candidate materialization, while its 30% benchmark takes
the full-candidate path with unchanged scan time. The benchmark covers the
Master scan in isolation; production evidence for SGLang traffic, SSD offload,
HA replication, and foreground latency remains pending.

### Direct solution direction

The eviction path should control bytes and elapsed work together with the
selected-key count. A production-oriented scheduler should provide:

1. high/low watermark hysteresis expressed in bytes;
2. per-tick budgets for scanned keys, selected keys, reclaim bytes, execution
   time, and per-holder offload bytes;
3. an incremental shard/key cursor that bounds each tick's materialization and
   locking scope;
4. queue-depth and queue-age feedback that pauses selection when holders,
   OpLog replication, or event consumers are behind;
5. bounded heartbeat delivery so a holder consumes offload work in controlled
   chunks;
6. admission throttling while reclaim is in progress to preserve reclaimed
   headroom; and
7. separate QoS for foreground restores and background offload writes.

A 30% total reclamation goal can serve as a gradual low-watermark destination,
with selection and execution distributed across paced cycles.

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
scan and mutation peak. Pacing remains a separate requirement: simultaneous
30% eviction cycles on several partitions can preserve or increase aggregate
SSD, network, and request pressure.

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
