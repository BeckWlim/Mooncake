# Mooncake Store HA: recent issues and upstream status

> Evidence snapshot: 2026-09-08
>
> Activity window: 2026-08-08 through 2026-09-08, inclusive
>
> Upstream: [kvcache-ai/Mooncake](https://github.com/kvcache-ai/Mooncake)
>
> Scope: Mooncake Store Master primary/standby HA, batch OpLog, snapshot
> recovery, promotion, and Store-client reattachment. Transfer Engine and
> Mooncake PG failover are outside this note.

## Summary

[Issue #3561](https://github.com/kvcache-ai/Mooncake/issues/3561) 
correctly identifies the central production limitation: the released HA path can follow an etcd-backed OpLog, 
but it does not yet provide a fully wired, bounded snapshot-plus-suffix recovery path. 
Enabling `enable_snapshot` together with `enable_oplog` does not make the primary write periodic legacy snapshots; 

the code explicitly skips primary snapshot generation because snapshots are intended to be owned by the standby. 
The replacement standby-generated path has been implemented in stages, 
but its production configuration switch, end-to-end gate, compaction floor, and OpLog pruning remain roadmap work in
[issue #3808](https://github.com/kvcache-ai/Mooncake/issues/3808).

The latest release at the evidence date is
[`v0.3.13.post1`](https://github.com/kvcache-ai/Mooncake/releases/tag/v0.3.13.post1).
It contains important writer, snapshot-format, capture, restore-validation,
and fencing foundations. It does not contain the post-release promotion gate,
snapshot publication/bootstrap, production writer fencing, bounded promotion,
or OpLog-pressure fixes listed below. Therefore, the release should not be
treated as providing bounded cold bootstrap, safe rolling Master replacement,
or bounded etcd OpLog retention.

Recent production reports also show that `ha_oplog_standby_lag=0` is necessary
but not sufficient for safe promotion:

- [Issue #3760](https://github.com/kvcache-ai/Mooncake/issues/3760) reproduced a
  300-to-0 key-count transition on `v0.3.13` when one overlapping descriptor
  caused all-or-nothing promotion restore to fail.
- [Issue #3740](https://github.com/kvcache-ai/Mooncake/issues/3740) measured an
  intact promoted index that remained unreadable for approximately two minutes
  while Store clients retried the old Master endpoint.
- [Issue #3774](https://github.com/kvcache-ai/Mooncake/issues/3774) found no
  demonstrated index-preserving Master rolling-upgrade path on `v0.3.13`.

The production position is consequently version- and topology-specific. Use an
exact commit rather than the label "recent main," verify inclusion of the
required PRs, and pass a real-etcd failover and byte-integrity gate before
deployment.

## Current mechanism and the gap in issue #3561

The released HA architecture has separate leadership and metadata-replication controls:

- `enable_ha` selects leader election through etcd, Redis, or Kubernetes.
- `enable_oplog` selects metadata replication. 
  It currently requires both HA and the etcd backend; Redis and Kubernetes provide election without OpLog replication.
- A serving primary admits ordered mutation batches and advances the durable prefix in etcd. 
  A standby applies complete batches in sequence and exports its in-memory state during promotion.
- `enable_snapshot_restore` can load a snapshot baseline during standby startup, followed by OpLog replay from the snapshot cursor.

Sources: [deployment HA configuration](../../source/deployment/mooncake-store-deployment-guide.md#ha-configuration),
[`BuildStandbyRuntimeCapabilities`](../../../mooncake-store/src/ha/standby_controller.cpp#L36),
and [`PrepareBootstrapBaselineLocked`](../../../mooncake-store/src/hot_standby_service.cpp#L151).

The legacy primary snapshot branch makes the mode interaction explicit:

```cpp
if (config.enable_snapshot && !enable_oplog_) {
    // Start the legacy primary snapshot manager.
} else if (config.enable_snapshot && enable_oplog_) {
    LOG(INFO) << "Skipping primary snapshot generation in batch-record "
                 "OpLog mode; snapshots are owned by standby";
}
```

Source: [`MasterService` snapshot startup](../../../mooncake-store/src/master_service.cpp#L529).

The intended replacement path is:

```text
primary:  fenced ordered batches ----------------------------+
                                                            |
standby:  restore latest/fallback snapshot -> replay suffix -> caught up
                  |                              |
                  +-> periodically publish a verified snapshot
                                      |
new standby:      restore snapshot -> replay suffix
                                      |
retention:        publish compaction floor -> prune old batches
```

The repository contains components for snapshot metadata, bounded capture,
chunked artifact writing, fenced publication, pointer bootstrap, and periodic coordination. 
In the evidence checkout, however, the coordinator is not constructed by `StandbyController`, and there is no public `enable_batch_oplog_snapshot` configuration. 
This matches the N08 production switch that remains planned in #3808. 
The `compaction_floor` key is defined, but floor-aware rebootstrap and pruning are also still planned. 
Operators should not manually delete batch records while a standby may require them.

Sources: [`BatchOpLogSnapshotCoordinator`](../../../mooncake-store/include/ha/snapshot/batch_oplog/batch_oplog_snapshot_coordinator.h#L45),
[`StandbyController` construction](../../../mooncake-store/src/ha/standby_controller.cpp#L73),
and [`OpLogBatchStorage` control-key validation](../../../mooncake-store/src/ha/oplog/oplog_batch_storage.cpp#L517).

## Mapping the four questions from issue #3561

| #3561 concern | Evidence-date status | Relevant upstream work |
|---|---|---|
| New standby starts from sequence zero and large history is expensive | Partially implemented on `main`, not production-wired in the latest release. Snapshot pointer bootstrap plus suffix replay exists, but the production switch/E2E gate remains N08. | #3642, #3794, #3808 |
| etcd OpLog has no GC | Open. Floor-aware rebootstrap, safe range deletion, snapshot object GC, floor publication, pruning, and etcd capacity operations are N09-N13. | #3167, #3808 |
| Snapshot is loaded only at standby initialization | The target design intentionally loads a verified snapshot at bootstrap and then continuously replays its suffix. Standbys also generate new snapshots periodically. The components exist, but full production wiring is not released. | #3326, #3447, #3640, #3794 |
| Snapshot and OpLog are mutually exclusive on the primary | Confirmed for the legacy snapshot manager. The planned resolution is not concurrent legacy primary snapshots; it is standby-generated batch-OpLog snapshots selected by `latest`/`fallback` pointers. N08 must activate the path. | #3640, #3642, #3808 |

## Pull-request progression during the activity window

### Foundations included in `v0.3.13` and `v0.3.13.post1`

The release notes and tag history include these HA foundations:

| PR | Merged | Effect | Production boundary |
|---|---:|---|---|
| #3178 | 2026-08-11 | Adds versioned batch-snapshot descriptors, cursors, and checksums. | Format/protocol only. |
| #3201 | 2026-08-11 | Adds producer-view compatibility to durable OpLog metadata. | Does not itself claim or enforce a view. |
| #3204 | 2026-08-13 | Adds terminal writer state and bounded retry failure reporting. | Supervisor fail-stop was a follow-up. |
| #3326 | 2026-08-18 | Captures a complete applied batch in bounded chunks on a standby. | Capture component, not scheduling or public wiring. |
| #3354 | 2026-08-17 | Makes standby restore errors explicit and failure-atomic. | The initial PR intentionally left serving behavior to a follow-up. |
| #3384 | 2026-08-18 | Adds storage-level producer-view claim and atomic batch fencing. | Production writer binding was deferred to F01. |
| #3447 | 2026-08-21 | Writes immutable, chunked, checksum-verified standby snapshot artifacts. | No publication schedule or production switch. |
| #3527 | 2026-08-21 | Rejects cross-shard duplicates during standby restore. | Does not make all other restore failures tolerant. |
| #3566 | 2026-08-24 | Allows HA promotion when OpLog replication is disabled. | This supports election-only HA; it does not add durable metadata recovery. |

`v0.3.13.post1` contains only release-branch build fixes relative to
`v0.3.13`; it does not import later `main` HA changes merely because its release
date is later.

### Merged to `main` after the `v0.3.13` release cut

| PR | Merged | Effect | Remaining boundary |
|---|---:|---|---|
| #3497 | 2026-08-28 | Keeps a failed standby restore non-serving and releases leadership. | Availability is intentionally lost on validation failure; tolerant repair is separate. |
| #3642 | 2026-08-28 | Restores `latest`, falls back to `fallback`, then replays the OpLog suffix. | Requires the new provider to be wired into production. |
| #3640 | 2026-08-31 | Adds an etcd maintenance lease and fenced `latest`/`fallback` publication. | Does not schedule snapshots by itself. |
| #3794 | 2026-09-01 | Adds the periodic batch-OpLog snapshot coordinator. | Its PR explicitly states that it is inert until N08 wiring. |
| #3810 | 2026-09-03 | Claims the acquired producer view and fences every production HA batch transaction. | Terminal fail-stop and durable mutation barriers are distinct gates. |
| #3811 | 2026-09-03 | Preserves `ReplicaID` during standby restore. | Required by bounded promotion; does not address every replica backend. |
| #3841 | 2026-09-07 | Moves standby state into the primary and installs it in bounded chunks. | Keeps fail-closed restore semantics and leaves public wiring to N08. |
| #3860 | 2026-09-07 | Stops repeated eviction after OpLog queue saturation and isolates leadership keep-alive from general etcd-client reset. | Explicitly does not implement the durable mutation barrier or complete #3808. |

Two late-window PRs require care when selecting a deployment commit:

- [#3885](https://github.com/kvcache-ai/Mooncake/pull/3885) prevents `PutEnd`
  from exposing `COMPLETE` state when OpLog reservation or admission fails. It
  does not wait for durable-prefix advancement, so an accepted record can
  still be lost before backend durability.
- [#3923](https://github.com/kvcache-ai/Mooncake/pull/3923) proposed
  supervisor fail-stop after a terminal writer failure. The GitHub search API
  reported it closed on 2026-09-08, while the latest accessible PR-page
  snapshot still showed it as a draft. Treat the behavior as unavailable until
  a containing merge commit or replacement PR is verified.

### Open corrective work

| Issue or PR | Status at evidence date | Operational consequence |
|---|---|---|
| #3561 | Open | Umbrella report for cold bootstrap, unbounded OpLog, and snapshot/OpLog integration. |
| #3496 | Open | Several etcd-named standby tests skip or do not execute the production etcd path. |
| #3740 / #3743 | Open / open | A promoted index can remain unreadable while clients block on old-endpoint connection retries. |
| #3760 / #3806 | Open / open | One invalid or overlapping descriptor can prevent promotion; the proposed repair discards ambiguous replicas and retains independently valid ones. |
| #3761 | Open | The legacy non-OpLog snapshot path can restore and then erase all objects on expired persisted leases. This is adjacent disaster-recovery risk, not the new batch-snapshot format. |
| #3774 | Open | No demonstrated rolling Master upgrade with a promotability/readiness predicate on `v0.3.13`. |
| #3826 / #3858 | Open / open | Restored NoF descriptors may reference offsets that a fresh allocator reallocates; the proposed first stage quarantines them and sacrifices availability to prevent wrong-data reads. |
| #3638 | Open | P2P HA promotion may reuse an existing OpLog sequence when local apply lags the stored latest sequence. |
| #3808 | Open | Tracks production wiring, real-etcd fault tests, bounded history, lifecycle durability, identity, and observability. |

## Release coverage

| Capability | `v0.3.13.post1` | Later `main` by 2026-09-08 | Production status |
|---|---:|---:|---|
| Batch writer, prefix validation, retry/terminal state | Yes | Yes | Foundation available. |
| Snapshot descriptor, bounded capture, artifact writer | Yes | Yes | Components available. |
| Fail-closed serving after restore failure (#3497) | No | Yes | Requires a post-release build. |
| Fenced snapshot publication and suffix bootstrap (#3640/#3642) | No | Yes | Components available. |
| Periodic coordinator (#3794) | No | Yes | Not publicly wired. |
| Production writer fencing (#3810) | No | Yes | Requires a post-release build. |
| Replica-ID preservation and bounded promotion (#3811/#3841) | No | Yes | Requires a post-release build. |
| HA stability under OpLog/eviction pressure (#3860) | No | Yes | Requires a post-release build. |
| Public batch-snapshot mode and real-etcd E2E gate (N08/X01) | No | No confirmed merged delivery | Not production-complete. |
| Compaction floor, rebootstrap, batch pruning, snapshot GC (N09-N13) | No | No confirmed merged delivery | History remains unbounded. |

## Production diagnosis

### Required evidence before failover

Record the image tag and commit SHA for every Master and Store client. On each
Master, collect:

```text
role
state
service_ready
leader
view_version
ha_standby_state
ha_oplog_last_sequence_id
ha_oplog_applied_sequence_id
ha_oplog_standby_lag
ha_oplog_pending_entries
ha_batch_record_durable_sequence
ha_batch_record_committed_queue_depth
ha_batch_record_callback_queue_depth
ha_oplog_etcd_write_failures_total
ha_oplog_etcd_write_retries_total
ha_oplog_checksum_failures_total
ha_oplog_watch_disconnections_total
master_key_count
master_allocated_bytes
master_total_capacity_bytes
```

The metric definitions are in
[`HAMetricManager`](../../../mooncake-store/src/ha_metric_manager.cpp#L19), and
the admin summary fields are documented in
[observability](../../source/getting_started/observability.md#master-metrics-log).

Also collect etcd member health, leader changes, database size, quota alarms,
request latency, and disk latency. Record the durable prefix and the candidate
standby's applied cursor before promotion. A zero lag value must be accompanied
by a plausible applied cursor, key count, segment count, and an application
byte-integrity probe.

### Failover timeline

Place these events on one clock:

```text
old leader stops renewing
-> new view acquired
-> standby final catch-up starts/completes
-> promotion context restore starts/completes
-> RPC listener becomes reachable
-> routable leader view is published
-> each Store client switches leader and remounts
-> first successful application read and byte verification
```

This separates election time, recovery time, service publication, client
reattachment, and data-plane recovery. Reporting only "leader elected" hides
the two-minute reattachment failure in #3740.

### Symptom classification

| Observation | Most likely match | Next verification |
|---|---|---|
| Fresh standby shows lag 0 but has a near-zero applied cursor and no baseline keys | #3561/#3774 cold-bootstrap gap | Compare primary durable prefix, standby applied cursor, key count, and snapshot pointer. |
| Restore logs one descriptor error and the candidate never serves | #3497 fail-closed gate working; #3760 data inconsistency remains | Identify tenant/key/endpoint, validate overlap, and evaluate #3806. |
| Restore error is logged but an empty candidate serves | Pre-#3497 behavior | Upgrade or backport #3497 before another drill. |
| Metadata survives promotion but reads fail for about 120 seconds | #3740 | Trace view publication, RPC listener readiness, old-IP connect timeout, and #3743 inclusion. |
| Leader changes during Store etcd-client reset or eviction pressure | #3860 | Verify dedicated lease client and keep-alive reset isolation. |
| `PutEnd` succeeds while OpLog admission fails | Pre-#3885 behavior | Audit live `COMPLETE` state against durable prefix and verify the exact containing commit. |
| Restored NoF replica remains `COMPLETE` while a remount reuses its offset | #3826 | Stop new allocation on the namespace; evaluate quarantine/import handling before reuse. |
| etcd database grows continuously | #3561/#3808 N09-N13 gap | Measure batch creation rate and database growth; do not delete history without a reader-safe floor. |

## Mitigation and acceptance criteria

1. Treat `v0.3.13.post1` as the minimum released foundation, not as completion
   of #3808. For HA correctness work after the release cut, build from a pinned
   commit and record which post-release PRs it contains.
2. Do not rely on `enable_snapshot=true` plus `enable_oplog=true` to produce
   periodic recoverable snapshots. Confirm actual snapshot pointer movement,
   artifact verification, and suffix replay in the selected build.
3. Maintain continuously running standbys until a production-wired snapshot
   bootstrap and bounded-retention path passes the deployment gate. A newly
   added standby must not be promoted based only on `lag=0`.
4. Do not manually prune `/oplog/{cluster_id}` records. If recovery state is
   proven unusable and cache loss is accepted, stop every process and follow
   the complete new-`cluster_id` or complete-namespace reset procedure in the
   deployment guide.
5. Keep Store clients on the HA discovery address (`etcd://...`) and measure
   both endpoint-switch and remount latency. Validate #3743 or an equivalent
   bounded runtime connection policy before calling failover complete.
6. For NoF, LOCAL_DISK, or DFS replicas, run backend-specific recovery tests.
   Metadata replication alone does not prove that the restored physical
   allocation is owned, readable, or protected from reallocation.

Accept a candidate build only when repeated real-etcd drills demonstrate:

- a fresh standby reconstructs a non-empty baseline and replays only the
  required suffix;
- promotion occurs from a plausible, complete cursor and never serves after a
  failed restore or fencing check;
- acknowledged objects remain present and byte-identical after leader loss;
- `ReplicaID`, segment allocation, capacity, and key counts remain consistent;
- every Store client reconnects within the declared recovery-time objective;
- OpLog queue pressure and general etcd-client reset do not break the
  leadership lease;
- etcd growth, snapshot size, capture pause, bootstrap time, promotion RSS, and
  replay time remain within explicit bounds; and
- the same results hold for cold bootstrap, rolling replacement, process kill,
  network interruption, etcd restart, object-store failure, corrupted latest
  snapshot with valid fallback, and `NOSPACE` injection.

Until the production switch and retention milestones are merged, released,
and validated, the acceptance target is a pinned deployment-specific HA
configuration rather than a general claim that Mooncake Store HA is complete.
