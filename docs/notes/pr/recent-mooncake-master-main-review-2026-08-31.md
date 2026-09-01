# Integrated Review: Recent Mooncake Master Changes on `main`

> - Review date: 2026-08-31
> - Upstream baseline: `kvcache-ai/Mooncake` `main` at `6e1dd41`
> - Review window: 2026-08-24 through 2026-08-31
> - Primary scope: Master HA, recovery authority, metadata mutation durability,
>   and region-resource ownership

## 1. Executive Summary

The reviewed changes form one correctness-oriented sequence even though they
were delivered through separate pull requests:

1. PR #3566 defines **absence of recovery work** as a valid HA promotion
   result. HA-only deployments can promote through an empty
   `PromotionContext`.
2. PR #3497 defines **failure of configured recovery work** as a serving
   failure. Election alone no longer authorizes a Master to serve.
3. PRs #3640 and #3642 add the publication and bootstrap primitives needed to
   recover from a bounded snapshot plus an OpLog suffix.
4. PR #3422 preserves mutation-state consistency when `BatchEvict()` cannot
   reserve or submit its OpLog record.
5. PR #3601 restores the local-storage ordering rule that bucket data must be
   stable before its metadata is committed.
6. PR #3703, the current upstream tip, introduces transactional region-resource
   drivers and centralized allocator-import validation as the first layer of
   the SegmentPool refactor.

The combined direction is coherent: a process may serve only from a state that
has passed its configured recovery and resource-validation gates, and durable
metadata must not describe a mutation or data object whose prerequisite work
failed. The principal remaining gaps are production wiring for the new batch
snapshot path, propagation of asynchronous terminal OpLog-writer failures, an
operator-safe generation-fenced cold-rebuild mechanism, and completion of the
SegmentPool stack above the new region drivers.

## 2. Repository and Review Baselines

### 2.1 Observed remote state

The workspace remote named `internal` uses
`ssh://git@code.iflytek.com:30004/HY_sparkv2/Mooncake.git` for fetch and push.

| Ref | Observed commit | Meaning |
|---|---:|---|
| `upstream/main` | `6e1dd41` | Live public head fetched through the configured proxy |
| `internal/main` | `eaa6c79` | Internal main branch, last commit dated 2026-08-03 |
| `internal/beta-main-pr3310` | `42a9c21` | Internal integration branch containing an internalized #3566 commit |
| `internal/upstream-main-pr3310` | `5cf2cca` | Customized #3310 integration baseline |

The branch name `beta-main-pr3310` describes its earlier integration base; its
tip is the #3566 change. The internal commit `42a9c21` and the public #3566 merge
commit `c3ba762` represent the same PR-level behavior but are not the same Git
object. The later public Master commits reviewed below are not present at the
tip of that internal branch.

### 2.2 Selected pull requests

| Date | PR | Public merge commit | Role in the integrated design |
|---|---|---:|---|
| 2026-08-24 | [#3566](https://github.com/kvcache-ai/Mooncake/pull/3566) | `c3ba762` | Allow expected HA promotion when no standby recovery capability is enabled |
| 2026-08-27 | [#3422](https://github.com/kvcache-ai/Mooncake/pull/3422) | `472b2b0` | Preserve exact `BatchEvict` OpLog failures and roll back rejected submissions |
| 2026-08-28 | [#3497](https://github.com/kvcache-ai/Mooncake/pull/3497) | `69c2287` | Gate serving on successful promotion, restore, and leadership revalidation |
| 2026-08-28 | [#3642](https://github.com/kvcache-ai/Mooncake/pull/3642) | `9233e38` | Restore from `latest`, `fallback`, or proven complete batch OpLog history |
| 2026-08-31 | [#3640](https://github.com/kvcache-ai/Mooncake/pull/3640) | `bf50227` | Fence and atomically publish batch-OpLog snapshot pointers |
| 2026-08-31 | [#3601](https://github.com/kvcache-ai/Mooncake/pull/3601) | `2be371a` | Flush bucket data before committing bucket metadata |
| 2026-08-31 | [#3703](https://github.com/kvcache-ai/Mooncake/pull/3703) | `6e1dd41` | Add stateful Memory/CXL region drivers and allocator-import boundaries |

Recent TENT, Transfer Engine, CI, EP, Reshard, and unrelated Store commits are
outside this review. PR #3620 and PR #3766 improve Store read and file-write
behavior, respectively, but they do not change the Master HA authority
contract analyzed here.

### 2.3 Validation basis

The review uses the live upstream commit graph and diffs fetched into a
temporary bare repository, together with PR descriptions, review discussions,
and reported check results. The current workspace remains on its existing
internal development baseline, so upstream `main` was not built or tested in
this workspace. The conclusions therefore assess the composed design and code
boundaries; the internal-adoption test matrix in Section 10 remains required.

## 3. System Relationship

The reviewed work spans three related planes.

| Plane | Components | Governing question |
|---|---|---|
| Serving authority | HA coordinator, supervisor, promotion context | Is this process permitted to expose the Master service? |
| Recovery authority | OpLog, snapshot artifacts, snapshot pointers, standby state | Which metadata state is authoritative and complete? |
| Resource and data integrity | allocators, regions, replicas, bucket storage | Do recovered metadata references correspond to valid resources and durable data? |

The normal relationship is:

```text
runtime mutation
      |
      +--> write/flush required data
      |
      `--> publish ordered metadata mutation to OpLog
                         |
                         v
                 standby applies batches
                         |
                         v
              snapshot artifact is written
                         |
                         v
       fenced latest/fallback pointer publication
                         |
                         v
       restore snapshot and replay OpLog suffix
                         |
                         v
       final catch-up and PromotionContext export
                         |
                         v
       restore and validate Master resources
                         |
                         v
               revalidate leadership
                         |
                         v
                       serve
```

HA controls role ownership. OpLog and snapshots reconstruct Master metadata.
Region and allocator validation determines whether reconstructed replicas can
be represented safely by the serving process. Local storage flushing protects
the data that metadata makes visible.

## 4. Promotion Semantics: #3566 and #3497

### 4.1 Observed behavior

PR #3566 changes `NoopStandbyController::PromoteStandbyAndExport()` from an
error result to a successful empty `PromotionContext`. The result represents a
valid configuration with no standby metadata to export.

PR #3497 then makes the supervisor apply one promotion contract to all
contexts, including the valid empty context:

```text
acquire leadership
    -> final standby catch-up and export
    -> RestoreFromStandby(context)
    -> revalidate leadership
    -> expose service delegate
```

Failure in promotion, restoration, or final leadership validation leaves the
candidate unavailable and releases leadership. Current `main` therefore
distinguishes two cases that previously shared an error path:

| Condition | Result |
|---|---|
| Recovery is intentionally disabled | Empty context passes the restore/capability gate; an empty Master may serve |
| Recovery is enabled but promotion or restoration fails | Candidate remains unavailable and relinquishes leadership |

### 4.2 Derived conclusion

The pair establishes the central HA invariant:

> Valid absence is success; failed recovery is not absence.

This resolves both sides of the earlier ambiguity. #3566 prevents the no-op
controller from fabricating a failure, while #3497 prevents a real failure from
being treated as an implicit empty-state fallback.

### 4.3 Operational consequence

The fail-closed policy can leave the cluster without a serving Master when all
candidates observe the same unusable recovery history. Current recovery for an
operator who explicitly accepts cache loss is to stop users of the old
`cluster_id` and start with a previously unused `cluster_id`. Automatic
generation-fenced reset is not part of these PRs.

## 5. Snapshot and OpLog Composition: #3640 and #3642

### 5.1 Publication safety in #3640

PR #3640 adds batch-OpLog snapshot publication primitives:

- a fixed-duration etcd maintenance lease;
- a unique owner token for each maintenance attempt;
- lease and lock-incarnation checks;
- compare-and-swap fencing against the pointer state previously read;
- monotonic comparison by `last_included_batch_id`; and
- atomic rotation of `fallback = old latest` and `latest = candidate`.

These rules prevent a stale snapshot worker from publishing over a newer
worker and prevent readers from observing a half-rotated pointer pair. The PR
does not schedule snapshots, restore them, delete covered OpLog batches, or
clean up unreferenced artifacts.

### 5.2 Bootstrap safety in #3642

PR #3642 defines the recovery order:

1. restore `latest`;
2. if that candidate is invalid, restore `fallback`;
3. if both candidates are invalid, replay only a provably complete OpLog
   history.

Snapshot descriptors and artifacts are validated by identity, size, CRC32C,
cursor, chunk index, object count, tenant identifiers, and replica identifiers.
Restoration and suffix replay occur in temporary standby state; the result is
installed only after the complete operation succeeds.

Invalid candidate content permits the next recovery candidate to be tried.
Infrastructure failures such as backend timeouts do not silently downgrade to
another authority source. Complete-OpLog fallback must begin at batch 1 and
sequence 1 unless a valid snapshot supplies the earlier baseline.

### 5.3 Integration status

The two PRs supply complementary writer-side and reader-side control
primitives. Their merge order does not imply full production activation:
#3642 explicitly leaves production configuration unchanged, and #3640 omits
scheduling and retention. The new path therefore provides tested mechanisms,
not a complete bounded-retention lifecycle by itself.

One accepted tradeoff remains. A namespace in which snapshot pointers, the
durable prefix, and all batches are absent is treated as a new empty cluster so
that first HA startup can proceed. The same visible state could result from
complete loss of recovery authority. Distinguishing those cases requires an
independent provisioning or generation protocol.

## 6. Mutation Durability: #3422 and #3601

### 6.1 `BatchEvict()` OpLog failures in #3422

Before #3422, `BatchEvict()` collapsed OpLog reservation and submission into a
boolean result. This lost the original error classification and could leave
replicas marked `REMOVED` when a commit was synchronously rejected.

The merged behavior:

- returns the exact reservation or submission `ErrorCode`;
- stops the eviction scan after the first OpLog failure;
- retries automatically only for `TASK_PENDING_LIMIT_EXCEEDED`;
- avoids a busy retry for non-transient writer failures;
- rolls replicas from `REMOVED` back to `COMPLETE` when submission is rejected;
  and
- consolidates eviction outcome metrics.

The rollback is intentionally limited to synchronous rejection. If `Commit()`
accepts an entry and the writer later enters a terminal state, durability may
be ambiguous. The PR discussion assigns that case to service-level fail-stop
and supervisor propagation, which remains follow-up work.

### 6.2 Data-before-metadata ordering in #3601

`BucketStorageBackend::WriteBucket()` uses the POSIX write path, but its
effective `fdatasync()` had become unreachable. Metadata could therefore be
committed before the bucket data was stable, allowing a crash to leave a
recorded bucket with incomplete durable content.

PR #3601 invokes `datasync()` after the final data write and before
`StoreBucketMetadata()`. A sync failure removes the newly created bucket file
and prevents metadata publication. This is adjacent to, rather than part of,
the HA supervisor: it enforces the same prerequisite-before-publication rule at
the local storage boundary.

## 7. Latest Master Foundation: #3703

### 7.1 Observed changes

PR #3703 is the first layer of the SegmentPool stack. At upstream head
`6e1dd41`, it adds:

- `RegionKind` and `RegionResourceSpec` as internal physical-resource types;
- stateful Memory and CXL `RegionDriver` implementations;
- stable `PlacementTarget` objects owned by region resources;
- move-only `PreparedRegionResource` staging with explicit `Commit()`;
- centralized creation and validation for CacheLib and offset allocators;
- conversion of canonical replica descriptors to checked live allocations;
- allocator import that preserves descriptor order; and
- preservation of the recovered replica's transfer protocol when its memory
  buffer handle is replaced.

The prepared-resource protocol is transactional within the process. Abandoning
a prepared open or adoption destroys the staged resource; committing activates
the replacement and keeps the previous resource alive until the prepared state
is released. It is not a durable distributed transaction.

### 7.2 Relationship to HA recovery

The current layer does not replace Master placement or RPC call sites with
SegmentPool. Existing `MasterService::ReMountSegment()` does, however, use the
new `BuildRegionLiveAllocations()` boundary before allocator import. Recovery
now validates region identity, endpoint, address range, overflow, and allocation
size before reconstructing allocator ownership.

This makes #3703 relevant to HA even though its principal purpose is the later
SegmentPool refactor: a restored metadata image must reconstruct resource
ownership without overlaps, out-of-range buffers, or loss of the advertised
transfer protocol.

### 7.3 Scope boundaries

This layer does not change RPC schemas, external configuration fields,
snapshot wire formats, or placement behavior. CXL recovery through the new
driver rejects live-allocation import, and adoption is not implemented for the
CXL driver. Later stack layers, beginning with #3704, are required before the
new drivers become the authoritative Master placement path.

## 8. Integrated Correctness Invariants

The reviewed changes jointly establish or reinforce these invariants:

1. **Election is necessary but insufficient for serving.** Recovery and final
   leadership validation must also succeed.
2. **No configured recovery state is a valid state.** It is represented by an
   empty context, not by an error.
3. **Configured recovery failure is fail-closed.** It cannot silently create a
   new empty authority under the same recovery namespace.
4. **Snapshot publication is fenced and atomic.** A stale writer cannot rotate
   `latest` and `fallback` after losing its maintenance authority.
5. **Bootstrap is transactional.** Partially restored metadata is not installed
   into the live standby.
6. **Mutation visibility follows durable prerequisites.** Rejected OpLog
   publication restores local eviction state; bucket metadata follows a
   successful data flush.
7. **Recovered replica metadata must map to valid resource ownership.** Region
   and allocator import reject malformed address and identity relationships.

## 9. Findings and Remaining Work

### 9.1 High-priority findings

1. **Terminal OpLog-writer failure propagation remains incomplete.** A batch
   accepted by `Commit()` can fail asynchronously without the narrow rollback
   available for synchronous rejection. Acceptance requires the service to
   fail-stop and the supervisor to withdraw serving authority on terminal
   writer state.
2. **Cold recovery is operational rather than protocolized.** Shared corrupt
   history can keep all candidates unavailable. A managed reset requires an
   explicit operator decision, a new recovery generation or namespace, and
   fencing of old writers and standbys.
3. **The batch snapshot lifecycle is not fully wired.** Scheduling, production
   selection, safe OpLog compaction, retention floors, and artifact cleanup
   remain outside #3640/#3642.

### 9.2 Medium-priority findings

1. **All-absent recovery authority is ambiguous.** First bootstrap and complete
   authority loss have the same visible key state without an independent
   generation marker.
2. **Region drivers are a foundation, not the completed SegmentPool change.**
   Master call-site migration, placement indexing, and full recovery integration
   must be reviewed across the remaining stack.
3. **The internal integration branch is behind the reviewed public sequence.**
   Importing only #3566 reproduces the HA-only success path but omits #3497's
   fail-closed serving gate and the later recovery/resource changes.

## 10. Recommended Validation Before Internal Adoption

The internal integration should validate the composed behavior rather than
each PR in isolation:

1. Test HA-only startup through the real supervisor:
   `NoopStandbyController -> empty context -> restore gate -> serving`.
2. Inject final catch-up, snapshot restore, allocator import, and leadership
   revalidation failures and verify that no service delegate is exposed.
3. Exercise `latest -> fallback -> complete OpLog` with corrupt candidates,
   transient backend failures, incomplete history, and an all-absent namespace.
4. Race two snapshot publishers across lease loss and pointer rotation.
5. Inject synchronous and asynchronous OpLog-writer failures during eviction;
   verify rollback for rejected submission and service withdrawal for terminal
   post-acceptance failure.
6. Crash between bucket data write, `fdatasync()`, and metadata commit; verify
   that no metadata references unstable or orphaned data.
7. Restore Memory regions with CacheLib and offset allocators, including
   overlapping, out-of-range, endpoint-mismatched, and protocol-bearing
   descriptors.
8. Rebase or merge the required public commits onto a named internal baseline,
   then record the rewritten internal commit mapping in this note.

## 11. Review Conclusion

The recent Master work changes the recovery contract from permissive
best-effort behavior to explicit, fail-closed state transitions. #3566 remains
an essential exception only for intentional absence of recovery capability; it
does not weaken #3497's handling of real failures. #3640/#3642 provide a sound
shape for bounded recovery, while #3422 and #3601 enforce the same publication
ordering at mutation and storage boundaries. #3703 extends that direction into
resource ownership by making allocator reconstruction validated and staged.

The design is internally consistent at the reviewed boundaries. Production
completeness depends on the remaining wiring and failure-propagation work, and
the internal branch should not treat #3566 alone as equivalent to the current
public Master behavior.

## 12. Sources

- [PR #3566: Allow HA promotion without OpLog](https://github.com/kvcache-ai/Mooncake/pull/3566)
- [PR #3497: Gate HA serving on successful standby restore](https://github.com/kvcache-ai/Mooncake/pull/3497)
- [PR #3640: Fenced snapshot publication](https://github.com/kvcache-ai/Mooncake/pull/3640)
- [PR #3642: Batch OpLog snapshot bootstrap](https://github.com/kvcache-ai/Mooncake/pull/3642)
- [PR #3422: Handle BatchEvict OpLog failures](https://github.com/kvcache-ai/Mooncake/pull/3422)
- [PR #3601: Flush data before metadata commit](https://github.com/kvcache-ai/Mooncake/pull/3601)
- [PR #3703: Stateful region resource drivers](https://github.com/kvcache-ai/Mooncake/pull/3703)
- [RFC #3167: Standby-generated snapshots and bounded OpLog retention](https://github.com/kvcache-ai/Mooncake/issues/3167)
- [RFC #3360: Segment management around SegmentPool](https://github.com/kvcache-ai/Mooncake/issues/3360)
