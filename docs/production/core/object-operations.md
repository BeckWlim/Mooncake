# Related object call chains

## Read path

```text
Client read orchestration
  -> MasterClient::GetReplicaList
  -> WrappedMasterService::GetReplicaList
  -> MasterService::GetReplicaList
       -> lookup by (tenant_id, key)
       -> select eligible COMPLETE replicas
       -> grant read lease
  <- descriptors, lease TTL, checksum
  -> client selects a descriptor
  -> direct Transfer Engine or storage-backend read
```

The Master returns metadata and renews the authoritative lease; it does not
carry the payload. A grouped object's `lease_` points to the group's shared
lease, so the ordinary object lookup refreshes group recency without a group
table lookup on the read path.

`BatchGetReplicaList` groups keys by metadata shard to reduce repeated lock
acquisition. Results remain aligned with individual keys.

Source: [`MasterService::GetReplicaList`](../../../mooncake-store/src/master_service.cpp#L3742).

## Remove path

```text
Client::Remove
  -> MasterClient::Remove
  -> WrappedMasterService::Remove
  -> MasterService::Remove
       -> resolve tenant and shard
       -> validate lease unless force=true
       -> reject incomplete/busy task state
       -> remove metadata and release accounting
          or mark REMOVED and finalize after HA durability
```

Remove is object-scoped even when the object belongs to a group. It unregisters
the removed key from group membership. Without `force`, an active lease blocks
removal; forcing bypasses the lease condition but does not turn incomplete or
unsafe replica/task states into valid removal candidates.

In the non-HA path, metadata erasure releases allocations and quota through
normal ownership cleanup. In the HA operation-log path, completed replicas can
be marked `REMOVED`, logged, and physically finalized after the removal record
becomes durable.

Source: [`MasterService::Remove`](../../../mooncake-store/src/master_service.cpp#L6464).

## Copy and move paths

Copy and move are also staged protocols:

```text
CopyStart / MoveStart
  -> validate source and target
  -> protect source replica with task/refcount state
  -> charge pending tenant MEMORY quota when required
  -> allocate PROCESSING target replica(s)
  <- source and target descriptors

client transfers bytes directly

CopyEnd / MoveEnd
  -> validate task ownership/state
  -> mark target COMPLETE
  -> settle quota and release task protection
  -> for Move, retire the selected source

CopyRevoke / MoveRevoke
  -> discard staged target
  -> refund pending quota
  -> release source task protection
```

The task records prevent source eviction while a transfer is active. These
operations act on one object and do not copy or move every member of a group.

Sources:

- [`MasterService::CopyStart`](../../../mooncake-store/src/master_service.cpp#L5658)
- [`MasterService::CopyEnd`](../../../mooncake-store/src/master_service.cpp#L5849)
- [`MasterService::CopyRevoke`](../../../mooncake-store/src/master_service.cpp#L6021)
- [`MasterService::MoveStart`](../../../mooncake-store/src/master_service.cpp#L6094)
- [`MasterService::MoveEnd`](../../../mooncake-store/src/master_service.cpp#L6220)
- [`MasterService::MoveRevoke`](../../../mooncake-store/src/master_service.cpp#L6405)

## Memory eviction

Memory eviction is triggered by the background watermark loop, an allocation
failure signal, or tenant quota pressure. Its general path is:

```text
trigger
  -> candidate census across metadata shards
  -> lease and pin ordering
  -> re-lookup and revalidation under target shard lock
  -> remove eligible MEMORY replicas
  -> release object quota charge for removed MEMORY replicas
  -> erase object if no valid replica remains
```

Global eviction searches the evictable population. Quota-driven eviction is
restricted to one tenant and a target byte deficit. A grouped candidate causes
the Master to inspect the group's current members, but each member is
revalidated and can be skipped independently.

The detailed production analysis of the global path is in
[`../eviction/architecture.md`](../eviction/architecture.md).

## Offload and promotion

When local SSD offload is enabled, a completed MEMORY replica can enqueue a
client-owned LOCAL_DISK mirror. The Master records an `OffloadingTask` and
protects the source while the client writes the mirror. Promotion reverses the
direction: it allocates a PROCESSING MEMORY target, charges pending tenant
memory quota, and authorizes the source owner to complete or abort the copy.

These paths follow the same architectural rule as `Put`: the Master owns
metadata and admission state, while a client performs the byte transfer.

## Snapshot, restore, and HA

Snapshot and operation-log recovery reconstruct object metadata and quota
usage. Group state is derived from restored `ObjectMetadata`; it is rebuilt as
a secondary index rather than restored as an independent object namespace.
Restored objects are routed by `(tenant_id, user_key)` even when they have a
group identifier.

Operation-log paths distinguish visible metadata transitions from durable
physical cleanup. For example, `PutEnd` can append a visible-before-durable
`PUT_END` record, while removal retains resources until its durable finalize
callback can safely release them. The detailed HA topology and recovery notes
are in [`../ha/community-track.md`](../ha/community-track.md).
