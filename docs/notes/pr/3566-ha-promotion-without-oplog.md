# PR #3566 Analysis: HA Promotion Without OpLog

> - Analysis date: 2026-08-31
> - Pull request: [kvcache-ai/Mooncake#3566](https://github.com/kvcache-ai/Mooncake/pull/3566)
> - Merge commit: `c3ba762`
> - Scope: Mooncake Store HA leader promotion when standby recovery is disabled

## 1. Summary

PR #3566 fixes the HA supervisor's expected-success path when both OpLog
following and snapshot restoration are disabled.

Before the change, a process could acquire HA leadership but fail the mandatory
standby-promotion step. The selected `NoopStandbyController` reported
`UNAVAILABLE_IN_CURRENT_STATUS` from `PromoteStandbyAndExport()`, so the
supervisor treated the configuration as a promotion failure and never entered
the serving state.

The fix makes the no-op controller return a successful, empty
`PromotionContext`. An empty context means that there is no standby metadata to
restore. At the time of #3566, that result allowed the supervisor to bypass the
metadata-copy branch and complete the normal leadership transition. Current
`main`, after #3497, passes the empty context through `RestoreFromStandby()` as
an explicit storage-capability check before entering the serving state.

This change fixes HA control flow. It does not add metadata recovery when OpLog
and snapshot restoration are disabled.

## 2. HA, OpLog, and Snapshot Responsibilities

The three mechanisms solve different parts of failover.

| Mechanism | Responsibility | State provided during failover |
|---|---|---|
| HA | Coordinates leader election, role transitions, fencing, and service availability | Determines which Master may serve |
| OpLog | Records ordered metadata mutations and lets a standby follow the active Master | Incremental changes through an applied sequence ID |
| Durable snapshot | Stores a point-in-time image of Master metadata | A recovery baseline associated with an OpLog boundary |

HA is the failover mechanism. OpLog and durable snapshots are recovery
mechanisms. Enabling HA does not by itself preserve Master metadata across a
leader change.

### 2.1 Normal recovery composition

With snapshot restoration and OpLog following enabled, the standby can restore
a baseline and then apply the mutations that occurred after that baseline.

```text
durable snapshot at sequence N
              |
              v
restore standby metadata
              |
              v
replay OpLog entries N+1 ... latest
              |
              v
acquire leadership and perform final catch-up
              |
              v
export PromotionContext
              |
              v
restore the new serving Master
```

The snapshot bounds recovery time by avoiding replay from the beginning of the
log. The OpLog closes the time gap between the snapshot boundary and the latest
accepted metadata mutation.

### 2.2 Snapshot terminology

Two related snapshot concepts appear in the promotion code:

- A **durable Master snapshot** is stored outside the process and is used to
  bootstrap standby metadata. Its restore path is controlled by
  `enable_snapshot_restore`.
- A **standby export snapshot** is the live in-memory state produced by
  `PromoteAndExportSnapshot()` during promotion. The controller converts this
  export into a `PromotionContext` containing the applied OpLog sequence ID,
  objects, and segments.

The standby export may have been built from a durable snapshot plus later
OpLog entries, but it is not itself the durable snapshot file.

### 2.3 Recovery capability combinations

| Configuration | Standby state source | Promotion result |
|---|---|---|
| HA + snapshot restore + OpLog | Snapshot baseline plus incremental log replay | Current standby metadata, subject to successful catch-up |
| HA + OpLog only | Available OpLog history | Metadata reconstructed by log replay |
| HA + snapshot restore only | Latest restorable snapshot | Snapshot state without continuous post-snapshot updates |
| HA only | No standby recovery state | Empty context and a fresh serving Master |

The last row is the configuration fixed by #3566.

## 3. Promotion Control Flow

The HA supervisor follows the same high-level promotion interface regardless of
the configured recovery capabilities:

```text
acquire leadership
        |
        v
PromoteStandbyAndExport()
        |
        +-- error ----------> return to standby / retry
        |
        `-- PromotionContext
                |
                v
        leadership warmup
                |
                v
        construct serving Master
                |
                v
        RestoreFromStandby(context)
                |
                +-- error ----------> release leadership / remain unavailable
                |
                `-- success
                |
                v
        revalidate leadership
                |
                v
             serving
```

The common interface is useful because the supervisor does not need separate
promotion state machines for each recovery configuration. It requires the
standby controller to distinguish these outcomes:

1. Recovery was configured and promotion failed: return an error.
2. Recovery was configured and produced state: return a populated context.
3. Recovery was not configured: return a successful empty context.

Before #3566, the no-op implementation incorrectly represented outcome 3 as
outcome 1. PR #3497 subsequently made restoration and leadership revalidation
mandatory for both populated and valid empty contexts; it did not change the
meaning assigned to the empty context by #3566.

## 4. Failure Before the Fix

The affected configuration was:

```text
enable_ha=true
enable_oplog=false
enable_snapshot_restore=false
```

Because neither standby capability was enabled,
`CreateStandbyController()` selected `NoopStandbyController`. Starting that
controller succeeded because there was no replication service to start.

After leadership acquisition, the supervisor still called
`PromoteStandbyAndExport()` as its promotion gate. The no-op implementation
returned `UNAVAILABLE_IN_CURRENT_STATUS`. The supervisor consequently:

1. rejected promotion;
2. returned the process to standby mode; and
3. withheld the serving RPC service.

The failure was therefore not a leader-election failure and not a metadata
restore failure. It was an invalid return value at the boundary between the
no-op standby controller and the common HA supervisor.

## 5. Implemented Fix

The production behavior change is the no-op implementation of
`PromoteStandbyAndExport()`:

```cpp
tl::expected<PromotionContext, ErrorCode> PromoteStandbyAndExport()
    override {
    return PromotionContext{};
}
```

The default-constructed context has no applied OpLog sequence and contains no
object or segment metadata. In the #3566 merge state, the supervisor checked
those fields and skipped the metadata-copy operation for this context. In
current `main`, #3497 makes the supervisor invoke `RestoreFromStandby()` even
for an empty context. The empty restore performs no metadata reconstruction,
but it verifies that the configured storage mode supports the promotion path.

The pull request also adds a regression test for the HA-without-OpLog path. The
reported validation covered the focused regression test, the complete
`hot_standby_snapshot_bootstrap_test` suite, manual startup against local etcd,
and scoped pre-commit hooks.

## 6. Operational Implications

### Observed behavior

- Leader election remains active.
- An elected process can enter `serving` without standby recovery capability.
- The serving Master starts without objects, segments, or an applied OpLog
  position from the previous leader.
- Both populated and valid empty promotion contexts pass the restoration gate
  on current `main`.
- Actual promotion or restoration errors prevent serving after #3497.

### Derived conclusion

HA-only mode now provides control-plane availability, but not recovered Master
metadata continuity. Any reconstruction from client activity or another
external mechanism is outside #3566. Deployments that require metadata
continuity across failover must configure and validate the applicable OpLog and
snapshot recovery paths.

## 7. Scope Boundaries

PR #3566 does not:

- enable or write OpLog entries;
- create or restore durable snapshots;
- change leader election or fencing;
- define the later unconditional restore gate for `PromotionContext`;
- convert genuine promotion failures into success; or
- address failure after a recovery-capable standby has attempted promotion.

The last case is addressed separately by
[PR #3497](https://github.com/kvcache-ai/Mooncake/pull/3497), which gates HA
serving on successful standby promotion and restoration. PR #3566 instead
defines the expected-success semantics for a controller with no recovery work.

## 8. Source References

- [PR #3566: Allow HA promotion without OpLog](https://github.com/kvcache-ai/Mooncake/pull/3566)
- [`standby_controller.cpp` on `main`](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/src/ha/standby_controller.cpp)
- [`master_service_supervisor.cpp` on `main`](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/src/ha/leadership/master_service_supervisor.cpp)
- [Issue #2971](https://github.com/kvcache-ai/Mooncake/issues/2971)
- [PR #3497](https://github.com/kvcache-ai/Mooncake/pull/3497)
