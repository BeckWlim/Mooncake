# Mooncake Store core call chains

> Source snapshot: 2026-09-08
>
> Scope: the C++ Mooncake Store client and Master Service paths in this
> repository.

## Purpose

This directory is a code-oriented map of the main Mooncake Store mechanisms.
It describes the control-plane and data-plane boundary, the three-stage write
protocol built around `PutStart`, data transfer, and `PutEnd` or `PutRevoke`,
and the namespace and accounting structures that support those operations.

The documents are organized as follows:

- [architecture.md](architecture.md) describes the service boundary, metadata
  hierarchy, replica state machine, and lock model.
- [put-call-chain.md](put-call-chain.md) follows a normal `Put` end to end and
  covers batch and upsert variants.
- [tenant-quota-group.md](tenant-quota-group.md) explains tenant application
  scenarios, memory quota accounting, and the relationship between tenants,
  objects, and groups.
- [object-operations.md](object-operations.md) summarizes read, remove,
  copy/move, eviction, and recovery paths.

## Source policy

Current implementation code is the authority for behavior described here.
The published documentation under `docs/source` was consulted as a read-only
description of intended behavior and deployment configuration. Existing
material under `docs/notes` was used as a reading guide and as supporting
context; statements that no longer match the implementation were not carried
forward.

Primary implementation entry points:

- [`Client`](../../../mooncake-store/src/client_service.cpp) orchestrates
  application requests and transfers object bytes.
- [`MasterClient`](../../../mooncake-store/src/master_client.cpp) translates
  client operations into Master RPC calls.
- [`WrappedMasterService`](../../../mooncake-store/src/rpc_service.cpp)
  validates RPC boundaries and resolves tenant identifiers.
- [`MasterService`](../../../mooncake-store/src/master_service.cpp) owns
  metadata, allocation, lifecycle transitions, quota accounting, and
  background maintenance.
- [`MasterService` data structures](../../../mooncake-store/include/master_service.h)
  define the metadata and task state.
- [`TenantQuotaLedger`](../../../mooncake-store/include/tenant_quota_ledger.h)
  records per-object pending, committed, and replacement charges.

Supporting notes:

- [`source-reading-guide.md`](../../notes/guide/source-reading-guide.md)
  identifies the Store `Put` path as a primary source-reading path.
- [`mooncake-store-report.md`](../../notes/docs/mooncake-store-report.md)
  provides the broader component and operation overview.
- [`mooncake-replica-placement-nof-l4.md`](../../notes/docs/mooncake-replica-placement-nof-l4.md)
  provides additional context for replica placement and flexible dual writes.
