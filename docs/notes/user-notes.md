# Mooncake

## SGLang HiCache L1-L4: Mooncake-side Beginner Tutorial

This section uses the same cache-level terminology as the SGLang notes:

```text
SGLang process                         Mooncake

L1  GPU KV pool
 ^
 |  SGLang H2D/D2H load and backup
 v
L2  Host KV pool  <---- Store API ----> object key -> replica metadata
                                              |-- MEMORY replica      (L3)
                                              `-- LOCAL_DISK replica  (L4)
```

The L1-L4 names are deployment-level KV-cache terminology.
Mooncake Store does not define an L1, L2, L3, or L4 API.
It manages byte objects and their physical replicas.
SGLang supplies the KV-page object keys and owns the L1/L2 page allocators;
Mooncake selects a readable external replica and moves its bytes into the destination supplied by SGLang.

### 1. Start at the SGLang-Mooncake boundary

For a cache restore, SGLang first reserves L2 Host pages.
The Mooncake adapter converts `host_indices` into final Host pointers and calls one of:

```python
store.batch_get_into(keys, host_pointers, sizes)
store.batch_get_into_multi_buffers(keys, host_pointers, sizes)
```

The relevant adapter code is:

- [`mooncake_store.py:1030`](</home/xffan2/code/sglang/python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py:1030>) -- `batch_get_v1()` resolves the Host page metadata.
- [`mooncake_store.py:1306`](</home/xffan2/code/sglang/python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py:1306>) -- selects the single- or multi-buffer Store call.

This interface has an important consequence:

```text
Mooncake read destination = SGLang L2 Host page
```

The current interface does not pass SGLang L1 `device_indices` or final GPU pointers to Mooncake.
A successful Mooncake read is therefore followed by SGLang's ordinary L2-to-L1 load-back.

The backup direction starts from the same boundary.
SGLang passes the L2 Host pointers to `batch_put_from()` or `batch_put_from_multi_buffers()`;
see [`mooncake_store.py:1059`](</home/xffan2/code/sglang/python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py:1059>) and [`mooncake_store.py:1277`](</home/xffan2/code/sglang/python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py:1277>).

### 2. Mooncake's replica model

The Master maps each exact object key to object metadata containing one or more
replicas. The relevant descriptor variants are:

```cpp
MemoryDescriptor
NoFDescriptor
DiskDescriptor
LocalDiskDescriptor
```

They are defined in
[`replica.h:167`](../../../mooncake-store/include/replica.h#L167) and stored in
`Replica::Descriptor` at
[`replica.h:471`](../../../mooncake-store/include/replica.h#L471).

Only `COMPLETE` replicas are returned to a normal reader. The query also grants
a lease so that the object is not removed while the client is reading it. The
single-key control-plane path is
[`MasterService::GetReplicaList()`](../../../mooncake-store/src/master_service.cpp#L2108),
and the batched SGLang path uses
[`MasterService::BatchGetReplicaList()`](../../../mooncake-store/src/master_service.cpp#L2213).

Mooncake then selects one descriptor in
[`SelectBestReplica()`](../../../mooncake-store/include/replica_selection.h#L122).
For the ordinary SGLang DRAM/SSD case, the essential rule is:

```text
at least one usable MEMORY replica
    -> choose MEMORY                     # logical L3

otherwise a usable LOCAL_DISK replica
    -> choose LOCAL_DISK                 # logical L4
```

The complete policy also considers locality, NoF SSD, an optional remote-memory
scorer, and `DISK`. Any usable MEMORY replica is preferred over
`LOCAL_DISK`. Selection happens before data movement; the Transfer Engine does
not inspect object keys or choose between the L3 and L4 media.

The current `batch_get_into_internal()` path does not retry another replica
type after the selected transfer fails. A failure is returned to SGLang, which
can fall back according to its own cache/recomputation policy.

### 3. L3 MEMORY read path

The main read dispatch begins in
[`RealClient::batch_get_into_internal()`](../../../mooncake-store/src/real_client.cpp#L4574):

```text
SGLang object key and L2 pointer
    -> RealClient::batch_get_into_internal()
    -> Client::BatchQuery()
    -> Master::BatchGetReplicaList()
    -> SelectBestReplica() returns MEMORY
    -> allocateSlices() describes the final SGLang L2 destination
    -> Client::BatchGet()
    -> TransferSubmitter::submit_batch()
    -> TransferEngine::submitTransfer()
    -> RDMA/TCP/local-copy completion
    -> bytes are ready in the reserved SGLang L2 page
```

`TransferSubmitter::submit_batch()` converts the selected replica and local
slices into Transfer Engine requests:

```cpp
request.opcode = TransferRequest::READ;
request.source = static_cast<char*>(slice.ptr);
request.target_id = remote_segment;
request.target_offset = remote_object_address + offset;
request.length = slice.size;
```

See
[`transfer_task.cpp:1038`](../../../mooncake-store/src/transfer_task.cpp#L1038).
In the classic Transfer Engine API, `source` means the local application
pointer. For a `READ`, it is the local destination; `target_offset` identifies
the remote source.

`MultiTransport::selectTransport()` reads the remote segment's protocol and
routes the request to the installed transport:

```text
remote SegmentDesc.protocol = rdma -> RdmaTransport
remote SegmentDesc.protocol = tcp  -> TcpTransport
multi-protocol segment             -> address/locality-based protocol choice
```

See
[`multi_transport.cpp:452`](../../../mooncake-transfer-engine/src/multi_transport.cpp#L452).

For RDMA, the request is split into transfer slices and converted into verbs
work requests. The final submission is conceptually:

```cpp
wr.opcode = IBV_WR_RDMA_READ;
wr.wr.rdma.remote_addr = slice->rdma.dest_addr;
wr.wr.rdma.rkey = slice->rdma.dest_rkey;
ibv_post_send(qp, &wr, &bad_wr);
```

The slice construction is in
[`rdma_transport.cpp:571`](../../../mooncake-transfer-engine/src/transport/rdma_transport/rdma_transport.cpp#L571),
and the verbs submission is in
[`rdma_endpoint.cpp:915`](../../../mooncake-transfer-engine/src/transport/rdma_transport/rdma_endpoint.cpp#L915).

The RNIC performs the payload transfer. The Master is only on the metadata
path; it does not proxy KV bytes.

### 4. L4 LOCAL_DISK read path

`LOCAL_DISK` represents an object stored by a particular Mooncake holder
client. Its descriptor contains the holder client ID, object size, and the
holder's RPC endpoint. The current path uses holder-side Host staging:

```text
requester SGLang/Mooncake client
    -> RPC: holder RealClient::batch_get_offload_object()
         -> FileStorage::AllocateBatch()
         -> SSD read into an aligned holder ClientBuffer
         -> return holder buffer addresses and TE endpoint
    -> Transfer Engine READ
         holder ClientBuffer -> requester SGLang L2 Host page
    -> requester releases the holder's temporary buffer
```

The requester groups `LOCAL_DISK` objects by holder endpoint in
[`real_client.cpp:4825`](../../../mooncake-store/src/real_client.cpp#L4825).
The complete requester orchestration is
[`batch_get_into_offload_object_internal()`](../../../mooncake-store/src/real_client.cpp#L5719).

On the holder, the RPC handler calls `FileStorage::BatchGet()`:

- [`real_client.cpp:5679`](../../../mooncake-store/src/real_client.cpp#L5679) -- holder RPC handler.
- [`file_storage.cpp:402`](../../../mooncake-store/src/file_storage.cpp#L402) -- allocate, load, and lease the temporary batch.
- [`file_storage.cpp:1102`](../../../mooncake-store/src/file_storage.cpp#L1102) -- aligned staging-buffer allocation.

The physical path is consequently:

```text
holder SSD
    -> holder aligned Host ClientBuffer
    -> RDMA or TCP
    -> requester SGLang L2 Host page
    -> SGLang layer-wise H2D
    -> requester L1 GPU page
```

The temporary holder buffer is operation-scoped staging. It is not a new
published `MEMORY` replica. It is released after the requester transfer, with a
lease/GC timeout as a recovery mechanism.

`DISK` is a separate descriptor type. Its generic file path currently reads
into a CPU temporary buffer and then scatters to the caller when necessary; see
[`real_client.cpp:4747`](../../../mooncake-store/src/real_client.cpp#L4747).
SGLang SSD offload normally concerns `LOCAL_DISK`.

### 5. How replicas move between L3 and L4

Replica movement is a background lifecycle operation, not the read-path
branch itself.

Depending on configuration, Mooncake can queue disk offload after Put or when
a MEMORY replica becomes an eviction candidate. The eviction-time form is:

```text
Master eviction policy selects a MEMORY replica
    -> pin the source replica with refcnt
    -> PushOffloadingQueue()
    -> holder receives OffloadTaskItem through heartbeat
    -> holder resolves the local MEMORY slices
    -> FileStorage writes them to its SSD backend
    -> NotifyOffloadSuccess()
    -> Master adds a COMPLETE LOCAL_DISK replica
    -> source pin is released
    -> MEMORY can be evicted safely
```

The Master-side offload-on-evict decision is in
[`master_service.cpp:6400`](../../../mooncake-store/src/master_service.cpp#L6400).
The holder receives tasks through
[`OffloadObjectHeartbeat()`](../../../mooncake-store/src/master_service.cpp#L4562)
and persists them in
[`FileStorage::OffloadObjects()`](../../../mooncake-store/src/file_storage.cpp#L443).
The Master publishes the disk replica in
[`NotifyOffloadSuccess()`](../../../mooncake-store/src/master_service.cpp#L4680).

The object key remains unchanged. The transition modifies the available
replicas behind that key:

```text
before: key -> MEMORY
during: key -> MEMORY + LOCAL_DISK
after:  key -> LOCAL_DISK               # if MEMORY is later evicted
```

With promotion-on-hit enabled, a read of a `LOCAL_DISK`-only object can queue
an asynchronous promotion:

```text
Get observes LOCAL_DISK without MEMORY
    -> Count-Min-Sketch/admission policy queues promotion
    -> Master allocates a PROCESSING MEMORY replica
    -> holder reads SSD into an aligned staging buffer
    -> Transfer Engine writes staging bytes to the new MEMORY replica
    -> NotifyPromotionSuccess()
    -> Master marks the MEMORY replica COMPLETE
```

The worker path is
[`FileStorage::ProcessPromotionTasks()`](../../../mooncake-store/src/file_storage.cpp#L884),
and the Master allocation/commit path starts at
[`PromotionAllocStart()`](../../../mooncake-store/src/master_service.cpp#L5380).

Promotion normally benefits later requests. The request that caused promotion
still uses the selected `LOCAL_DISK` read path.

### 6. Related low-level and kernel code

There is no Mooncake CUDA compute kernel in the normal SGLang L3/L4 restore
path. Three different meanings of "kernel" should remain separate:

| Operation | Owner and low-level mechanism |
|---|---|
| L3 MEMORY -> SGLang L2 | Mooncake TE; RDMA verbs/RNIC DMA or TCP |
| L4 SSD -> holder staging buffer | Mooncake Store; Linux file I/O, optionally `io_uring` + `O_DIRECT` |
| SGLang L2 -> L1 | SGLang/PyTorch CUDA copy and CUDA events |
| Attention reads L1 KV | SGLang attention backend kernels |

For an `io_uring`-enabled bucket backend, Mooncake aligns the SSD read range and
reads into the holder staging allocation. The backend path is
[`storage_backend.cpp:1844`](../../../mooncake-store/src/storage_backend.cpp#L1844).
The ring implementation prepares read SQEs and collects CQEs in:

- [`uring_file.cpp:284`](../../../mooncake-store/src/uring_file.cpp#L284) -- chunked SQE preparation.
- [`uring_file.cpp:258`](../../../mooncake-store/src/uring_file.cpp#L258) -- submission and completion collection.
- [`uring_file.cpp:584`](../../../mooncake-store/src/uring_file.cpp#L584) -- aligned read entry point.

With `O_DIRECT`, address, length, and file offset must satisfy the alignment
requirements. Registered fixed buffers use `io_uring` buffer registration;
failure falls back to ordinary non-fixed-buffer `io_uring`. The registration
code begins at
[`uring_file.cpp:690`](../../../mooncake-store/src/uring_file.cpp#L690).

Classic TE and TENT also contain GPUDirect RDMA and GDS capabilities. Those
capabilities do not make the current SGLang storage read direct-to-L1 because
the current adapter passes Host pointers. Direct L3/L4-to-L1 would additionally
require SGLang to reserve GPU pages, expose layout-correct GPU destinations,
connect completion to CUDA stream visibility, publish `device_indices`, and
roll back partial or cancelled transfers.

### 7. Main Python-to-Mooncake read call

The remaining discussion assumes ordinary MHA/GQA with a page-first Host pool.
MLA, split-head, layer-first, and multi-buffer paths are deferred.

For each logical page, `_batch_preprocess()` expands the page hash into one K
object and one V object and resolves their contiguous L2 Host addresses:

```text
logical page hashes + host_indices
    -> [page_0_rank_k, page_0_rank_v, ...]
    -> [k_page_0_ptr, v_page_0_ptr, ...]
    -> [k_page_0_size, v_page_0_size, ...]
```

`_get_batch_zero_copy_impl()` therefore selects the flat-buffer call:

```python
self.store.batch_get_into(key_strs, buffer_ptrs, buffer_sizes)
```

Here, `self.store` is the native
`mooncake.store.MooncakeDistributedStore` instance created in
[`mooncake_store.py:380`](</home/xffan2/code/sglang/python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py:380>).
Its main path is:

```text
SGLang MooncakeStore.self.store
    -> MooncakeDistributedStore
    -> pybind11 MooncakeStorePyWrapper
    -> PyClient virtual interface
    -> RealClient::batch_get_into()
    -> RealClient::batch_get_into_internal()
    -> mooncake::Client
         +-- BatchQuery() / Master replica lookup
         `-- BatchGet() / Transfer Engine read
    -> supplied SGLang L2 Host pages
```

The pybind wrapper converts the Python integer addresses to `void*`, releases
the GIL, and invokes `PyClient::batch_get_into()`; see
[`store_py.cpp:2743`](../../../mooncake-integration/store/store_py.cpp#L2743).
Normal-mode dispatch reaches
[`RealClient::batch_get_into()`](../../../mooncake-store/src/real_client.cpp#L4328)
and its internal read path at
[`real_client.cpp:4574`](../../../mooncake-store/src/real_client.cpp#L4574).

With `standalone_storage=True`, the wrapper holds a `DummyClient` and forwards
the same keys, addresses, and sizes to a separate `RealClient`. This changes
process placement, not the object lookup or transfer semantics.

### 8. Recommended initial reading order

1. [`replica_selection.h`](../../../mooncake-store/include/replica_selection.h#L122) -- understand the L3/L4 choice.
2. [`real_client.cpp`](../../../mooncake-store/src/real_client.cpp#L4574) -- follow the MEMORY/LOCAL_DISK dispatch.
3. [`transfer_task.cpp`](../../../mooncake-store/src/transfer_task.cpp#L1038) -- see how MEMORY reads become TE requests.
4. [`real_client.cpp`](../../../mooncake-store/src/real_client.cpp#L5719) -- follow the remote SSD holder-staging path.
5. [`rdma_endpoint.cpp`](../../../mooncake-transfer-engine/src/transport/rdma_transport/rdma_endpoint.cpp#L915) -- inspect the RDMA work request.
6. [`file_storage.cpp`](../../../mooncake-store/src/file_storage.cpp#L443) -- inspect background offload and promotion.

The central engineering distinction is:

```text
SGLang chooses local KV pages and when a prefix is usable.
Mooncake chooses an external replica and moves object bytes.
Transfer Engine chooses or follows the transport for the selected replica.
Linux/CUDA completion mechanisms establish when those bytes are visible.
```
