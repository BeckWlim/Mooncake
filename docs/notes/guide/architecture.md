# Mooncake Architecture

> Synthesized from the FAST'25 paper *"Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving"* and codebase.
> <span style="color:#9e9e9e">For LLM context loading · file paths link to actual code</span>

---

## Table of Contents

1. [Prefill vs. Decode](#1-prefill-vs-decode) — [KV Cache Storage: DRAM vs VRAM](#kv-cache-storage-why-prefill--dram-but-decode--vram)
2. [Metrics: TTFT, TBT & Goodput](#2-metrics-ttft-tbt--goodput)
3. [Batching & MFU](#3-batching--mfu)
4. [KV Cache](#4-kv-cache) — [Mechanics](#41-mechanics) · [Prefix Caching](#42-prefix-caching) · [Layer-wise Load/Store](#43-layer-wise-load-and-store) · [HiCache](#44-hicache-hierarchical-kv-cache)
5. [Transfer Engine & RDMA](#5-transfer-engine--rdma)
6. [Inter-node Data Flow](#6-inter-node-data-flow)
7. [PD Disaggregation + Cache Pooling](#7-pd-disaggregation--cache-pooling)
8. [Admission Control & Load Prediction](#8-admission-control--load-prediction)
9. [Intra-node Parallelism](#9-intra-node-parallelism-tpppspep)

---

## 1. Prefill vs. Decode

LLM inference has two phases, both executed by the **same decoder-only transformer** with <span style="color:#e63946">**shared weights**</span>.
[`mooncake-wheel/mooncake/mooncake_connector_v1.py`](../../../mooncake-wheel/mooncake/mooncake_connector_v1.py): `register_kv_caches()` registers the same KV cache tensors used by both roles.

| | Prefill | Decode |
|---|---|---|
| **Bottleneck** | <span style="color:#e63946">GPU Compute</span> (FLOPS) | <span style="color:#f4a261">GPU Memory Bandwidth</span> |
| **Parallelism** | All prompt tokens at once | 1 token/request/step |
| **Latency driver** | Prompt length | Output length |

```text
                    +-----------------------------+   +-------------------------+
                    |           PREFILL           |   |    DECODE (N steps)     |
Prompt ------------>| Token 1 ... Token N         |-->| Token N+1, N+2, ...     |--> Output
"Hello, how are"    | all layers, prompt-parallel |   | all layers, sequential  |    tokens
                    +-----------------------------+   +-------------------------+
                       compute-bound                    memory-bound
```

> **KEY INSIGHT:** Prefill **is** a forward pass through the decoder transformer in causal mode. The distinction is <span style="color:#2a9d8f">operational, not architectural</span>. Weights are shared (W_Q, W_K, W_V, W_O, FFN, LM Head). In PD disaggregation, both prefill and decode nodes hold a full model copy — disaggregation saves via hardware specialization and KV cache pooling, not weight memory reduction.

### Prefill ≠ Encoder

| | Encoder | Prefill |
|---|---|---|
| **Category** | <span style="color:#457b9d">Architectural component</span> | <span style="color:#2a9d8f">Operational phase</span> |
| **Attention** | Bidirectional | Causal (masked, left→right) |
| **Used in** | BERT, T5, multimodal VLMs | GPT-style decoder-only models |

SGLang's EPD disaggregation decouples true encoders (ViT) from decoders, streaming encoder outputs through Transfer Engine — analogous to KV cache flow in text-only PD. See [`docs/source/deployment/integrations/sglang/pd-disaggregation.md`](../../../docs/source/deployment/integrations/sglang/pd-disaggregation.md).

### KV Cache Storage: Why Prefill < DRAM but Decode < VRAM

The two phases have **opposite KV cache storage requirements**, which is the core motivation for disaggregation.

| | Prefill | Decode |
|---|---|---|
| **Bottleneck** | <span style="color:#e63946">GPU Compute</span> (FLOPS) | <span style="color:#f4a261">GPU Memory Bandwidth</span> |
| **KV Cache role** | **Producer** — write-once output | **Consumer** — read-every-step |
| **KV access per step** | Write all tokens' K,V once | Read **entire** KV cache (growing) |
| **Storage requirement** | KV cache ≤ <span style="color:#457b9d">Store DRAM pool</span> | KV cache ≤ <span style="color:#e63946">single GPU VRAM</span> |

#### Prefill: Compute is the bottleneck, KV writes are cheap

```text
Prefill GPU (compute-bound)
+------------------------------------------+
| Token 1, Token 2, ... Token N            |  all tokens computed in parallel
|                  |                       |
|                  v                       |
| KV 1, KV 2, ... KV N                     |
+------------------+-----------------------+
                   | batched RDMA write
                   v
+------------------------------------------+
| Mooncake Store: shared CPU DRAM pool     |
+------------------------------------------+
```

- Prefill 的任务是**一次性算出所有 prompt token 的 K,V**，然后通过 RDMA 写到 Store。
- KV Cache 对 Prefill GPU 来说是**产出物**——写出去就完成了，后续不需要再访问。
- Prefill 的瓶颈是**计算**（矩阵乘法），不是访存。即使几百个 token 的 KV 写入 GPU 显存也非常快。
- 因此 KV Cache 只要 ≤ Store DRAM 池总容量即可（集群级 TB，远大于单卡 80GB VRAM）。

#### Decode: Memory bandwidth is the bottleneck, every byte of KV matters

```text
Decode GPU (memory-bound)
Step 1: Token 5 -> Attention(K_all, V_all, q_5) -> Token 6
Step 2: Token 6 -> Attention(K_all, V_all, q_6) -> Token 7
...
Step N: Token n -> Attention(K_all, V_all, q_n) -> EOS

+--------------------------------------------------------+
| One token of compute per step, but all historical K/V  |
| must be read. Memory bandwidth dominates.              |
+--------------------------------------------------------+
```

- Decode 生成每个新 token 时，必须用**所有历史 token 的 K,V** 做 self-attention。
- 每一步的 FLOPs 极少（只有 1 个新 token），但 KV Cache 读取量巨大且随生成长度**线性增长**。
- 如果 KV Cache 不在 GPU VRAM 中，每步生成都要通过 PCIe 从 CPU DRAM 读取 → **延迟灾难**（TBT 不可接受）。
- PCIe x16 带宽 ~64 GB/s，GPU HBM 带宽 ~2-3 TB/s，差距约 **40 倍**。
- 因此 KV Cache 必须 ≤ **单卡 VRAM**（~80GB HBM），保证 decode 时全在 GPU 本地。

#### This is WHY Disaggregation Exists

```
                     Prefill 集群                       Decode 集群
                  (Compute-optimized)              (Memory-optimized)
                         │                                  │
                    产 KVCache                          存 KVCache
                    写完即弃                            每步全读
                         │                                  │
                         └─────── Mooncake Store ───────────┘
                                  (CPU DRAM 池)
                                充当中间缓冲层
```

<span style="color:#e63946">**核心矛盾**</span>：Prefill 只把 KV Cache 当"废纸"写完就不管了，Decode 却需要每步反复查阅整本"字典"。两者对存储位置的矛盾需求（一个可以放远端，一个必须在本地）正是 PD Disaggregation + 共享 KV Cache Pool 存在的根本原因。

> **对比**：如果没有 Mooncake Store 作为中间缓冲，Prefill 产出的 KV Cache 必须直接塞进目标 Decode GPU 的 VRAM → 引入严格的反压和调度耦合。有了 Store，Prefill 和 Decode 完全解耦——Prefill 只管"生产后写入 Store"，Decode 在需要时"从 Store 预取"。

---

## 2. Metrics: TTFT, TBT & Goodput

Two latency metrics define SLOs:

| | <span style="color:#e63946">TTFT</span> (Time to First Token) | <span style="color:#f4a261">TBT</span> (Time Between Tokens) |
|---|---|---|
| **Dominated by** | Prefill + KV transfer + first decode step | Per-step decode latency |
| **Bottleneck** | GPU compute | GPU memory bandwidth |
| **Scales with** | Prompt length | ~constant per step |

```text
Request arrival ------------------------------------------------------------>
        |<----------- TTFT ----------->|
        | prefill | KV transfer | first decode
        v         v             v
   +---------+    +---------------------------------------------------------+
   | Prefill |--->| Decode: token 1 | token 2 | token 3 | ... | EOS         |
   +---------+    +---------------------------------------------------------+
                                  |<-- TBT -->|<-- TBT -->|
```

<span style="color:#2a9d8f">**Disaggregation strategy:**</span> each SLO maps to an independently-scalable cluster:
- Scale prefill → optimize <span style="color:#e63946">TTFT</span>
- Scale decode + offload KV cache to Store → optimize <span style="color:#f4a261">TBT</span> under concurrency

### Goodput: Throughput Under SLO Constraints

```
                     Goodput = requests completed WITHIN SLO / time
```

```
                        ▲
                        │           ╭─────── Peak Goodput ╮
    Goodput             │          ╱                      ╲
    (req/s in SLO)      │    ╱────╱                        ╲────╲
                        │  ╱                                      ╲
                        │╱                                           ╲
                        └────────────────────────────────────────────▶  Load (req/s)
                                          ↑
                                     Saturation point:
                               beyond here, queuing → SLO violations → goodput DROPS
```

> <span style="color:#e63946">**Critical insight:**</span> Requests violating either SLO burn GPU compute for nothing. Beyond the saturation point, adding load **reduces** goodput (queuing → SLO violations). Mooncake shifts this curve upward via:

1. <span style="color:#2a9d8f">**Prefill-decode isolation**</span> → decode TBT unaffected by prefill bursts
2. <span style="color:#2a9d8f">**KV cache offloading**</span> → larger decode batches at higher concurrency
3. <span style="color:#2a9d8f">**Admission control**</span> → early reject before wasted prefill ([see §8](#8-admission-control--load-prediction))
4. <span style="color:#2a9d8f">**Prefix caching**</span> → Store hits skip redundant prefill → lower TTFT

The paper's 75% figure is a **goodput** improvement.

---

## 3. Batching & MFU

<span style="color:#e63946">**MFU (Model FLOPs Utilization):**</span> weights loaded once per batch → more tokens = more FLOPs per byte loaded = higher MFU.

```text
Batch with 1 token:     load W -> 1 unit of compute      low MFU
Batch with 1000 tokens: load W -> 1000 units of compute high MFU

+-------------------------------------------------------+
| Load W once per batch                                 |
| [ T1 ][ T2 ][ T3 ][ ... ][ Tn ] x W                   |
| Compute grows with n while the weight load is shared. |
+-------------------------------------------------------+
```

- **Prefill** has high MFU naturally: long prompts → many tokens per weight load.
- **Decode** struggles: 1 token/request/step, making each weight load amortize poorly.
- <span style="color:#2a9d8f">**Mooncake's solution:**</span> offload KV caches to a distributed pool → free VRAM → more concurrent requests → larger batches → higher decode MFU.

> **KEY:** Decode batching works because the auto-regressive constraint is **within** a single request, not between requests. Multiple users each contribute 1 token to the same batch with independent KV caches.

---

## 4. KV Cache

### 4.1 Mechanics

KV cache is a <span style="color:#e63946">**deterministic function**</span> of the input token sequence.

```text
Layer L, one decode step
+----------------------------------------------------------------+
| x_t -> W_Q/W_K/W_V -> q_t, k_t, v_t                            |
|                      |                                         |
|                      +-> append k_t and v_t to the KV cache    |
|                                                                |
| attention = softmax(q_t @ K_cache^T / sqrt(d)) @ V_cache       |
+----------------------------------------------------------------+
```

K and V are produced *during* the forward pass that generates the probability distribution, serving both current attention and all future steps.
[`mooncake_connector_v1.py`](../../../mooncake-wheel/mooncake/mooncake_connector_v1.py): `register_kv_caches()` registers per-layer GPU tensors for RDMA access.

### 4.2 Prefix Caching

```
    Request A:  [System prompt: 1024 tokens] [User: "What is AI?"]
    Request B:  [System prompt: 1024 tokens] [User: "Explain RDMA"]
                └────────  SAME ──────────┘   └──── DIFFERENT ────┘
                         ▲
                Prefix cache HIT: skip 1024 tokens of prefill for Request B
```

<span style="color:#457b9d">**Block IDs**</span> are hashes of 512-token chunks, created **before prefill** at the scheduler level from cheap token-ID metadata.
Trace format ([`FAST25-release/traces/`](../../../FAST25-release/traces/)): `hash_ids` — remapped block hashes where identical values indicate reusable prefix KV cache blocks.

Two block concepts coexist:

| | Prefix-cache block | vLLM KV page |
|---|---|---|
| **Unit** | 512 tokens | 16 tokens |
| **Role** | Hash granularity | Physical allocation |
| **Set at** | `vllm_config.cache_config.block_size` in the connector | Same |

- <span style="color:#f4a261">**Prefix sharing constraint:**</span> blocks bound to exact (position, token_content) pairs — not freely composable.
- <span style="color:#f4a261">**Block-boundary alignment problem:**</span> fixed-block hashing requires shared content at block-aligned positions. System prompts at position 0 guarantee alignment; random mid-document prefixes do not.
- <span style="color:#2a9d8f">**Mitigation:**</span> SGLang's RadixAttention (HiRadixTree in [`docs/source/design/hicache-design.md`](../../../docs/source/design/hicache-design.md)) uses token-level trie matching to avoid block-boundary limitations.

### 4.3 Layer-wise Load and Store

<span style="color:#e63946">**Core idea:**</span> overlap compute with data movement — while layer N computes, layer N+1's KV cache transfers via RDMA.

```
    Time ──────────────────────────────────────────────────────▶

    GPU Compute:    [Layer 0] [Layer 1] [Layer 2] ... [Layer L-1] [Layer L]
                         │          │          │               │
    RDMA Transfer:  [Load L1] [Load L2] [Load L3] ... [Load LL] [Store L0..L]
                         │          │          │               │
                    ◀── COMPUTE-TRANSFER OVERLAP ───────────────▶

    GPU SM cores and DMA engine are independent hardware units.
```

<span style="color:#f4a261">**Layout challenge:**</span> GPU produces layer-first `[L0_tok0..L0_tokN, L1_tok0..]`, Store stores page-first `[page0(L0_tok0..L0_tok15, L1_tok0..), page1..]`. HiCache's "page-first direct" bridges this by grouping all tokens of one layer contiguously within a page.

Hooks: `save_kv_layer()` / `wait_for_layer_load()` in [`mooncake_connector_v1.py`](../../../mooncake-wheel/mooncake/mooncake_connector_v1.py). The P2P connector transfers a batch of blocks together; SGLang HiCache implements layer-wise overlap.

### 4.4 HiCache: Hierarchical KV Cache

```text
+--------------------------------------------------------------+
| Mooncake Store: shared remote CPU DRAM (L3)                  |
+-----------------------------+--------------------------------+
                              | RDMA prefetch / write-back
                              v
+-----------------------------+--------------------------------+
| GPU node                                                     |
| +--------------------+      +-----------------------------+  |
| | L1: GPU VRAM       |<---->| L2: local CPU DRAM          |  |
| | hot, private       |      | warm, private               |  |
| +--------------------+      +-----------------------------+  |
+--------------------------------------------------------------+
```

| Tier | Medium | Capacity | Scope | Backend |
|---|---|---|---|---|
| **L1** | <span style="color:#e63946">GPU VRAM</span> | ~80 GB | Private | vLLM block table |
| **L2** | <span style="color:#f4a261">CPU DRAM</span> | 512 GB – 2 TB | Private | HiRadixTree + GPU I/O kernels |
| **L3** | <span style="color:#457b9d">Mooncake Store</span> | TBs | Shared | Master/Client + RDMA |

See [`docs/source/design/hicache-design.md`](../../../docs/source/design/hicache-design.md) for full design.

**HiRadixTree:** extends RadixAttention's trie — each node records where KV cache resides across tiers. L1/L2 store precise addresses; L3 queries Mooncake Store backend in real time.

**Workflow:**
```
    Request ──▶ L1+L2 local match ──▶ L3 prefetch (RDMA if hit > 256 tokens)
                  │                      │
                  ▼                      ▼
            Prefill compute ◀──── KV cache ready
                  │
                  ▼
            Write-back (async, policy-dependent)
```

| Strategy | Behavior |
|---|---|
| <span style="color:#2a9d8f">`write_through`</span> | Every KV cache access → Store |
| <span style="color:#2a9d8f">`write_through_selective`</span> | Only hot data → Store |
| <span style="color:#2a9d8f">`write_back`</span> | Write to Store on eviction only |

| Prefetch | Behavior |
|---|---|
| `best_effort` | Return immediately, use whatever loaded |
| `wait_complete` | Block until all L3 data fetched |
| `timeout` | `prefetch_timeout_base + prefetch_timeout_per_ki_token × tokens/1024` |

- <span style="color:#457b9d">**TP sync:**</span> `all_reduce(op=min)` ensures all ranks agree on hit length and prefetch decisions.
- <span style="color:#457b9d">**Zero-copy:**</span> L2↔L3 via Mooncake RDMA; L2↔GPU via GPU-assisted I/O kernels (up to 3× faster than `cudaMemcpyAsync` baseline).

---

## 5. Transfer Engine & RDMA

<span style="color:#e63946">**RDMA:**</span> NIC directly reads/writes remote memory, bypassing remote CPU/OS. One-sided operations + kernel bypass = microsecond latency, zero-copy.

```text
+--------------------------------------------------------------------------+
| GPUDirect RDMA data path                                                 |
|                                                                          |
| +------------+  PCIe  +------+  RoCE/IB  +------+  PCIe  +------------+  |
| | GPU node A |<------>| RNIC |<=========>| RNIC |<------>| GPU node B |  |
| +------------+        +------+           +------+        +------------+  |
|                                                                          |
| Data bypasses the remote CPU data path.                                  |
+--------------------------------------------------------------------------+
```

### Protocol Stack

```text
+----------------------------------------------------------------+
| TransferEngine                                                 |
| init | registerLocalMemory | submitTransfer | poll status      |
+----------------------------------------------------------------+
| Transport abstraction: requests, segments, batches             |
+----------+----------+----------+----------+----------+---------+
| RDMA     | TCP      | NVMe-oF | NVLink   | CXL      | others   |
+----------+----------+----------+----------+----------+---------+
| Topology: CPU / NUMA / GPU / RNIC affinity                     |
+----------------------------------------------------------------+
```

### Key Implementation Files

| Layer | Header | Key Classes |
|---|---|---|
| Public API | [`mooncake-transfer-engine/include/transfer_engine.h`](../../../mooncake-transfer-engine/include/transfer_engine.h) | `TransferEngine` |
| C API | [`.../transfer_engine_c.h`](../../../mooncake-transfer-engine/include/transfer_engine_c.h) | C-compatible wrappers |
| Transport base | [`.../transport/transport.h`](../../../mooncake-transfer-engine/include/transport/transport.h) | `Transport` (abstract), `TransferRequest`, `SegmentHandle`, `BatchID` |
| <span style="color:#e63946">RDMA</span> | [`.../rdma_transport/rdma_transport.h`](../../../mooncake-transfer-engine/include/transport/rdma_transport/rdma_transport.h) | `RdmaTransport`, `RdmaEndpoint`, `EndpointStore` (SIEVE/FIFO) |
| TCP | [`.../tcp_transport/tcp_transport.h`](../../../mooncake-transfer-engine/include/transport/tcp_transport/tcp_transport.h) | `TcpTransport` |
| Device (EP) | [`.../device/device_transport.h`](../../../mooncake-transfer-engine/include/transport/device/device_transport.h) | `P2pTransport`, `RdmaTransport` |
| Topology | [`mooncake-transfer-engine/include/topology.h`](../../../mooncake-transfer-engine/include/topology.h) | `Topology` — CPU/NUMA/GPU/RNIC affinity matrix |
| Python bindings | [`mooncake-integration/transfer_engine/transfer_engine_py.cpp`](../../../mooncake-integration/transfer_engine/transfer_engine_py.cpp) | Pybind11 → `mooncake.engine.TransferEngine` |

### Key Features

| Feature | Detail |
|---|---|
| <span style="color:#2a9d8f">Topology-aware routing</span> | Selects optimal NIC based on NUMA/GPU affinity |
| <span style="color:#2a9d8f">Multi-NIC aggregation</span> | Slices transfers >64KB across multiple NICs |
| <span style="color:#2a9d8f">SIEVE endpoint pool</span> | Connection reuse; avoids per-transfer setup |
| <span style="color:#2a9d8f">Auto failover</span> | Detects NIC failure → reroutes to healthy path |
| **Peak bandwidth** | <span style="color:#e63946">142.25 GB/s</span> (8× RoCE) |

**Key env vars:** `MC_SLICE_SIZE`, `MC_RETRY_CNT`, `MC_TE_FILTERS`, `MC_ENDPOINT_STORE_TYPE`, `MC_FORCE_TCP`, `MC_NUM_CQ_PER_CTX`.

### TENT (Next-gen Transfer Engine)

[`mooncake-transfer-engine/tent/include/tent/transfer_engine.h`](../../../mooncake-transfer-engine/tent/include/tent/transfer_engine.h):

```text
+----------------------------------------------------------------+
| TENT runtime                                                   |
+--------------------+--------------------+----------------------+
| TransportSelector  | QoS contract       | Admission queue      |
| policy and fallback| bandwidth/deadline |dispatch/backpressure |
+--------------------+--------------------+----------------------+
| Backends: RDMA, MNNVL, SHM, NVLink, GDS, io_uring, TCP, ...    |
+----------------------------------------------------------------+
```

---

## 6. Inter-node Data Flow

### 6.1 Direct P2P Transfer (Real-time Path)

```text
+--------------------------+                  +--------------------------+
| Prefill producer         |  handoff metadata| Decode consumer          |
| register_kv_caches()     |<---------------->| start_load_kv()          |
|          |               |                  |          |               |
|          v               |                  |          v               |
| register GPU VRAM in TE  |<==== RDMA read===| build and send requests  |
+--------------------------+  decode pulls    +--------------------------+
```

Implemented by [`mooncake_connector_v1.py`](../../../mooncake-wheel/mooncake/mooncake_connector_v1.py). Transfer is initiated by the **decode** side — prefill registers memory and publishes transfer metadata; decode pulls the requested blocks.

### 6.2 CPU-Managed Store Allocation (Reuse Path)

```text
                          +----------------------+
                          | MasterService        |
                          | metadata + placement |
                          +-----+-----------+----+
                                |           |
                         allocate|           |query
                                v           v
                  +-------------------------------+
                  | Buffer nodes                  |
                  | CPU DRAM segments | SSD tiers |
                  +-------------------------------+
                       ^                       |
        Put(hash, data) |                       | Get(hash)
                       |                       v
                +------------+           +------------+
                | Prefill    |           | Decode     |
                | producer   |           | consumer   |
                +------------+           +------------+
```

实现入口：`mooncake-store/include/master_service.h`、`allocator.h`、
`eviction_strategy.h`。Master 只管理描述符和分配状态，不承载对象字节。

| Tier | Medium | Capacity | Managed by |
|---|---|---|---|
| <span style="color:#e63946">Hot</span> | GPU VRAM | ~80 GB | Inference engine |
| <span style="color:#f4a261">Warm</span> | CPU DRAM | 512 GB – 2 TB | Mooncake Master Service |
| <span style="color:#457b9d">Cold</span> | SSD (NVMe-of) | TBs | Master + [`file_storage.h`](../../../mooncake-store/include/file_storage.h) |

**Flow:** Prefill → RDMA → CPU DRAM pool. Master records `block_hash → node_id → segment → offset → length`. Later hit: query Master → RDMA read CPU DRAM → GPU VRAM.

Python API: `MooncakeDistributedStore` ([`mooncake-wheel/mooncake/__init__.py`](../../../mooncake-wheel/mooncake/__init__.py) → `mooncake.store`). Methods: `setup()`, `put()`, `get()`, `put_from()`, `get_into()`, `register_buffer()`, `put_batch()`, `get_batch()`, `is_exist()`, `remove()`.

HA: leader election via etcd/redis/k8s ([`ha/leadership/`](../../../mooncake-store/include/ha/leadership/)), oplog replication ([`ha/oplog/`](../../../mooncake-store/include/ha/oplog/)), periodic metadata snapshots ([`ha/snapshot/`](../../../mooncake-store/include/ha/snapshot/)).

> **Two transfer patterns coexist** sharing the same Transfer Engine: <span style="color:#e63946">P2P Direct</span> (real-time, GPU→GPU) and <span style="color:#457b9d">Store-mediated</span> (async, GPU→CPU DRAM→future GPU).

---

## 7. PD Disaggregation + Cache Pooling

Three-layer production architecture deployed together:

```text
                         +-----------------------+
                         | FastAPI proxy         |
                         +-----------+-----------+
                                     |
                        request routing + handoff metadata
                                     |
                  +------------------+------------------+
                  |                                     |
                  v                                     v
          +---------------+    direct P2P RDMA   +---------------+
          | Prefill       |=====================>| Decode        |
          | KV producer   |                      | KV consumer   |
          +-------+-------+                      +-------+-------+
                  | Store write                          | Store hit
                  v                                      v
          +-------------------------------------------------------+
          | Mooncake Store: shared DRAM / SSD cache tiers         |
          +-------------------------------------------------------+
```

| Layer | Name | Path | Trigger | Latency |
|---|---|---|---|---|
| **①** | <span style="color:#e63946">P2P Direct</span> | Prefill GPU → Decode GPU | Every request | μs-level RDMA |
| **②** | <span style="color:#f4a261">Store Write</span> | Prefill GPU → CPU DRAM pool | Async, every request | Background |
| **③** | <span style="color:#2a9d8f">Store Hit</span> | CPU DRAM pool → Decode GPU | Cross-request reuse | RDMA on demand |

**Proxy flow** ([`vllm_v1_proxy_server.py`](../../../mooncake-wheel/mooncake/vllm_v1_proxy_server.py)):
1. Send to prefill with `max_tokens=1`
2. Prefill returns `kv_transfer_params` (block addresses)
3. Forward to decode with `do_remote_prefill=True` + `kv_transfer_params`
4. Decode pulls KV cache via RDMA, continues generation

> <span style="color:#e63946">**Architecture principle:**</span> KV cache is the shared artifact; Transfer Engine is the transport. Three layers share one Transfer Engine instance.

---

## 8. Admission Control & Load Prediction

<span style="color:#f4a261">**Problem:**</span> prefill is expensive (~200ms GPU time). If decode has no slot when prefill finishes, compute is wasted.

<span style="color:#f4a261">**Temporal mismatch:**</span> between prefill start and finish, decode slots change. Must predict **at prefill completion time**.

```text
Time ------------------------------------------------------------>

Prefill starts                 Prefill completes
      |                               |
      v                               v
+------------------------------------+
| Prefill GPU work                    |----> decode slot needed here
+------------------------------------+
      |<------ prediction window ---->|
      |                               |
      v                               v
+-----------------------------------------------------------------+
| Decode occupancy changes: completions free slots while new      |
| prefills already in flight claim future capacity.               |
+-----------------------------------------------------------------+
```

### Prediction Model

```
    t_prefill  = f(input_length, prefill_load)
    freed      = R_complete × t_prefill          ← slots freed during prefill
    claimed    = Q_prefill                        ← prefills ahead in queue
    available  = (total - N_active) + freed - claimed
    → available < 1 → reject (503)
```

| Approach | Mechanism | When to use |
|---|---|---|
| <span style="color:#e63946">Conservative</span> | Assume `max_tokens` for all active | Safe, underutilized |
| <span style="color:#f4a261">Watermark</span> | P(reject) = max(0, (occ - 0.80)/0.15) | Balanced |
| <span style="color:#2a9d8f">Survival analysis</span> | Historical completion-time distribution | Highest accuracy |

**Placement:** proxy/router, before prefill invocation. Dual purpose: <span style="color:#e63946">cost-saving</span> (no wasted prefill) + <span style="color:#2a9d8f">SLO-preserving</span> (decode occupancy below queuing threshold). TENT provides an in-engine implementation via [`admission_queue.h`](../../../mooncake-transfer-engine/tent/include/tent/runtime/admission_queue.h).

---

## 9. Intra-node Parallelism (TP/PP/SP/EP)

```text
+--------------------------------------------------------------------------+
| Intra-node parallelism                                                   |
+------------------+------------------+------------------+-----------------+
| TP               | PP               | SP               | EP              |
| shard tensors    | shard layers     | shard sequences  | shard experts   |
| all-reduce       | send/recv        | gather/scatter   | all-to-all      |
+------------------+------------------+------------------+-----------------+
```

| Strategy | Shards | Communication | Pattern |
|---|---|---|---|
| <span style="color:#e63946">**TP**</span> | Weight matrices column/row | All-reduce per layer | High-frequency, small msg |
| <span style="color:#f4a261">**PP**</span> | Layers across GPU groups | Activations between stages | Low-frequency, large msg |
| <span style="color:#2a9d8f">**SP**</span> | Sequence dim (LayerNorm etc.) | All-gather / reduce-scatter | Complements TP |
| <span style="color:#457b9d">**EP**</span> | MoE experts | Token dispatch/combine (all-to-all) | Bursty, large msg |

### Mooncake Backend (PG)

[`mooncake-wheel/mooncake/pg.py`](../../../mooncake-wheel/mooncake/pg.py) — registers `mooncake` and `mooncake-cpu` backends for `torch.distributed`. Uses Transfer Engine RDMA via process group extension.

```text
+----------------------------------------------------------------+
| Mooncake PG                                                    |
| backend="mooncake" + MooncakeBackendOptions(active_ranks=...)  |
|                                                                |
| collectives: all_reduce, broadcast, gather/scatter, all_to_all |
| P2P: isend / irecv | recovery: peer state and rank rejoin      |
+----------------------------------------------------------------+
```

See [`docs/source/design/mooncake-backend-pg.md`](../../../docs/source/design/mooncake-backend-pg.md).

### Mooncake EP (Expert Parallelism)

[`mooncake-wheel/mooncake/mooncake_ep_buffer.py`](../../../mooncake-wheel/mooncake/mooncake_ep_buffer.py) and [`mooncake-wheel/mooncake/ep.py`](../../../mooncake-wheel/mooncake/ep.py) — `Buffer(group, num_bytes)` wrapping native `MooncakeEpBuffer`.

```text
+----------------------------------------------------------------+
| Mooncake EP                                                    |
| Tokens -> dispatch() -> local experts -> combine()             |
|             |                              |                   |
|             +------ all-to-all via TE -----+                   |
| Options include FP8, zero-copy, and timeout handling.          |
+----------------------------------------------------------------+
```

See [`docs/source/design/mooncake-ep.md`](../../../docs/source/design/mooncake-ep.md).

> <span style="color:#e63946">**Two communication layers coexist**</span> using the same Transfer Engine: intra-node model sharding (NCCL / Mooncake PG / Mooncake EP) + inter-node KV cache transfer (RDMA).
