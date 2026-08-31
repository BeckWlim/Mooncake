# Mooncake Project Overview

> Auto-generated project knowledge — read this first in a new session to understand what's what.

## What is Mooncake?

Mooncake is the **LLM serving platform behind Kimi** (Moonshot AI). It uses a KVCache-centric **disaggregated architecture**: prefill and decode run on separate clusters, while idle CPU/DRAM/SSD resources across GPU nodes form a distributed KV cache pool. Under real workloads, it lets Kimi handle **75% more requests** while meeting SLOs. Won **Best Paper at FAST 2025**.

## Architecture (4 components)

### 1. Transfer Engine (`mooncake-transfer-engine/`) — Foundation
High-performance data transport. Unified API over RDMA, TCP, NVLink, NVMe-oF, AWS EFA, CXL, Ascend NPU, HIP, MUSA, etc. Provides topology-aware routing, multi-NIC bandwidth aggregation, and automatic failover. Performance: **87 GB/s** on 4×200Gbps RoCE, **190 GB/s** on 8×400Gbps.

- `include/transfer_engine.h` — public API
- `include/transport/transport.h` — abstract transport base class
- `src/transport/rdma_transport/` — RDMA/RoCE with GPUDirect
- `src/transport/tcp_transport/` — TCP fallback
- `src/transport/nvmeof_transport/` — NVMe over Fabrics
- `src/transport/nvlink_transport/` — GPU-to-GPU NVLink
- `src/transport/efa_transport/` — AWS EFA
- `src/transport/ascend_transport/` — Huawei Ascend NPU
- `include/topology.h` — NUMA-aware topology discovery
- `include/multi_transport.h` — multi-protocol orchestration
- `tent/` — next-gen TENT engine (declarative, QoS, admission control)

### 2. Mooncake Store (`mooncake-store/`) — Distributed KV Cache
Built on Transfer Engine. Put/Get/Replicate/Evict for KV caches across a cluster. Multi-tier (DRAM + SSD), zero-copy I/O, tenant quotas.

- `include/master_service.h` — central master (object→segment mapping, replication, eviction)
- `include/real_client.h` — client library (Put/Get/List/Del/Replicate)
- `include/client_service.h` — client-side transfer orchestration
- `include/segment.h` — memory segment abstraction (DRAM/SSD)
- `include/replica.h` — replication strategy (SSD Free-Ratio-First)
- `include/allocator.h` — cachelib-based SlabAllocator (DRAM), offset_allocator (SSD)
- `include/ha/` — high availability (etcd/Redis/K8s leader election, oplog, snapshots)
- `include/storage_backend.h` — pluggable slow storage (local/HF3FS/S3)
- `include/http_metadata_server.h` — REST metadata server
- `include/store_c.h` — C ABI for Go/Rust FFI
- `go/` — Go bindings (cgo)
- `rust/` — Rust bindings (FFI)

### 3. Mooncake EP (`mooncake-ep/`) — Expert Parallelism
MoE serving primitives implemented by CUDA kernels and exposed through a Python extension.

- `include/mooncake_ep_api.cuh` — dispatch/combine API
- `include/mooncake_ep_buffer.h` — communication buffer abstraction
- `src/mooncake_ep_buffer.cpp` — regular buffer implementation
- `src/mooncake_ep_elastic_buffer.cpp` — elastic buffer implementation

### 4. Mooncake PG (`mooncake-pg/`) — Process Group Backend
PyTorch `torch.distributed` backend with connection management, P2P proxying, and GPU worker kernels.

- `include/mooncake_backend.h` / `src/mooncake_backend.cpp` — backend API and operation dispatch
- `include/connection_poller.h` / `src/connection_poller.cpp` — connection progress
- `include/p2p_proxy.h` / `src/p2p_proxy.cpp` — P2P proxy path
- `src/mooncake_worker*.{cpp,cu}` — host/thread/device workers

## Other Directories

| Directory | Purpose |
|-----------|---------|
| `mooncake-common/` | Shared C++ utils, etcd wrapper (Go), K8s lease, CMake find modules |
| `mooncake-integration/` | Pybind11 bridges: `store_py.cpp`, `transfer_engine_py.cpp`, allocators |
| `mooncake-wheel/` | Python package (`mooncake`): CLI, config, vLLM connectors, EP/PG wrappers |
| `mooncake-p2p-store/` | P2P checkpoint/weight store (used in K1.5/K2 production training) |
| `mooncake-rl/` | RL usage examples |
| `benchmarks/` | Performance benchmarks |
| `docs/` | Sphinx docs (API refs, design docs, deployment/integration guides) |
| `monitoring/` | Grafana + Prometheus docker-compose |
| `scripts/` | Dev utilities, CI, formatting, wheel building |
| `docker/` | Dockerfiles (master, CUDA 13, MUSA) |
| `extern/` | Third-party: pybind11, yalantinglibs |
| `FAST25-release/` | Paper PDF, slides, anonymized traces |
| `image/` | Docs images, architecture diagrams |

## Python Package (`mooncake-wheel/mooncake/`)

| File | Role |
|------|------|
| `__init__.py` | Public API: `BufferPool`, `RegisteredBufferPool` |
| `mooncake_config.py` | `MooncakeConfig` for TE and Store |
| `mooncake_store_service.py` | Manages master/meta/client processes |
| `structured_object_store.py` | High-level structured object store API |
| `mooncake_connector_v1.py` | vLLM v1 KV Connector |
| `vllm_v1_proxy_server.py` | Proxy server for vLLM PD disaggregation |
| `ep.py` / `pg.py` | EP and PG Python wrappers |
| `cli.py` | Main CLI entry point |

## Build System

Root `CMakeLists.txt` with feature flags:
- `WITH_TE` (ON) → Transfer Engine
- `WITH_STORE` (ON) → Mooncake Store + integration
- `WITH_EP` (OFF) → Expert Parallelism
- `WITH_P2P_STORE` (OFF) → P2P Store
- `WITH_STORE_GO` (OFF) → Go bindings
- `WITH_STORE_RUST` (ON) → Rust bindings

Python wheel bundles compiled C++ `.so` files. Install: `pip install mooncake-transfer-engine`.

## Key Integrations

Mooncake is integrated into: **vLLM**, **SGLang**, **TensorRT-LLM**, **LMDeploy**, **LMCache**, **NIXL**, **checkpoint-engine**, **xLLM**, **FlexKV**, **LightX2V**, **TorchSpec**.

## Where to Start (by interest)

- **Data transfer / RDMA / networking**: `mooncake-transfer-engine/src/transport/`
- **KV cache / storage**: `mooncake-store/src/` (master_service, client_service, real_client)
- **MoE / expert parallelism**: `mooncake-ep/include/`
- **PyTorch distributed**: `mooncake-pg/`
- **Python API / integrations**: `mooncake-integration/` + `mooncake-wheel/mooncake/`
- **Go/Rust SDK**: `mooncake-store/go/` or `mooncake-store/rust/`
- **Docs**: `docs/` (Sphinx, `make html`)

## Project Conventions

- PR titles use prefixes: `[Bugfix]`, `[CI/Build]`, `[Doc]`, `[Store]`, `[TransferEngine]`, `[EP]`, `[PG]`, etc.
- RFC issue required for changes >500 LOC (excluding tests)
- Pre-commit hooks: clang-format, ruff, codespell, cmake-format
- Run the relevant tests plus `pre-commit run --files <touched-files>` before handoff
- Read `docs/AGENTS.md` before modifying docs
- `.claude/` stores Claude Code config (flat structure, `.local.` suffix for private files)
