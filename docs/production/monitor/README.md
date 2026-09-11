# Mooncake Production Monitor Analysis

This directory contains the source exports, source-derived causal assessment,
deployment context, and commit-ready PDF for the client transition from a
shared Mooncake master to an independent master on 2026-09-10.

## Index

| Path | Purpose |
| --- | --- |
| [analysis.md](analysis.md) | Architecture-derived causal assessment and production validation design |
| [batchevict_latency_causal_analysis.pdf](batchevict_latency_causal_analysis.pdf) | Commit-ready PDF report with embedded figures |
| [data/README.md](data/README.md) | Dataset map, original-to-current filename index, coverage, and fields |
| [analyze_batchevict.py](analyze_batchevict.py) | Reproducible data preparation and architecture/context figures |
| [render_analysis_pdf.py](render_analysis_pdf.py) | Reproducible vector PDF renderer with unified A4 portrait pages and 20 mm margins |
| [test_analyze_batchevict.py](test_analyze_batchevict.py) | Focused unit and dataset-boundary checks |
| [figures/request_chain_and_contention.png](figures/request_chain_and_contention.png) | Ordinary SGLang–Mooncake cache-read path and BatchEvict lock lifecycle |
| [figures/production_timeline.png](figures/production_timeline.png) | Whole-range client attachment, master state, KV-transfer latency, and TTFT timeline |
| [figures/high_pressure_30min_fragment.png](figures/high_pressure_30min_fragment.png) | Descriptive 03:00–03:30 view of inferred eviction bins, transfer latency, and TTFT |
| [figures/event_process_correlation.png](figures/event_process_correlation.png) | Resolution-limited event association with uncertainty intervals |
| [results/phase_summary.csv](results/phase_summary.csv) | Phase-level event and metric summary |
| [results/event_process_correlations_high_pressure.csv](results/event_process_correlations_high_pressure.csv) | High-pressure association estimates across baselines and lags |
| [results/event_process_summary_high_pressure.csv](results/event_process_summary_high_pressure.csv) | High-pressure zero-lag intervals and event/control contrasts |

PDF versions of every figure are stored beside the PNG versions for publication
and review.

## Directory map

```text
monitor/
├── README.md
├── analysis.md
├── batchevict_latency_causal_analysis.pdf
├── analyze_batchevict.py
├── render_analysis_pdf.py
├── test_analyze_batchevict.py
├── requirements-analysis.txt
├── data/
│   ├── README.md
│   ├── client_kv_transfer_latency.csv
│   ├── client_ttft.csv
│   ├── independent_master_allocated_memory.csv
│   ├── independent_master_key_count.csv
│   ├── shared_master_allocated_memory.csv
│   └── shared_master_key_count.csv
├── figures/
│   ├── event_process_correlation.{png,pdf}
│   ├── high_pressure_30min_fragment.{png,pdf}
│   ├── production_timeline.{png,pdf}
│   └── request_chain_and_contention.{png,pdf}
└── results/
    ├── event_process_correlations_high_pressure.csv
    ├── event_process_summary_high_pressure.csv
    └── phase_summary.csv
```

## Reproduction

Use Python 3.11 or later from this directory:

```bash
python -m pip install -r requirements-analysis.txt
python -m unittest -v test_analyze_batchevict.py
python analyze_batchevict.py
python render_analysis_pdf.py
```

The analysis script regenerates the architecture figure, production context,
and phase summary from the six CSV inputs. The PDF renderer embeds the vector
figure PDFs referenced by `analysis.md` and normalizes every report page to A4
portrait with 20 mm content margins.
