# Dataset Map

The filenames encode the semantic role supplied with the deployment context:
the client initially used a master shared with other clients and then used a
dedicated master. CSV contents retain the original Grafana column names and
values.

## Source-file index

| Current filename | Original export filename | Role | Rows | Time coverage |
| --- | --- | --- | ---: | --- |
| `client_ttft.csv` | `TTFT (Time To First Token)-data-as-joinbyfield-2026-09-11 10_48_02.csv` | Client TTFT average, P95, and P99 | 2,128 | 2026-09-10 12:00:00–2026-09-11 06:00:00 |
| `client_kv_transfer_latency.csv` | `KV 传输时延-等待有数据后再校验-data-as-joinbyfield-2026-09-11 10_48_19.csv` | Model-specific KV-transfer P50, P90, P95, and P99 | 2,129 | 2026-09-10 12:00:00–2026-09-11 06:00:00 |
| `shared_master_key_count.csv` | `Key 数量趋势-data-2026-09-11 11_17_38.csv` | Aggregate key inventory on the shared master | 2,161 | 2026-09-10 12:00:00–2026-09-11 06:00:00 |
| `shared_master_allocated_memory.csv` | `已分配内存趋势-data-2026-09-11 11_16_41.csv` | Aggregate allocated memory on the shared master | 2,161 | 2026-09-10 12:00:00–2026-09-11 06:00:00 |
| `independent_master_key_count.csv` | `Key 数量趋势-data-2026-09-11 11_11_26.csv` | Key inventory on the independent master | 1,723 | 2026-09-10 15:39:00–2026-09-11 06:00:00 |
| `independent_master_allocated_memory.csv` | `已分配内存趋势-data-2026-09-11 11_09_34.csv` | Allocated memory on the independent master | 1,723 | 2026-09-10 15:39:00–2026-09-11 06:00:00 |

## Join and unit conventions

- `Time` is the join key. The series use a 30-second cadence.
- Times are timezone-naive in the CSV exports and are interpreted as
  Asia/Shanghai for this analysis.
- TTFT values are normalized to milliseconds from the exported `ms` and `s`
  strings.
- KV-transfer values are normalized to milliseconds.
- Allocated memory is normalized to GiB from the exported byte units.
- The KV-transfer series is labeled `spark-x2.5-1.7b-test` in its source
  columns.

## Coverage events

The client metric exports contain two visible gaps: approximately 14:02–14:07
and 15:41:30–15:52:00. The second gap coincides with the supplied master
transition context. Client metrics resume at 15:52:30 as the independent master
begins its sustained key-inventory ramp.
