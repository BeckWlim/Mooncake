"""Analyze BatchEvict associations in the Mooncake production monitor export."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.colors import SymLogNorm
from matplotlib.patches import FancyArrowPatch


ROOT_DIRECTORY = Path(__file__).resolve().parent
DATA_DIRECTORY = ROOT_DIRECTORY / "data"
FIGURE_DIRECTORY = ROOT_DIRECTORY / "figures"
RESULT_DIRECTORY = ROOT_DIRECTORY / "results"

SAMPLE_INTERVAL_SECONDS = 30
KEY_DROP_THRESHOLD = 40_000
MEMORY_DROP_THRESHOLD_GIB = 35.0
STRATUM_FREQUENCY = "15min"
PRESSURE_STRATUM_FREQUENCY = "1h"
PRESSURE_WINDOW_MINUTES = 30
WINDOW_MINUTES = (1, 2, 5, 10, 15, 30, 60)
LOW_PRESSURE_MAX_EVENTS = 3
HIGH_PRESSURE_MIN_EVENTS = 15
LOCAL_BASELINE_MINUTES = 30
BASELINE_SENSITIVITY_MINUTES = (15, 30, 60, 120)
EVENT_LAG_STEPS = tuple(range(-8, 9))
BOOTSTRAP_REPETITIONS = 5_000
BOOTSTRAP_SEED = 20260911

SHARED_START = datetime.fromisoformat("2026-09-10 12:00:00")
SHARED_END = datetime.fromisoformat("2026-09-10 15:41:00")
INDEPENDENT_START = datetime.fromisoformat("2026-09-10 15:52:30")
INDEPENDENT_END = datetime.fromisoformat("2026-09-11 06:00:00")
HIGH_PRESSURE_START = datetime.fromisoformat("2026-09-10 20:00:00")
HIGH_PRESSURE_FRAGMENT_START = datetime.fromisoformat("2026-09-11 03:00:00")
HIGH_PRESSURE_FRAGMENT_MINUTES = 30


@dataclass(frozen=True)
class Phase:
    """Client attachment phase and its corresponding master."""

    role: str
    display_name: str
    start: datetime
    end: datetime


PHASES = (
    Phase("shared", "Shared master", SHARED_START, SHARED_END),
    Phase("independent", "Independent master", INDEPENDENT_START, INDEPENDENT_END),
)

METRIC_LABELS = {
    "kv_p50_ms": "KV transfer P50",
    "kv_p95_ms": "KV transfer P95",
    "kv_p99_ms": "KV transfer P99",
    "ttft_avg_ms": "TTFT average",
    "ttft_p95_ms": "TTFT P95",
    "ttft_p99_ms": "TTFT P99",
}

COLORS = {
    "shared": "#6B7280",
    "independent": "#176B87",
    "event": "#B64A3A",
    "p50": "#1B9E77",
    "p95": "#D95F02",
    "p99": "#7570B3",
    "average": "#3B6FB6",
}


def numeric_column(frame: pd.DataFrame, column_name: str) -> np.ndarray:
    """Return one DataFrame column as a one-dimensional float array."""

    return frame[[column_name]].to_numpy(dtype=float).reshape(-1)


def datetime_column(frame: pd.DataFrame, column_name: str) -> np.ndarray:
    """Return one DataFrame column as a minute-resolution datetime array."""

    return frame[[column_name]].to_numpy(dtype="datetime64[m]").reshape(-1)


def numeric_series(frame: pd.DataFrame, column_name: str) -> pd.Series:
    """Return one DataFrame column as a one-dimensional float Series."""

    column_values = numeric_column(frame, column_name)
    return pd.Series(
        column_values,
        index=frame.index,
        dtype=float,
        name=column_name,
    )


def pearson_correlation(left_values: np.ndarray, right_values: np.ndarray) -> float:
    """Calculate a Pearson correlation from equally sized numeric arrays."""

    return float(np.corrcoef(left_values, right_values)[0, 1])


def spearman_correlation(left_series: pd.Series, right_series: pd.Series) -> float:
    """Calculate a Spearman correlation without a SciPy dependency."""

    left_ranks = left_series.rank(method="average").to_numpy(dtype=float)
    right_ranks = right_series.rank(method="average").to_numpy(dtype=float)
    return pearson_correlation(left_ranks, right_ranks)


def parse_duration_ms(raw_value: str) -> float:
    """Convert a Grafana duration string to milliseconds."""

    magnitude_text, unit = raw_value.split()
    magnitude = float(magnitude_text)
    if unit == "s":
        return magnitude * 1_000.0
    if unit == "ms":
        return magnitude
    raise ValueError(f"Unsupported duration unit: {unit}")


def parse_memory_gib(raw_value: str) -> float:
    """Convert a Grafana byte-size string to GiB."""

    magnitude_text, unit = raw_value.split()
    magnitude = float(magnitude_text)
    if unit == "GiB":
        return magnitude
    if unit == "MiB":
        return magnitude / 1_024.0
    if unit == "B":
        return magnitude / (1_024.0**3)
    raise ValueError(f"Unsupported memory unit: {unit}")


def load_metrics() -> pd.DataFrame:
    """Load and normalize client TTFT and KV-transfer metrics."""

    ttft_frame = pd.read_csv(DATA_DIRECTORY / "client_ttft.csv")
    ttft_frame["Time"] = pd.to_datetime(ttft_frame["Time"])
    ttft_columns = {
        "AVG": "ttft_avg_ms",
        "P95": "ttft_p95_ms",
        "P99": "ttft_p99_ms",
    }
    normalized_ttft = ttft_frame.rename(columns=ttft_columns)
    for metric_name in ttft_columns.values():
        normalized_ttft[metric_name] = normalized_ttft[metric_name].map(
            parse_duration_ms
        )

    transfer_frame = pd.read_csv(DATA_DIRECTORY / "client_kv_transfer_latency.csv")
    transfer_frame["Time"] = pd.to_datetime(transfer_frame["Time"])
    raw_transfer_columns = list(transfer_frame.columns[1:])
    transfer_columns = {
        raw_transfer_columns[0]: "kv_p99_ms",
        raw_transfer_columns[1]: "kv_p95_ms",
        raw_transfer_columns[2]: "kv_p90_ms",
        raw_transfer_columns[3]: "kv_p50_ms",
    }
    normalized_transfer = transfer_frame.rename(columns=transfer_columns)
    for metric_name in transfer_columns.values():
        normalized_transfer[metric_name] = normalized_transfer[metric_name].map(
            parse_duration_ms
        )

    return normalized_ttft.merge(normalized_transfer, on="Time", how="inner")


def load_master(role: str) -> pd.DataFrame:
    """Load one master's key and memory series and derive event signatures."""

    key_path = DATA_DIRECTORY / f"{role}_master_key_count.csv"
    memory_path = DATA_DIRECTORY / f"{role}_master_allocated_memory.csv"

    key_frame = pd.read_csv(key_path).rename(columns={"Key 总量": "key_count"})
    key_frame["Time"] = pd.to_datetime(key_frame["Time"])

    memory_frame = pd.read_csv(memory_path).rename(columns={"已分配": "allocated"})
    memory_frame["Time"] = pd.to_datetime(memory_frame["Time"])
    memory_frame["allocated_memory_gib"] = memory_frame["allocated"].map(
        parse_memory_gib
    )

    master_frame = key_frame[["Time", "key_count"]].merge(
        memory_frame[["Time", "allocated_memory_gib"]], on="Time", how="inner"
    )
    master_frame["key_change"] = master_frame["key_count"].diff()
    master_frame["memory_change_gib"] = master_frame["allocated_memory_gib"].diff()
    master_frame["evicted_keys"] = (-master_frame["key_change"]).clip(lower=0.0)
    master_frame["released_memory_gib"] = (-master_frame["memory_change_gib"]).clip(
        lower=0.0
    )
    master_frame["batch_evict"] = (
        master_frame["key_change"] <= -KEY_DROP_THRESHOLD
    ) & (master_frame["memory_change_gib"] <= -MEMORY_DROP_THRESHOLD_GIB)
    return master_frame


def select_phase(master_frame: pd.DataFrame, phase: Phase) -> pd.DataFrame:
    """Select samples for one client attachment phase."""

    phase_mask = master_frame["Time"].between(phase.start, phase.end)
    return master_frame.loc[phase_mask].copy()


def residualize_by_stratum(
    analysis_frame: pd.DataFrame, metric_name: str
) -> pd.DataFrame:
    """Demean event status and a metric within valid 15-minute strata."""

    working_frame = analysis_frame.copy()
    working_frame["stratum"] = working_frame["Time"].dt.floor(STRATUM_FREQUENCY)
    has_both_states = working_frame.groupby("stratum")["batch_evict"].transform(
        lambda event_values: event_values.any() and (~event_values).any()
    )
    matched_frame = working_frame.loc[has_both_states].copy()
    event_as_float = matched_frame["batch_evict"].astype(float)
    matched_frame["event_residual"] = event_as_float - matched_frame.groupby("stratum")[
        "batch_evict"
    ].transform("mean")
    matched_frame["metric_residual"] = matched_frame[
        metric_name
    ] - matched_frame.groupby("stratum")[metric_name].transform("mean")
    return matched_frame


def weighted_stratum_statistics(
    matched_frame: pd.DataFrame, metric_name: str
) -> pd.DataFrame:
    """Compute event-control contrasts and regression weights by stratum."""

    stratum_records: list[dict[str, float | str]] = []
    for stratum_time, stratum_frame in matched_frame.groupby("stratum"):
        event_frame = stratum_frame.loc[stratum_frame["batch_evict"]]
        control_frame = stratum_frame.loc[~stratum_frame["batch_evict"]]
        event_metric_values = numeric_column(event_frame, metric_name)
        control_metric_values = numeric_column(control_frame, metric_name)
        event_residuals = numeric_column(stratum_frame, "event_residual")
        metric_residuals = numeric_column(stratum_frame, "metric_residual")
        event_count = float(len(event_frame))
        control_count = float(len(control_frame))
        total_count = event_count + control_count
        stratum_records.append(
            {
                "stratum": str(stratum_time),
                "weight": event_count * control_count / total_count,
                "difference_ms": float(np.mean(event_metric_values))
                - float(np.mean(control_metric_values)),
                "control_mean_ms": float(np.mean(control_metric_values)),
                "event_sum_squares": float(np.sum(event_residuals**2)),
                "metric_sum_squares": float(np.sum(metric_residuals**2)),
                "cross_product": float(np.sum(event_residuals * metric_residuals)),
            }
        )
    return pd.DataFrame.from_records(stratum_records)


def bootstrap_adjusted_statistics(
    stratum_statistics: pd.DataFrame,
    seed: int,
) -> tuple[float, float, float, float]:
    """Bootstrap 15-minute strata for effect and correlation intervals."""

    random_generator = np.random.default_rng(seed)
    sampled_indices = random_generator.integers(
        0,
        len(stratum_statistics),
        size=(BOOTSTRAP_REPETITIONS, len(stratum_statistics)),
    )
    weights = stratum_statistics["weight"].to_numpy()[sampled_indices]
    differences = stratum_statistics["difference_ms"].to_numpy()[sampled_indices]
    baselines = stratum_statistics["control_mean_ms"].to_numpy()[sampled_indices]
    sampled_effect_ms = np.sum(weights * differences, axis=1) / np.sum(weights, axis=1)
    sampled_baseline_ms = np.sum(weights * baselines, axis=1) / np.sum(weights, axis=1)
    effect_samples = 100.0 * sampled_effect_ms / sampled_baseline_ms

    event_sums = stratum_statistics["event_sum_squares"].to_numpy()[sampled_indices]
    metric_sums = stratum_statistics["metric_sum_squares"].to_numpy()[sampled_indices]
    cross_products = stratum_statistics["cross_product"].to_numpy()[sampled_indices]
    correlation_samples = np.sum(cross_products, axis=1) / np.sqrt(
        np.sum(event_sums, axis=1) * np.sum(metric_sums, axis=1)
    )

    effect_lower, effect_upper = np.quantile(effect_samples, [0.025, 0.975])
    correlation_lower, correlation_upper = np.quantile(
        correlation_samples, [0.025, 0.975]
    )
    return (
        float(effect_lower),
        float(effect_upper),
        float(correlation_lower),
        float(correlation_upper),
    )


def estimate_metric_association(
    phase_frame: pd.DataFrame,
    metric_name: str,
    seed: int,
) -> dict[str, float | int | str]:
    """Estimate raw and time-stratified BatchEvict associations."""

    matched_frame = residualize_by_stratum(phase_frame, metric_name)
    stratum_statistics = weighted_stratum_statistics(matched_frame, metric_name)
    stratum_weights = stratum_statistics["weight"].to_numpy()
    adjusted_effect_ms = np.average(
        stratum_statistics["difference_ms"], weights=stratum_weights
    )
    adjusted_baseline_ms = np.average(
        stratum_statistics["control_mean_ms"], weights=stratum_weights
    )
    adjusted_effect_percent = 100.0 * adjusted_effect_ms / adjusted_baseline_ms

    event_frame = phase_frame.loc[phase_frame["batch_evict"]]
    control_frame = phase_frame.loc[~phase_frame["batch_evict"]]
    event_values = numeric_column(event_frame, metric_name)
    control_values = numeric_column(control_frame, metric_name)
    raw_control_mean_ms = float(np.mean(control_values))
    raw_effect_ms = float(np.mean(event_values)) - raw_control_mean_ms
    raw_effect_percent = 100.0 * raw_effect_ms / raw_control_mean_ms
    raw_correlation = pearson_correlation(
        numeric_column(phase_frame, "batch_evict"),
        numeric_column(phase_frame, metric_name),
    )
    adjusted_correlation = pearson_correlation(
        numeric_column(matched_frame, "event_residual"),
        numeric_column(matched_frame, "metric_residual"),
    )
    (
        adjusted_effect_lower_percent,
        adjusted_effect_upper_percent,
        adjusted_correlation_lower,
        adjusted_correlation_upper,
    ) = bootstrap_adjusted_statistics(stratum_statistics, seed)

    return {
        "metric": metric_name,
        "metric_label": METRIC_LABELS[metric_name],
        "event_samples": int(len(event_values)),
        "control_samples": int(len(control_values)),
        "matched_strata": int(len(stratum_statistics)),
        "raw_correlation": raw_correlation,
        "adjusted_correlation": adjusted_correlation,
        "adjusted_correlation_ci_lower": adjusted_correlation_lower,
        "adjusted_correlation_ci_upper": adjusted_correlation_upper,
        "raw_control_mean_ms": raw_control_mean_ms,
        "raw_effect_ms": raw_effect_ms,
        "raw_effect_percent": raw_effect_percent,
        "adjusted_control_mean_ms": float(adjusted_baseline_ms),
        "adjusted_effect_ms": float(adjusted_effect_ms),
        "adjusted_effect_percent": float(adjusted_effect_percent),
        "adjusted_effect_ci_lower_percent": adjusted_effect_lower_percent,
        "adjusted_effect_ci_upper_percent": adjusted_effect_upper_percent,
    }


def calculate_phase_summary(
    phase: Phase, master_frame: pd.DataFrame, metrics_frame: pd.DataFrame
) -> dict[str, float | int | str]:
    """Summarize master activity and client metrics for one phase."""

    phase_master = select_phase(master_frame, phase)
    phase_metrics = metrics_frame.loc[
        metrics_frame["Time"].between(phase.start, phase.end)
    ]
    event_frame = phase_master.loc[phase_master["batch_evict"]]
    inclusive_duration_hours = (
        (phase.end - phase.start).total_seconds() + SAMPLE_INTERVAL_SECONDS
    ) / 3_600.0
    return {
        "phase": phase.role,
        "display_name": phase.display_name,
        "start": phase.start.isoformat(sep=" "),
        "end": phase.end.isoformat(sep=" "),
        "master_samples": int(len(phase_master)),
        "metric_samples": int(len(phase_metrics)),
        "batch_evict_events": int(len(event_frame)),
        "batch_evict_events_per_hour": len(event_frame) / inclusive_duration_hours,
        "first_batch_evict": str(event_frame["Time"].iloc[0]),
        "last_batch_evict": str(event_frame["Time"].iloc[-1]),
        "median_evicted_keys": float(
            np.median(numeric_column(event_frame, "evicted_keys"))
        ),
        "median_released_memory_gib": float(
            np.median(numeric_column(event_frame, "released_memory_gib"))
        ),
        "median_kv_p50_ms": float(
            np.median(numeric_column(phase_metrics, "kv_p50_ms"))
        ),
        "median_kv_p95_ms": float(
            np.median(numeric_column(phase_metrics, "kv_p95_ms"))
        ),
        "median_kv_p99_ms": float(
            np.median(numeric_column(phase_metrics, "kv_p99_ms"))
        ),
        "median_ttft_avg_ms": float(
            np.median(numeric_column(phase_metrics, "ttft_avg_ms"))
        ),
        "median_ttft_p95_ms": float(
            np.median(numeric_column(phase_metrics, "ttft_p95_ms"))
        ),
        "median_ttft_p99_ms": float(
            np.median(numeric_column(phase_metrics, "ttft_p99_ms"))
        ),
    }


def calculate_lag_correlations(
    phase_frame: pd.DataFrame, metrics_frame: pd.DataFrame
) -> pd.DataFrame:
    """Calculate adjusted event-metric correlations across temporal lags."""

    lag_records: list[dict[str, float | int | str]] = []
    metric_lookup = metrics_frame.set_index("Time")
    for lag_steps in range(-8, 9):
        lag_seconds = lag_steps * SAMPLE_INTERVAL_SECONDS
        event_timeline = phase_frame[["Time", "batch_evict"]].copy()
        event_timeline["outcome_time"] = event_timeline["Time"] + pd.Timedelta(
            seconds=lag_seconds
        )
        lagged_frame = event_timeline.merge(
            metric_lookup,
            left_on="outcome_time",
            right_index=True,
            how="inner",
        )
        for metric_name in METRIC_LABELS:
            matched_frame = residualize_by_stratum(lagged_frame, metric_name)
            adjusted_correlation = pearson_correlation(
                numeric_column(matched_frame, "event_residual"),
                numeric_column(matched_frame, "metric_residual"),
            )
            lag_records.append(
                {
                    "lag_seconds": lag_seconds,
                    "metric": metric_name,
                    "metric_label": METRIC_LABELS[metric_name],
                    "adjusted_correlation": adjusted_correlation,
                }
            )
    return pd.DataFrame.from_records(lag_records)


def add_eviction_pressure(
    phase_frame: pd.DataFrame, window_minutes: int
) -> pd.DataFrame:
    """Add trailing BatchEvict exposure fields to a phase timeline."""

    indexed_frame = phase_frame.sort_values("Time").set_index("Time").copy()
    event_values = indexed_frame["batch_evict"].astype(int)
    evicted_key_values = indexed_frame["evicted_keys"].where(
        indexed_frame["batch_evict"], 0.0
    )
    rolling_window = f"{window_minutes}min"
    indexed_frame["recent_batch_evict_count"] = event_values.rolling(
        rolling_window, closed="right"
    ).sum()
    indexed_frame["recent_evicted_keys_millions"] = (
        evicted_key_values.rolling(rolling_window, closed="right").sum() / 1_000_000.0
    )
    return indexed_frame.reset_index()


def residualize_pressure_by_hour(
    pressure_frame: pd.DataFrame, metric_name: str
) -> tuple[pd.Series, pd.Series]:
    """Demean rolling eviction pressure and one metric within one-hour strata."""

    hourly_strata = pressure_frame["Time"].dt.floor(PRESSURE_STRATUM_FREQUENCY)
    pressure_values = pressure_frame["recent_batch_evict_count"]
    metric_values = pressure_frame[metric_name]
    pressure_residuals = pressure_values - pressure_values.groupby(
        hourly_strata
    ).transform("mean")
    metric_residuals = metric_values - metric_values.groupby(hourly_strata).transform(
        "mean"
    )
    return pressure_residuals, metric_residuals


def calculate_window_associations(phase_frame: pd.DataFrame) -> pd.DataFrame:
    """Relate recent BatchEvict density to latency over wider windows."""

    association_records: list[dict[str, float | int | str]] = []
    for window_minutes in WINDOW_MINUTES:
        pressure_frame = add_eviction_pressure(phase_frame, window_minutes)
        exposure_values = numeric_series(pressure_frame, "recent_batch_evict_count")
        exposure_coverage = float((exposure_values > 0).mean())
        for metric_name in METRIC_LABELS:
            metric_values = numeric_series(pressure_frame, metric_name)
            pressure_residuals, metric_residuals = residualize_pressure_by_hour(
                pressure_frame, metric_name
            )
            association_records.append(
                {
                    "window_minutes": window_minutes,
                    "metric": metric_name,
                    "metric_label": METRIC_LABELS[metric_name],
                    "samples": int(len(pressure_frame)),
                    "exposure_coverage": exposure_coverage,
                    "mean_events_per_window": float(exposure_values.mean()),
                    "raw_pearson_correlation": float(
                        exposure_values.corr(metric_values)
                    ),
                    "raw_spearman_correlation": spearman_correlation(
                        exposure_values, metric_values
                    ),
                    "within_hour_pearson_correlation": float(
                        pressure_residuals.corr(metric_residuals)
                    ),
                    "within_hour_spearman_correlation": spearman_correlation(
                        pressure_residuals, metric_residuals
                    ),
                }
            )
    return pd.DataFrame.from_records(association_records)


def classify_pressure_regime(event_count: float) -> str:
    """Map a 30-minute event count to one of the observed pressure modes."""

    if event_count <= LOW_PRESSURE_MAX_EVENTS:
        return "low"
    if event_count >= HIGH_PRESSURE_MIN_EVENTS:
        return "high"
    return "transition"


def bootstrap_pressure_ratio(
    low_pressure_frame: pd.DataFrame,
    high_pressure_frame: pd.DataFrame,
    metric_name: str,
    seed: int,
) -> tuple[float, float]:
    """Bootstrap 30-minute time blocks for a high/low median ratio interval."""

    low_block_ids = low_pressure_frame["Time"].dt.floor(f"{PRESSURE_WINDOW_MINUTES}min")
    high_block_ids = high_pressure_frame["Time"].dt.floor(
        f"{PRESSURE_WINDOW_MINUTES}min"
    )
    low_blocks = [
        numeric_column(block_frame, metric_name)
        for _, block_frame in low_pressure_frame.groupby(low_block_ids)
    ]
    high_blocks = [
        numeric_column(block_frame, metric_name)
        for _, block_frame in high_pressure_frame.groupby(high_block_ids)
    ]
    random_generator = np.random.default_rng(seed)
    ratio_samples = np.empty(BOOTSTRAP_REPETITIONS, dtype=float)
    for repetition_index in range(BOOTSTRAP_REPETITIONS):
        sampled_low_indices = random_generator.integers(
            0, len(low_blocks), size=len(low_blocks)
        )
        sampled_high_indices = random_generator.integers(
            0, len(high_blocks), size=len(high_blocks)
        )
        sampled_low_values = np.concatenate(
            [low_blocks[block_index] for block_index in sampled_low_indices]
        )
        sampled_high_values = np.concatenate(
            [high_blocks[block_index] for block_index in sampled_high_indices]
        )
        ratio_samples[repetition_index] = float(
            np.median(sampled_high_values) / np.median(sampled_low_values)
        )
    ratio_lower, ratio_upper = np.quantile(ratio_samples, [0.025, 0.975])
    return float(ratio_lower), float(ratio_upper)


def calculate_pressure_regimes(
    phase_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Summarize latency in low, transition, and high eviction-pressure modes."""

    pressure_frame = add_eviction_pressure(phase_frame, PRESSURE_WINDOW_MINUTES)
    pressure_frame["pressure_regime"] = pressure_frame["recent_batch_evict_count"].map(
        classify_pressure_regime
    )

    summary_records: list[dict[str, float | int | str]] = []
    for regime_name in ("low", "transition", "high"):
        regime_frame = pressure_frame.loc[
            pressure_frame["pressure_regime"] == regime_name
        ]
        summary_record: dict[str, float | int | str] = {
            "pressure_regime": regime_name,
            "definition": (
                f"<={LOW_PRESSURE_MAX_EVENTS} events/30min"
                if regime_name == "low"
                else (
                    f">={HIGH_PRESSURE_MIN_EVENTS} events/30min"
                    if regime_name == "high"
                    else "4-14 events/30min"
                )
            ),
            "samples": int(len(regime_frame)),
            "start": str(regime_frame["Time"].min()),
            "end": str(regime_frame["Time"].max()),
            "median_events_per_30min": float(
                regime_frame["recent_batch_evict_count"].median()
            ),
        }
        for metric_name in METRIC_LABELS:
            summary_record[f"median_{metric_name}"] = float(
                regime_frame[metric_name].median()
            )
        summary_records.append(summary_record)

    summary_frame = pd.DataFrame.from_records(summary_records)
    low_summary = summary_frame.loc[summary_frame["pressure_regime"] == "low"].iloc[0]
    high_summary = summary_frame.loc[summary_frame["pressure_regime"] == "high"].iloc[0]
    low_pressure_frame = pressure_frame.loc[pressure_frame["pressure_regime"] == "low"]
    high_pressure_frame = pressure_frame.loc[
        pressure_frame["pressure_regime"] == "high"
    ]
    contrast_records: list[dict[str, float | str]] = []
    for metric_index, (metric_name, metric_label) in enumerate(METRIC_LABELS.items()):
        median_column = f"median_{metric_name}"
        low_median_ms = float(low_summary[median_column])
        high_median_ms = float(high_summary[median_column])
        ratio_lower, ratio_upper = bootstrap_pressure_ratio(
            low_pressure_frame,
            high_pressure_frame,
            metric_name,
            BOOTSTRAP_SEED + 500 + metric_index,
        )
        contrast_records.append(
            {
                "metric": metric_name,
                "metric_label": metric_label,
                "low_pressure_median_ms": low_median_ms,
                "high_pressure_median_ms": high_median_ms,
                "difference_ms": high_median_ms - low_median_ms,
                "high_to_low_ratio": high_median_ms / low_median_ms,
                "ratio_ci_lower": ratio_lower,
                "ratio_ci_upper": ratio_upper,
                "relative_change_percent": 100.0
                * (high_median_ms - low_median_ms)
                / low_median_ms,
            }
        )
    return summary_frame, pd.DataFrame.from_records(contrast_records)


def calculate_latency_propagation(phase_frame: pd.DataFrame) -> pd.DataFrame:
    """Quantify contemporaneous transfer-latency and TTFT co-movement."""

    metric_pairs = (
        ("kv_p50_ms", "ttft_avg_ms"),
        ("kv_p95_ms", "ttft_p95_ms"),
        ("kv_p99_ms", "ttft_p99_ms"),
        ("kv_p95_ms", "ttft_avg_ms"),
        ("kv_p99_ms", "ttft_avg_ms"),
    )
    hourly_strata = phase_frame["Time"].dt.floor(PRESSURE_STRATUM_FREQUENCY)
    propagation_records: list[dict[str, float | str]] = []
    for transfer_metric, ttft_metric in metric_pairs:
        transfer_values = numeric_series(phase_frame, transfer_metric)
        ttft_values = numeric_series(phase_frame, ttft_metric)
        transfer_residuals = transfer_values - transfer_values.groupby(
            hourly_strata
        ).transform("mean")
        ttft_residuals = ttft_values - ttft_values.groupby(hourly_strata).transform(
            "mean"
        )
        propagation_records.append(
            {
                "transfer_metric": transfer_metric,
                "transfer_metric_label": METRIC_LABELS[transfer_metric],
                "ttft_metric": ttft_metric,
                "ttft_metric_label": METRIC_LABELS[ttft_metric],
                "whole_phase_pearson_correlation": float(
                    transfer_values.corr(ttft_values)
                ),
                "whole_phase_spearman_correlation": spearman_correlation(
                    transfer_values, ttft_values
                ),
                "within_hour_pearson_correlation": float(
                    transfer_residuals.corr(ttft_residuals)
                ),
                "within_hour_spearman_correlation": spearman_correlation(
                    transfer_residuals, ttft_residuals
                ),
            }
        )
    return pd.DataFrame.from_records(propagation_records)


def add_local_metric_fluctuations(
    phase_frame: pd.DataFrame, baseline_minutes: int
) -> pd.DataFrame:
    """Express each latency metric as a deviation from its centered local median."""

    indexed_frame = phase_frame.sort_values("Time").set_index("Time").copy()
    minimum_samples = max(3, baseline_minutes)
    rolling_window = f"{baseline_minutes}min"
    for metric_name in METRIC_LABELS:
        metric_values = numeric_series(indexed_frame, metric_name)
        local_baseline = metric_values.rolling(
            rolling_window,
            center=True,
            min_periods=minimum_samples,
        ).median()
        indexed_frame[f"{metric_name}_fluctuation_percent"] = (
            100.0 * (metric_values - local_baseline) / local_baseline
        )
    return indexed_frame.reset_index()


def valid_correlation(left_series: pd.Series, right_series: pd.Series) -> float:
    """Calculate Pearson correlation after selecting paired finite values."""

    paired_frame = pd.DataFrame(
        {
            "left": left_series.to_numpy(dtype=float),
            "right": right_series.to_numpy(dtype=float),
        }
    ).dropna()
    return pearson_correlation(
        numeric_column(paired_frame, "left"),
        numeric_column(paired_frame, "right"),
    )


def valid_spearman_correlation(
    left_series: pd.Series, right_series: pd.Series
) -> float:
    """Calculate Spearman correlation after selecting paired finite values."""

    paired_frame = pd.DataFrame(
        {
            "left": left_series.to_numpy(dtype=float),
            "right": right_series.to_numpy(dtype=float),
        }
    ).dropna()
    return spearman_correlation(
        numeric_series(paired_frame, "left"),
        numeric_series(paired_frame, "right"),
    )


def calculate_event_process_correlations(phase_frame: pd.DataFrame) -> pd.DataFrame:
    """Correlate BatchEvict status with local metric fluctuations across lags."""

    correlation_records: list[dict[str, float | int | str]] = []
    for baseline_minutes in BASELINE_SENSITIVITY_MINUTES:
        fluctuation_frame = add_local_metric_fluctuations(phase_frame, baseline_minutes)
        event_values = numeric_series(fluctuation_frame, "batch_evict")
        for lag_steps in EVENT_LAG_STEPS:
            lag_seconds = lag_steps * SAMPLE_INTERVAL_SECONDS
            for metric_name, metric_label in METRIC_LABELS.items():
                fluctuation_column = f"{metric_name}_fluctuation_percent"
                shifted_frame = pd.DataFrame(
                    {
                        "metric_fluctuation": numeric_series(
                            fluctuation_frame, fluctuation_column
                        ).shift(-lag_steps)
                    }
                )
                metric_fluctuations = numeric_series(
                    shifted_frame, "metric_fluctuation"
                )
                correlation_records.append(
                    {
                        "baseline_minutes": baseline_minutes,
                        "lag_seconds": lag_seconds,
                        "metric": metric_name,
                        "metric_label": metric_label,
                        "event_fluctuation_correlation": valid_correlation(
                            event_values, metric_fluctuations
                        ),
                    }
                )
    return pd.DataFrame.from_records(correlation_records)


def bootstrap_event_correlation(
    fluctuation_frame: pd.DataFrame, metric_name: str, seed: int
) -> tuple[float, float]:
    """Bootstrap 30-minute blocks for a zero-lag event correlation interval."""

    fluctuation_column = f"{metric_name}_fluctuation_percent"
    analysis_frame = pd.DataFrame(
        {
            "Time": datetime_column(fluctuation_frame, "Time"),
            "batch_evict": numeric_column(fluctuation_frame, "batch_evict"),
            fluctuation_column: numeric_column(fluctuation_frame, fluctuation_column),
        }
    )
    valid_frame = analysis_frame.dropna()
    time_values = datetime_column(valid_frame, "Time")
    minute_ids = time_values.astype(np.int64)
    block_ids = minute_ids // LOCAL_BASELINE_MINUTES
    event_values = numeric_column(valid_frame, "batch_evict")
    fluctuation_values = numeric_column(valid_frame, fluctuation_column)
    time_blocks: list[np.ndarray] = []
    for block_id in np.unique(block_ids):
        block_mask = block_ids == block_id
        time_blocks.append(
            np.column_stack((event_values[block_mask], fluctuation_values[block_mask]))
        )
    random_generator = np.random.default_rng(seed)
    correlation_samples = np.empty(BOOTSTRAP_REPETITIONS, dtype=float)
    for repetition_index in range(BOOTSTRAP_REPETITIONS):
        sampled_block_indices = random_generator.integers(
            0, len(time_blocks), size=len(time_blocks)
        )
        sampled_values = np.concatenate(
            [time_blocks[block_index] for block_index in sampled_block_indices]
        )
        correlation_samples[repetition_index] = pearson_correlation(
            sampled_values[:, 0], sampled_values[:, 1]
        )
    finite_samples = correlation_samples[np.isfinite(correlation_samples)]
    correlation_lower, correlation_upper = np.quantile(finite_samples, [0.025, 0.975])
    return float(correlation_lower), float(correlation_upper)


def calculate_event_process_summary(
    phase_frame: pd.DataFrame, correlation_frame: pd.DataFrame
) -> pd.DataFrame:
    """Summarize zero-lag, peak-lag, and event/control metric contrasts."""

    fluctuation_frame = add_local_metric_fluctuations(
        phase_frame, LOCAL_BASELINE_MINUTES
    )
    event_mask = fluctuation_frame["batch_evict"].astype(bool)
    selected_correlations = correlation_frame.loc[
        correlation_frame["baseline_minutes"] == LOCAL_BASELINE_MINUTES
    ]
    summary_records: list[dict[str, float | int | str]] = []
    for metric_index, (metric_name, metric_label) in enumerate(METRIC_LABELS.items()):
        metric_correlations = selected_correlations.loc[
            selected_correlations["metric"] == metric_name
        ]
        zero_lag_record = metric_correlations.loc[
            metric_correlations["lag_seconds"] == 0
        ].iloc[0]
        peak_record = metric_correlations.loc[
            metric_correlations["event_fluctuation_correlation"].abs().idxmax()
        ]
        fluctuation_column = f"{metric_name}_fluctuation_percent"
        metric_fluctuations = numeric_series(fluctuation_frame, fluctuation_column)
        raw_metric_values = numeric_series(fluctuation_frame, metric_name)
        correlation_lower, correlation_upper = bootstrap_event_correlation(
            fluctuation_frame,
            metric_name,
            BOOTSTRAP_SEED + 700 + metric_index,
        )
        event_fluctuations = metric_fluctuations.loc[event_mask].dropna()
        control_fluctuations = metric_fluctuations.loc[~event_mask].dropna()
        event_raw_values = raw_metric_values.loc[event_mask]
        control_raw_values = raw_metric_values.loc[~event_mask]
        summary_records.append(
            {
                "metric": metric_name,
                "metric_label": metric_label,
                "event_samples": int(len(event_fluctuations)),
                "control_samples": int(len(control_fluctuations)),
                "zero_lag_correlation": float(
                    zero_lag_record["event_fluctuation_correlation"]
                ),
                "zero_lag_ci_lower": correlation_lower,
                "zero_lag_ci_upper": correlation_upper,
                "peak_absolute_lag_seconds": int(peak_record["lag_seconds"]),
                "peak_absolute_correlation": float(
                    peak_record["event_fluctuation_correlation"]
                ),
                "event_mean_fluctuation_percent": float(event_fluctuations.mean()),
                "control_mean_fluctuation_percent": float(control_fluctuations.mean()),
                "event_minus_control_mean_percent": float(
                    event_fluctuations.mean() - control_fluctuations.mean()
                ),
                "event_median_fluctuation_percent": float(event_fluctuations.median()),
                "control_median_fluctuation_percent": float(
                    control_fluctuations.median()
                ),
                "event_raw_mean_ms": float(event_raw_values.mean()),
                "control_raw_mean_ms": float(control_raw_values.mean()),
                "event_minus_control_raw_mean_ms": float(
                    event_raw_values.mean() - control_raw_values.mean()
                ),
                "event_raw_median_ms": float(event_raw_values.median()),
                "control_raw_median_ms": float(control_raw_values.median()),
                "event_minus_control_raw_median_ms": float(
                    event_raw_values.median() - control_raw_values.median()
                ),
            }
        )
    return pd.DataFrame.from_records(summary_records)


def configure_plot_style() -> None:
    """Set a restrained, publication-oriented Matplotlib style."""

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "legend.fontsize": 8,
            "figure.titlesize": 12,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "grid.alpha": 0.22,
            "grid.linewidth": 0.6,
            "savefig.bbox": "tight",
        }
    )


def add_phase_context(axes: np.ndarray) -> None:
    """Add attachment-phase context to timeline axes."""

    for axis in axes:
        axis.axvspan(
            SHARED_END + pd.Timedelta(seconds=SAMPLE_INTERVAL_SECONDS),
            INDEPENDENT_START,
            color="#D9D9D9",
            alpha=0.5,
            linewidth=0,
        )
        axis.axvline(INDEPENDENT_START, color="#303030", linewidth=0.8, linestyle="--")


def save_figure(figure: plt.Figure, stem: str) -> None:
    """Save a figure in review and publication formats."""

    figure.savefig(FIGURE_DIRECTORY / f"{stem}.png", dpi=220)
    figure.savefig(FIGURE_DIRECTORY / f"{stem}.pdf")
    plt.close(figure)


def draw_process_box(
    axis: Axes,
    center_x: float,
    center_y: float,
    text_value: str,
    face_color: str,
    edge_color: str,
) -> None:
    """Draw one labeled stage in the architecture figure."""

    axis.text(
        center_x,
        center_y,
        text_value,
        ha="center",
        va="center",
        fontsize=8.2,
        linespacing=1.25,
        bbox={
            "boxstyle": "round,pad=0.5",
            "facecolor": face_color,
            "edgecolor": edge_color,
            "linewidth": 1.0,
        },
    )


def draw_process_arrow(
    axis: Axes,
    start_point: tuple[float, float],
    end_point: tuple[float, float],
    color: str = "#4B5563",
    line_style: str = "-",
) -> None:
    """Draw one directional connection in the architecture figure."""

    arrow = FancyArrowPatch(
        start_point,
        end_point,
        arrowstyle="-|>",
        mutation_scale=10,
        linewidth=1.0,
        linestyle=line_style,
        color=color,
        shrinkA=2,
        shrinkB=2,
    )
    axis.add_patch(arrow)


def plot_request_chain_and_contention() -> None:
    """Plot the cache-read path beside the complete BatchEvict lifecycle."""

    configure_plot_style()
    figure, axis = plt.subplots(figsize=(15.5, 8.4))
    axis.set_xlim(0.0, 12.0)
    axis.set_ylim(0.0, 10.0)
    axis.axis("off")

    read_stages = (
        (1.0, "S1  Request admitted\nSGLang L1 lookup"),
        (3.0, "S2  L3 prefix query\nallocate transient L2 slots"),
        (5.0, "S3  Mooncake get\nRealClient::BatchQuery"),
        (7.0, "S4  Master metadata lookup\nshared lock on occupied shard"),
        (9.0, "S5  Replica selected\nRDMA/TCP into L2 staging"),
        (11.0, "S6  Publish L2 prefix\nL2→L1, prefill, first token"),
    )
    eviction_stages = (
        (1.0, "E1  Memory pressure\n10 ms trigger loop"),
        (3.0, "E2  BatchEvict starts\nshared snapshot lock held"),
        (5.0, "E3  Candidate census\nper shard: lock, scan, release"),
        (7.0, "E4  Optional frontier / refill\nnew per-shard lock intervals"),
        (9.0, "E5  Serial candidate apply\nfresh lookup, lock, revalidate"),
        (11.0, "E6  Shrink affected shards\nper-shard lock; cycle ends"),
    )

    axis.text(
        0.05,
        8.65,
        "Ordinary L3 cache read",
        fontsize=10,
        fontweight="bold",
        color="#184E66",
        va="center",
    )
    axis.text(
        0.05,
        3.45,
        "BatchEvict lifecycle",
        fontsize=10,
        fontweight="bold",
        color="#8F3429",
        va="center",
    )

    for stage_x, stage_label in read_stages:
        stage_face_color = "#EAF4F7" if stage_x != 7.0 else "#FFF1D6"
        stage_edge_color = "#176B87" if stage_x != 7.0 else "#B87916"
        draw_process_box(
            axis, stage_x, 7.55, stage_label, stage_face_color, stage_edge_color
        )
    for stage_x, stage_label in eviction_stages:
        draw_process_box(axis, stage_x, 2.35, stage_label, "#FBEDEA", "#B64A3A")

    for left_index in range(len(read_stages) - 1):
        left_x = read_stages[left_index][0]
        right_x = read_stages[left_index + 1][0]
        draw_process_arrow(axis, (left_x + 0.72, 7.55), (right_x - 0.72, 7.55))
        draw_process_arrow(axis, (left_x + 0.72, 2.35), (right_x - 0.72, 2.35))

    axis.annotate(
        "representative E3\nsame-shard collision",
        xy=(6.65, 6.85),
        xytext=(5.15, 3.1),
        ha="center",
        va="bottom",
        fontsize=8.5,
        color=COLORS["event"],
        arrowprops={
            "arrowstyle": "<->",
            "color": COLORS["event"],
            "linewidth": 1.3,
        },
    )
    axis.text(
        8.9,
        5.15,
        "Separate shard-lock critical sections\n"
        "E3: lock one shard → scan → release; repeat.\n"
        "E4–E6 reacquire only the shard being processed.",
        ha="center",
        va="center",
        fontsize=8.7,
        color="#303030",
        bbox={
            "boxstyle": "round,pad=0.45",
            "facecolor": "white",
            "edgecolor": "#9CA3AF",
            "linewidth": 0.9,
        },
    )
    axis.text(
        6.0,
        1.48,
        "worker join\nall census shard locks released",
        ha="center",
        va="center",
        fontsize=7.6,
        color="#4B5563",
        bbox={
            "boxstyle": "round,pad=0.3",
            "facecolor": "#F3F4F6",
            "edgecolor": "#9CA3AF",
            "linewidth": 0.8,
        },
    )

    draw_process_arrow(axis, (4.15, 9.0), (9.85, 9.0), "#176B87")
    axis.text(
        7.0,
        9.25,
        "Measured storage-read interval: metadata RPC + shard wait + replica preparation + payload transfer",
        ha="center",
        va="bottom",
        fontsize=8.5,
        color="#184E66",
    )
    draw_process_arrow(axis, (0.3, 9.65), (11.7, 9.65), "#3B6FB6")
    axis.text(
        6.0,
        9.82,
        "TTFT propagation interval",
        ha="center",
        va="bottom",
        fontsize=8.5,
        color="#3B6FB6",
    )
    axis.text(
        6.0,
        0.55,
        "Snapshot lock: shared for the full cycle and compatible with the read path.  "
        "Census: up to 16 workers scan all 1,024 shards, holding at most one shard lock each; "
        "later phases acquire separate shard locks.",
        ha="center",
        va="center",
        fontsize=8.4,
        color="#4B5563",
    )
    figure.suptitle(
        "SGLang–Mooncake cache-read path and BatchEvict contention boundary",
        y=0.99,
    )
    save_figure(figure, "request_chain_and_contention")


def plot_production_timeline(
    masters: dict[str, pd.DataFrame], metrics_frame: pd.DataFrame
) -> None:
    """Plot the master transition, BatchEvict signatures, and client metrics."""

    configure_plot_style()
    figure, axes = plt.subplots(4, 1, figsize=(13.5, 10.5), sharex=True)
    shared_master = masters["shared"]
    independent_master = masters["independent"]

    axes[0].plot(
        shared_master["Time"],
        shared_master["key_count"] / 1_000_000.0,
        color=COLORS["shared"],
        alpha=0.32,
        linewidth=0.8,
        label="Shared master (full export)",
    )
    axes[0].plot(
        independent_master["Time"],
        independent_master["key_count"] / 1_000_000.0,
        color=COLORS["independent"],
        alpha=0.32,
        linewidth=0.8,
        label="Independent master (full export)",
    )
    for phase in PHASES:
        active_master = select_phase(masters[phase.role], phase)
        event_frame = active_master.loc[active_master["batch_evict"]]
        axes[0].plot(
            active_master["Time"],
            active_master["key_count"] / 1_000_000.0,
            color=COLORS[phase.role],
            linewidth=1.25,
            label=f"{phase.display_name} (client active)",
        )
        axes[0].scatter(
            event_frame["Time"],
            event_frame["key_count"] / 1_000_000.0,
            marker="v",
            s=11,
            color=COLORS["event"],
            linewidths=0,
            zorder=3,
        )
    axes[0].set_ylabel("Keys (millions)")
    axes[0].set_title("(a) Master key inventory and inferred BatchEvict intervals")
    axes[0].legend(ncol=2, loc="upper right")

    for phase in PHASES:
        active_master = select_phase(masters[phase.role], phase)
        event_frame = active_master.loc[active_master["batch_evict"]]
        axes[1].plot(
            active_master["Time"],
            active_master["allocated_memory_gib"],
            color=COLORS[phase.role],
            linewidth=1.1,
            label=phase.display_name,
        )
        axes[1].scatter(
            event_frame["Time"],
            event_frame["allocated_memory_gib"],
            marker="v",
            s=11,
            color=COLORS["event"],
            linewidths=0,
            zorder=3,
        )
    axes[1].set_ylabel("Allocated memory (GiB)")
    axes[1].set_title("(b) Allocated memory on the client-active master")
    axes[1].legend(loc="lower right")

    regular_timeline = pd.date_range(
        metrics_frame["Time"].min(), metrics_frame["Time"].max(), freq="30s"
    )
    regular_metrics = metrics_frame.set_index("Time").reindex(regular_timeline)
    for metric_name, label, color in (
        ("kv_p50_ms", "P50", COLORS["p50"]),
        ("kv_p95_ms", "P95", COLORS["p95"]),
        ("kv_p99_ms", "P99", COLORS["p99"]),
    ):
        axes[2].plot(
            regular_metrics.index,
            regular_metrics[metric_name],
            label=label,
            color=color,
            linewidth=0.9,
        )
    axes[2].set_ylabel("Latency (ms)")
    axes[2].set_title("(c) Client KV-transfer latency")
    axes[2].legend(ncol=3, loc="upper right")

    for metric_name, label, color in (
        ("ttft_avg_ms", "Average", COLORS["average"]),
        ("ttft_p95_ms", "P95", COLORS["p95"]),
        ("ttft_p99_ms", "P99", COLORS["p99"]),
    ):
        axes[3].plot(
            regular_metrics.index,
            regular_metrics[metric_name] / 1_000.0,
            label=label,
            color=color,
            linewidth=0.9,
        )
    axes[3].set_ylabel("TTFT (s)")
    axes[3].set_title("(d) Client time to first token")
    axes[3].legend(ncol=3, loc="upper right")
    axes[3].set_xlabel("Local time (Asia/Shanghai), 2026-09-10 to 2026-09-11")

    add_phase_context(axes)
    axes[0].annotate(
        "Cutover 15:52:30",
        xy=(INDEPENDENT_START, 0.04),
        xycoords=("data", "axes fraction"),
        xytext=(8, 2),
        textcoords="offset points",
        color="#303030",
        fontsize=8,
    )
    locator = mdates.AutoDateLocator(minticks=7, maxticks=12)
    axes[3].xaxis.set_major_locator(locator)
    axes[3].xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    figure.suptitle(
        "Mooncake client transition and production latency, 30-second observations",
        y=0.995,
    )
    figure.tight_layout(rect=(0, 0, 1, 0.985))
    save_figure(figure, "production_timeline")


def plot_association_summary(
    lag_frame: pd.DataFrame, association_frame: pd.DataFrame
) -> None:
    """Plot lag correlations and adjusted BatchEvict effect estimates."""

    configure_plot_style()
    figure = plt.figure(figsize=(13.5, 8.2))
    grid = figure.add_gridspec(
        2, 2, height_ratios=(1.0, 1.18), hspace=0.42, wspace=0.22
    )
    kv_axis = figure.add_subplot(grid[0, 0])
    ttft_axis = figure.add_subplot(grid[0, 1])
    effect_axis = figure.add_subplot(grid[1, :])

    plot_groups = (
        (
            kv_axis,
            ("kv_p50_ms", "kv_p95_ms", "kv_p99_ms"),
            "(a) KV-transfer correlation around BatchEvict",
        ),
        (
            ttft_axis,
            ("ttft_avg_ms", "ttft_p95_ms", "ttft_p99_ms"),
            "(b) TTFT correlation around BatchEvict",
        ),
    )
    metric_colors = {
        "kv_p50_ms": COLORS["p50"],
        "kv_p95_ms": COLORS["p95"],
        "kv_p99_ms": COLORS["p99"],
        "ttft_avg_ms": COLORS["average"],
        "ttft_p95_ms": COLORS["p95"],
        "ttft_p99_ms": COLORS["p99"],
    }
    for axis, metric_names, title in plot_groups:
        for metric_name in metric_names:
            metric_lags = lag_frame.loc[lag_frame["metric"] == metric_name]
            axis.plot(
                metric_lags["lag_seconds"] / 60.0,
                metric_lags["adjusted_correlation"],
                marker="o",
                markersize=2.8,
                linewidth=1.0,
                color=metric_colors[metric_name],
                label=METRIC_LABELS[metric_name]
                .replace("KV transfer ", "")
                .replace("TTFT ", ""),
            )
        axis.axhline(0.0, color="#303030", linewidth=0.7)
        axis.axvline(0.0, color=COLORS["event"], linewidth=0.8, linestyle="--")
        axis.set_xlim(-4.0, 4.0)
        axis.set_ylim(-0.075, 0.075)
        axis.set_xlabel("Metric lag relative to event (minutes)")
        axis.set_ylabel("15-minute adjusted correlation, r")
        axis.set_title(title)
        axis.legend(ncol=3, loc="upper left")

    independent_associations = association_frame.loc[
        association_frame["phase"] == "independent"
    ].copy()
    ordered_metrics = list(METRIC_LABELS)
    independent_associations["order"] = independent_associations["metric"].map(
        {metric_name: index for index, metric_name in enumerate(ordered_metrics)}
    )
    ordered_associations = independent_associations.sort_values("order")
    y_positions = np.arange(len(ordered_associations))
    adjusted_effects = ordered_associations["adjusted_effect_percent"].to_numpy()
    lower_errors = (
        adjusted_effects
        - ordered_associations["adjusted_effect_ci_lower_percent"].to_numpy()
    )
    upper_errors = (
        ordered_associations["adjusted_effect_ci_upper_percent"].to_numpy()
        - adjusted_effects
    )
    effect_axis.errorbar(
        adjusted_effects,
        y_positions,
        xerr=np.vstack((lower_errors, upper_errors)),
        fmt="o",
        color=COLORS["independent"],
        ecolor=COLORS["independent"],
        capsize=3,
        markersize=5,
        linewidth=1.1,
        label="15-minute adjusted estimate (95% bootstrap CI)",
    )
    effect_axis.scatter(
        ordered_associations["raw_effect_percent"],
        y_positions,
        marker="D",
        facecolors="white",
        edgecolors=COLORS["event"],
        s=34,
        linewidths=1.1,
        label="Unadjusted phase contrast",
        zorder=3,
    )
    effect_axis.axvline(0.0, color="#303030", linewidth=0.8)
    effect_axis.set_yticks(y_positions)
    effect_axis.set_yticklabels(ordered_associations["metric_label"])
    effect_axis.invert_yaxis()
    effect_axis.set_xlabel("Event-associated change relative to non-event baseline (%)")
    effect_axis.set_title(
        "(c) Independent-master BatchEvict effect estimates at zero lag"
    )
    effect_axis.legend(loc="lower right")

    figure.suptitle(
        "BatchEvict association after adjustment for 15-minute production strata",
        y=0.995,
    )
    save_figure(figure, "batchevict_association")


def plot_pressure_association(
    window_frame: pd.DataFrame, pressure_contrast_frame: pd.DataFrame
) -> None:
    """Plot wider-window pressure correlations and production-regime contrasts."""

    configure_plot_style()
    figure, axes = plt.subplots(2, 2, figsize=(13.5, 9.0))
    metric_colors = {
        "kv_p50_ms": COLORS["p50"],
        "kv_p95_ms": COLORS["p95"],
        "kv_p99_ms": COLORS["p99"],
        "ttft_avg_ms": COLORS["average"],
        "ttft_p95_ms": COLORS["p95"],
        "ttft_p99_ms": COLORS["p99"],
    }
    plot_groups = (
        (
            axes[0, 0],
            ("kv_p50_ms", "kv_p95_ms", "kv_p99_ms"),
            "(a) KV-transfer association with recent eviction density",
        ),
        (
            axes[0, 1],
            ("ttft_avg_ms", "ttft_p95_ms", "ttft_p99_ms"),
            "(b) TTFT association with recent eviction density",
        ),
    )
    for axis, metric_names, title in plot_groups:
        for metric_name in metric_names:
            metric_windows = window_frame.loc[window_frame["metric"] == metric_name]
            short_label = (
                METRIC_LABELS[metric_name]
                .replace("KV transfer ", "")
                .replace("TTFT ", "")
            )
            axis.plot(
                metric_windows["window_minutes"],
                metric_windows["raw_pearson_correlation"],
                marker="o",
                markersize=3.4,
                linewidth=1.2,
                color=metric_colors[metric_name],
                label=f"{short_label}: whole phase",
            )
            axis.plot(
                metric_windows["window_minutes"],
                metric_windows["within_hour_pearson_correlation"],
                marker="s",
                markersize=2.8,
                linewidth=0.9,
                linestyle="--",
                color=metric_colors[metric_name],
                alpha=0.85,
                label=f"{short_label}: within hour",
            )
        axis.axhline(0.0, color="#303030", linewidth=0.7)
        axis.set_xscale("log")
        axis.set_xlim(0.8, 70.0)
        axis.set_xticks(WINDOW_MINUTES)
        axis.set_xticklabels([str(window) for window in WINDOW_MINUTES])
        axis.set_xlabel("Trailing BatchEvict-count window (minutes)")
        axis.set_ylabel("Pearson correlation, r")
        axis.set_title(title)
        axis.legend(ncol=2, loc="best", fontsize=7.2)

    selected_window_frame = window_frame.loc[
        window_frame["window_minutes"] == PRESSURE_WINDOW_MINUTES
    ].copy()
    ordered_metrics = list(METRIC_LABELS)
    selected_window_frame["order"] = selected_window_frame["metric"].map(
        {metric_name: index for index, metric_name in enumerate(ordered_metrics)}
    )
    ordered_window_frame = selected_window_frame.sort_values("order")
    x_positions = np.arange(len(ordered_window_frame))
    bar_width = 0.36
    axes[1, 0].bar(
        x_positions - bar_width / 2,
        ordered_window_frame["raw_pearson_correlation"],
        width=bar_width,
        color=COLORS["independent"],
        label="Whole independent-master phase",
    )
    axes[1, 0].bar(
        x_positions + bar_width / 2,
        ordered_window_frame["within_hour_pearson_correlation"],
        width=bar_width,
        color="#8CB8C8",
        label="After one-hour demeaning",
    )
    axes[1, 0].axhline(0.0, color="#303030", linewidth=0.7)
    axes[1, 0].set_xticks(x_positions)
    axes[1, 0].set_xticklabels(
        [
            label.replace("KV transfer ", "KV ").replace("TTFT average", "TTFT avg")
            for label in ordered_window_frame["metric_label"]
        ],
        rotation=25,
        ha="right",
    )
    axes[1, 0].set_ylabel("Pearson correlation, r")
    axes[1, 0].set_title("(c) Thirty-minute pressure association")
    axes[1, 0].legend(loc="upper left")

    ordered_contrasts = pressure_contrast_frame.copy()
    metric_order = {
        metric_name: index for index, metric_name in enumerate(ordered_metrics)
    }
    ordered_contrasts["order"] = ordered_contrasts["metric"].map(
        lambda metric_name: metric_order[str(metric_name)]
    )
    ordered_contrasts = ordered_contrasts.sort_values("order")
    ratio_values = ordered_contrasts["high_to_low_ratio"].to_numpy(dtype=float)
    ratio_lower_errors = ratio_values - ordered_contrasts["ratio_ci_lower"].to_numpy(
        dtype=float
    )
    ratio_upper_errors = (
        ordered_contrasts["ratio_ci_upper"].to_numpy(dtype=float) - ratio_values
    )
    ratio_colors = [
        COLORS["p50"],
        COLORS["p95"],
        COLORS["p99"],
        COLORS["average"],
        COLORS["p95"],
        COLORS["p99"],
    ]
    axes[1, 1].bar(
        x_positions,
        ratio_values,
        yerr=np.vstack((ratio_lower_errors, ratio_upper_errors)),
        color=ratio_colors,
        alpha=0.9,
        capsize=3,
        error_kw={"elinewidth": 0.9},
    )
    axes[1, 1].axhline(1.0, color="#303030", linewidth=0.8)
    axes[1, 1].set_xticks(x_positions)
    axes[1, 1].set_xticklabels(
        [
            label.replace("KV transfer ", "KV ").replace("TTFT average", "TTFT avg")
            for label in ordered_contrasts["metric_label"]
        ],
        rotation=25,
        ha="right",
    )
    axes[1, 1].set_ylabel("High-pressure / low-pressure median")
    axes[1, 1].set_title("(d) Thirty-minute production-pressure contrast")
    for bar_patch, ratio_value in zip(axes[1, 1].patches, ratio_values, strict=True):
        axes[1, 1].text(
            bar_patch.get_x() + bar_patch.get_width() / 2,
            ratio_value + 0.05,
            f"{ratio_value:.2f}×",
            ha="center",
            va="bottom",
            fontsize=8,
        )

    figure.suptitle(
        "Independent-master latency across wider BatchEvict pressure windows",
        y=0.995,
    )
    figure.tight_layout(rect=(0, 0, 1, 0.975))
    save_figure(figure, "batchevict_association")


def plot_eviction_pressure_regions(phase_frame: pd.DataFrame) -> None:
    """Align 30-minute eviction-pressure regions with transfer latency and TTFT."""

    configure_plot_style()
    pressure_frame = add_eviction_pressure(phase_frame, PRESSURE_WINDOW_MINUTES)
    pressure_frame["pressure_regime"] = pressure_frame["recent_batch_evict_count"].map(
        classify_pressure_regime
    )
    low_pressure_frame = pressure_frame.loc[pressure_frame["pressure_regime"] == "low"]
    high_pressure_frame = pressure_frame.loc[
        pressure_frame["pressure_regime"] == "high"
    ]
    low_region_end = low_pressure_frame["Time"].max() + pd.Timedelta(
        seconds=SAMPLE_INTERVAL_SECONDS
    )
    high_region_start = high_pressure_frame.loc[
        high_pressure_frame["Time"] >= low_region_end, "Time"
    ].min()
    timeline_start = pressure_frame["Time"].min()
    timeline_end = pressure_frame["Time"].max()

    figure, axes = plt.subplots(3, 1, figsize=(13.5, 9.0), sharex=True)
    region_definitions = (
        (timeline_start, low_region_end, "Predominantly low", "#DDEFE4"),
        (low_region_end, high_region_start, "Sustained transition", "#FFF0C7"),
        (high_region_start, timeline_end, "Predominantly high", "#F5D8D3"),
    )
    for axis in axes:
        for region_start, region_end, _, region_color in region_definitions:
            axis.axvspan(
                region_start,
                region_end,
                color=region_color,
                alpha=0.52,
                linewidth=0,
                zorder=0,
            )

    axes[0].plot(
        pressure_frame["Time"],
        pressure_frame["recent_batch_evict_count"],
        color=COLORS["event"],
        linewidth=1.3,
        label="BatchEvict signatures in prior 30 minutes",
    )
    event_frame = pressure_frame.loc[pressure_frame["batch_evict"]]
    axes[0].scatter(
        event_frame["Time"],
        np.full(len(event_frame), -0.6),
        marker="|",
        s=14,
        color="#7F1D1D",
        linewidths=0.65,
        label="Inferred 30-second BatchEvict signature",
    )
    axes[0].axhline(
        LOW_PRESSURE_MAX_EVENTS,
        color="#4B7F61",
        linewidth=0.8,
        linestyle="--",
    )
    axes[0].axhline(
        HIGH_PRESSURE_MIN_EVENTS,
        color="#A33E32",
        linewidth=0.8,
        linestyle="--",
    )
    axes[0].set_ylim(-1.2, 17.5)
    axes[0].set_ylabel("Events / 30 min")
    axes[0].set_title("(a) Independent-master BatchEvict pressure state", loc="left")
    axes[0].legend(loc="upper left", ncol=2)

    time_indexed_frame = pressure_frame.set_index("Time")
    rolling_metrics = (
        time_indexed_frame[list(METRIC_LABELS)]
        .rolling(f"{PRESSURE_WINDOW_MINUTES}min", closed="right")
        .median()
    )
    for metric_name, short_label, metric_color in (
        ("kv_p50_ms", "P50", COLORS["p50"]),
        ("kv_p95_ms", "P95", COLORS["p95"]),
        ("kv_p99_ms", "P99", COLORS["p99"]),
    ):
        axes[1].plot(
            rolling_metrics.index,
            rolling_metrics[metric_name],
            color=metric_color,
            linewidth=1.25,
            label=short_label,
        )
    axes[1].set_ylabel("Latency (ms)")
    axes[1].set_title("(b) KV-transfer latency, 30-minute rolling median")
    axes[1].legend(loc="upper left", ncol=3)
    axes[1].annotate(
        "P99 median: 91 → 368 ms",
        xy=(high_region_start, 368.0),
        xytext=(12, -32),
        textcoords="offset points",
        fontsize=8.5,
        color=COLORS["p99"],
        arrowprops={"arrowstyle": "->", "color": COLORS["p99"], "linewidth": 0.8},
    )

    for metric_name, short_label, metric_color in (
        ("ttft_avg_ms", "Average", COLORS["average"]),
        ("ttft_p95_ms", "P95", COLORS["p95"]),
        ("ttft_p99_ms", "P99", COLORS["p99"]),
    ):
        axes[2].plot(
            rolling_metrics.index,
            rolling_metrics[metric_name] / 1_000.0,
            color=metric_color,
            linewidth=1.25,
            label=short_label,
        )
    axes[2].set_ylabel("TTFT (s)")
    axes[2].set_title("(c) TTFT, 30-minute rolling median")
    axes[2].legend(loc="upper left", ncol=3)
    axes[2].annotate(
        "P99 median: 1.96 → 7.65 s",
        xy=(high_region_start, 7.65),
        xytext=(12, -34),
        textcoords="offset points",
        fontsize=8.5,
        color=COLORS["p99"],
        arrowprops={"arrowstyle": "->", "color": COLORS["p99"], "linewidth": 0.8},
    )
    axes[2].set_xlabel("Local time (Asia/Shanghai), 2026-09-10 to 2026-09-11")

    for region_start, region_end, region_label, _ in region_definitions:
        region_midpoint = region_start + (region_end - region_start) / 2
        axes[0].text(
            region_midpoint,
            0.10,
            region_label,
            transform=axes[0].get_xaxis_transform(),
            ha="center",
            va="bottom",
            fontsize=8.5,
            color="#303030",
            bbox={
                "boxstyle": "round,pad=0.2",
                "facecolor": "white",
                "edgecolor": "none",
                "alpha": 0.7,
            },
        )
    locator = mdates.AutoDateLocator(minticks=7, maxticks=12)
    axes[2].xaxis.set_major_locator(locator)
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    figure.suptitle(
        "Eviction-pressure regions and contemporaneous client latency",
        y=0.995,
    )
    figure.tight_layout(rect=(0, 0, 1, 0.98))
    save_figure(figure, "eviction_pressure_regions")


def plot_eviction_process_fluctuations(phase_frame: pd.DataFrame) -> None:
    """Align event status with fluctuations in a 30-minute production fragment."""

    configure_plot_style()
    complete_fluctuation_frame = add_local_metric_fluctuations(
        phase_frame, LOCAL_BASELINE_MINUTES
    )
    fragment_end = HIGH_PRESSURE_FRAGMENT_START + timedelta(
        minutes=HIGH_PRESSURE_FRAGMENT_MINUTES
    )
    fluctuation_frame = complete_fluctuation_frame.loc[
        (complete_fluctuation_frame["Time"] >= HIGH_PRESSURE_FRAGMENT_START)
        & (complete_fluctuation_frame["Time"] < fragment_end)
    ].copy()

    figure = plt.figure(figsize=(13.5, 7.4), layout="constrained")
    grid = figure.add_gridspec(3, 1, height_ratios=(0.25, 1.0, 1.0), hspace=0.12)
    status_axis = figure.add_subplot(grid[0, 0])
    transfer_axis = figure.add_subplot(grid[1, 0], sharex=status_axis)
    ttft_axis = figure.add_subplot(grid[2, 0], sharex=status_axis)

    status_axis.fill_between(
        fluctuation_frame["Time"],
        0.0,
        fluctuation_frame["batch_evict"].astype(float),
        step="mid",
        color=COLORS["event"],
        alpha=0.9,
    )
    status_axis.set_ylim(0.0, 1.05)
    status_axis.set_yticks([0.5])
    status_axis.set_yticklabels(["BatchEvict\nsignature"])
    status_axis.grid(False)
    status_axis.tick_params(axis="x", labelbottom=False)
    status_axis.set_title(
        "(a) Inferred eviction-process status at 30-second resolution", loc="left"
    )

    metric_groups = (
        (
            transfer_axis,
            ("kv_p50_ms", "kv_p95_ms", "kv_p99_ms"),
            ("P50", "P95", "P99"),
            "(b) KV-transfer fluctuations around a 30-minute local baseline",
        ),
        (
            ttft_axis,
            ("ttft_avg_ms", "ttft_p95_ms", "ttft_p99_ms"),
            ("Average", "P95", "P99"),
            "(c) TTFT fluctuations around a 30-minute local baseline",
        ),
    )
    timeline_values = mdates.date2num(fluctuation_frame["Time"])
    half_sample_days = SAMPLE_INTERVAL_SECONDS / (2.0 * 86_400.0)
    image_extent = (
        timeline_values[0] - half_sample_days,
        timeline_values[-1] + half_sample_days,
        0.0,
        3.0,
    )
    fluctuation_norm = SymLogNorm(
        linthresh=5.0,
        linscale=0.8,
        vmin=-300.0,
        vmax=300.0,
        base=10.0,
    )
    metric_image = None
    for axis, metric_names, short_labels, title in metric_groups:
        fluctuation_matrix = np.vstack(
            [
                numeric_column(fluctuation_frame, f"{metric_name}_fluctuation_percent")
                for metric_name in metric_names
            ]
        )
        metric_image = axis.imshow(
            fluctuation_matrix,
            aspect="auto",
            origin="lower",
            interpolation="nearest",
            extent=image_extent,
            cmap="RdBu_r",
            norm=fluctuation_norm,
        )
        axis.set_yticks(np.arange(3) + 0.5)
        axis.set_yticklabels(short_labels)
        axis.grid(False)
        axis.set_title(title, loc="left")

    if metric_image is not None:
        color_bar = figure.colorbar(
            metric_image,
            ax=[transfer_axis, ttft_axis],
            location="right",
            shrink=0.88,
            pad=0.015,
        )
        color_bar.set_label("Deviation from local median (%)")

    ttft_axis.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    ttft_axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ttft_axis.set_xlim(
        float(mdates.date2num(HIGH_PRESSURE_FRAGMENT_START)),
        float(mdates.date2num(fragment_end)),
    )
    ttft_axis.set_xlabel("Local time (Asia/Shanghai), 2026-09-11")
    figure.suptitle(
        "High-pressure independent-master fragment: eviction status and latency fluctuations",
        y=1.02,
    )
    save_figure(figure, "eviction_process_fluctuations")


def plot_high_pressure_fragment(phase_frame: pd.DataFrame) -> None:
    """Plot a representative 30 minutes of the high-pressure interval."""

    fragment_end = HIGH_PRESSURE_FRAGMENT_START + timedelta(
        minutes=HIGH_PRESSURE_FRAGMENT_MINUTES
    )
    fragment_frame = phase_frame.loc[
        (phase_frame["Time"] >= HIGH_PRESSURE_FRAGMENT_START)
        & (phase_frame["Time"] < fragment_end)
    ].copy()
    event_frame = fragment_frame.loc[fragment_frame["batch_evict"]]

    figure, axes = plt.subplots(
        3,
        1,
        figsize=(14.0, 8.6),
        sharex=True,
        gridspec_kw={"height_ratios": (0.55, 2.3, 2.3)},
        layout="constrained",
    )

    axes[0].vlines(
        event_frame["Time"],
        0.0,
        1.0,
        color=COLORS["event"],
        linewidth=4.0,
    )
    axes[0].set_ylim(0.0, 1.0)
    axes[0].set_yticks([0.5], ["BatchEvict\nsignature"])
    axes[0].set_title("(a) Inferred eviction-process status", loc="left")

    for axis in axes[1:]:
        for event_time in event_frame["Time"]:
            axis.axvspan(
                event_time,
                event_time + pd.Timedelta(seconds=SAMPLE_INTERVAL_SECONDS),
                color=COLORS["event"],
                alpha=0.11,
                linewidth=0.0,
            )

    for metric_name, percentile_name in (
        ("kv_p50_ms", "p50"),
        ("kv_p95_ms", "p95"),
        ("kv_p99_ms", "p99"),
    ):
        axes[1].plot(
            fragment_frame["Time"],
            fragment_frame[metric_name],
            color=COLORS[percentile_name],
            linewidth=1.3,
            marker="o",
            markersize=2.4,
            label=percentile_name.upper(),
        )
    axes[1].set_ylabel("KV-transfer latency (ms)")
    axes[1].set_title("(b) Composite Mooncake storage-read latency", loc="left")
    axes[1].legend(ncol=3, loc="upper left")

    for metric_name, line_name, color_name in (
        ("ttft_avg_ms", "Average", "average"),
        ("ttft_p95_ms", "P95", "p95"),
        ("ttft_p99_ms", "P99", "p99"),
    ):
        axes[2].plot(
            fragment_frame["Time"],
            fragment_frame[metric_name] / 1_000.0,
            color=COLORS[color_name],
            linewidth=1.3,
            marker="o",
            markersize=2.4,
            label=line_name,
        )
    axes[2].set_ylabel("TTFT (s)")
    axes[2].set_title("(c) Client time to first token", loc="left")
    axes[2].legend(ncol=3, loc="upper left")
    axes[2].xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    axes[2].set_xlabel("Local time (Asia/Shanghai), 2026-09-11")

    for axis in axes:
        axis.grid(axis="y", alpha=0.22)
        axis.set_xlim(HIGH_PRESSURE_FRAGMENT_START, fragment_end)

    figure.suptitle(
        "Representative 30-minute fragment of the high-pressure independent-master interval",
        y=1.015,
    )
    save_figure(figure, "high_pressure_30min_fragment")


def plot_event_process_correlation(
    correlation_frame: pd.DataFrame,
    summary_frame: pd.DataFrame,
) -> None:
    """Plot event-aligned correlations and event/control contrasts."""

    configure_plot_style()
    figure, axes = plt.subplots(2, 2, figsize=(13.5, 8.8))
    selected_correlations = correlation_frame.loc[
        correlation_frame["baseline_minutes"] == LOCAL_BASELINE_MINUTES
    ]
    metric_colors = {
        "kv_p50_ms": COLORS["p50"],
        "kv_p95_ms": COLORS["p95"],
        "kv_p99_ms": COLORS["p99"],
        "ttft_avg_ms": COLORS["average"],
        "ttft_p95_ms": COLORS["p95"],
        "ttft_p99_ms": COLORS["p99"],
    }
    lag_groups = (
        (
            axes[0, 0],
            ("kv_p50_ms", "kv_p95_ms", "kv_p99_ms"),
            "(a) KV-transfer fluctuation correlation",
        ),
        (
            axes[0, 1],
            ("ttft_avg_ms", "ttft_p95_ms", "ttft_p99_ms"),
            "(b) TTFT fluctuation correlation",
        ),
    )
    for axis, metric_names, title in lag_groups:
        for metric_name in metric_names:
            metric_lags = selected_correlations.loc[
                selected_correlations["metric"] == metric_name
            ]
            short_label = (
                METRIC_LABELS[metric_name]
                .replace("KV transfer ", "")
                .replace("TTFT ", "")
            )
            axis.plot(
                metric_lags["lag_seconds"] / 60.0,
                metric_lags["event_fluctuation_correlation"],
                marker="o",
                markersize=3.0,
                linewidth=1.0,
                color=metric_colors[metric_name],
                label=short_label,
            )
        axis.axhline(0.0, color="#303030", linewidth=0.7)
        axis.axvline(0.0, color=COLORS["event"], linewidth=0.8, linestyle="--")
        axis.set_xlim(-4.0, 4.0)
        axis.set_ylim(-0.11, 0.11)
        axis.set_xlabel("Metric lag from BatchEvict signature (minutes)")
        axis.set_ylabel("Event/fluctuation correlation, r")
        axis.set_title(title)
        axis.legend(ncol=3, loc="upper left")

    ordered_summary = summary_frame.copy()
    ordered_summary["order"] = ordered_summary["metric"].map(
        lambda metric_name: list(METRIC_LABELS).index(str(metric_name))
    )
    ordered_summary = ordered_summary.sort_values("order")
    y_positions = np.arange(len(ordered_summary))
    zero_correlations = ordered_summary["zero_lag_correlation"].to_numpy(dtype=float)
    lower_errors = zero_correlations - ordered_summary["zero_lag_ci_lower"].to_numpy(
        dtype=float
    )
    upper_errors = (
        ordered_summary["zero_lag_ci_upper"].to_numpy(dtype=float) - zero_correlations
    )
    axes[1, 0].errorbar(
        zero_correlations,
        y_positions,
        xerr=np.vstack((lower_errors, upper_errors)),
        fmt="o",
        color=COLORS["independent"],
        capsize=3,
        linewidth=1.0,
    )
    axes[1, 0].axvline(0.0, color="#303030", linewidth=0.8)
    axes[1, 0].set_yticks(y_positions)
    axes[1, 0].set_yticklabels(ordered_summary["metric_label"])
    axes[1, 0].invert_yaxis()
    axes[1, 0].set_xlabel("Zero-lag correlation, r (95% block-bootstrap interval)")
    axes[1, 0].set_title("(c) Exact-bin event association")

    effect_positions = np.arange(len(ordered_summary))
    effect_values = ordered_summary["event_minus_control_mean_percent"].to_numpy(
        dtype=float
    )
    effect_colors = [
        metric_colors[str(metric_name)] for metric_name in ordered_summary["metric"]
    ]
    axes[1, 1].bar(
        effect_positions,
        effect_values,
        color=effect_colors,
    )
    axes[1, 1].axhline(0.0, color="#303030", linewidth=0.7)
    axes[1, 1].set_xticks(effect_positions)
    axes[1, 1].set_xticklabels(ordered_summary["metric_label"], rotation=25, ha="right")
    axes[1, 1].set_ylabel("Event minus control mean fluctuation (percentage points)")
    axes[1, 1].set_title("(d) Exact-bin event/control contrast")

    figure.suptitle(
        "High-pressure interval: BatchEvict correlation with local latency fluctuations",
        y=0.995,
    )
    figure.tight_layout(rect=(0, 0, 1, 0.975))
    save_figure(figure, "event_process_correlation")


def main() -> None:
    """Write the deployment context table and architecture figures."""

    FIGURE_DIRECTORY.mkdir(exist_ok=True)
    RESULT_DIRECTORY.mkdir(exist_ok=True)

    metrics_frame = load_metrics()
    masters = {phase.role: load_master(phase.role) for phase in PHASES}

    phase_summary_records: list[dict[str, float | int | str]] = []
    for phase in PHASES:
        master_frame = masters[phase.role]
        phase_summary_records.append(
            calculate_phase_summary(phase, master_frame, metrics_frame)
        )

    phase_summary_frame = pd.DataFrame.from_records(phase_summary_records)
    phase_summary_frame.to_csv(RESULT_DIRECTORY / "phase_summary.csv", index=False)
    plot_request_chain_and_contention()
    plot_production_timeline(masters, metrics_frame)
    independent_master = select_phase(masters["independent"], PHASES[1])
    independent_phase_frame = independent_master.merge(
        metrics_frame, on="Time", how="inner"
    )
    high_pressure_frame = independent_phase_frame.loc[
        independent_phase_frame["Time"] >= HIGH_PRESSURE_START
    ].copy()
    plot_high_pressure_fragment(high_pressure_frame)
    high_event_correlation_frame = calculate_event_process_correlations(
        high_pressure_frame
    )
    high_event_summary_frame = calculate_event_process_summary(
        high_pressure_frame, high_event_correlation_frame
    )
    high_event_correlation_frame.to_csv(
        RESULT_DIRECTORY / "event_process_correlations_high_pressure.csv",
        index=False,
    )
    high_event_summary_frame.to_csv(
        RESULT_DIRECTORY / "event_process_summary_high_pressure.csv", index=False
    )
    plot_event_process_correlation(
        high_event_correlation_frame,
        high_event_summary_frame,
    )


if __name__ == "__main__":
    main()
