"""Focused checks for the production BatchEvict analysis."""

from __future__ import annotations

import unittest

import numpy as np

from analyze_batchevict import (
    PHASES,
    load_master,
    load_metrics,
    numeric_column,
    parse_duration_ms,
    parse_memory_gib,
    select_phase,
)


class UnitParsingTests(unittest.TestCase):
    """Validate exported-unit normalization."""

    def test_duration_normalization(self) -> None:
        self.assertEqual(parse_duration_ms("50 ms"), 50.0)
        self.assertEqual(parse_duration_ms("1.76 s"), 1_760.0)

    def test_memory_normalization(self) -> None:
        self.assertEqual(parse_memory_gib("667 GiB"), 667.0)
        self.assertEqual(parse_memory_gib("1024 MiB"), 1.0)
        self.assertEqual(parse_memory_gib("0 B"), 0.0)


class DatasetAnalysisTests(unittest.TestCase):
    """Validate the event signature and client cutover boundaries."""

    def test_phase_event_counts(self) -> None:
        expected_counts = {"shared": 48, "independent": 315}
        for phase in PHASES:
            master_frame = load_master(phase.role)
            active_master = select_phase(master_frame, phase)
            observed_count = int(
                np.count_nonzero(numeric_column(active_master, "batch_evict"))
            )
            self.assertEqual(observed_count, expected_counts[phase.role])

    def test_independent_event_population_is_separated(self) -> None:
        independent_master = select_phase(load_master("independent"), PHASES[1])
        contraction_frame = independent_master.loc[independent_master["key_change"] < 0]
        inferred_events = contraction_frame.loc[contraction_frame["batch_evict"]]
        other_contractions = contraction_frame.loc[~contraction_frame["batch_evict"]]
        minimum_event_size = float(
            np.min(numeric_column(inferred_events, "evicted_keys"))
        )
        maximum_other_contraction = float(
            np.max(numeric_column(other_contractions, "evicted_keys"))
        )
        self.assertGreaterEqual(minimum_event_size, 55_000)
        self.assertLessEqual(maximum_other_contraction, 26_000)

    def test_metrics_resume_at_independent_phase_start(self) -> None:
        metrics_frame = load_metrics()
        independent_start = PHASES[1].start
        self.assertIn(independent_start, set(metrics_frame["Time"]))


if __name__ == "__main__":
    unittest.main()
