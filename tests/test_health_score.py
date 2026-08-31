import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from intelligence.health_score import calculate_sustainability_score


BASELINE = {
    "run_count": 12,
    "duration_seconds_mean": 100.0,
    "total_energy_kwh_mean": 10.0,
    "total_carbon_kg_mean": 5.0,
}


def make_current(energy=10.0, carbon=5.0, duration=100.0, status="success", overhead=0.0):
    return pd.DataFrame(
        [
            {
                "stage": "release",
                "duration_seconds": duration,
                "total_energy_kwh": energy,
                "total_carbon_kg": carbon,
                "overhead_percentage": overhead,
                "infrastructure_overhead_seconds": 0.0,
                "status": status,
            }
        ]
    )


def score_for_ratio(ratio):
    return calculate_sustainability_score(
        make_current(energy=10.0 * ratio, carbon=5.0 * ratio, duration=100.0 * ratio),
        BASELINE,
        [],
    )["score"]


def test_run_exactly_at_baseline_uses_weighted_component_starting_score():
    health = calculate_sustainability_score(make_current(), BASELINE, [])
    assert health["score"] == 100
    assert health["grade"] == "Excellent"
    assert health["status"] == "Normal"


def test_run_10_percent_above_baseline_scores_continuously():
    assert score_for_ratio(1.10) == 90


def test_run_20_percent_above_baseline_scores_continuously():
    assert score_for_ratio(1.20) == 80


def test_run_30_percent_above_baseline_scores_continuously():
    assert score_for_ratio(1.30) == 65


def test_run_50_percent_above_baseline_scores_continuously():
    assert score_for_ratio(1.50) == 40


def test_run_75_percent_above_baseline_scores_continuously():
    assert score_for_ratio(1.75) == 20


def test_run_100_percent_or_more_above_baseline_scores_zero():
    assert score_for_ratio(2.00) == 0
    assert score_for_ratio(2.50) == 0


def test_run_below_baseline_does_not_score_above_component_cap():
    assert score_for_ratio(0.75) == 100


def test_energy_carbon_and_duration_can_have_different_deviations():
    health = calculate_sustainability_score(
        make_current(energy=15.0, carbon=5.0, duration=120.0),
        BASELINE,
        [],
    )
    assert health["score"] == 72
    assert health["status"] == "Warning"
    assert "energy is above historical baseline" not in health["explanation"]
    assert "energy is above baseline" in health["explanation"]


def test_anomaly_penalties_still_apply_after_continuous_score():
    anomalies = [
        {"severity": "critical", "message": "Release energy is high."},
        {"severity": "warning", "message": "Deploy duration is elevated."},
    ]
    health = calculate_sustainability_score(make_current(), BASELINE, anomalies)
    assert health["score"] == 78
    assert health["status"] == "Critical"


def test_failed_stage_penalty_still_applies_after_continuous_score():
    health = calculate_sustainability_score(make_current(status="failed"), BASELINE, [])
    assert health["score"] == 80
    assert health["status"] == "Critical"


def test_high_overhead_penalty_still_applies_after_continuous_score():
    warning_health = calculate_sustainability_score(make_current(overhead=75.0), BASELINE, [])
    critical_health = calculate_sustainability_score(make_current(overhead=90.0), BASELINE, [])
    assert warning_health["score"] == 95
    assert critical_health["score"] == 88


def test_missing_baseline_values_are_safe_and_neutral_for_components():
    health = calculate_sustainability_score(
        make_current(energy=99.0, carbon=99.0, duration=999.0),
        {"run_count": 0},
        [],
    )
    assert health["score"] == 100
    assert health["grade"] == "Excellent"


def test_empty_or_skipped_only_current_run_receives_conservative_score():
    health = calculate_sustainability_score(pd.DataFrame(), BASELINE, [])
    assert health["score"] == 50
    assert health["grade"] == "Moderate"
    assert health["status"] == "Warning"
    assert "insufficient lifecycle telemetry" in health["explanation"]


def test_final_score_is_clamped_to_zero_to_one_hundred():
    anomalies = [{"severity": "critical", "message": "x"} for _ in range(20)]
    low_health = calculate_sustainability_score(make_current(energy=100.0, carbon=100.0, duration=1000.0), BASELINE, anomalies)
    high_health = calculate_sustainability_score(make_current(), BASELINE, [])
    assert 0 <= low_health["score"] <= 100
    assert 0 <= high_health["score"] <= 100


def test_grade_thresholds_remain_unchanged():
    assert calculate_sustainability_score(make_current(), BASELINE, [])["grade"] == "Excellent"
    assert calculate_sustainability_score(make_current(energy=12.0, carbon=6.0, duration=120.0), BASELINE, [])["grade"] == "Good"
    assert calculate_sustainability_score(make_current(energy=13.0, carbon=6.5, duration=130.0), BASELINE, [])["grade"] == "Moderate"
    assert calculate_sustainability_score(make_current(energy=15.0, carbon=7.5, duration=150.0), BASELINE, [])["grade"] == "Poor"
