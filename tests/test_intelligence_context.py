import pandas as pd
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from intelligence.anomaly_model import detect_stage_anomalies
from intelligence.baseline_model import LIFECYCLE_STAGES, MIN_CONTEXT_SAMPLES, calculate_stage_baselines, select_historical_context
from intelligence.health_score import calculate_sustainability_score
from intelligence.ml_anomaly_model import STAGE_SPECIFIC_MODELS, IsolationForest, detect_ml_anomalies


def make_rows(stage, count, pipeline="pipe-a", strategy="rolling", start=0, run_prefix=None, energy=1.0):
    prefix = run_prefix or f"{pipeline}-{stage}-{strategy}"
    return [
        {
            "run_id": f"{prefix}-{start + index}",
            "pipeline_name": pipeline,
            "stage": stage,
            "strategy": strategy,
            "duration_seconds": 10.0,
            "workload_duration_seconds": 10.0,
            "jenkins_stage_duration_seconds": 12.0,
            "infrastructure_overhead_seconds": 2.0,
            "overhead_percentage": 16.0,
            "avg_cpu_percent": 20.0,
            "peak_cpu_percent": 40.0,
            "total_energy_kwh": energy,
            "active_energy_kwh": energy,
            "total_carbon_kg": energy * 0.5,
            "active_carbon_kg": energy * 0.5,
            "carbon_intensity_kg_per_kwh": 0.5,
            "status": "success",
        }
        for index in range(count)
    ]


def test_lifecycle_stage_configuration_uses_monitor_lifecycle_terms():
    assert LIFECYCLE_STAGES == ["release", "deploy", "operate"]
    assert STAGE_SPECIFIC_MODELS == ["release", "deploy", "operate"]


def test_same_pipeline_stage_strategy_is_preferred():
    historical = pd.DataFrame(
        make_rows("deploy", MIN_CONTEXT_SAMPLES, pipeline="pipe-a", strategy="rolling")
        + make_rows("deploy", MIN_CONTEXT_SAMPLES, pipeline="pipe-a", strategy="recreate")
        + make_rows("deploy", MIN_CONTEXT_SAMPLES, pipeline="pipe-b", strategy="rolling")
    )
    selected, context = select_historical_context(historical, "deploy", "pipe-a", "rolling")
    assert len(selected) == MIN_CONTEXT_SAMPLES
    assert context["context_scope"] == "pipeline_stage_strategy"
    assert context["strategy_specific"] is True
    assert set(selected["pipeline_name"]) == {"pipe-a"}
    assert set(selected["strategy"]) == {"rolling"}


def test_strategy_fallback_uses_pipeline_stage_when_strategy_history_is_small():
    historical = pd.DataFrame(
        make_rows("deploy", 6, pipeline="pipe-a", strategy="rolling")
        + make_rows("deploy", 6, pipeline="pipe-a", strategy="recreate")
        + make_rows("deploy", MIN_CONTEXT_SAMPLES, pipeline="pipe-b", strategy="rolling")
    )
    selected, context = select_historical_context(historical, "deploy", "pipe-a", "rolling")
    assert len(selected) == 12
    assert context["context_scope"] == "pipeline_stage"
    assert context["strategy_specific"] is False
    assert context["fallback_reason"]
    assert set(selected["pipeline_name"]) == {"pipe-a"}


def test_stage_only_fallback_keeps_lifecycle_isolated():
    historical = pd.DataFrame(
        make_rows("deploy", 5, pipeline="pipe-a", strategy="rolling")
        + make_rows("deploy", 5, pipeline="pipe-b", strategy="rolling")
        + make_rows("release", 30, pipeline="pipe-a", strategy="rolling", energy=99.0)
    )
    selected, context = select_historical_context(historical, "deploy", "pipe-a", "rolling")
    assert len(selected) == MIN_CONTEXT_SAMPLES
    assert context["context_scope"] == "stage"
    assert set(selected["stage"]) == {"deploy"}
    assert "release" not in set(selected["stage"])


def test_insufficient_context_reports_warming_state():
    historical = pd.DataFrame(make_rows("operate", 3))
    selected, context = select_historical_context(historical, "operate", "pipe-a", "rolling")
    assert len(selected) == 3
    assert context["context_scope"] == "insufficient"
    assert context["historical_samples"] == 3


def test_current_run_is_excluded_from_ml_historical_context():
    current = pd.DataFrame(make_rows("deploy", 1, pipeline="pipe-a", strategy="rolling", run_prefix="current"))
    historical = pd.DataFrame(
        make_rows("deploy", MIN_CONTEXT_SAMPLES, pipeline="pipe-a", strategy="rolling")
        + current.to_dict(orient="records")
    )
    result = detect_ml_anomalies(current, historical)
    deploy_result = next(item for item in result["results"] if item["stage"] == "deploy")
    assert deploy_result["historical_samples"] == MIN_CONTEXT_SAMPLES
    if IsolationForest is not None:
        assert deploy_result["model_status"] == "active"


def test_isolation_forest_builds_independent_lifecycle_results():
    if IsolationForest is None:
        pytest.skip("scikit-learn is not installed in this environment")
    historical = pd.DataFrame(
        make_rows("release", MIN_CONTEXT_SAMPLES, pipeline="pipe-a", strategy="")
        + make_rows("deploy", MIN_CONTEXT_SAMPLES, pipeline="pipe-a", strategy="rolling")
        + make_rows("operate", MIN_CONTEXT_SAMPLES, pipeline="pipe-a", strategy="")
    )
    current = pd.DataFrame(
        make_rows("release", 1, pipeline="pipe-a", strategy="", run_prefix="current-release")
        + make_rows("deploy", 1, pipeline="pipe-a", strategy="rolling", run_prefix="current-deploy")
        + make_rows("operate", 1, pipeline="pipe-a", strategy="", run_prefix="current-operate")
    )
    result = detect_ml_anomalies(current, historical)
    by_stage = {item["stage"]: item for item in result["results"]}
    assert set(by_stage) == {"release", "deploy", "operate"}
    assert all(by_stage[stage]["model_status"] == "active" for stage in by_stage)


def test_statistical_anomaly_detection_remains_explainable():
    historical = pd.DataFrame(make_rows("deploy", MIN_CONTEXT_SAMPLES, pipeline="pipe-a", strategy="rolling", energy=1.0))
    current = pd.DataFrame(make_rows("deploy", 1, pipeline="pipe-a", strategy="rolling", run_prefix="current", energy=2.0))
    baselines = calculate_stage_baselines(historical, current)
    anomalies = detect_stage_anomalies(current, baselines)
    energy_anomaly = next(item for item in anomalies if item["metric"] == "total_energy_kwh")
    assert energy_anomaly["current_value"] == 2.0
    assert energy_anomaly["baseline_mean"] == 1.0
    assert energy_anomaly["percentage_change"] == 100.0
    assert energy_anomaly["context_scope"] == "pipeline_stage_strategy"
    assert "energy" in energy_anomaly["message"].lower()


def test_health_score_uses_contextual_stage_baseline_and_stays_bounded():
    current = pd.DataFrame(make_rows("deploy", 1, pipeline="pipe-a", strategy="rolling", run_prefix="current", energy=2.0))
    historical = pd.DataFrame(
        make_rows("deploy", MIN_CONTEXT_SAMPLES, pipeline="pipe-a", strategy="rolling", energy=1.0)
        + make_rows("deploy", MIN_CONTEXT_SAMPLES, pipeline="pipe-a", strategy="canary", energy=100.0)
    )
    baselines = calculate_stage_baselines(historical, current)
    health = calculate_sustainability_score(current, {"run_count": 0}, [], baselines)
    assert 0 <= health["score"] <= 100
    assert health["score"] < 100
    assert health["baseline_context"]["strategy_specific"] is True
    assert "Deploy" in health["baseline_context"]["label"]


def test_failed_stage_penalty_still_applies():
    current = pd.DataFrame(make_rows("release", 1, pipeline="pipe-a", strategy="", run_prefix="current"))
    current.loc[0, "status"] = "failed"
    health = calculate_sustainability_score(current, {"run_count": 0}, [])
    assert health["score"] <= 80
    assert health["status"] == "Critical"


def test_empty_inputs_remain_safe():
    empty = pd.DataFrame()
    assert calculate_stage_baselines(empty).empty
    assert detect_stage_anomalies(empty, empty) == []
    ml_result = detect_ml_anomalies(empty, empty)
    assert ml_result["results"] == []
    health = calculate_sustainability_score(empty, {"run_count": 0}, [])
    assert 0 <= health["score"] <= 100
