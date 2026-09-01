import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import dashboard


def _row(run_id, stage, duration, energy, carbon, cpu, memory, timestamp):
    return {
        "run_id": run_id,
        "pipeline_name": "compare-pipeline",
        "stage": stage,
        "strategy": "rolling",
        "status": "success",
        "start_timestamp": timestamp,
        "end_timestamp": timestamp,
        "stage_start_timestamp": timestamp,
        "stage_end_timestamp": timestamp,
        "duration_seconds": duration,
        "workload_duration_seconds": duration,
        "jenkins_stage_duration_seconds": duration,
        "jenkins_stage_duration_captured": True,
        "infrastructure_overhead_seconds": 0.0,
        "overhead_percentage": 0.0,
        "avg_cpu_percent": cpu,
        "peak_cpu_percent": cpu + 10.0,
        "avg_memory_percent": memory,
        "peak_memory_percent": memory + 8.0,
        "total_energy_kwh": energy,
        "active_energy_kwh": energy,
        "total_carbon_kg": carbon,
        "active_carbon_kg": carbon,
        "carbon_intensity_kg_per_kwh": 0.5,
        "carbon_source": "test",
        "skipped": False,
        "skip_reason": "",
    }


def _compare_metrics():
    return pd.DataFrame(
        [
            _row("compare-pipeline-100", "release", 60.0, 0.00008, 0.00004, 20.0, 30.0, "2026-08-31T10:00:00"),
            _row("compare-pipeline-100", "deploy", 40.0, 0.00004, 0.00002, 18.0, 28.0, "2026-08-31T10:01:00"),
            _row("compare-pipeline-101", "release", 45.0, 0.00006, 0.00003, 17.0, 26.0, "2026-08-31T11:00:00"),
            _row("compare-pipeline-101", "deploy", 35.0, 0.00003, 0.000015, 15.0, 24.0, "2026-08-31T11:01:00"),
        ]
    )


def test_compare_page_renders_two_selected_runs(monkeypatch):
    monkeypatch.setattr(dashboard, "load_metrics", lambda: (_compare_metrics(), "test data"))
    monkeypatch.setattr(dashboard, "load_release_builds", lambda: [])
    monkeypatch.setattr(dashboard, "load_deploy_data", lambda *args, **kwargs: None)

    client = dashboard.app.test_client()
    response = client.get("/compare?run_a=compare-pipeline-100&run_b=compare-pipeline-101")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Green DevOps <span>Compare</span>" in body
    assert "Run A" in body
    assert "Run B" in body
    assert body.count('<div class="compare-vs-card">') == 8
    expected_order = [
        "Release Runtime",
        "Deploy Runtime",
        "Total Runtime",
        "Average CPU",
        "Average Memory",
        "Total Energy",
        "Total Carbon",
        "Health Score",
    ]
    positions = [body.index(label) for label in expected_order]
    assert positions == sorted(positions)
    assert "Difference = ((Run B - Run A) / Run A) x 100" not in body
    assert "Each metric highlights the better-performing run." in body
    assert "Peak CPU" not in body
    assert "Peak Memory" not in body
    assert "Total Runtime" in body
    assert "Total Energy" in body
    assert "Total Carbon" in body
    assert "Health Score" in body
    assert "Release Runtime" in body
    assert "Deploy Runtime" in body
    assert "Average CPU" in body
    assert "Average Memory" in body
    assert "20.0% faster" in body
    assert "25.0% lower" in body


def test_compare_page_marks_run_a_as_better_when_left_side_wins(monkeypatch):
    monkeypatch.setattr(dashboard, "load_metrics", lambda: (_compare_metrics(), "test data"))
    monkeypatch.setattr(dashboard, "load_release_builds", lambda: [])
    monkeypatch.setattr(dashboard, "load_deploy_data", lambda *args, **kwargs: None)

    client = dashboard.app.test_client()
    response = client.get("/compare?run_a=compare-pipeline-101&run_b=compare-pipeline-100")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "compare-side better" in body
    assert "20.0% faster" in body
    assert "25.0% lower" in body


def test_compare_winner_metric_handles_equal_values():
    metric = dashboard._winner_compare_metric("Total Runtime", "timer", 50.0, 50.0, dashboard.format_seconds)

    assert metric["a_class"] == "equal"
    assert metric["b_class"] == "equal"
    assert metric["a_result"] == "Equal"
    assert metric["b_result"] == "Equal"


def test_compare_winner_metric_handles_missing_values():
    metric = dashboard._winner_compare_metric("Total Energy", "zap", None, 0.00006, dashboard.format_kwh)

    assert metric["a_display"] == "N/A"
    assert metric["b_display"] == "0.00006000 kWh"
    assert metric["a_result"] == "N/A"
    assert metric["b_result"] == "N/A"


def test_compare_winner_metric_handles_zero_values_without_invalid_math():
    equal_zero = dashboard._winner_compare_metric("Total Carbon", "leaf", 0.0, 0.0, dashboard.format_gco2_from_kg)
    zero_wins = dashboard._winner_compare_metric("Total Energy", "zap", 0.0, 0.00006, dashboard.format_kwh)

    assert equal_zero["a_result"] == "Equal"
    assert equal_zero["b_result"] == "Equal"
    assert zero_wins["a_result"] == "100.0% lower"
    assert zero_wins["b_result"] == ""


def test_compare_winner_metric_treats_health_as_higher_is_better():
    metric = dashboard._winner_compare_metric(
        "Health Score",
        "heart-pulse",
        70.0,
        94.0,
        lambda value: f"{int(round(value))}/100",
        lower_is_better=False,
    )

    assert metric["a_class"] == "worse"
    assert metric["b_class"] == "better"
    assert metric["b_result"] == "24 points higher"
