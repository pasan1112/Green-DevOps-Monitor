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
    assert "20.0% lower" in body
    assert "Run B used 25.0% less energy than Run A." in body
