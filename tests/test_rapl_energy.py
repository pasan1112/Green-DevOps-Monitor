import csv
import os
import shutil
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import monitor_runner
from energy.energy_model import (
    RaplReadError,
    calculate_energy_from_rapl,
    calculate_rapl_delta_uj,
    microjoules_to_kwh,
    read_rapl_package_counter,
)


class DummyProcess:
    pid = os.getpid()


@pytest.fixture()
def monitor_env(monkeypatch):
    runtime_dir = Path("data") / "test_runtime" / uuid.uuid4().hex
    session_dir = runtime_dir / "sessions"
    csv_path = runtime_dir / "metrics.csv"
    monkeypatch.setenv("MONITOR_SESSION_DIR", str(session_dir))
    monkeypatch.setenv("MONITOR_CSV_PATH", str(csv_path))
    monkeypatch.delenv("MONGO_URI", raising=False)
    monkeypatch.setattr(monitor_runner, "start_sampler_process", lambda session_file: DummyProcess())
    monkeypatch.setattr(monitor_runner, "request_sampler_stop", lambda session, paths: None)
    monkeypatch.setattr(monitor_runner, "save_to_mongo", lambda record: False)
    yield {"runtime_dir": runtime_dir, "session_dir": session_dir, "csv_path": csv_path}
    shutil.rmtree(runtime_dir, ignore_errors=True)


def write_samples(stage, pipeline, run_id, cpu_values=(10.0, 30.0), memory_values=(40.0, 60.0)):
    paths = monitor_runner.session_paths(stage, pipeline, run_id)
    paths["samples"].parent.mkdir(parents=True, exist_ok=True)
    with open(paths["samples"], "w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=["timestamp", "cpu_percent", "memory_percent"])
        writer.writeheader()
        for index, cpu in enumerate(cpu_values):
            writer.writerow(
                {
                    "timestamp": monitor_runner.utc_now_iso(),
                    "cpu_percent": cpu,
                    "memory_percent": memory_values[index],
                }
            )


def read_csv_rows(csv_path):
    with open(csv_path, "r", newline="", encoding="utf-8") as file_handle:
        return list(csv.DictReader(file_handle))


def test_normal_rapl_delta():
    assert calculate_rapl_delta_uj(1_000, 4_000, 10_000) == 3_000


def test_rapl_wraparound_delta():
    assert calculate_rapl_delta_uj(9_000, 500, 10_000) == 1_500


def test_microjoule_to_kwh_conversion():
    assert microjoules_to_kwh(3_600_000_000_000) == 1.0


def test_rapl_average_power_calculation():
    result = calculate_energy_from_rapl(1_000_000, 11_000_000, 2.0, 100_000_000)
    assert result["total_energy_kwh"] == pytest.approx(10_000_000 / 3_600_000_000_000)
    assert result["active_energy_kwh"] == pytest.approx(result["total_energy_kwh"])
    assert result["total_power_watts"] == pytest.approx(5.0)
    assert result["active_power_watts"] == pytest.approx(5.0)


def test_rapl_zero_duration_keeps_energy_and_avoids_division_by_zero():
    result = calculate_energy_from_rapl(1_000_000, 11_000_000, 0.0, 100_000_000)
    assert result["total_energy_kwh"] == pytest.approx(10_000_000 / 3_600_000_000_000)
    assert result["active_energy_kwh"] == pytest.approx(result["total_energy_kwh"])
    assert result["total_power_watts"] == 0.0
    assert result["active_power_watts"] == 0.0


def test_missing_rapl_source_raises_read_error():
    missing_dir = Path("data") / "test_runtime" / ("missing_rapl_" + uuid.uuid4().hex)
    with pytest.raises(RaplReadError):
        read_rapl_package_counter(
            energy_path=missing_dir / "missing_energy_uj",
            max_range_path=missing_dir / "missing_max_energy_range_uj",
        )


def test_start_stop_use_rapl_and_preserve_csv_fields(monitor_env, monkeypatch):
    reads = iter(
        [
            {"energy_uj": 1_000_000, "max_energy_range_uj": 100_000_000},
            {"energy_uj": 11_000_000, "max_energy_range_uj": 100_000_000},
        ]
    )
    monkeypatch.setattr(monitor_runner, "read_rapl_package_counter", lambda: next(reads))

    assert monitor_runner.start_session("release", "pipe", "run-rapl", "LK") == 0
    write_samples("release", "pipe", "run-rapl")
    session = monitor_runner.read_json(monitor_runner.session_paths("release", "pipe", "run-rapl")["session"])
    assert session["rapl_start_uj"] == 1_000_000
    assert session["rapl_max_energy_range_uj"] == 100_000_000

    assert monitor_runner.stop_session("release", "pipe", "run-rapl", "LK") == 0
    row = read_csv_rows(monitor_env["csv_path"])[0]
    for field in ["total_power_watts", "active_power_watts", "total_energy_kwh", "active_energy_kwh"]:
        assert field in row
    assert float(row["total_energy_kwh"]) == pytest.approx(10_000_000 / 3_600_000_000_000, rel=1e-3)
    assert float(row["active_energy_kwh"]) == pytest.approx(float(row["total_energy_kwh"]))
    assert float(row["total_power_watts"]) == pytest.approx(float(row["active_power_watts"]))


def test_rapl_unavailable_falls_back_to_legacy_record_shape(monitor_env, monkeypatch):
    monkeypatch.setattr(
        monitor_runner,
        "read_rapl_package_counter",
        lambda: (_ for _ in ()).throw(RaplReadError("permission denied")),
    )

    assert monitor_runner.start_session("deploy", "pipe", "run-fallback", "LK") == 0
    write_samples("deploy", "pipe", "run-fallback", cpu_values=(50.0, 50.0), memory_values=(10.0, 20.0))
    assert monitor_runner.stop_session("deploy", "pipe", "run-fallback", "LK") == 0
    row = read_csv_rows(monitor_env["csv_path"])[0]
    assert set(["total_power_watts", "active_power_watts", "total_energy_kwh", "active_energy_kwh"]).issubset(row)
    assert float(row["total_power_watts"]) == 9.0
    assert float(row["active_power_watts"]) == 6.0


def test_skipped_lifecycle_does_not_attempt_rapl(monitor_env, monkeypatch):
    def fail_if_called():
        raise AssertionError("RAPL should not be read for skipped lifecycle records")

    monkeypatch.setattr(monitor_runner, "read_rapl_package_counter", fail_if_called)

    assert monitor_runner.skip_session("deploy", "pipe", "run-skip", "LK", "app_not_affected") == 0
    row = read_csv_rows(monitor_env["csv_path"])[0]
    assert row["skipped"] == "True"
    assert float(row["total_energy_kwh"]) == 0.0
    assert float(row["active_energy_kwh"]) == 0.0
