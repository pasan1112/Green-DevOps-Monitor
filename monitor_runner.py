import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil

from energy.carbon_model import estimate_carbon_kg
from energy.energy_model import estimate_energy_kwh
from storage.mongo_store import save_to_mongo


SCHEMA_VERSION = "1.0"
COMPONENT_NAME = "monitor"
SUPPORTED_LIFECYCLE_STAGES = {"release", "deploy", "operate"}
FINITE_LIFECYCLE_STAGES = {"release", "deploy"}
SKIP_REASONS = {
    "no_affected_components",
    "no_release_work",
    "dry_run",
    "app_not_affected",
    "deployment_not_required",
}
DEFAULT_SESSION_DIR = Path("data") / "monitor_sessions"
DEFAULT_CSV_PATH = Path("data") / "metrics.csv"


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def validate_lifecycle_stage(stage_name):
    normalized_stage = str(stage_name or "").strip().lower()
    if normalized_stage not in SUPPORTED_LIFECYCLE_STAGES:
        allowed = ", ".join(sorted(SUPPORTED_LIFECYCLE_STAGES))
        raise ValueError(
            f"Unsupported lifecycle stage '{stage_name}'. Use one of: {allowed}. "
            "Prototype lifecycle values such as build/test are not supported."
        )
    return normalized_stage


def validate_finite_lifecycle_stage(stage_name):
    stage_name = validate_lifecycle_stage(stage_name)
    if stage_name == "operate":
        raise ValueError(
            "Operate is a continuous lifecycle and is reserved for bounded "
            "observation windows; use release or deploy with start/stop/cancel."
        )
    return stage_name


def validate_skip_reason(reason):
    normalized_reason = str(reason or "").strip().lower()
    if normalized_reason not in SKIP_REASONS:
        allowed = ", ".join(sorted(SKIP_REASONS))
        raise ValueError(f"Unsupported skip reason '{reason}'. Use one of: {allowed}.")
    return normalized_reason


def normalize_text(value):
    return " ".join(str(value or "").split())


def get_session_dir():
    return Path(os.getenv("MONITOR_SESSION_DIR", DEFAULT_SESSION_DIR))


def get_csv_path():
    return Path(os.getenv("MONITOR_CSV_PATH", DEFAULT_CSV_PATH))


def session_key(stage_name, pipeline_name, run_id):
    safe_parts = [stage_name, pipeline_name, run_id]
    return "__".join(
        "".join(character if character.isalnum() or character in ("-", "_") else "_" for character in str(part))
        for part in safe_parts
    )


def session_paths(stage_name, pipeline_name, run_id, session_dir=None):
    session_dir = Path(session_dir or get_session_dir())
    key = session_key(stage_name, pipeline_name, run_id)
    return {
        "session_dir": session_dir,
        "session": session_dir / f"{key}.json",
        "samples": session_dir / f"{key}.samples.csv",
        "stop": session_dir / f"{key}.stop",
    }


def read_json(path):
    with open(path, "r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, indent=2, sort_keys=True)


def _load_existing_csv_rows(file_path):
    file_path = Path(file_path)
    if not file_path.is_file():
        return [], []

    with open(file_path, "r", newline="", encoding="utf-8") as file_handle:
        reader = csv.DictReader(file_handle)
        return list(reader), reader.fieldnames or []


def _rewrite_csv(file_path, rows, fieldnames):
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def save_to_csv(record, file_path=None):
    file_path = Path(file_path or get_csv_path())
    existing_rows, existing_fieldnames = _load_existing_csv_rows(file_path)
    fieldnames = list(existing_fieldnames)

    for key in record.keys():
        if key not in fieldnames:
            fieldnames.append(key)

    existing_rows.append({field: record.get(field, "") for field in fieldnames})
    _rewrite_csv(file_path, existing_rows, fieldnames)


def persist_record(record, csv_path=None):
    csv_saved = False
    try:
        save_to_csv(record, csv_path)
        csv_saved = True
        print("Record saved to CSV.")
    except Exception as exc:
        print(f"CSV storage error: {exc}")

    try:
        save_to_mongo(record.copy())
    except Exception as exc:
        print(f"MongoDB storage error: {exc}. Continuing without MongoDB.")

    return csv_saved


def is_process_running(pid):
    if not pid:
        return False
    try:
        return psutil.pid_exists(int(pid))
    except (TypeError, ValueError):
        return False


def start_sampler_process(session_file):
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "_sample",
        "--session-file",
        str(session_file),
    ]
    kwargs = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": os.name != "nt",
    }
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(command, **kwargs)


def start_session(stage_name, pipeline_name, run_id, zone, interval=1.0, metadata=None):
    stage_name = validate_finite_lifecycle_stage(stage_name)
    paths = session_paths(stage_name, pipeline_name, run_id)

    if paths["session"].exists():
        existing = read_json(paths["session"])
        if is_process_running(existing.get("sampler_pid")):
            print("Monitoring session already exists.")
            return 1
        cleanup_session_files(paths)

    paths["session_dir"].mkdir(parents=True, exist_ok=True)
    if paths["stop"].exists():
        paths["stop"].unlink()

    session = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "lifecycle_stage": stage_name,
        "component_name": COMPONENT_NAME,
        "record_type": stage_name,
        "zone": zone,
        "start_timestamp": utc_now_iso(),
        "samples_path": str(paths["samples"]),
        "stop_path": str(paths["stop"]),
        "sampling_interval_seconds": float(interval),
        "metadata": metadata or {},
    }
    write_json(paths["session"], session)
    process = start_sampler_process(paths["session"])
    session["sampler_pid"] = process.pid
    write_json(paths["session"], session)

    print("Monitoring session started.")
    print(f"Lifecycle stage: {stage_name}")
    print(f"Pipeline: {pipeline_name}")
    print(f"Run ID: {run_id}")
    return 0


def _try_terminate_sampler(pid, timeout_seconds=5):
    if not is_process_running(pid):
        return
    try:
        process = psutil.Process(int(pid))
        process.terminate()
        process.wait(timeout=timeout_seconds)
    except psutil.TimeoutExpired:
        process.kill()
    except (psutil.Error, ValueError):
        return


def cleanup_session_files(paths):
    for key in ("session", "samples", "stop"):
        path = paths[key]
        if path.exists():
            path.unlink()


def request_sampler_stop(session, paths):
    paths["stop"].write_text(utc_now_iso(), encoding="utf-8")
    deadline = time.time() + 10
    while time.time() < deadline and is_process_running(session.get("sampler_pid")):
        time.sleep(0.1)
    _try_terminate_sampler(session.get("sampler_pid"))


def read_samples(samples_path):
    samples_path = Path(samples_path)
    if not samples_path.is_file():
        return [], []

    cpu_values = []
    memory_values = []
    with open(samples_path, "r", newline="", encoding="utf-8") as file_handle:
        reader = csv.DictReader(file_handle)
        for row in reader:
            try:
                cpu_values.append(float(row["cpu_percent"]))
                memory_values.append(float(row["memory_percent"]))
            except (KeyError, TypeError, ValueError):
                continue
    return cpu_values, memory_values


def seconds_between(start_timestamp, end_timestamp):
    start = datetime.fromisoformat(start_timestamp)
    end = datetime.fromisoformat(end_timestamp)
    return max(0.0, (end - start).total_seconds())


def build_lifecycle_record(session, status="success", return_code=0, command="", metadata=None):
    end_timestamp = utc_now_iso()
    duration = seconds_between(session["start_timestamp"], end_timestamp)
    cpu_values, memory_values = read_samples(session["samples_path"])

    avg_cpu = sum(cpu_values) / len(cpu_values) if cpu_values else 0.0
    peak_cpu = max(cpu_values) if cpu_values else 0.0
    avg_memory = sum(memory_values) / len(memory_values) if memory_values else 0.0
    peak_memory = max(memory_values) if memory_values else 0.0

    energy_result = estimate_energy_kwh(avg_cpu, duration)
    total_carbon_result = estimate_carbon_kg(energy_result["total_energy_kwh"], session["zone"])
    active_carbon_result = estimate_carbon_kg(energy_result["active_energy_kwh"], session["zone"])
    merged_metadata = dict(session.get("metadata") or {})
    merged_metadata.update(metadata or {})

    return {
        "schema_version": SCHEMA_VERSION,
        "pipeline_name": session["pipeline_name"],
        "run_id": session["run_id"],
        "lifecycle_stage": session["lifecycle_stage"],
        "component_name": COMPONENT_NAME,
        "record_type": session["record_type"],
        "status": status,
        "skipped": False,
        "skip_reason": "",
        "return_code": int(return_code),
        "command": normalize_text(command),
        "zone": session["zone"],
        "start_timestamp": session["start_timestamp"],
        "end_timestamp": end_timestamp,
        "duration_seconds": round(duration, 4),
        "avg_cpu_percent": round(avg_cpu, 4),
        "peak_cpu_percent": round(peak_cpu, 4),
        "avg_memory_percent": round(avg_memory, 4),
        "peak_memory_percent": round(peak_memory, 4),
        "total_power_watts": round(energy_result["total_power_watts"], 6),
        "active_power_watts": round(energy_result["active_power_watts"], 6),
        "total_energy_kwh": round(energy_result["total_energy_kwh"], 10),
        "active_energy_kwh": round(energy_result["active_energy_kwh"], 10),
        "carbon_intensity_kg_per_kwh": round(total_carbon_result["carbon_intensity_kg_per_kwh"], 6),
        "total_carbon_kg": round(total_carbon_result["carbon_kg"], 10),
        "active_carbon_kg": round(active_carbon_result["carbon_kg"], 10),
        "carbon_source": total_carbon_result["carbon_source"],
        "metadata": json.dumps(merged_metadata, sort_keys=True),
    }


def build_skipped_lifecycle_record(stage_name, pipeline_name, run_id, zone, skip_reason, status="success", metadata=None):
    stage_name = validate_finite_lifecycle_stage(stage_name)
    skip_reason = validate_skip_reason(skip_reason)
    timestamp = utc_now_iso()
    carbon_result = estimate_carbon_kg(0.0, zone)
    merged_metadata = dict(metadata or {})

    return {
        "schema_version": SCHEMA_VERSION,
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "lifecycle_stage": stage_name,
        "component_name": COMPONENT_NAME,
        "record_type": stage_name,
        "status": status,
        "skipped": True,
        "skip_reason": skip_reason,
        "return_code": 0,
        "command": "",
        "zone": zone,
        "start_timestamp": timestamp,
        "end_timestamp": timestamp,
        "duration_seconds": 0.0,
        "avg_cpu_percent": 0.0,
        "peak_cpu_percent": 0.0,
        "avg_memory_percent": 0.0,
        "peak_memory_percent": 0.0,
        "total_power_watts": 0.0,
        "active_power_watts": 0.0,
        "total_energy_kwh": 0.0,
        "active_energy_kwh": 0.0,
        "carbon_intensity_kg_per_kwh": round(carbon_result["carbon_intensity_kg_per_kwh"], 6),
        "total_carbon_kg": 0.0,
        "active_carbon_kg": 0.0,
        "carbon_source": carbon_result["carbon_source"],
        "metadata": json.dumps(merged_metadata, sort_keys=True),
    }


def stop_session(stage_name, pipeline_name, run_id, zone, status="success", return_code=0, command="", metadata=None):
    stage_name = validate_finite_lifecycle_stage(stage_name)
    paths = session_paths(stage_name, pipeline_name, run_id)
    if not paths["session"].exists():
        print("No monitoring session exists; nothing to stop.")
        return 0

    session = read_json(paths["session"])
    if session.get("zone") != zone:
        print(f"Warning: stop zone '{zone}' differs from session zone '{session.get('zone')}'. Using session zone.")

    request_sampler_stop(session, paths)
    record = build_lifecycle_record(session, status=status, return_code=return_code, command=command, metadata=metadata)
    csv_saved = persist_record(record)
    cleanup_session_files(paths)

    print("Lifecycle monitoring complete.")
    print(f"Lifecycle stage: {record['lifecycle_stage']}")
    print(f"Status: {record['status']}")
    print(f"Duration: {record['duration_seconds']} seconds")
    print(f"Total Energy: {record['total_energy_kwh']} kWh")
    print(f"Total Carbon: {record['total_carbon_kg']} kgCO2eq")
    return 0 if csv_saved else 1


def skip_session(stage_name, pipeline_name, run_id, zone, skip_reason, status="success", metadata=None):
    stage_name = validate_finite_lifecycle_stage(stage_name)
    paths = session_paths(stage_name, pipeline_name, run_id)
    if paths["session"].exists():
        existing = read_json(paths["session"])
        if is_process_running(existing.get("sampler_pid")):
            print("Monitoring session already exists; refusing to write a skipped record over an active workload.")
            return 1
        cleanup_session_files(paths)

    record = build_skipped_lifecycle_record(
        stage_name,
        pipeline_name,
        run_id,
        zone,
        skip_reason,
        status=status,
        metadata=metadata,
    )
    csv_saved = persist_record(record)

    print("Lifecycle monitoring skipped.")
    print(f"Lifecycle stage: {record['lifecycle_stage']}")
    print(f"Status: {record['status']}")
    print(f"Skipped: {record['skipped']}")
    print(f"Skip reason: {record['skip_reason']}")
    return 0 if csv_saved else 1


def cancel_session(stage_name, pipeline_name, run_id, zone):
    stage_name = validate_finite_lifecycle_stage(stage_name)
    paths = session_paths(stage_name, pipeline_name, run_id)
    if not paths["session"].exists():
        print("No monitoring session exists; cancel is a no-op.")
        return 0

    session = read_json(paths["session"])
    if session.get("zone") != zone:
        print(f"Warning: cancel zone '{zone}' differs from session zone '{session.get('zone')}'.")
    request_sampler_stop(session, paths)
    cleanup_session_files(paths)
    print("Monitoring session canceled without writing a lifecycle record.")
    return 0


def operate_placeholder(args):
    stage_name = validate_lifecycle_stage(args.stage)
    if stage_name != "operate":
        raise ValueError("Operate placeholder only accepts --stage operate.")
    print(
        "Operate monitoring is reserved for bounded observation windows: "
        "observation start, collect telemetry, observation stop, save one operate_window record."
    )
    return 0


def run_sampler(session_file):
    session = read_json(Path(session_file))
    samples_path = Path(session["samples_path"])
    stop_path = Path(session["stop_path"])
    interval = float(session.get("sampling_interval_seconds") or 1.0)
    samples_path.parent.mkdir(parents=True, exist_ok=True)

    with open(samples_path, "w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=["timestamp", "cpu_percent", "memory_percent"])
        writer.writeheader()
        while not stop_path.exists():
            cpu_percent = psutil.cpu_percent(interval=interval)
            memory_percent = psutil.virtual_memory().percent
            writer.writerow(
                {
                    "timestamp": utc_now_iso(),
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory_percent,
                }
            )
            file_handle.flush()


def build_parser():
    parser = argparse.ArgumentParser(description="Green DevOps lifecycle monitoring runner")
    subparsers = parser.add_subparsers(dest="action", required=True)

    for action in ("start", "stop", "cancel"):
        subparser = subparsers.add_parser(action)
        subparser.add_argument("--stage", required=True, type=validate_finite_lifecycle_stage, metavar="{release,deploy}")
        subparser.add_argument("--pipeline", required=True)
        subparser.add_argument("--run-id", required=True)
        subparser.add_argument("--zone", default="LK")
        if action == "start":
            subparser.add_argument("--interval", type=float, default=1.0)
        if action == "stop":
            subparser.add_argument("--status", default="success", choices=["success", "failed", "canceled"])
            subparser.add_argument("--return-code", type=int, default=0)
            subparser.add_argument("--command", default="")

    skip_parser = subparsers.add_parser("skip")
    skip_parser.add_argument("--stage", required=True, type=validate_finite_lifecycle_stage, metavar="{release,deploy}")
    skip_parser.add_argument("--pipeline", required=True)
    skip_parser.add_argument("--run-id", required=True)
    skip_parser.add_argument("--zone", default="LK")
    skip_parser.add_argument("--reason", required=True, type=validate_skip_reason, choices=sorted(SKIP_REASONS))
    skip_parser.add_argument("--status", default="success", choices=["success", "failed", "canceled"])

    operate_parser = subparsers.add_parser("operate")
    operate_parser.add_argument("--stage", required=True, type=validate_lifecycle_stage, metavar="{operate}")

    sample_parser = subparsers.add_parser("_sample")
    sample_parser.add_argument("--session-file", required=True)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.action == "start":
            return start_session(args.stage, args.pipeline, args.run_id, args.zone, interval=args.interval)
        if args.action == "stop":
            return stop_session(
                args.stage,
                args.pipeline,
                args.run_id,
                args.zone,
                status=args.status,
                return_code=args.return_code,
                command=args.command,
            )
        if args.action == "cancel":
            return cancel_session(args.stage, args.pipeline, args.run_id, args.zone)
        if args.action == "skip":
            return skip_session(args.stage, args.pipeline, args.run_id, args.zone, args.reason, status=args.status)
        if args.action == "operate":
            return operate_placeholder(args)
        if args.action == "_sample":
            run_sampler(args.session_file)
            return 0
    except ValueError as exc:
        parser.error(str(exc))
    return 1


if __name__ == "__main__":
    sys.exit(main())
