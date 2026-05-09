import argparse
import csv
import os
import subprocess
import threading
import time
from datetime import datetime
from storage.mongo_store import save_to_mongo, update_stage_record

import psutil

from energy.energy_model import estimate_energy_kwh
from energy.carbon_model import estimate_carbon_kg


def monitor_resources(stop_event, cpu_values, memory_values, interval=1):
    while not stop_event.is_set():
        cpu_values.append(psutil.cpu_percent(interval=interval))
        memory_values.append(psutil.virtual_memory().percent)


def calculate_stage_timing(workload_duration, jenkins_stage_duration=None):
    """Build workload and full-stage timing fields while keeping duration_seconds backward compatible."""
    workload_duration = max(0.0, float(workload_duration or 0.0))
    full_stage_duration = workload_duration if jenkins_stage_duration is None else max(0.0, float(jenkins_stage_duration))
    overhead_seconds = max(0.0, full_stage_duration - workload_duration)
    overhead_percentage = (overhead_seconds / full_stage_duration * 100.0) if full_stage_duration > 0 else 0.0

    return {
        # duration_seconds stays as workload runtime for backward compatibility.
        "duration_seconds": round(workload_duration, 4),
        "workload_duration_seconds": round(workload_duration, 4),
        "jenkins_stage_duration_seconds": round(full_stage_duration, 4),
        # Infrastructure overhead captures orchestration time outside the monitored command.
        "infrastructure_overhead_seconds": round(overhead_seconds, 4),
        "overhead_percentage": round(overhead_percentage, 4),
    }


def _load_existing_csv_rows(file_path):
    if not os.path.isfile(file_path):
        return [], []

    with open(file_path, "r", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    return rows, fieldnames


def _rewrite_csv(file_path, rows, fieldnames):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w", newline="") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            normalized_row = {field: row.get(field, "") for field in fieldnames}
            writer.writerow(normalized_row)


def save_to_csv(record, file_path="data/metrics.csv"):
    """Append a record while preserving CSV fallback compatibility as fields evolve."""
    existing_rows, existing_fieldnames = _load_existing_csv_rows(file_path)
    fieldnames = list(existing_fieldnames)

    for key in record.keys():
        if key not in fieldnames:
            fieldnames.append(key)

    existing_rows.append({field: record.get(field, "") for field in fieldnames})
    _rewrite_csv(file_path, existing_rows, fieldnames)


def update_csv_stage_record(run_id, stage_name, updates, file_path="data/metrics.csv"):
    """Update the latest CSV record for the given stage/run so CSV fallback keeps richer timing metadata."""
    rows, fieldnames = _load_existing_csv_rows(file_path)
    if not rows:
        print("No CSV stage record found to update.")
        return False

    for key in updates.keys():
        if key not in fieldnames:
            fieldnames.append(key)

    matching_indexes = [
        index for index, row in enumerate(rows)
        if str(row.get("run_id", "")) == str(run_id) and str(row.get("stage", "")) == str(stage_name)
    ]
    if not matching_indexes:
        print("No CSV stage record found to update.")
        return False

    latest_index = matching_indexes[-1]
    for key, value in updates.items():
        rows[latest_index][key] = value

    _rewrite_csv(file_path, rows, fieldnames)
    print("CSV stage record updated.")
    return True


def build_stage_metadata_updates(workload_duration, jenkins_stage_duration=None, stage_start_timestamp=None, stage_end_timestamp=None):
    timing = calculate_stage_timing(workload_duration, jenkins_stage_duration)
    return {
        **timing,
        "stage_start_timestamp": stage_start_timestamp or "",
        "stage_end_timestamp": stage_end_timestamp or "",
    }


def run_monitored_stage(
    stage_name,
    command,
    pipeline_name,
    run_id,
    zone,
    jenkins_stage_duration=None,
    stage_start_timestamp=None,
    stage_end_timestamp=None,
):
    """Run and monitor a stage command.

    duration_seconds is retained for backward compatibility.
    workload_duration_seconds stores the actual monitored command runtime.
    jenkins_stage_duration_seconds stores broader CI/CD stage time when Jenkins provides it.
    """
    print(f"\n--- Running monitored stage: {stage_name} ---")
    print(f"Command: {command}")

    cpu_values = []
    memory_values = []
    stop_event = threading.Event()

    monitor_thread = threading.Thread(
        target=monitor_resources,
        args=(stop_event, cpu_values, memory_values)
    )

    start_time = time.time()
    start_timestamp = datetime.now().isoformat()

    monitor_thread.start()

    process = subprocess.run(
        command,
        shell=True,
        text=True
    )

    stop_event.set()
    monitor_thread.join()

    end_time = time.time()
    duration = end_time - start_time
    timing_fields = build_stage_metadata_updates(
        workload_duration=duration,
        jenkins_stage_duration=jenkins_stage_duration,
        stage_start_timestamp=stage_start_timestamp,
        stage_end_timestamp=stage_end_timestamp,
    )

    avg_cpu = sum(cpu_values) / len(cpu_values) if cpu_values else 0
    peak_cpu = max(cpu_values) if cpu_values else 0

    avg_memory = sum(memory_values) / len(memory_values) if memory_values else 0
    peak_memory = max(memory_values) if memory_values else 0

    energy_result = estimate_energy_kwh(avg_cpu, duration)

    total_carbon_result = estimate_carbon_kg(
        energy_result["total_energy_kwh"],
        zone
    )

    active_carbon_result = estimate_carbon_kg(
        energy_result["active_energy_kwh"],
        zone
    )

    status = "success" if process.returncode == 0 else "failed"

    record = {
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "stage": stage_name,
        "command": " ".join(command.split()),
        "status": status,
        "return_code": process.returncode,
        **timing_fields,
        "avg_cpu_percent": round(avg_cpu, 4),
        "peak_cpu_percent": round(peak_cpu, 4),
        "avg_memory_percent": round(avg_memory, 4),
        "peak_memory_percent": round(peak_memory, 4),
        "total_power_watts": round(energy_result["total_power_watts"], 6),
        "active_power_watts": round(energy_result["active_power_watts"], 6),
        "total_energy_kwh": round(energy_result["total_energy_kwh"], 10),
        "active_energy_kwh": round(energy_result["active_energy_kwh"], 10),
        "carbon_intensity_kg_per_kwh": round(
            total_carbon_result["carbon_intensity_kg_per_kwh"], 6
        ),
        "total_carbon_kg": round(total_carbon_result["carbon_kg"], 10),
        "active_carbon_kg": round(active_carbon_result["carbon_kg"], 10),
        "carbon_source": total_carbon_result["carbon_source"],
        "zone": zone,
        "start_timestamp": start_timestamp,
        "end_timestamp": datetime.now().isoformat(),
    }

    save_to_csv(record)
    save_to_mongo(record.copy())

    print("\nStage monitoring complete")
    print(f"Stage: {stage_name}")
    print(f"Status: {status}")
    print(f"Workload duration: {record['workload_duration_seconds']} seconds")
    print(f"Full stage duration: {record['jenkins_stage_duration_seconds']} seconds")
    print(f"Infrastructure overhead: {record['infrastructure_overhead_seconds']} seconds ({record['overhead_percentage']}%)")
    print(f"Avg CPU: {record['avg_cpu_percent']}%")
    print(f"Total Energy: {record['total_energy_kwh']} kWh")
    print(f"Active Energy: {record['active_energy_kwh']} kWh")
    print(f"Total Carbon: {record['total_carbon_kg']} kgCO2eq")
    print(f"Active Carbon: {record['active_carbon_kg']} kgCO2eq")
    print(f"Carbon source: {record['carbon_source']}")
    print(f"WORKLOAD_DURATION_SECONDS={record['workload_duration_seconds']}")

    return process.returncode


def update_stage_metadata(run_id, stage_name, workload_duration, jenkins_stage_duration=None, stage_start_timestamp=None, stage_end_timestamp=None):
    """Attach full Jenkins stage timing after the monitored command has already saved its record."""
    updates = build_stage_metadata_updates(
        workload_duration=workload_duration,
        jenkins_stage_duration=jenkins_stage_duration,
        stage_start_timestamp=stage_start_timestamp,
        stage_end_timestamp=stage_end_timestamp,
    )
    csv_updated = update_csv_stage_record(run_id, stage_name, updates)
    mongo_updated = update_stage_record(run_id, stage_name, updates)

    if not csv_updated and not mongo_updated:
        print("No matching monitoring record was updated.")
        return 1

    print("Stage timing metadata update complete.")
    print(f"Stage: {stage_name}")
    print(f"Workload duration: {updates['workload_duration_seconds']} seconds")
    print(f"Full stage duration: {updates['jenkins_stage_duration_seconds']} seconds")
    print(f"Infrastructure overhead: {updates['infrastructure_overhead_seconds']} seconds ({updates['overhead_percentage']}%)")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Green DevOps stage monitoring runner")

    parser.add_argument("--stage", required=True)
    parser.add_argument("--cmd")
    parser.add_argument("--pipeline", default="green-devops-pipeline")
    parser.add_argument("--run-id", default=str(int(time.time())))
    parser.add_argument("--zone", default="LK")
    parser.add_argument("--jenkins-stage-duration", type=float)
    parser.add_argument("--stage-start-timestamp")
    parser.add_argument("--stage-end-timestamp")
    parser.add_argument("--workload-duration", type=float)

    args = parser.parse_args()

    if args.cmd:
        exit_code = run_monitored_stage(
            stage_name=args.stage,
            command=args.cmd,
            pipeline_name=args.pipeline,
            run_id=args.run_id,
            zone=args.zone,
            jenkins_stage_duration=args.jenkins_stage_duration,
            stage_start_timestamp=args.stage_start_timestamp,
            stage_end_timestamp=args.stage_end_timestamp,
        )
    else:
        if args.workload_duration is None:
            parser.error("--workload-duration is required when --cmd is not provided.")
        exit_code = update_stage_metadata(
            run_id=args.run_id,
            stage_name=args.stage,
            workload_duration=args.workload_duration,
            jenkins_stage_duration=args.jenkins_stage_duration,
            stage_start_timestamp=args.stage_start_timestamp,
            stage_end_timestamp=args.stage_end_timestamp,
        )

    exit(exit_code)
