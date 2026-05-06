import argparse
import csv
import os
import subprocess
import threading
import time
from datetime import datetime
from storage.mongo_store import save_to_mongo

import psutil

from energy.energy_model import estimate_energy_kwh
from energy.carbon_model import estimate_carbon_kg


def monitor_resources(stop_event, cpu_values, memory_values, interval=1):
    while not stop_event.is_set():
        cpu_values.append(psutil.cpu_percent(interval=interval))
        memory_values.append(psutil.virtual_memory().percent)


def save_to_csv(record, file_path="data/metrics.csv"):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    file_exists = os.path.isfile(file_path)

    with open(file_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=record.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(record)


def run_monitored_stage(stage_name, command, pipeline_name, run_id, zone):
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
        "duration_seconds": round(duration, 4),
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
    print(f"Duration: {record['duration_seconds']} seconds")
    print(f"Avg CPU: {record['avg_cpu_percent']}%")
    print(f"Total Energy: {record['total_energy_kwh']} kWh")
    print(f"Active Energy: {record['active_energy_kwh']} kWh")
    print(f"Total Carbon: {record['total_carbon_kg']} kgCO2eq")
    print(f"Active Carbon: {record['active_carbon_kg']} kgCO2eq")
    print(f"Carbon source: {record['carbon_source']}")

    return process.returncode


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Green DevOps stage monitoring runner")

    parser.add_argument("--stage", required=True)
    parser.add_argument("--cmd", required=True)
    parser.add_argument("--pipeline", default="green-devops-pipeline")
    parser.add_argument("--run-id", default=str(int(time.time())))
    parser.add_argument("--zone", default="LK")

    args = parser.parse_args()

    exit_code = run_monitored_stage(
        stage_name=args.stage,
        command=args.cmd,
        pipeline_name=args.pipeline,
        run_id=args.run_id,
        zone=args.zone
    )

    exit(exit_code)