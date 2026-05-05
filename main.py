import psutil
import threading
import time
import csv
from datetime import datetime

from pipeline.build import run_build
from pipeline.test import run_tests
from pipeline.deploy import run_deploy

from monitor.collector import collect_metrics
from energy.energy_model import estimate_energy
from energy.carbon_model import estimate_carbon

def monitor_resources(stop_flag, cpu_values, memory_values):
    while not stop_flag["stop"]:
        cpu = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory().percent
        cpu_values.append(cpu)
        memory_values.append(memory)


def run_stage(stage_name, func):
    print(f"\n--- {stage_name} ---")

    cpu_values = []
    memory_values = []

    stop_flag = {"stop": False}

    monitor_thread = threading.Thread(
        target=monitor_resources,
        args=(stop_flag, cpu_values, memory_values)
    )

    start = time.time()

    monitor_thread.start()
    func()  # run workload
    stop_flag["stop"] = True
    monitor_thread.join()

    duration = time.time() - start

    avg_cpu = sum(cpu_values) / len(cpu_values) if cpu_values else 0
    avg_memory = sum(memory_values) / len(memory_values) if memory_values else 0

    energy = estimate_energy(avg_cpu, duration)
    carbon = estimate_carbon(energy)

    print(f"Stage: {stage_name} | CPU avg: {avg_cpu:.2f} | Energy: {energy:.6f}")
    
    return {
        "stage": stage_name,
        "duration": duration,
        "cpu": avg_cpu,
        "memory": avg_memory,
        "energy": energy,
        "carbon": carbon,
        "timestamp": datetime.now().isoformat()
    }

def save_to_csv(data):
    import os

    file_exists = os.path.isfile("data/metrics.csv")

    with open("data/metrics.csv", "a", newline="") as f:
        writer = csv.writer(f)

        # Write header once
        if not file_exists:
            writer.writerow([
                "stage", "duration", "cpu", "memory", "energy", "carbon", "timestamp"
            ])

        writer.writerow([
            data["stage"],
            data["duration"],
            data["cpu"],
            data["memory"],
            data["energy"],
            data["carbon"],
            data["timestamp"]
        ])

if __name__ == "__main__":
    stages = [
        ("build", run_build),
        ("test", run_tests),
        ("deploy", run_deploy)
    ]

    for name, func in stages:
        result = run_stage(name, func)
        save_to_csv(result)

    print("\nPipeline execution complete")