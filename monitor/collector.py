import psutil
import time

def collect_metrics(duration=5):
    cpu_values = []
    memory_values = []

    for _ in range(duration):
        cpu_values.append(psutil.cpu_percent(interval=1))
        memory_values.append(psutil.virtual_memory().percent)

    return cpu_values, memory_values