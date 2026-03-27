import psutil
cores = psutil.cpu_count()

def estimate_energy(avg_cpu, duration):
    # Typical CPU power values (in watts)
    P_idle = 10
    P_max = 80

    power = (P_idle + (avg_cpu / 100) * (P_max - P_idle)) * cores
    
    # Convert watts → kWh
    energy = (power * duration) / (1000 * 3600)

    return energy