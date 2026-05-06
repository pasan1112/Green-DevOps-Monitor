def estimate_energy_kwh(avg_cpu_percent, duration_seconds):
    P_IDLE_WATTS = 10
    P_MAX_WATTS = 80

    active_power_watts = (avg_cpu_percent / 100) * (P_MAX_WATTS - P_IDLE_WATTS)
    total_power_watts = P_IDLE_WATTS + active_power_watts

    total_energy_kwh = (total_power_watts * duration_seconds) / 3_600_000
    active_energy_kwh = (active_power_watts * duration_seconds) / 3_600_000

    return {
        "total_power_watts": total_power_watts,
        "active_power_watts": active_power_watts,
        "total_energy_kwh": total_energy_kwh,
        "active_energy_kwh": active_energy_kwh,
    }