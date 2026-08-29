"""Energy calculations for Monitor lifecycle records.

Intel RAPL reports measured CPU package energy in microjoules. The package
domain is not whole-laptop wall energy; it is the CPU package energy consumed
between the lifecycle START and STOP boundaries.
"""

from __future__ import annotations

import os
from pathlib import Path


DEFAULT_RAPL_PACKAGE_DIR = Path("/sys/class/powercap/intel-rapl:0")
DEFAULT_RAPL_ENERGY_UJ_PATH = DEFAULT_RAPL_PACKAGE_DIR / "energy_uj"
DEFAULT_RAPL_MAX_RANGE_UJ_PATH = DEFAULT_RAPL_PACKAGE_DIR / "max_energy_range_uj"
UJ_PER_KWH = 3_600_000_000_000
UJ_PER_JOULE = 1_000_000


class RaplReadError(RuntimeError):
    """Raised when the host RAPL package counter cannot be read."""


def get_rapl_energy_path():
    return Path(os.getenv("MONITOR_RAPL_ENERGY_UJ_PATH", DEFAULT_RAPL_ENERGY_UJ_PATH))


def get_rapl_max_range_path():
    return Path(os.getenv("MONITOR_RAPL_MAX_ENERGY_RANGE_UJ_PATH", DEFAULT_RAPL_MAX_RANGE_UJ_PATH))


def read_int_file(path):
    try:
        return int(Path(path).read_text(encoding="utf-8").strip())
    except (OSError, ValueError) as exc:
        raise RaplReadError(f"Unable to read RAPL value from {path}: {exc}") from exc


def read_rapl_package_counter(energy_path=None, max_range_path=None):
    """Read package-level Intel RAPL counter and max range from sysfs."""
    energy_path = Path(energy_path or get_rapl_energy_path())
    max_range_path = Path(max_range_path or get_rapl_max_range_path())
    return {
        "energy_uj": read_int_file(energy_path),
        "max_energy_range_uj": read_int_file(max_range_path),
    }


def calculate_rapl_delta_uj(start_uj, end_uj, max_range_uj):
    start_uj = int(start_uj)
    end_uj = int(end_uj)
    max_range_uj = int(max_range_uj)
    if start_uj < 0 or end_uj < 0 or max_range_uj <= 0:
        raise ValueError("RAPL counter values must be non-negative and max range must be positive.")
    if end_uj >= start_uj:
        return end_uj - start_uj
    return (max_range_uj - start_uj) + end_uj


def microjoules_to_kwh(energy_uj):
    return float(energy_uj) / UJ_PER_KWH


def calculate_energy_from_rapl(start_uj, end_uj, duration_seconds, max_range_uj):
    """Calculate measured CPU package energy and average package power."""
    duration_seconds = max(0.0, float(duration_seconds or 0.0))
    delta_uj = calculate_rapl_delta_uj(start_uj, end_uj, max_range_uj)
    energy_kwh = microjoules_to_kwh(delta_uj)
    energy_joules = delta_uj / UJ_PER_JOULE
    average_power_watts = energy_joules / duration_seconds if duration_seconds > 0 else 0.0

    return {
        "total_power_watts": average_power_watts,
        "active_power_watts": average_power_watts,
        "total_energy_kwh": energy_kwh,
        "active_energy_kwh": energy_kwh,
    }


def estimate_energy_kwh(avg_cpu_percent, duration_seconds):
    """Legacy fallback used only when RAPL is unavailable.

    This preserves older Monitor behavior for compatibility, but successful
    START/STOP RAPL reads must use calculate_energy_from_rapl() instead.
    """
    p_idle_watts = 10
    p_max_watts = 80

    active_power_watts = (avg_cpu_percent / 100) * (p_max_watts - p_idle_watts)
    total_power_watts = p_idle_watts + active_power_watts

    total_energy_kwh = (total_power_watts * duration_seconds) / 3_600_000
    active_energy_kwh = (active_power_watts * duration_seconds) / 3_600_000

    return {
        "total_power_watts": total_power_watts,
        "active_power_watts": active_power_watts,
        "total_energy_kwh": total_energy_kwh,
        "active_energy_kwh": active_energy_kwh,
    }
