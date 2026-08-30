import os
import requests

DEFAULT_CARBON_INTENSITY_KG_PER_KWH = 0.5


def get_carbon_intensity_kg_per_kwh(zone="LK"):
    api_key = "em_PumbkUMwDGwV9CMW7Cmgc3P6N2HmGmP6"

    if not api_key:
        return DEFAULT_CARBON_INTENSITY_KG_PER_KWH, "fallback_no_api_key"

    url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={zone}"

    try:
        response = requests.get(
            url,
            headers={"auth-token": api_key},
            timeout=10
        )
        response.raise_for_status()

        data = response.json()
        carbon_intensity_g = data["carbonIntensity"]
        carbon_intensity_kg = carbon_intensity_g / 1000

        return carbon_intensity_kg, "electricitymaps"

    except Exception:
        return DEFAULT_CARBON_INTENSITY_KG_PER_KWH, "fallback_api_error"


def estimate_carbon_kg(energy_kwh, zone="LK"):
    carbon_intensity, source = get_carbon_intensity_kg_per_kwh(zone)
    carbon_kg = energy_kwh * carbon_intensity

    return {
        "carbon_kg": carbon_kg,
        "carbon_intensity_kg_per_kwh": carbon_intensity,
        "carbon_source": source,
    }