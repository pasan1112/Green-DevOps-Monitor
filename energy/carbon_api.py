import requests

API_KEY = "YOUR_API_KEY"

def get_carbon_intensity(zone="LK"):
    url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={zone}"

    headers = {
        "auth-token": "BpjVnaE4hWm9CE947xSP"
    }

    response = requests.get(url, headers=headers)

    data = response.json()

    return data["carbonIntensity"] / 1000  # convert gCO2 → kgCO2