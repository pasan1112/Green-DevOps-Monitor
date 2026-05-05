from energy.carbon_api import get_carbon_intensity

def estimate_carbon(energy):
    try:
        intensity = get_carbon_intensity()
        print ("Intensity: ", intensity)
    except Exception as e:
        print("API failed, using fallback:", e)
        intensity = 0.5

    return energy * intensity