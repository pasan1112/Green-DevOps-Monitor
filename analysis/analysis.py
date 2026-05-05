import pandas as pd
import matplotlib.pyplot as plt

# Load data
df = pd.read_csv("data/metrics.csv")
# Assign column names
df.columns = ["stage", "duration", "cpu", "memory", "energy", "carbon", "timestamp"]

# 🔥 Convert numeric columns properly
df["duration"] = pd.to_numeric(df["duration"], errors="coerce")
df["cpu"] = pd.to_numeric(df["cpu"], errors="coerce")
df["memory"] = pd.to_numeric(df["memory"], errors="coerce")
df["energy"] = pd.to_numeric(df["energy"], errors="coerce")
df["carbon"] = pd.to_numeric(df["carbon"], errors="coerce")

# Drop bad rows (if any)
df = df.dropna()

# Group by stage
grouped = df.groupby("stage").mean(numeric_only=True)

print("\n=== AVERAGE VALUES ===")
print(grouped)

# Plot energy
grouped["energy"].plot(kind="bar", title="Energy per Stage")
plt.ylabel("Energy (kWh)")
plt.savefig("energy.png")
plt.close()

# Plot carbon
grouped["carbon"].plot(kind="bar", title="Carbon per Stage")
plt.ylabel("Carbon (kg CO2)")
plt.savefig("carbon.png")
plt.close()

print("\nGraphs saved as energy.png and carbon.png")