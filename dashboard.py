from flask import Flask, render_template_string, request
import pandas as pd
import os
import json
from pymongo import MongoClient

app = Flask(__name__)

MONGO_DB_NAME = "green_devops_monitor"
MONGO_COLLECTION_NAME = "pipeline_metrics"
CSV_FALLBACK_PATH = "data/metrics.csv"

def load_metrics():
    mongo_uri = os.getenv("MONGO_URI")
    if mongo_uri:
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            db = client[MONGO_DB_NAME]
            collection = db[MONGO_COLLECTION_NAME]
            records = list(collection.find({}, {"_id": 0}))
            client.close()
            if records:
                return pd.DataFrame(records), "MongoDB Atlas"
        except Exception as e:
            print(f"MongoDB read failed. Falling back to CSV. Error: {e}")

    if os.path.exists(CSV_FALLBACK_PATH):
        try:
            return pd.read_csv(CSV_FALLBACK_PATH), "CSV fallback"
        except Exception:
            pass
    return pd.DataFrame(), "No data source"

HTML = """
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Green DevOps Monitor</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://unpkg.com/lucide@latest"></script>
    <meta http-equiv="refresh" content="15">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');
        
        :root {
            --glass: rgba(15, 23, 42, 0.65);
            --border: rgba(255, 255, 255, 0.08);
        }

        body {
            font-family: 'Plus Jakarta Sans', sans-serif;
            background: #020617;
            background-image: 
                radial-gradient(at 0% 0%, rgba(16, 185, 129, 0.1) 0px, transparent 50%),
                radial-gradient(at 100% 0%, rgba(59, 130, 246, 0.1) 0px, transparent 50%);
            color: #f1f5f9;
            min-height: 100vh;
        }

        .glass-panel {
            background: var(--glass);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid var(--border);
            border-radius: 1rem;
        }

        .sidebar-scroll::-webkit-scrollbar { width: 4px; }
        .sidebar-scroll::-webkit-scrollbar-track { background: transparent; }
        .sidebar-scroll::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 10px; }

        .status-pulse {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
            box-shadow: 0 0 0 rgba(34, 197, 94, 0.4);
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.7); }
            70% { box-shadow: 0 0 0 10px rgba(34, 197, 94, 0); }
            100% { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0); }
        }
        
        .nav-item-active {
            background: rgba(16, 185, 129, 0.15);
            border-color: rgba(16, 185, 129, 0.4) !important;
        }
    </style>
</head>
<body class="p-4 md:p-8">
    <div class="max-w-[1600px] mx-auto">
        <!-- Header -->
        <header class="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 gap-4">
            <div>
                <div class="flex items-center gap-3">
                    <div class="p-2 bg-emerald-500/20 rounded-lg">
                        <i data-lucide="leaf" class="text-emerald-400 w-8 h-8"></i>
                    </div>
                    <h1 class="text-3xl font-extrabold tracking-tight text-white">Green DevOps <span class="text-emerald-400">Monitor</span></h1>
                </div>
                <p class="text-slate-400 mt-1 font-medium">Real-time CI/CD sustainability & performance analytics</p>
            </div>
            <div class="flex gap-2">
                <div class="glass-panel px-4 py-2 flex items-center gap-2 border-emerald-500/20">
                    <span class="status-pulse bg-emerald-500"></span>
                    <span class="text-sm font-semibold text-emerald-100">{{ data_source }}</span>
                </div>
                <div class="glass-panel px-4 py-2 flex items-center gap-2">
                    <i data-lucide="hash" class="w-4 h-4 text-slate-400"></i>
                    <span class="text-sm font-semibold text-slate-300">Run: {{ selected_run }}</span>
                </div>
            </div>
        </header>

        <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">
            <!-- Sidebar Navigation -->
            <aside class="lg:col-span-3 flex flex-col gap-4 max-h-[85vh]">
                <div class="glass-panel p-4 flex-1 flex flex-col overflow-hidden">
                    <div class="flex items-center justify-between mb-4 px-2">
                        <h2 class="text-xs font-bold uppercase tracking-widest text-slate-500">Run History</h2>
                        <i data-lucide="history" class="w-4 h-4 text-slate-500"></i>
                    </div>
                    <div class="sidebar-scroll overflow-y-auto space-y-2 pr-2">
                        {% for run in runs %}
                        <a href="/?run_id={{ run.run_id }}" 
                           class="block p-3 rounded-xl border border-transparent transition-all hover:border-white/10 hover:bg-white/5 {% if run.run_id == selected_run %}nav-item-active{% endif %}">
                            <div class="flex justify-between items-start mb-2">
                                <span class="text-sm font-bold text-slate-200 truncate w-2/3">#{{ run.run_id }}</span>
                                <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase tracking-tighter {% if run.status == 'success' %}bg-emerald-500/20 text-emerald-400 border border-emerald-500/30{% else %}bg-rose-500/20 text-rose-400 border border-rose-500/30{% endif %}">
                                    {{ run.status }}
                                </span>
                            </div>
                            <div class="grid grid-cols-2 gap-2 text-[11px] text-slate-400 font-medium">
                                <span class="flex items-center gap-1"><i data-lucide="zap" class="w-3 h-3"></i> {{ run.total_energy_kwh | round(4) }} kWh</span>
                                <span class="flex items-center gap-1"><i data-lucide="clock" class="w-3 h-3"></i> {{ run.duration_seconds | int }}s</span>
                            </div>
                        </a>
                        {% endfor %}
                    </div>
                </div>
            </aside>

            <!-- Main Content -->
            <main class="lg:col-span-9 space-y-6">
                
                <!-- KPI Section -->
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    <div class="glass-panel p-5 relative overflow-hidden group">
                        <div class="absolute -right-2 -bottom-2 opacity-5 transition-transform group-hover:scale-110">
                            <i data-lucide="zap" class="w-24 h-24 text-emerald-400"></i>
                        </div>
                        <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Total Energy</p>
                        <p class="text-3xl font-black text-emerald-400">{{ total_energy }} <span class="text-sm font-normal text-slate-500">kWh</span></p>
                        <p class="text-[10px] text-slate-500 mt-2 font-medium">Stage accumulation for this run</p>
                    </div>

                    <div class="glass-panel p-5 relative overflow-hidden group">
                        <div class="absolute -right-2 -bottom-2 opacity-5 transition-transform group-hover:scale-110">
                            <i data-lucide="cloud" class="w-24 h-24 text-sky-400"></i>
                        </div>
                        <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Carbon Footprint</p>
                        <p class="text-3xl font-black text-sky-400">{{ total_carbon }} <span class="text-sm font-normal text-slate-500">kgCO₂</span></p>
                        <p class="text-[10px] text-slate-500 mt-2 font-medium">Based on local grid intensity</p>
                    </div>

                    <div class="glass-panel p-5 relative overflow-hidden group">
                        <div class="absolute -right-2 -bottom-2 opacity-5 transition-transform group-hover:scale-110">
                            <i data-lucide="activity" class="w-24 h-24 text-amber-400"></i>
                        </div>
                        <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Avg CPU Load</p>
                        <p class="text-3xl font-black text-amber-400">{{ (cpu_values_raw | sum / cpu_values_raw | length) | round(2) if cpu_values_raw else 0 }} <span class="text-sm font-normal text-slate-500">%</span></p>
                        <p class="text-[10px] text-slate-500 mt-2 font-medium">Mean utilization across stages</p>
                    </div>

                    <div class="glass-panel p-5 relative overflow-hidden group">
                        <div class="absolute -right-2 -bottom-2 opacity-5 transition-transform group-hover:scale-110">
                            <i data-lucide="timer" class="w-24 h-24 text-purple-400"></i>
                        </div>
                        <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Total Duration</p>
                        <p class="text-3xl font-black text-purple-400">{{ pipeline_duration }} <span class="text-sm font-normal text-slate-500">s</span></p>
                        <p class="text-[10px] text-slate-500 mt-2 font-medium">Wall-clock execution time</p>
                    </div>
                </div>

                <!-- Insights Row -->
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div class="bg-emerald-500/5 border border-emerald-500/20 rounded-2xl p-5 flex gap-4">
                        <div class="mt-1"><i data-lucide="info" class="w-5 h-5 text-emerald-400"></i></div>
                        <div>
                            <h4 class="text-sm font-bold text-emerald-300 mb-1">Pipeline Sustainability</h4>
                            <p class="text-xs text-emerald-100/70 leading-relaxed">{{ pipeline_insight }}</p>
                        </div>
                    </div>
                    <div class="bg-sky-500/5 border border-sky-500/20 rounded-2xl p-5 flex gap-4">
                        <div class="mt-1"><i data-lucide="bar-chart-3" class="w-5 h-5 text-sky-400"></i></div>
                        <div>
                            <h4 class="text-sm font-bold text-sky-300 mb-1">Stage Dynamics</h4>
                            <p class="text-xs text-sky-100/70 leading-relaxed">{{ stage_insight }}</p>
                        </div>
                    </div>
                </div>

                <!-- Charts Section -->
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <div class="glass-panel p-6">
                        <div class="flex items-center justify-between mb-6">
                            <h3 class="text-sm font-bold text-slate-200 uppercase tracking-wider">Energy Consumption by Stage</h3>
                            <i data-lucide="zap" class="w-4 h-4 text-emerald-400"></i>
                        </div>
                        <div class="h-[250px]">
                            <canvas id="energyChart"></canvas>
                        </div>
                    </div>
                    <div class="glass-panel p-6">
                        <div class="flex items-center justify-between mb-6">
                            <h3 class="text-sm font-bold text-slate-200 uppercase tracking-wider">CPU Utilization Profile</h3>
                            <i data-lucide="cpu" class="w-4 h-4 text-amber-400"></i>
                        </div>
                        <div class="h-[250px]">
                            <canvas id="cpuChart"></canvas>
                        </div>
                    </div>
                </div>

                <!-- Table Section -->
                <div class="glass-panel overflow-hidden">
                    <div class="px-6 py-4 border-b border-white/5 flex items-center justify-between bg-white/2">
                        <h3 class="text-sm font-bold text-slate-200 uppercase tracking-wider">Stage Breakdown</h3>
                        <span class="text-[10px] font-bold text-slate-500 uppercase tracking-widest">{{ stage_count }} Stages Tracked</span>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left">
                            <thead>
                                <tr class="text-[11px] font-bold text-slate-400 uppercase tracking-wider bg-slate-900/40">
                                    <th class="px-6 py-4">Stage</th>
                                    <th class="px-6 py-4 text-center">Status</th>
                                    <th class="px-6 py-4">Duration</th>
                                    <th class="px-6 py-4">Avg CPU</th>
                                    <th class="px-6 py-4">Energy (kWh)</th>
                                    <th class="px-6 py-4 text-right">Carbon (kg)</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-white/5">
                                {% for row in rows %}
                                <tr class="hover:bg-white/5 transition-colors group">
                                    <td class="px-6 py-4">
                                        <div class="flex items-center gap-3">
                                            <div class="w-2 h-2 rounded-full bg-emerald-500 group-hover:scale-125 transition-transform"></div>
                                            <span class="font-bold text-slate-200">{{ row.stage }}</span>
                                        </div>
                                    </td>
                                    <td class="px-6 py-4 text-center">
                                        <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {% if row.status == 'success' %}text-emerald-400 bg-emerald-500/10{% else %}text-rose-400 bg-rose-500/10{% endif %}">
                                            {{ row.status }}
                                        </span>
                                    </td>
                                    <td class="px-6 py-4 text-slate-300 font-medium">{{ row.duration_seconds }}s</td>
                                    <td class="px-6 py-4">
                                        <div class="flex items-center gap-2">
                                            <span class="text-slate-300">{{ row.avg_cpu_percent }}%</span>
                                            <div class="flex-1 w-12 h-1 bg-slate-800 rounded-full overflow-hidden">
                                                <div class="h-full bg-amber-500/60" style="width: {{ row.avg_cpu_percent }}%"></div>
                                            </div>
                                        </div>
                                    </td>
                                    <td class="px-6 py-4 text-emerald-400 font-mono text-sm">{{ row.total_energy_kwh }}</td>
                                    <td class="px-6 py-4 text-right text-sky-400 font-mono text-sm">{{ row.total_carbon_kg }}</td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>

                <footer class="text-center text-slate-500 text-[10px] uppercase tracking-[0.2em] pt-4 border-t border-white/5">
                    Pipeline Engine v2.4 • Monitoring Active • Refreshes in 15s
                </footer>
            </main>
        </div>
    </div>

    <script>
        lucide.createIcons();

        const stages = {{ stages | safe }};
        const totalEnergy = {{ total_energy_values | safe }};
        const activeEnergy = {{ active_energy_values | safe }};
        const avgCpu = {{ cpu_values | safe }};

        const gridColor = "rgba(255, 255, 255, 0.05)";
        const tickColor = "#64748b";

        Chart.defaults.font.family = "'Plus Jakarta Sans', sans-serif";
        Chart.defaults.color = tickColor;

        const chartConfig = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                y: { grid: { color: gridColor }, border: { display: false } },
                x: { grid: { display: false } }
            }
        };

        new Chart(document.getElementById("energyChart"), {
            type: "bar",
            data: {
                labels: stages,
                datasets: [
                    {
                        label: "Total kWh",
                        data: totalEnergy,
                        backgroundColor: "rgba(16, 185, 129, 0.6)",
                        borderColor: "rgba(16, 185, 129, 1)",
                        borderWidth: 1,
                        borderRadius: 6,
                        barThickness: 20
                    },
                    {
                        label: "Active kWh",
                        data: activeEnergy,
                        backgroundColor: "rgba(56, 189, 248, 0.6)",
                        borderColor: "rgba(56, 189, 248, 1)",
                        borderWidth: 1,
                        borderRadius: 6,
                        barThickness: 20
                    }
                ]
            },
            options: chartConfig
        });

        new Chart(document.getElementById("cpuChart"), {
            type: "line",
            data: {
                labels: stages,
                datasets: [{
                    data: avgCpu,
                    borderColor: "rgba(245, 158, 11, 1)",
                    backgroundColor: "rgba(245, 158, 11, 0.1)",
                    fill: true,
                    tension: 0.4,
                    pointBackgroundColor: "rgba(245, 158, 11, 1)",
                    pointRadius: 4
                }]
            },
            options: chartConfig
        });
    </script>
</body>
</html>
"""

@app.route("/")
def dashboard():
    df, data_source = load_metrics()

    if df.empty:
        return "<div style='background:#020617; color:white; height:100vh; display:flex; align-items:center; justify-content:center; font-family:sans-serif;'><h2>No monitoring data found.</h2></div>"

    numeric_cols = [
        "duration_seconds", "avg_cpu_percent", "peak_cpu_percent",
        "total_energy_kwh", "active_energy_kwh", "total_carbon_kg",
        "active_carbon_kg", "carbon_intensity_kg_per_kwh"
    ]

    for col in numeric_cols:
        if col not in df.columns: df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "status" not in df.columns: df["status"] = "unknown"
    if "end_timestamp" not in df.columns: df["end_timestamp"] = ""
    if "carbon_source" not in df.columns: df["carbon_source"] = "unknown"

    run_summary = (
        df.groupby("run_id")
        .agg(
            total_energy_kwh=("total_energy_kwh", "sum"),
            duration_seconds=("duration_seconds", "sum"),
            status=("status", lambda x: "failed" if "failed" in list(x) else "success"),
            latest_time=("end_timestamp", "max")
        )
        .reset_index()
        .sort_values("latest_time", ascending=False)
    )

    requested_run = request.args.get("run_id")
    selected_run = requested_run if requested_run in run_summary["run_id"].values else run_summary.iloc[0]["run_id"]

    latest = df[df["run_id"] == selected_run].copy()
    stage_order = ["build", "test", "deploy"]
    latest["stage"] = pd.Categorical(latest["stage"], categories=stage_order, ordered=True)
    latest = latest.sort_values("stage")

    summary = latest.groupby("stage", observed=True).mean(numeric_only=True).reset_index()

    total_energy = round(latest["total_energy_kwh"].sum(), 6)
    total_carbon = round(latest["total_carbon_kg"].sum(), 6)
    carbon_intensity = round(latest["carbon_intensity_kg_per_kwh"].mean(), 6)
    carbon_source = latest["carbon_source"].iloc[-1] if not latest.empty else "N/A"
    pipeline_duration = round(latest["duration_seconds"].sum(), 2)

    highest_active_stage = summary.sort_values("active_energy_kwh", ascending=False).iloc[0]["stage"] if not summary.empty else "N/A"
    highest_total_stage = summary.sort_values("total_energy_kwh", ascending=False).iloc[0]["stage"] if not summary.empty else "N/A"

    pipeline_insight = f"Run {selected_run} reached {total_energy} kWh with {total_carbon} kgCO₂eq. Tracking data sourced via {carbon_source}."
    stage_insight = f"The {highest_active_stage} stage dominated active compute demand, while {highest_total_stage} had the largest power footprint."

    display_rows = latest.copy()
    for col in numeric_cols:
        display_rows[col] = display_rows[col].round(6)

    return render_template_string(
        HTML,
        selected_run=selected_run,
        data_source=data_source,
        runs=run_summary.to_dict(orient="records"),
        total_energy=total_energy,
        total_carbon=total_carbon,
        carbon_intensity=carbon_intensity,
        carbon_source=carbon_source,
        pipeline_duration=pipeline_duration,
        stage_count=len(latest),
        pipeline_insight=pipeline_insight,
        stage_insight=stage_insight,
        rows=display_rows.to_dict(orient="records"),
        stages=json.dumps(summary["stage"].astype(str).tolist()),
        total_energy_values=json.dumps(summary["total_energy_kwh"].tolist()),
        active_energy_values=json.dumps(summary["active_energy_kwh"].tolist()),
        cpu_values=json.dumps(summary["avg_cpu_percent"].tolist()),
        cpu_values_raw=summary["avg_cpu_percent"].tolist()
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5051, debug=True)