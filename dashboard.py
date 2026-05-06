from flask import Flask, render_template_string
import pandas as pd
import os
import json

app = Flask(__name__)

DATA_PATH = "data/metrics.csv"

HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Green DevOps Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <meta http-equiv="refresh" content="10">
    <style>
        * { box-sizing: border-box; }

        body {
            margin: 0;
            font-family: Inter, Segoe UI, Arial, sans-serif;
            background:
                radial-gradient(circle at top left, #064e3b 0, transparent 32%),
                radial-gradient(circle at top right, #1e3a8a 0, transparent 28%),
                #020617;
            color: #e5e7eb;
        }

        .page {
            padding: 28px;
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 26px;
        }

        .title h1 {
            margin: 0;
            font-size: 34px;
            color: #ffffff;
        }

        .title p {
            margin: 6px 0 0;
            color: #94a3b8;
        }

        .badge {
            background: rgba(34, 197, 94, 0.14);
            color: #86efac;
            border: 1px solid rgba(34, 197, 94, 0.35);
            padding: 10px 14px;
            border-radius: 999px;
            font-size: 14px;
        }

        .cards {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 18px;
            margin-bottom: 22px;
        }

        .card, .panel, .insight, table {
            background: rgba(15, 23, 42, 0.78);
            backdrop-filter: blur(14px);
            border: 1px solid rgba(148, 163, 184, 0.16);
            border-radius: 18px;
            box-shadow: 0 18px 40px rgba(0,0,0,0.35);
        }

        .card {
            padding: 20px;
            min-height: 120px;
        }

        .card .label {
            color: #94a3b8;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        .card .value {
            margin-top: 14px;
            font-size: 28px;
            font-weight: 800;
            color: #ffffff;
        }

        .card .sub {
            margin-top: 8px;
            font-size: 13px;
            color: #64748b;
        }

        .green { color: #22c55e !important; }
        .cyan { color: #38bdf8 !important; }
        .amber { color: #f59e0b !important; }
        .red { color: #fb7185 !important; }

        .grid {
            display: grid;
            grid-template-columns: 1.15fr 0.85fr;
            gap: 20px;
            margin-bottom: 20px;
        }

        .panel {
            padding: 20px;
        }

        .panel h2 {
            margin: 0 0 16px;
            font-size: 18px;
            color: #f8fafc;
        }

        .two-charts {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 18px;
            margin-bottom: 20px;
        }

        .insight {
            padding: 18px 20px;
            margin-bottom: 20px;
            border-left: 4px solid #22c55e;
        }

        .insight h3 {
            margin: 0 0 8px;
            color: #86efac;
        }

        .insight p {
            margin: 0;
            color: #d1fae5;
            line-height: 1.6;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            overflow: hidden;
        }

        th, td {
            padding: 14px 16px;
            border-bottom: 1px solid rgba(148, 163, 184, 0.14);
            font-size: 14px;
        }

        th {
            text-align: left;
            color: #86efac;
            background: rgba(15, 23, 42, 0.9);
        }

        td {
            color: #cbd5e1;
        }

        tr:hover {
            background: rgba(34, 197, 94, 0.06);
        }

        .footer {
            margin-top: 18px;
            color: #64748b;
            font-size: 13px;
            text-align: right;
        }

        @media (max-width: 1100px) {
            .cards, .grid, .two-charts {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>

<body>
<div class="page">

    <div class="header">
        <div class="title">
            <h1>Green DevOps Sustainability Monitor</h1>
            <p>Stage-level CI/CD energy and carbon intelligence from Jenkins pipeline executions.</p>
        </div>
        <div class="badge">
            Latest Run: {{ latest_run }}
        </div>
    </div>

    <div class="cards">
        <div class="card">
            <div class="label">Total Energy</div>
            <div class="value green">{{ total_energy }}</div>
            <div class="sub">kWh across latest pipeline run</div>
        </div>

        <div class="card">
            <div class="label">Active Compute Energy</div>
            <div class="value cyan">{{ active_energy }}</div>
            <div class="sub">CPU-driven workload energy</div>
        </div>

        <div class="card">
            <div class="label">Total Carbon</div>
            <div class="value amber">{{ total_carbon }}</div>
            <div class="sub">kgCO₂eq estimated emissions</div>
        </div>

        <div class="card">
            <div class="label">Carbon Intensity</div>
            <div class="value red">{{ carbon_intensity }}</div>
            <div class="sub">kgCO₂/kWh | Source: {{ carbon_source }}</div>
        </div>
    </div>

    <div class="insight">
        <h3>Automated Sustainability Insight</h3>
        <p>{{ insight }}</p>
    </div>

    <div class="grid">
        <div class="panel">
            <h2>Total vs Active Energy by Stage</h2>
            <canvas id="energyChart"></canvas>
        </div>

        <div class="panel">
            <h2>Average CPU Usage by Stage</h2>
            <canvas id="cpuChart"></canvas>
        </div>
    </div>

    <div class="two-charts">
        <div class="panel">
            <h2>Total Carbon by Stage</h2>
            <canvas id="carbonChart"></canvas>
        </div>

        <div class="panel">
            <h2>Stage Duration</h2>
            <canvas id="durationChart"></canvas>
        </div>
    </div>

    <table>
        <thead>
            <tr>
                <th>Stage</th>
                <th>Status</th>
                <th>Duration (s)</th>
                <th>Avg CPU %</th>
                <th>Total Energy</th>
                <th>Active Energy</th>
                <th>Total Carbon</th>
                <th>Active Carbon</th>
            </tr>
        </thead>
        <tbody>
        {% for row in rows %}
            <tr>
                <td>{{ row.stage }}</td>
                <td>{{ row.status }}</td>
                <td>{{ row.duration_seconds }}</td>
                <td>{{ row.avg_cpu_percent }}</td>
                <td>{{ row.total_energy_kwh }}</td>
                <td>{{ row.active_energy_kwh }}</td>
                <td>{{ row.total_carbon_kg }}</td>
                <td>{{ row.active_carbon_kg }}</td>
            </tr>
        {% endfor %}
        </tbody>
    </table>

    <div class="footer">
        Auto-refreshes every 10 seconds | Data source: data/metrics.csv
    </div>

</div>

<script>
const stages = {{ stages | safe }};
const totalEnergy = {{ total_energy_values | safe }};
const activeEnergy = {{ active_energy_values | safe }};
const totalCarbon = {{ total_carbon_values | safe }};
const avgCpu = {{ cpu_values | safe }};
const duration = {{ duration_values | safe }};

const gridColor = "rgba(148, 163, 184, 0.18)";
const tickColor = "#94a3b8";

Chart.defaults.color = tickColor;
Chart.defaults.borderColor = gridColor;

new Chart(document.getElementById("energyChart"), {
    type: "bar",
    data: {
        labels: stages,
        datasets: [
            {
                label: "Total Energy (kWh)",
                data: totalEnergy,
                backgroundColor: "rgba(34, 197, 94, 0.75)",
                borderRadius: 8
            },
            {
                label: "Active Energy (kWh)",
                data: activeEnergy,
                backgroundColor: "rgba(56, 189, 248, 0.75)",
                borderRadius: 8
            }
        ]
    },
    options: {
        responsive: true,
        plugins: { legend: { labels: { color: tickColor } } },
        scales: {
            y: { beginAtZero: true, grid: { color: gridColor } },
            x: { grid: { display: false } }
        }
    }
});

new Chart(document.getElementById("cpuChart"), {
    type: "line",
    data: {
        labels: stages,
        datasets: [{
            label: "Average CPU %",
            data: avgCpu,
            tension: 0.38,
            fill: true,
            borderColor: "rgba(251, 191, 36, 1)",
            backgroundColor: "rgba(251, 191, 36, 0.15)",
            pointRadius: 5
        }]
    },
    options: {
        responsive: true,
        scales: {
            y: { beginAtZero: true, grid: { color: gridColor } },
            x: { grid: { display: false } }
        }
    }
});

new Chart(document.getElementById("carbonChart"), {
    type: "doughnut",
    data: {
        labels: stages,
        datasets: [{
            data: totalCarbon,
            backgroundColor: [
                "rgba(34, 197, 94, 0.8)",
                "rgba(59, 130, 246, 0.8)",
                "rgba(244, 63, 94, 0.8)",
                "rgba(245, 158, 11, 0.8)"
            ],
            borderColor: "#020617"
        }]
    },
    options: {
        responsive: true,
        plugins: { legend: { position: "bottom" } }
    }
});

new Chart(document.getElementById("durationChart"), {
    type: "bar",
    data: {
        labels: stages,
        datasets: [{
            label: "Duration (seconds)",
            data: duration,
            backgroundColor: "rgba(168, 85, 247, 0.75)",
            borderRadius: 8
        }]
    },
    options: {
        responsive: true,
        scales: {
            y: { beginAtZero: true, grid: { color: gridColor } },
            x: { grid: { display: false } }
        }
    }
});
</script>

</body>
</html>
"""


@app.route("/")
def dashboard():
    if not os.path.exists(DATA_PATH):
        return "<h2>No monitoring data found. Run Jenkins pipeline first.</h2>"

    df = pd.read_csv(DATA_PATH)

    if df.empty:
        return "<h2>Metrics file is empty. Run Jenkins pipeline first.</h2>"

    numeric_cols = [
        "duration_seconds",
        "avg_cpu_percent",
        "total_energy_kwh",
        "active_energy_kwh",
        "total_carbon_kg",
        "active_carbon_kg",
        "carbon_intensity_kg_per_kwh"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    latest_run = df["run_id"].iloc[-1]
    latest = df[df["run_id"] == latest_run].copy()

    stage_order = ["build", "test", "deploy"]
    latest["stage"] = pd.Categorical(latest["stage"], categories=stage_order, ordered=True)
    latest = latest.sort_values("stage")

    summary = latest.groupby("stage", observed=True).mean(numeric_only=True).reset_index()

    total_energy = round(latest["total_energy_kwh"].sum(), 10)
    active_energy = round(latest["active_energy_kwh"].sum(), 10)
    total_carbon = round(latest["total_carbon_kg"].sum(), 10)
    carbon_intensity = round(latest["carbon_intensity_kg_per_kwh"].mean(), 6)
    carbon_source = latest["carbon_source"].iloc[-1]

    highest_active_stage = summary.sort_values("active_energy_kwh", ascending=False).iloc[0]["stage"]
    highest_total_stage = summary.sort_values("total_energy_kwh", ascending=False).iloc[0]["stage"]

    insight = (
        f"The {highest_active_stage} stage has the highest active compute energy, "
        f"indicating the greatest workload-driven resource demand. "
        f"The {highest_total_stage} stage has the highest total energy, which includes baseline server power during execution time. "
        f"This separation helps identify both compute-heavy stages and duration-driven energy cost."
    )

    return render_template_string(
        HTML,
        latest_run=latest_run,
        total_energy=total_energy,
        active_energy=active_energy,
        total_carbon=total_carbon,
        carbon_intensity=carbon_intensity,
        carbon_source=carbon_source,
        insight=insight,
        rows=latest.to_dict(orient="records"),
        stages=json.dumps(summary["stage"].astype(str).tolist()),
        total_energy_values=json.dumps(summary["total_energy_kwh"].round(10).tolist()),
        active_energy_values=json.dumps(summary["active_energy_kwh"].round(10).tolist()),
        total_carbon_values=json.dumps(summary["total_carbon_kg"].round(10).tolist()),
        cpu_values=json.dumps(summary["avg_cpu_percent"].round(4).tolist()),
        duration_values=json.dumps(summary["duration_seconds"].round(4).tolist()),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5051)