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
        return pd.read_csv(CSV_FALLBACK_PATH), "CSV fallback"

    return pd.DataFrame(), "No data source"


HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Green DevOps Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <meta http-equiv="refresh" content="15">
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

        .page { padding: 28px; }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 24px;
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
            margin-left: 10px;
        }

        .badge-row {
            display: flex;
            flex-wrap: wrap;
            justify-content: flex-end;
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
            min-height: 118px;
        }

        .label {
            color: #94a3b8;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        .value {
            margin-top: 14px;
            font-size: 28px;
            font-weight: 800;
            color: #ffffff;
        }

        .sub {
            margin-top: 8px;
            font-size: 13px;
            color: #64748b;
        }

        .green { color: #22c55e !important; }
        .cyan { color: #38bdf8 !important; }
        .amber { color: #f59e0b !important; }
        .red { color: #fb7185 !important; }
        .purple { color: #c084fc !important; }

        .layout {
            display: grid;
            grid-template-columns: 380px 1fr;
            gap: 20px;
            align-items: start;
        }

        .run-list {
            padding: 18px;
            max-height: calc(100vh - 120px);
            overflow-y: auto;
        }

        .run-list h2 {
            margin-top: 0;
            font-size: 18px;
        }

        .run-item {
            display: block;
            text-decoration: none;
            color: #cbd5e1;
            padding: 14px;
            border-radius: 14px;
            margin-bottom: 10px;
            background: rgba(30, 41, 59, 0.65);
            border: 1px solid rgba(148, 163, 184, 0.12);
            transition: 0.2s ease;
        }

        .run-item:hover {
            transform: translateY(-2px);
            background: rgba(34, 197, 94, 0.10);
            border-color: rgba(34, 197, 94, 0.35);
        }

        .run-item.active {
            background: rgba(34, 197, 94, 0.18);
            border-color: rgba(34, 197, 94, 0.55);
        }

        .run-id {
            font-weight: 800;
            color: #f8fafc;
            font-size: 14px;
            word-break: break-word;
        }

        .run-meta {
            margin-top: 8px;
            font-size: 12px;
            color: #94a3b8;
            line-height: 1.5;
        }

        .status-pill {
            display: inline-block;
            margin-top: 8px;
            padding: 4px 9px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: 700;
        }

        .status-success {
            color: #86efac;
            background: rgba(34, 197, 94, 0.14);
            border: 1px solid rgba(34, 197, 94, 0.35);
        }

        .status-failed {
            color: #fda4af;
            background: rgba(244, 63, 94, 0.14);
            border: 1px solid rgba(244, 63, 94, 0.35);
        }

        .grid {
            display: grid;
            grid-template-columns: 1.15fr 0.85fr;
            gap: 20px;
            margin-bottom: 20px;
        }

        .two-charts {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 18px;
            margin-bottom: 20px;
        }

        .panel { padding: 20px; }

        .panel h2 {
            margin: 0 0 16px;
            font-size: 18px;
            color: #f8fafc;
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

        td { color: #cbd5e1; }

        tr:hover { background: rgba(34, 197, 94, 0.06); }

        .footer {
            margin-top: 18px;
            color: #64748b;
            font-size: 13px;
            text-align: right;
        }

        @media (max-width: 1200px) {
            .cards, .grid, .two-charts, .layout {
                grid-template-columns: 1fr;
            }

            .run-list {
                max-height: none;
            }
        }
    </style>
</head>

<body>
<div class="page">

    <div class="header">
        <div class="title">
            <h1>Green DevOps Sustainability Monitor</h1>
            <p>Live MongoDB-backed CI/CD pipeline sustainability analytics with stage-level drilldown.</p>
        </div>
        <div class="badge-row">
            <div class="badge">Selected Run: {{ selected_run }}</div>
            <div class="badge">Source: {{ data_source }}</div>
        </div>
    </div>

    <div class="layout">

        <div class="panel run-list">
            <h2>Pipeline Run History</h2>

            {% for run in runs %}
            <a class="run-item {% if run.run_id == selected_run %}active{% endif %}" href="/?run_id={{ run.run_id }}">
                <div class="run-id">{{ run.run_id }}</div>
                <div class="run-meta">
                    Energy: {{ run.total_energy_kwh }} kWh<br>
                    Carbon: {{ run.total_carbon_kg }} kgCO₂eq<br>
                    Duration: {{ run.duration_seconds }}s<br>
                    Completed: {{ run.latest_time }}
                </div>
                <span class="status-pill {% if run.status == 'success' %}status-success{% else %}status-failed{% endif %}">
                    {{ run.status }}
                </span>
            </a>
            {% endfor %}
        </div>

        <div>

            <div class="cards">
                <div class="card">
                    <div class="label">Pipeline Energy</div>
                    <div class="value green">{{ total_energy }}</div>
                    <div class="sub">Total kWh for selected run</div>
                </div>

                <div class="card">
                    <div class="label">Active Energy</div>
                    <div class="value cyan">{{ active_energy }}</div>
                    <div class="sub">Workload-driven compute energy</div>
                </div>

                <div class="card">
                    <div class="label">Pipeline Carbon</div>
                    <div class="value amber">{{ total_carbon }}</div>
                    <div class="sub">kgCO₂eq for selected run</div>
                </div>

                <div class="card">
                    <div class="label">Carbon Intensity</div>
                    <div class="value red">{{ carbon_intensity }}</div>
                    <div class="sub">kgCO₂/kWh | {{ carbon_source }}</div>
                </div>
            </div>

            <div class="cards">
                <div class="card">
                    <div class="label">Pipeline Duration</div>
                    <div class="value purple">{{ pipeline_duration }}</div>
                    <div class="sub">Total measured stage seconds</div>
                </div>

                <div class="card">
                    <div class="label">Stages Captured</div>
                    <div class="value green">{{ stage_count }}</div>
                    <div class="sub">Build / Test / Deploy records</div>
                </div>

                <div class="card">
                    <div class="label">Highest Active Stage</div>
                    <div class="value cyan">{{ highest_active_stage }}</div>
                    <div class="sub">Most workload-driven stage</div>
                </div>

                <div class="card">
                    <div class="label">Highest Total Stage</div>
                    <div class="value amber">{{ highest_total_stage }}</div>
                    <div class="sub">Largest total energy stage</div>
                </div>
            </div>

            <div class="insight">
                <h3>Pipeline-Level Insight</h3>
                <p>{{ pipeline_insight }}</p>
            </div>

            <div class="insight">
                <h3>Stage-Level Insight</h3>
                <p>{{ stage_insight }}</p>
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
                        <th>Peak CPU %</th>
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
                        <td>{{ row.peak_cpu_percent }}</td>
                        <td>{{ row.total_energy_kwh }}</td>
                        <td>{{ row.active_energy_kwh }}</td>
                        <td>{{ row.total_carbon_kg }}</td>
                        <td>{{ row.active_carbon_kg }}</td>
                    </tr>
                {% endfor %}
                </tbody>
            </table>

        </div>
    </div>

    <div class="footer">
        Auto-refreshes every 15 seconds | MongoDB primary with CSV fallback
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
                "rgba(244, 63, 94, 0.8)"
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
    df, data_source = load_metrics()

    if df.empty:
        return "<h2>No monitoring data found. Run Jenkins pipeline first or check MongoDB connection.</h2>"

    required_cols = ["run_id", "stage"]
    for col in required_cols:
        if col not in df.columns:
            return f"<h2>Missing required column: {col}</h2>"

    numeric_cols = [
        "duration_seconds",
        "avg_cpu_percent",
        "peak_cpu_percent",
        "total_energy_kwh",
        "active_energy_kwh",
        "total_carbon_kg",
        "active_carbon_kg",
        "carbon_intensity_kg_per_kwh"
    ]

    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "status" not in df.columns:
        df["status"] = "unknown"

    if "end_timestamp" not in df.columns:
        df["end_timestamp"] = ""

    if "carbon_source" not in df.columns:
        df["carbon_source"] = "unknown"

    run_summary = (
        df.groupby("run_id")
        .agg(
            total_energy_kwh=("total_energy_kwh", "sum"),
            active_energy_kwh=("active_energy_kwh", "sum"),
            total_carbon_kg=("total_carbon_kg", "sum"),
            duration_seconds=("duration_seconds", "sum"),
            status=("status", lambda x: "failed" if "failed" in list(x) else "success"),
            latest_time=("end_timestamp", "max")
        )
        .reset_index()
        .sort_values("latest_time", ascending=False)
    )

    run_summary["total_energy_kwh"] = run_summary["total_energy_kwh"].round(10)
    run_summary["active_energy_kwh"] = run_summary["active_energy_kwh"].round(10)
    run_summary["total_carbon_kg"] = run_summary["total_carbon_kg"].round(10)
    run_summary["duration_seconds"] = run_summary["duration_seconds"].round(4)

    requested_run = request.args.get("run_id")
    selected_run = requested_run if requested_run in run_summary["run_id"].values else run_summary.iloc[0]["run_id"]

    latest = df[df["run_id"] == selected_run].copy()

    stage_order = ["build", "test", "deploy"]
    latest["stage"] = pd.Categorical(latest["stage"], categories=stage_order, ordered=True)
    latest = latest.sort_values("stage")

    summary = latest.groupby("stage", observed=True).mean(numeric_only=True).reset_index()

    total_energy = round(latest["total_energy_kwh"].sum(), 10)
    active_energy = round(latest["active_energy_kwh"].sum(), 10)
    total_carbon = round(latest["total_carbon_kg"].sum(), 10)
    carbon_intensity = round(latest["carbon_intensity_kg_per_kwh"].mean(), 6)
    carbon_source = latest["carbon_source"].iloc[-1]
    pipeline_duration = round(latest["duration_seconds"].sum(), 4)
    stage_count = len(latest)

    highest_active_stage = summary.sort_values("active_energy_kwh", ascending=False).iloc[0]["stage"]
    highest_total_stage = summary.sort_values("total_energy_kwh", ascending=False).iloc[0]["stage"]

    pipeline_insight = (
        f"Pipeline run {selected_run} consumed {total_energy} kWh total energy and emitted "
        f"{total_carbon} kgCO₂eq. These values are calculated from monitored execution metrics "
        f"and carbon intensity data from {carbon_source}."
    )

    stage_insight = (
        f"The {highest_active_stage} stage has the highest active compute energy, showing the greatest workload-driven demand. "
        f"The {highest_total_stage} stage has the highest total energy, which includes baseline server power during execution."
    )

    display_rows = latest.copy()
    for col in numeric_cols:
        display_rows[col] = display_rows[col].round(10)

    return render_template_string(
        HTML,
        selected_run=selected_run,
        data_source=data_source,
        runs=run_summary.to_dict(orient="records"),
        total_energy=total_energy,
        active_energy=active_energy,
        total_carbon=total_carbon,
        carbon_intensity=carbon_intensity,
        carbon_source=carbon_source,
        pipeline_duration=pipeline_duration,
        stage_count=stage_count,
        highest_active_stage=highest_active_stage,
        highest_total_stage=highest_total_stage,
        pipeline_insight=pipeline_insight,
        stage_insight=stage_insight,
        rows=display_rows.to_dict(orient="records"),
        stages=json.dumps(summary["stage"].astype(str).tolist()),
        total_energy_values=json.dumps(summary["total_energy_kwh"].round(10).tolist()),
        active_energy_values=json.dumps(summary["active_energy_kwh"].round(10).tolist()),
        total_carbon_values=json.dumps(summary["total_carbon_kg"].round(10).tolist()),
        cpu_values=json.dumps(summary["avg_cpu_percent"].round(4).tolist()),
        duration_values=json.dumps(summary["duration_seconds"].round(4).tolist()),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5051)