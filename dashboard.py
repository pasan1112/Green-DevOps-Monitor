from flask import Flask, render_template_string, request
import json
import os

import pandas as pd
from pymongo import MongoClient

from intelligence import (
    calculate_pipeline_baseline,
    calculate_stage_baselines,
    calculate_sustainability_score,
    detect_stage_anomalies,
    summarize_anomalies,
)

app = Flask(__name__)

MONGO_DB_NAME = "green_devops_monitor"
MONGO_COLLECTION_NAME = "pipeline_metrics"
CSV_FALLBACK_PATH = "data/metrics.csv"

NUMERIC_COLS = [
    "duration_seconds",
    "avg_cpu_percent",
    "peak_cpu_percent",
    "total_energy_kwh",
    "active_energy_kwh",
    "total_carbon_kg",
    "active_carbon_kg",
    "carbon_intensity_kg_per_kwh",
]

DEFAULT_STAGE_ORDER = ["build", "test", "deploy"]


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


def prepare_metrics_dataframe(df):
    prepared = df.copy()

    if prepared.empty:
        return prepared

    if "run_id" not in prepared.columns:
        prepared["run_id"] = "run-1"
    prepared["run_id"] = prepared["run_id"].fillna("unknown-run").astype(str)

    if "stage" not in prepared.columns:
        prepared["stage"] = "unknown"
    prepared["stage"] = prepared["stage"].fillna("unknown").astype(str)

    for col in NUMERIC_COLS:
        if col not in prepared.columns:
            prepared[col] = 0.0
        prepared[col] = pd.to_numeric(prepared[col], errors="coerce").fillna(0.0)

    if "status" not in prepared.columns:
        prepared["status"] = "unknown"
    prepared["status"] = prepared["status"].fillna("unknown").astype(str)

    if "end_timestamp" not in prepared.columns:
        prepared["end_timestamp"] = ""
    prepared["end_timestamp"] = prepared["end_timestamp"].fillna("").astype(str)

    if "carbon_source" not in prepared.columns:
        prepared["carbon_source"] = "unknown"
    prepared["carbon_source"] = prepared["carbon_source"].fillna("unknown").astype(str)

    return prepared


def build_run_summary(df):
    if df.empty:
        return pd.DataFrame(
            columns=[
                "run_id",
                "total_energy_kwh",
                "total_carbon_kg",
                "duration_seconds",
                "status",
                "latest_time",
            ]
        )

    return (
        df.groupby("run_id")
        .agg(
            total_energy_kwh=("total_energy_kwh", "sum"),
            total_carbon_kg=("total_carbon_kg", "sum"),
            duration_seconds=("duration_seconds", "sum"),
            status=("status", lambda x: "failed" if x.astype(str).str.lower().eq("failed").any() else "success"),
            latest_time=("end_timestamp", "max"),
        )
        .reset_index()
        .sort_values(["latest_time", "run_id"], ascending=[False, False])
    )


def ordered_stage_categories(stages):
    stage_values = [str(stage) for stage in stages if pd.notna(stage)]
    extras = sorted(stage for stage in stage_values if stage not in DEFAULT_STAGE_ORDER)
    ordered = [stage for stage in DEFAULT_STAGE_ORDER if stage in stage_values]
    ordered.extend(stage for stage in extras if stage not in ordered)
    return ordered or DEFAULT_STAGE_ORDER


def confidence_from_run_count(run_count):
    if run_count >= 10:
        return "High"
    if run_count >= 3:
        return "Medium"
    return "Low"


def safe_percentage_change(current_value, baseline_value):
    if baseline_value is None or pd.isna(baseline_value) or baseline_value == 0:
        return None
    return ((current_value - baseline_value) / baseline_value) * 100.0


def format_change_label(change):
    if change is None:
        return "Baseline unavailable"
    sign = "+" if change >= 0 else ""
    return f"{sign}{round(change, 1)}%"


def format_kwh(value):
    numeric = 0.0 if value is None or pd.isna(value) else float(value)
    if numeric == 0:
        return "0.00000000 kWh"
    if abs(numeric) < 0.01:
        return f"{numeric:.8f} kWh"
    return f"{numeric:.4f} kWh"


def format_gco2_from_kg(value):
    numeric = 0.0 if value is None or pd.isna(value) else float(value)
    grams = numeric * 1000.0
    if grams == 0:
        return "0.000 gCO2"
    if abs(grams) < 1:
        return f"{grams:.3f} gCO2"
    return f"{grams:.2f} gCO2"


def format_seconds(value):
    numeric = 0.0 if value is None or pd.isna(value) else float(value)
    if numeric >= 60:
        minutes = int(numeric // 60)
        seconds = numeric - (minutes * 60)
        return f"{minutes}m {seconds:.2f}s"
    return f"{numeric:.2f}s"


def format_percent(value):
    numeric = 0.0 if value is None or pd.isna(value) else float(value)
    return f"{numeric:.2f}%"


def format_count(value):
    numeric = 0.0 if value is None or pd.isna(value) else float(value)
    if numeric.is_integer():
        return f"{int(numeric)}"
    return f"{numeric:.2f}"


def metric_label(metric):
    labels = {
        "duration_seconds": "Duration",
        "total_energy_kwh": "Total energy",
        "active_energy_kwh": "Active energy",
        "total_carbon_kg": "Carbon footprint",
        "avg_cpu_percent": "Average CPU load",
    }
    return labels.get(metric, str(metric).replace("_", " ").title())


def format_metric_value(metric, value):
    if metric in {"total_energy_kwh", "active_energy_kwh"}:
        return format_kwh(value)
    if metric == "total_carbon_kg":
        return format_gco2_from_kg(value)
    if metric == "duration_seconds":
        return format_seconds(value)
    if metric == "avg_cpu_percent":
        return format_percent(value)
    return format_count(value)


def build_comparison_rows(current_run_df, pipeline_baseline):
    comparisons = [
        ("Total Energy", "kWh", "total_energy_kwh"),
        ("Total Carbon", "kgCO2e", "total_carbon_kg"),
        ("Total Duration", "s", "duration_seconds"),
    ]
    rows = []
    for label, unit, metric in comparisons:
        current_value = float(current_run_df[metric].sum()) if metric in current_run_df.columns else 0.0
        baseline_mean = pipeline_baseline.get(f"{metric}_mean")
        change = safe_percentage_change(current_value, baseline_mean)
        rows.append(
            {
                "label": label,
                "unit": unit,
                "current_value": round(current_value, 4),
                "baseline_mean": None if baseline_mean is None else round(float(baseline_mean), 4),
                "current_display": format_metric_value(metric, current_value),
                "baseline_display": None if baseline_mean is None else format_metric_value(metric, baseline_mean),
                "change_label": format_change_label(change),
                "is_above": change is not None and change > 0,
            }
        )
    return rows


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

        <header class="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 gap-4">
            <div>
                <div class="flex items-center gap-3">
                    <div class="p-2 bg-emerald-500/20 rounded-lg">
                        <i data-lucide="leaf" class="text-emerald-400 w-8 h-8"></i>
                    </div>
                    <h1 class="text-3xl font-extrabold tracking-tight text-white">
                        Green DevOps <span class="text-emerald-400">Monitor</span>
                    </h1>
                </div>
                <p class="text-slate-400 mt-1 font-medium">
                    CI/CD sustainability, carbon, and pipeline efficiency analytics
                </p>
            </div>

            <div class="flex gap-2 flex-wrap">
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

            <aside class="lg:col-span-3 flex flex-col gap-4 max-h-[85vh]">
                <div class="glass-panel p-4 flex-1 flex flex-col overflow-hidden">
                    <div class="flex items-center justify-between mb-4 px-2">
                        <h2 class="text-xs font-bold uppercase tracking-widest text-slate-500">Run History</h2>
                        <i data-lucide="history" class="w-4 h-4 text-slate-500"></i>
                    </div>

                    <p class="px-2 text-xs text-slate-400 mb-4">
                        Pick a run to explore its sustainability story from summary to stage-level impact.
                    </p>

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
                                <span class="flex items-center gap-1"><i data-lucide="zap" class="w-3 h-3"></i> {{ run.total_energy_display }}</span>
                                <span class="flex items-center gap-1"><i data-lucide="clock" class="w-3 h-3"></i> {{ run.duration_display }}</span>
                            </div>
                        </a>
                        {% endfor %}
                    </div>
                </div>
            </aside>

            <main class="lg:col-span-9 space-y-6">

                <section class="glass-panel p-6">
                    <div class="flex items-start justify-between gap-4 mb-6">
                        <div>
                            <p class="text-xs font-bold text-emerald-300 uppercase tracking-[0.2em] mb-2">Overall Sustainability Health</p>
                            <h2 class="text-2xl font-extrabold text-white">How sustainable was this run?</h2>
                            <p class="text-sm text-slate-400 mt-2 max-w-3xl">
                                Start here for the high-level view. This score combines current resource usage, historical baselines, and anomaly signals into one simple summary.
                            </p>
                        </div>
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
                        <div class="glass-panel p-5">
                            <div class="flex items-center justify-between mb-3">
                                <p class="text-xs font-bold text-slate-400 uppercase tracking-wider">Sustainability Health Score</p>
                                <i data-lucide="shield-check" class="w-4 h-4 text-emerald-400"></i>
                            </div>
                            <p class="text-3xl font-black text-white">{{ health_score.score }}<span class="text-sm font-normal text-slate-500">/100</span></p>
                            <p class="text-sm font-semibold text-emerald-300 mt-1">{{ health_score.grade }}</p>
                            <p class="text-[11px] text-slate-500 mt-2">{{ health_score.explanation }}</p>
                        </div>

                        <div class="glass-panel p-5">
                            <div class="flex items-center justify-between mb-3">
                                <p class="text-xs font-bold text-slate-400 uppercase tracking-wider">Monitoring Confidence</p>
                                <i data-lucide="radar" class="w-4 h-4 text-sky-400"></i>
                            </div>
                            <p class="text-3xl font-black text-sky-300">{{ confidence_level }}</p>
                            <p class="text-sm font-semibold text-slate-300 mt-1">{{ baseline_run_count }} historical run{{ '' if baseline_run_count == 1 else 's' }}</p>
                            <p class="text-[11px] text-slate-500 mt-2">The baseline excludes this run when earlier runs are available, so the comparison stays fair.</p>
                        </div>

                        <div class="glass-panel p-5">
                            <div class="flex items-center justify-between mb-3">
                                <p class="text-xs font-bold text-slate-400 uppercase tracking-wider">Anomaly Status</p>
                                <i data-lucide="triangle-alert" class="w-4 h-4 {% if anomaly_summary.overall_status == 'Critical' %}text-rose-400{% elif anomaly_summary.overall_status == 'Warning' %}text-amber-400{% else %}text-emerald-400{% endif %}"></i>
                            </div>
                            <p class="text-3xl font-black {% if anomaly_summary.overall_status == 'Critical' %}text-rose-300{% elif anomaly_summary.overall_status == 'Warning' %}text-amber-300{% else %}text-emerald-300{% endif %}">{{ anomaly_summary.overall_status }}</p>
                            <p class="text-sm font-semibold text-slate-300 mt-1">{{ anomaly_summary.critical_count }} critical / {{ anomaly_summary.warning_count }} warning</p>
                            <p class="text-[11px] text-slate-500 mt-2">{{ anomaly_summary.summary_message }}</p>
                        </div>

                        <div class="glass-panel p-5">
                            <div class="flex items-center justify-between mb-3">
                                <p class="text-xs font-bold text-slate-400 uppercase tracking-wider">Baseline Snapshot</p>
                                <i data-lucide="scale" class="w-4 h-4 text-purple-400"></i>
                            </div>
                            <p class="text-sm font-semibold text-slate-200">Energy {{ comparison_rows[0].change_label }}</p>
                            <p class="text-sm font-semibold text-slate-200 mt-1">Carbon {{ comparison_rows[1].change_label }}</p>
                            <p class="text-sm font-semibold text-slate-200 mt-1">Duration {{ comparison_rows[2].change_label }}</p>
                            <p class="text-[11px] text-slate-500 mt-2">A quick look at whether this run is lighter or heavier than normal.</p>
                        </div>
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
                        <div class="bg-emerald-500/5 border border-emerald-500/20 rounded-2xl p-5 flex gap-4">
                            <div class="mt-1"><i data-lucide="info" class="w-5 h-5 text-emerald-400"></i></div>
                            <div>
                                <h4 class="text-sm font-bold text-emerald-300 mb-1">What this means</h4>
                                <p class="text-xs text-emerald-100/70 leading-relaxed">{{ pipeline_insight }}</p>
                            </div>
                        </div>

                        <div class="bg-sky-500/5 border border-sky-500/20 rounded-2xl p-5 flex gap-4">
                            <div class="mt-1"><i data-lucide="bar-chart-3" class="w-5 h-5 text-sky-400"></i></div>
                            <div>
                                <h4 class="text-sm font-bold text-sky-300 mb-1">What to watch next</h4>
                                <p class="text-xs text-sky-100/70 leading-relaxed">{{ stage_insight }}</p>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="glass-panel p-6">
                    <div class="mb-6">
                        <p class="text-xs font-bold text-emerald-300 uppercase tracking-[0.2em] mb-2">Current Run Summary</p>
                        <h2 class="text-2xl font-extrabold text-white">What happened in this run?</h2>
                        <p class="text-sm text-slate-400 mt-2">
                            These are the main usage and impact numbers for the selected run, shown in plain units that are easier to scan.
                        </p>
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                        <div class="glass-panel p-5 relative overflow-hidden group">
                            <div class="absolute -right-2 -bottom-2 opacity-5 transition-transform group-hover:scale-110">
                                <i data-lucide="zap" class="w-24 h-24 text-emerald-400"></i>
                            </div>
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Total Energy</p>
                            <p class="text-3xl font-black text-emerald-400">{{ total_energy_display }}</p>
                            <p class="text-[10px] text-slate-500 mt-2 font-medium">Estimated infrastructure energy used by this run</p>
                        </div>

                        <div class="glass-panel p-5 relative overflow-hidden group">
                            <div class="absolute -right-2 -bottom-2 opacity-5 transition-transform group-hover:scale-110">
                                <i data-lucide="cloud" class="w-24 h-24 text-sky-400"></i>
                            </div>
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Carbon Footprint</p>
                            <p class="text-3xl font-black text-sky-400">{{ total_carbon_display }}</p>
                            <p class="text-[10px] text-slate-500 mt-2 font-medium">Displayed in grams for easier reading</p>
                        </div>

                        <div class="glass-panel p-5 relative overflow-hidden group">
                            <div class="absolute -right-2 -bottom-2 opacity-5 transition-transform group-hover:scale-110">
                                <i data-lucide="activity" class="w-24 h-24 text-amber-400"></i>
                            </div>
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Avg CPU Load</p>
                            <p class="text-3xl font-black text-amber-400">{{ avg_cpu_display }}</p>
                            <p class="text-[10px] text-slate-500 mt-2 font-medium">Average compute utilization across all tracked stages</p>
                        </div>

                        <div class="glass-panel p-5 relative overflow-hidden group">
                            <div class="absolute -right-2 -bottom-2 opacity-5 transition-transform group-hover:scale-110">
                                <i data-lucide="timer" class="w-24 h-24 text-purple-400"></i>
                            </div>
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Total Duration</p>
                            <p class="text-3xl font-black text-purple-400">{{ pipeline_duration_display }}</p>
                            <p class="text-[10px] text-slate-500 mt-2 font-medium">Total monitored runtime for the selected run</p>
                        </div>
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Phone Charge Equivalent</p>
                            <p class="text-2xl font-black text-emerald-300">{{ phone_charges_display }}</p>
                            <p class="text-xs text-slate-500 mt-2">Approximate smartphone charges from the same energy use</p>
                        </div>

                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">LED Bulb Equivalent</p>
                            <p class="text-2xl font-black text-amber-300">{{ led_hours_display }}</p>
                            <p class="text-xs text-slate-500 mt-2">Approximate runtime for a 10W LED bulb</p>
                        </div>

                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Driving Equivalent</p>
                            <p class="text-2xl font-black text-sky-300">{{ car_meters_display }}</p>
                            <p class="text-xs text-slate-500 mt-2">Approximate petrol car travel for the same carbon impact</p>
                        </div>
                    </div>
                </section>

                <section class="glass-panel overflow-hidden">
                    <div class="px-6 py-5 border-b border-white/5 bg-white/2">
                        <p class="text-xs font-bold text-emerald-300 uppercase tracking-[0.2em] mb-2">Anomaly Detection</p>
                        <h2 class="text-2xl font-extrabold text-white">Was anything unusual?</h2>
                        <p class="text-sm text-slate-400 mt-2">
                            This section highlights stages that were noticeably above their usual energy, carbon, duration, or CPU pattern.
                        </p>
                    </div>

                    <div class="grid grid-cols-1 xl:grid-cols-2 gap-6 p-6">
                        <div>
                            <div class="flex items-center justify-between mb-4">
                                <h3 class="text-sm font-bold text-slate-200 uppercase tracking-wider">Top Anomalies</h3>
                                <span class="text-[10px] font-bold text-slate-500 uppercase tracking-widest">{{ anomaly_list|length }} Flagged</span>
                            </div>
                            <div class="space-y-3">
                                {% if anomaly_list %}
                                    {% for anomaly in anomaly_list[:3] %}
                                    <div class="rounded-xl border {% if anomaly.severity == 'critical' %}border-rose-500/25 bg-rose-500/5{% else %}border-amber-500/25 bg-amber-500/5{% endif %} p-4">
                                        <p class="text-sm font-semibold {% if anomaly.severity == 'critical' %}text-rose-300{% else %}text-amber-300{% endif %}">{{ anomaly.message }}</p>
                                        <p class="text-[11px] text-slate-400 mt-1">
                                            Current {{ anomaly.current_display }}, usual {{ anomaly.baseline_display }}, z-score {{ anomaly.z_score_display }}
                                        </p>
                                    </div>
                                    {% endfor %}
                                {% else %}
                                    <div class="rounded-xl border border-emerald-500/20 bg-emerald-500/5 p-4">
                                        <p class="text-sm font-semibold text-emerald-300">No warning or critical anomalies detected for this run.</p>
                                        <p class="text-[11px] text-slate-500 mt-1">The current run stayed within normal stage-level behavior.</p>
                                    </div>
                                {% endif %}
                            </div>
                        </div>

                        <div class="rounded-2xl border border-white/5 bg-slate-950/30 p-5">
                            <h3 class="text-sm font-bold text-slate-200 uppercase tracking-wider mb-3">How to read this</h3>
                            <div class="space-y-3 text-sm text-slate-400">
                                <p><span class="text-slate-200 font-semibold">Critical</span> means a stage moved far above normal and likely deserves investigation.</p>
                                <p><span class="text-slate-200 font-semibold">Warning</span> means the stage was higher than expected, but the signal is less severe.</p>
                                <p><span class="text-slate-200 font-semibold">Z-score</span> shows how far the run is from normal behavior. Bigger positive values mean a larger deviation.</p>
                            </div>
                        </div>
                    </div>

                    <div class="overflow-x-auto border-t border-white/5">
                        <table class="w-full text-left">
                            <thead>
                                <tr class="text-[11px] font-bold text-slate-400 uppercase tracking-wider bg-slate-900/40">
                                    <th class="px-6 py-4">Stage</th>
                                    <th class="px-6 py-4">Metric</th>
                                    <th class="px-6 py-4">Current</th>
                                    <th class="px-6 py-4">Usual</th>
                                    <th class="px-6 py-4">Change</th>
                                    <th class="px-6 py-4">Z-Score</th>
                                    <th class="px-6 py-4 text-right">Severity</th>
                                </tr>
                            </thead>

                            <tbody class="divide-y divide-white/5">
                                {% if anomaly_list %}
                                    {% for anomaly in anomaly_list %}
                                    <tr class="hover:bg-white/5 transition-colors">
                                        <td class="px-6 py-4 text-slate-200 font-semibold">{{ anomaly.stage_label }}</td>
                                        <td class="px-6 py-4 text-slate-300">{{ anomaly.metric_label }}</td>
                                        <td class="px-6 py-4 text-white">{{ anomaly.current_display }}</td>
                                        <td class="px-6 py-4 text-slate-300">{{ anomaly.baseline_display }}</td>
                                        <td class="px-6 py-4 text-slate-300">{{ anomaly.percentage_change_display }}</td>
                                        <td class="px-6 py-4 text-slate-300">{{ anomaly.z_score_display }}</td>
                                        <td class="px-6 py-4 text-right">
                                            <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {% if anomaly.severity == 'critical' %}text-rose-300 bg-rose-500/10{% else %}text-amber-300 bg-amber-500/10{% endif %}">
                                                {{ anomaly.severity }}
                                            </span>
                                        </td>
                                    </tr>
                                    {% endfor %}
                                {% else %}
                                    <tr>
                                        <td colspan="7" class="px-6 py-6 text-center text-slate-500">No warning or critical anomalies to display for the selected run.</td>
                                    </tr>
                                {% endif %}
                            </tbody>
                        </table>
                    </div>
                </section>

                <section class="glass-panel overflow-hidden">
                    <div class="px-6 py-5 border-b border-white/5 bg-white/2">
                        <p class="text-xs font-bold text-emerald-300 uppercase tracking-[0.2em] mb-2">Baseline Comparison</p>
                        <h2 class="text-2xl font-extrabold text-white">How does this compare with normal behavior?</h2>
                        <p class="text-sm text-slate-400 mt-2">
                            Each card compares this run with the historical average from earlier runs so you can see whether it was heavier or lighter than normal.
                        </p>
                    </div>
                    <div class="divide-y divide-white/5">
                        {% for item in comparison_rows %}
                        <div class="px-6 py-5 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                            <div>
                                <p class="text-sm font-bold text-slate-200">{{ item.label }}</p>
                                <p class="text-[11px] text-slate-500">Historical average: {% if item.baseline_display is not none %}{{ item.baseline_display }}{% else %}Unavailable{% endif %}</p>
                            </div>
                            <div class="text-left md:text-right">
                                <p class="text-sm font-bold text-white">{{ item.current_display }}</p>
                                <p class="text-[11px] font-semibold {% if item.change_label == 'Baseline unavailable' %}text-slate-500{% elif item.is_above %}text-amber-300{% else %}text-emerald-300{% endif %}">{{ item.change_label }}</p>
                            </div>
                        </div>
                        {% endfor %}
                    </div>
                </section>

                <section class="space-y-6">
                    <div>
                        <p class="text-xs font-bold text-emerald-300 uppercase tracking-[0.2em] mb-2">Stage Breakdown and Charts</p>
                        <h2 class="text-2xl font-extrabold text-white">Which stage caused the most impact?</h2>
                        <p class="text-sm text-slate-400 mt-2">
                            Use the charts for a quick visual scan, then check the stage table below for the exact values behind them.
                        </p>
                    </div>

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
                                        <th class="px-6 py-4">Energy</th>
                                        <th class="px-6 py-4 text-right">Carbon</th>
                                    </tr>
                                </thead>

                                <tbody class="divide-y divide-white/5">
                                    {% for row in rows %}
                                    <tr class="hover:bg-white/5 transition-colors group">
                                        <td class="px-6 py-4">
                                            <div class="flex items-center gap-3">
                                                <div class="w-2 h-2 rounded-full bg-emerald-500 group-hover:scale-125 transition-transform"></div>
                                                <span class="font-bold text-slate-200">{{ row.stage_label }}</span>
                                            </div>
                                        </td>

                                        <td class="px-6 py-4 text-center">
                                            <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {% if row.status == 'success' %}text-emerald-400 bg-emerald-500/10{% else %}text-rose-400 bg-rose-500/10{% endif %}">
                                                {{ row.status }}
                                            </span>
                                        </td>

                                        <td class="px-6 py-4 text-slate-300 font-medium">{{ row.duration_display }}</td>

                                        <td class="px-6 py-4">
                                            <div class="flex items-center gap-2">
                                                <span class="text-slate-300">{{ row.avg_cpu_display }}</span>
                                                <div class="flex-1 w-12 h-1 bg-slate-800 rounded-full overflow-hidden">
                                                    <div class="h-full bg-amber-500/60" style="width: {{ row.avg_cpu_percent }}%"></div>
                                                </div>
                                            </div>
                                        </td>

                                        <td class="px-6 py-4 text-emerald-400 font-mono text-sm">{{ row.total_energy_display }}</td>
                                        <td class="px-6 py-4 text-right text-sky-400 font-mono text-sm">{{ row.total_carbon_display }}</td>
                                    </tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </section>

                <footer class="text-center text-slate-500 text-[10px] uppercase tracking-[0.2em] pt-4 border-t border-white/5">
                    Pipeline Engine v2.5 | MongoDB-backed monitoring active | Refreshes every 15s
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
            plugins: {
                legend: {
                    display: true,
                    labels: { color: tickColor }
                }
            },
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
                    label: "Average CPU %",
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
        return """
        <div style='background:#020617; color:white; height:100vh; display:flex; align-items:center; justify-content:center; font-family:sans-serif;'>
            <h2>No monitoring data found.</h2>
        </div>
        """

    df = prepare_metrics_dataframe(df)
    run_summary = build_run_summary(df)

    if run_summary.empty:
        return """
        <div style='background:#020617; color:white; height:100vh; display:flex; align-items:center; justify-content:center; font-family:sans-serif;'>
            <h2>No monitoring data found.</h2>
        </div>
        """

    requested_run = request.args.get("run_id")
    available_run_ids = set(run_summary["run_id"].astype(str).tolist())
    selected_run = requested_run if requested_run in available_run_ids else str(run_summary.iloc[0]["run_id"])

    current_run_df = df[df["run_id"] == selected_run].copy()
    historical_df = df[df["run_id"] != selected_run].copy()

    baseline_run_count = int(historical_df["run_id"].nunique()) if not historical_df.empty else 0
    confidence_level = confidence_from_run_count(baseline_run_count)
    stage_baseline_df = calculate_stage_baselines(historical_df)
    pipeline_baseline = calculate_pipeline_baseline(historical_df)

    stage_order = ordered_stage_categories(current_run_df["stage"].tolist())
    current_run_df["stage"] = pd.Categorical(current_run_df["stage"], categories=stage_order, ordered=True)
    current_run_df = current_run_df.sort_values("stage")

    summary = (
        current_run_df.groupby("stage", observed=True)
        .agg(
            duration_seconds=("duration_seconds", "sum"),
            avg_cpu_percent=("avg_cpu_percent", "mean"),
            total_energy_kwh=("total_energy_kwh", "sum"),
            active_energy_kwh=("active_energy_kwh", "sum"),
            total_carbon_kg=("total_carbon_kg", "sum"),
        )
        .reset_index()
    )

    anomalies = detect_stage_anomalies(current_run_df, stage_baseline_df)
    anomalies = sorted(
        anomalies,
        key=lambda item: (
            0 if item["severity"] == "critical" else 1,
            -(item["percentage_change"] or 0),
            str(item["stage"]),
            str(item["metric"]),
        ),
    )
    anomaly_summary = summarize_anomalies(anomalies)
    health_score = calculate_sustainability_score(current_run_df, pipeline_baseline, anomalies)
    comparison_rows = build_comparison_rows(current_run_df, pipeline_baseline)

    total_energy = round(float(current_run_df["total_energy_kwh"].sum()), 8)
    active_energy = round(float(current_run_df["active_energy_kwh"].sum()), 8)
    total_carbon = round(float(current_run_df["total_carbon_kg"].sum()), 8)
    pipeline_duration = round(float(current_run_df["duration_seconds"].sum()), 2)
    avg_cpu = round(float(summary["avg_cpu_percent"].mean()), 2) if not summary.empty else 0

    phone_charges = round(total_energy / 0.01, 3) if total_energy else 0
    led_hours = round(total_energy / 0.01, 3) if total_energy else 0
    car_meters = round(((total_carbon * 1000) / 120) * 1000, 3) if total_carbon else 0

    highest_active_stage = (
        summary.sort_values("active_energy_kwh", ascending=False).iloc[0]["stage"] if not summary.empty else "N/A"
    )
    highest_total_stage = (
        summary.sort_values("total_energy_kwh", ascending=False).iloc[0]["stage"] if not summary.empty else "N/A"
    )

    anomaly_teaser = anomalies[0]["message"] if anomalies else "No warning or critical anomalies were detected."
    pipeline_insight = (
        f"Run {selected_run} used {format_kwh(total_energy)} and emitted {format_gco2_from_kg(total_carbon)}. "
        f"Health score: {health_score['score']}/100 ({health_score['grade']}). {anomaly_teaser}"
    )

    stage_insight = (
        f"The {str(highest_active_stage).replace('_', ' ').title()} stage had the highest active compute demand, while "
        f"{str(highest_total_stage).replace('_', ' ').title()} had the largest total energy footprint. Monitoring confidence is {confidence_level.lower()} "
        f"based on {baseline_run_count} historical run(s)."
    )

    stage_status = (
        current_run_df.groupby("stage", observed=True)["status"]
        .agg(lambda x: "failed" if x.astype(str).str.lower().eq("failed").any() else "success")
        .reset_index()
    )
    display_rows = summary.merge(stage_status, on="stage", how="left")
    display_rows["stage_label"] = display_rows["stage"].astype(str).str.replace("_", " ").str.title()
    display_rows["duration_display"] = display_rows["duration_seconds"].apply(format_seconds)
    display_rows["avg_cpu_display"] = display_rows["avg_cpu_percent"].apply(format_percent)
    display_rows["total_energy_display"] = display_rows["total_energy_kwh"].apply(format_kwh)
    display_rows["total_carbon_display"] = display_rows["total_carbon_kg"].apply(format_gco2_from_kg)

    run_summary["total_energy_display"] = run_summary["total_energy_kwh"].apply(format_kwh)
    run_summary["total_carbon_display"] = run_summary["total_carbon_kg"].apply(format_gco2_from_kg)
    run_summary["duration_display"] = run_summary["duration_seconds"].apply(format_seconds)

    formatted_anomalies = []
    for anomaly in anomalies:
        formatted_anomaly = dict(anomaly)
        formatted_anomaly["stage_label"] = str(anomaly["stage"]).replace("_", " ").title()
        formatted_anomaly["metric_label"] = metric_label(anomaly["metric"])
        formatted_anomaly["current_display"] = format_metric_value(anomaly["metric"], anomaly["current_value"])
        formatted_anomaly["baseline_display"] = format_metric_value(anomaly["metric"], anomaly["baseline_mean"])
        formatted_anomaly["percentage_change_display"] = (
            format_percent(anomaly["percentage_change"]) if anomaly["percentage_change"] is not None else "N/A"
        )
        formatted_anomaly["z_score_display"] = (
            f"{float(anomaly['z_score']):.2f}" if anomaly["z_score"] is not None else "N/A"
        )
        formatted_anomalies.append(formatted_anomaly)

    return render_template_string(
        HTML,
        selected_run=selected_run,
        data_source=data_source,
        runs=run_summary.to_dict(orient="records"),
        total_energy=total_energy,
        active_energy=active_energy,
        total_carbon=total_carbon,
        pipeline_duration=pipeline_duration,
        avg_cpu=avg_cpu,
        total_energy_display=format_kwh(total_energy),
        active_energy_display=format_kwh(active_energy),
        total_carbon_display=format_gco2_from_kg(total_carbon),
        pipeline_duration_display=format_seconds(pipeline_duration),
        avg_cpu_display=format_percent(avg_cpu),
        phone_charges=phone_charges,
        led_hours=led_hours,
        car_meters=car_meters,
        phone_charges_display=format_count(phone_charges),
        led_hours_display=f"{format_count(led_hours)}h",
        car_meters_display=f"{format_count(car_meters)}m",
        stage_count=len(display_rows),
        pipeline_insight=pipeline_insight,
        stage_insight=stage_insight,
        rows=display_rows.to_dict(orient="records"),
        stages=json.dumps(summary["stage"].astype(str).tolist()),
        total_energy_values=json.dumps(summary["total_energy_kwh"].round(8).tolist()),
        active_energy_values=json.dumps(summary["active_energy_kwh"].round(8).tolist()),
        cpu_values=json.dumps(summary["avg_cpu_percent"].round(2).tolist()),
        health_score=health_score,
        confidence_level=confidence_level,
        baseline_run_count=baseline_run_count,
        anomaly_summary=anomaly_summary,
        comparison_rows=comparison_rows,
        anomaly_list=formatted_anomalies,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5051, debug=False)
