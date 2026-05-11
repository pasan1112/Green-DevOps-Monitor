from flask import Flask, render_template_string, request
import json
import os

import os

os.environ.setdefault(
    "MONGO_URI",
    "mongodb+srv://admin:admin1234@green-devops-monitor.xxflzzs.mongodb.net/?appName=Green-DevOps-Monitor"
)

import pandas as pd

from intelligence import (
    calculate_pipeline_baseline,
    calculate_stage_baselines,
    calculate_sustainability_score,
    detect_ml_anomalies,
    detect_stage_anomalies,
    summarize_anomalies,
)

app = Flask(__name__)

MONGO_DB_NAME = "green_devops_monitor"
MONGO_COLLECTION_NAME = "pipeline_metrics"
CSV_FALLBACK_PATH = "data/metrics.csv"

NUMERIC_COLS = [
    "duration_seconds",
    "workload_duration_seconds",
    "jenkins_stage_duration_seconds",
    "infrastructure_overhead_seconds",
    "overhead_percentage",
    "avg_cpu_percent",
    "peak_cpu_percent",
    "total_energy_kwh",
    "active_energy_kwh",
    "total_carbon_kg",
    "active_carbon_kg",
    "carbon_intensity_kg_per_kwh",
]

DEFAULT_STAGE_ORDER = ["build", "test", "deploy"]
PP1_ANOMALY_METRICS = [
    "workload_duration_seconds",
    "overhead_percentage",
    "total_energy_kwh",
    "total_carbon_kg",
]


def get_mongo_client():
    """Import pymongo lazily so the dashboard can fall back to CSV if the package is unavailable."""
    try:
        from pymongo import MongoClient
    except ImportError:
        return None

    return MongoClient


def load_metrics():
    mongo_uri = os.getenv("MONGO_URI")
    mongo_client_cls = get_mongo_client()

    if mongo_uri and mongo_client_cls is not None:
        try:
            client = mongo_client_cls(mongo_uri, serverSelectionTimeoutMS=5000)
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

    jenkins_stage_duration_present = "jenkins_stage_duration_seconds" in prepared.columns
    prepared["jenkins_stage_duration_captured"] = (
        prepared["jenkins_stage_duration_seconds"].notna() if jenkins_stage_duration_present else False
    )
    prepared["workload_duration_captured"] = (
        prepared["workload_duration_seconds"].notna() if "workload_duration_seconds" in prepared.columns else True
    )

    for col in NUMERIC_COLS:
        if col not in prepared.columns:
            prepared[col] = 0.0
        prepared[col] = pd.to_numeric(prepared[col], errors="coerce").fillna(0.0)

    if "workload_duration_seconds" not in df.columns:
        prepared["workload_duration_seconds"] = prepared["duration_seconds"]
    else:
        prepared.loc[~prepared["workload_duration_captured"], "workload_duration_seconds"] = prepared.loc[
            ~prepared["workload_duration_captured"], "duration_seconds"
        ]
    if "jenkins_stage_duration_seconds" not in df.columns:
        prepared["jenkins_stage_duration_seconds"] = prepared["workload_duration_seconds"]
    else:
        prepared.loc[~prepared["jenkins_stage_duration_captured"], "jenkins_stage_duration_seconds"] = prepared.loc[
            ~prepared["jenkins_stage_duration_captured"], "workload_duration_seconds"
        ]
    if "infrastructure_overhead_seconds" not in df.columns:
        prepared["infrastructure_overhead_seconds"] = (
            prepared["jenkins_stage_duration_seconds"] - prepared["workload_duration_seconds"]
        ).clip(lower=0)
    else:
        prepared["infrastructure_overhead_seconds"] = prepared["infrastructure_overhead_seconds"].clip(lower=0)
    if "overhead_percentage" not in df.columns:
        prepared["overhead_percentage"] = 0.0
        non_zero_duration = prepared["jenkins_stage_duration_seconds"] > 0
        prepared.loc[non_zero_duration, "overhead_percentage"] = (
            prepared.loc[non_zero_duration, "infrastructure_overhead_seconds"]
            / prepared.loc[non_zero_duration, "jenkins_stage_duration_seconds"]
            * 100.0
        )
    else:
        prepared.loc[~prepared["jenkins_stage_duration_captured"], "overhead_percentage"] = 0.0

    if "status" not in prepared.columns:
        prepared["status"] = "unknown"
    prepared["status"] = prepared["status"].fillna("unknown").astype(str)

    if "end_timestamp" not in prepared.columns:
        prepared["end_timestamp"] = ""
    prepared["end_timestamp"] = prepared["end_timestamp"].fillna("").astype(str)

    if "carbon_source" not in prepared.columns:
        prepared["carbon_source"] = "unknown"
    prepared["carbon_source"] = prepared["carbon_source"].fillna("unknown").astype(str)

    for col in ["stage_start_timestamp", "stage_end_timestamp"]:
        if col not in prepared.columns:
            prepared[col] = ""
        prepared[col] = prepared[col].fillna("").astype(str)

    return prepared


def build_run_summary(df):
    if df.empty:
        return pd.DataFrame(
            columns=[
                "run_id",
                "total_energy_kwh",
                "total_carbon_kg",
                "duration_seconds",
                "jenkins_stage_duration_seconds",
                "status",
                "latest_time",
                "jenkins_stage_duration_captured",
            ]
        )

    return (
        df.groupby("run_id")
        .agg(
            total_energy_kwh=("total_energy_kwh", "sum"),
            total_carbon_kg=("total_carbon_kg", "sum"),
            duration_seconds=("duration_seconds", "sum"),
            jenkins_stage_duration_seconds=("jenkins_stage_duration_seconds", "sum"),
            status=("status", lambda x: "failed" if x.astype(str).str.lower().eq("failed").any() else "success"),
            latest_time=("end_timestamp", "max"),
            jenkins_stage_duration_captured=("jenkins_stage_duration_captured", "max"),
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
    return f"{numeric:.2f}s"


def format_percent(value):
    numeric = 0.0 if value is None or pd.isna(value) else float(value)
    return f"{numeric:.2f}%"


def format_count(value):
    numeric = 0.0 if value is None or pd.isna(value) else float(value)
    if numeric.is_integer():
        return f"{int(numeric)}"
    return f"{numeric:.2f}"


def format_decimal(value, decimals=4):
    numeric = 0.0 if value is None or pd.isna(value) else float(value)
    return f"{numeric:.{decimals}f}"


def format_equivalent(value, unit_suffix, tiny_suffix):
    numeric = 0.0 if value is None or pd.isna(value) else float(value)
    if numeric < 0.01:
        return f"< 0.01{tiny_suffix}"
    return f"{format_count(numeric)}{unit_suffix}"


def severity_rank(severity):
    ranks = {
        "critical": 0,
        "warning": 1,
        "normal": 2,
    }
    return ranks.get(normalize_display_severity(severity), 3)


def normalize_display_severity(severity):
    normalized = str(severity or "normal").strip().lower()
    if normalized == "info":
        return "normal"
    if normalized in {"critical", "warning", "normal"}:
        return normalized
    return "normal"


def metric_label(metric):
    labels = {
        "duration_seconds": "Workload duration",
        "workload_duration_seconds": "Workload duration",
        "jenkins_stage_duration_seconds": "Full stage duration",
        "infrastructure_overhead_seconds": "Infrastructure overhead",
        "overhead_percentage": "Overhead percentage",
        "total_energy_kwh": "Total energy",
        "active_energy_kwh": "Active energy",
        "total_carbon_kg": "Carbon footprint",
        "avg_cpu_percent": "Average CPU load",
    }
    return labels.get(metric, str(metric).replace("_", " ").title())


def ml_status_color(status):
    normalized = normalize_display_severity(status)
    if normalized == "critical":
        return "rose"
    if normalized == "warning":
        return "amber"
    return "emerald"


def severity_theme(severity):
    normalized = normalize_display_severity(severity)
    themes = {
        "critical": {
            "label": "Critical",
            "text_class": "text-rose-300",
            "badge_class": "text-rose-300 bg-rose-500/10",
            "panel_class": "border-rose-500/25 bg-rose-500/5",
        },
        "warning": {
            "label": "Warning",
            "text_class": "text-amber-300",
            "badge_class": "text-amber-300 bg-amber-500/10",
            "panel_class": "border-amber-500/25 bg-amber-500/5",
        },
        "normal": {
            "label": "Normal",
            "text_class": "text-emerald-300",
            "badge_class": "text-emerald-300 bg-emerald-500/10",
            "panel_class": "border-emerald-500/25 bg-emerald-500/5",
        },
    }
    return themes.get(normalized, themes["normal"])


def deduplicate_anomalies(anomalies, allowed_metrics=None, limit=8):
    filtered = []
    for anomaly in anomalies:
        if allowed_metrics and anomaly.get("metric") not in allowed_metrics:
            continue
        filtered.append(anomaly)

    best_by_key = {}
    for anomaly in filtered:
        key = (str(anomaly.get("stage")), str(anomaly.get("metric")))
        existing = best_by_key.get(key)
        candidate_score = (
            severity_rank(anomaly.get("severity")),
            -abs(float(anomaly.get("percentage_change") or 0.0)),
        )
        if existing is None:
            best_by_key[key] = anomaly
            continue
        existing_score = (
            severity_rank(existing.get("severity")),
            -abs(float(existing.get("percentage_change") or 0.0)),
        )
        if candidate_score < existing_score:
            best_by_key[key] = anomaly

    deduped = list(best_by_key.values())
    deduped.sort(
        key=lambda item: (
            severity_rank(item.get("severity")),
            -abs(float(item.get("percentage_change") or 0.0)),
            str(item.get("stage")),
            str(item.get("metric")),
        )
    )
    return deduped[:limit]


def format_metric_value(metric, value):
    if metric in {"total_energy_kwh", "active_energy_kwh"}:
        return format_kwh(value)
    if metric == "total_carbon_kg":
        return format_gco2_from_kg(value)
    if metric == "duration_seconds":
        return format_seconds(value)
    if metric in {"workload_duration_seconds", "jenkins_stage_duration_seconds", "infrastructure_overhead_seconds"}:
        return format_seconds(value)
    if metric == "overhead_percentage":
        return format_percent(value)
    if metric == "avg_cpu_percent":
        return format_percent(value)
    return format_count(value)


def build_comparison_rows(current_run_df, pipeline_baseline):
    comparisons = [
        ("Total Energy", "kWh", "total_energy_kwh"),
        ("Total Carbon", "kgCO2e", "total_carbon_kg"),
        ("Total Workload Duration", "s", "duration_seconds"),
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


def build_statistical_metric_groups(summary_df, stage_baseline_df, anomalies):
    metric_groups = [
        ("Workload Duration", "workload_duration_seconds"),
        ("Infrastructure Overhead %", "overhead_percentage"),
        ("Total Energy", "total_energy_kwh"),
        ("Carbon Emission", "total_carbon_kg"),
    ]

    stage_lookup = {}
    if summary_df is not None and not summary_df.empty:
        for _, row in summary_df.iterrows():
            stage_lookup[str(row["stage"])] = row

    baseline_lookup = {}
    if stage_baseline_df is not None and not stage_baseline_df.empty and "stage" in stage_baseline_df.columns:
        for _, row in stage_baseline_df.iterrows():
            baseline_lookup[str(row["stage"])] = row

    anomaly_lookup = {}
    for anomaly in anomalies or []:
        key = (str(anomaly.get("stage")), str(anomaly.get("metric")))
        existing = anomaly_lookup.get(key)
        if existing is None:
            anomaly_lookup[key] = anomaly
            continue
        candidate_score = (
            severity_rank(anomaly.get("severity")),
            -abs(float(anomaly.get("percentage_change") or 0.0)),
        )
        existing_score = (
            severity_rank(existing.get("severity")),
            -abs(float(existing.get("percentage_change") or 0.0)),
        )
        if candidate_score < existing_score:
            anomaly_lookup[key] = anomaly

    stage_order = ordered_stage_categories(
        list(stage_lookup.keys()) + list(baseline_lookup.keys()) + DEFAULT_STAGE_ORDER
    )

    groups = []
    for title, metric in metric_groups:
        rows = []
        for stage in stage_order:
            summary_row = stage_lookup.get(stage)
            baseline_row = baseline_lookup.get(stage)
            current_value = float(summary_row.get(metric, 0.0)) if summary_row is not None else 0.0
            baseline_mean = None
            baseline_std = None
            if baseline_row is not None:
                baseline_mean = baseline_row.get(f"{metric}_mean")
                baseline_std = baseline_row.get(f"{metric}_std")
            baseline_mean = None if baseline_mean is None or pd.isna(baseline_mean) else float(baseline_mean)
            baseline_std = None if baseline_std is None or pd.isna(baseline_std) else float(baseline_std)

            anomaly = anomaly_lookup.get((stage, metric), {})
            severity = normalize_display_severity(anomaly.get("severity"))
            percentage_change = anomaly.get("percentage_change")
            if percentage_change is None:
                percentage_change = safe_percentage_change(current_value, baseline_mean)
            z_score = anomaly.get("z_score")
            if z_score is None and baseline_mean is not None and baseline_std is not None and baseline_std > 0:
                z_score = (current_value - baseline_mean) / baseline_std

            stage_label = str(stage).replace("_", " ").title()
            current_display = format_metric_value(metric, current_value)
            baseline_display = (
                format_metric_value(metric, baseline_mean) if baseline_mean is not None else "Baseline unavailable"
            )
            change_display = format_percent(percentage_change) if percentage_change is not None else "N/A"
            z_score_display = f"{float(z_score):.2f}" if z_score is not None else "N/A"
            theme = severity_theme(severity)
            insight = anomaly.get("message")
            if not insight:
                if baseline_mean is None:
                    insight = f"{stage_label} baseline is not available yet."
                elif severity == "normal":
                    insight = f"{stage_label} is stable compared with baseline."
                else:
                    insight = f"{stage_label} moved away from its usual {metric_label(metric).lower()} pattern."

            rows.append(
                {
                    "stage": stage,
                    "stage_label": stage_label,
                    "current_value": current_value,
                    "baseline_mean": baseline_mean,
                    "percentage_change": percentage_change,
                    "z_score": z_score,
                    "severity": severity,
                    "severity_label": theme["label"],
                    "severity_badge_class": theme["badge_class"],
                    "current_display": current_display,
                    "baseline_display": baseline_display,
                    "change_display": change_display,
                    "z_score_display": z_score_display,
                    "insight": insight,
                }
            )

        rows.sort(
            key=lambda item: (
                severity_rank(item["severity"]),
                -abs(float(item["percentage_change"] or 0.0)),
                item["stage_label"],
            )
        )
        worst_row = rows[0] if rows else None
        overall_severity = worst_row["severity"] if worst_row else "normal"
        overall_theme = severity_theme(overall_severity)
        has_major_anomaly = any(row["severity"] in {"critical", "warning"} for row in rows)

        if worst_row is None:
            insight = "Stable compared with baseline."
            worst_stage_label = "No stage data"
            current_vs_usual = "No data available"
            change_display = "N/A"
        elif has_major_anomaly:
            insight = worst_row["insight"]
            worst_stage_label = worst_row["stage_label"]
            current_vs_usual = f"{worst_row['current_display']} vs {worst_row['baseline_display']}"
            change_display = worst_row["change_display"]
        else:
            insight = "Stable compared with baseline."
            normal_candidate = next((row for row in rows if row["baseline_mean"] is not None), worst_row)
            worst_stage_label = normal_candidate["stage_label"]
            current_vs_usual = f"{normal_candidate['current_display']} vs {normal_candidate['baseline_display']}"
            change_display = normal_candidate["change_display"]

        rows.sort(key=lambda item: (stage_order.index(item["stage"]), item["stage_label"]))
        groups.append(
            {
                "title": title,
                "metric": metric,
                "severity": overall_severity,
                "severity_label": overall_theme["label"],
                "severity_text_class": overall_theme["text_class"],
                "severity_badge_class": overall_theme["badge_class"],
                "panel_class": overall_theme["panel_class"],
                "worst_stage_label": worst_stage_label,
                "current_vs_usual": current_vs_usual,
                "change_display": change_display,
                "insight": insight,
                "rows": rows,
            }
        )

    return groups


def normalize_dashboard_anomaly(anomaly):
    normalized = dict(anomaly)
    normalized_severity = normalize_display_severity(anomaly.get("severity"))
    normalized["severity"] = normalized_severity
    if normalized_severity == "normal":
        stage_label = str(anomaly.get("stage", "unknown")).replace("_", " ").title()
        normalized["message"] = f"{stage_label} is stable compared with baseline."
    return normalized


def format_dashboard_anomaly(anomaly):
    formatted = dict(anomaly)
    normalized_severity = normalize_display_severity(anomaly.get("severity"))
    theme = severity_theme(normalized_severity)
    formatted["severity"] = normalized_severity
    formatted["severity_label"] = theme["label"]
    formatted["severity_badge_class"] = theme["badge_class"]
    formatted["stage_label"] = str(anomaly.get("stage", "unknown")).replace("_", " ").title()
    formatted["metric_label"] = metric_label(anomaly.get("metric"))
    formatted["current_display"] = format_metric_value(anomaly.get("metric"), anomaly.get("current_value"))
    baseline_value = anomaly.get("baseline_mean")
    formatted["baseline_display"] = (
        format_metric_value(anomaly.get("metric"), baseline_value) if baseline_value is not None else "Baseline unavailable"
    )
    percentage_change = anomaly.get("percentage_change")
    formatted["percentage_change_display"] = (
        format_percent(percentage_change) if percentage_change is not None else "N/A"
    )
    return formatted

def format_ml_alert(item):
    severity = normalize_display_severity(item.get("severity"))
    theme = severity_theme(severity)
    stage_label = str(item.get("stage", "unknown")).replace("_", " ").title()
    score = item.get("anomaly_score", 0.0)

    return {
        "source": "Isolation Forest",
        "severity": severity,
        "severity_label": theme["label"],
        "severity_badge_class": theme["badge_class"],
        "metric_label": "ML anomaly score",
        "stage_label": stage_label,
        "current_display": format_decimal(score, decimals=4),
        "baseline_display": "Learned normal pattern",
        "percentage_change_display": "N/A",
        "message": item.get(
            "message",
            f"{stage_label} was flagged by the Isolation Forest model."
        ),
    }

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Green DevOps Monitor</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://unpkg.com/lucide@latest"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');

        :root {
            --panel: #ffffff;
            --border: #e2e8f0;
            --text: #0f172a;
            --muted: #64748b;
            --bg: #f8fafc;
            --shadow: 0 12px 30px rgba(15, 23, 42, 0.08);
        }

        body {
            font-family: 'Plus Jakarta Sans', sans-serif;
            background: var(--bg);
            background-image:
                radial-gradient(at 0% 0%, rgba(16, 185, 129, 0.08) 0px, transparent 45%),
                radial-gradient(at 100% 0%, rgba(59, 130, 246, 0.08) 0px, transparent 40%);
            color: var(--text);
            min-height: 100vh;
        }

        .glass-panel {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 1rem;
            box-shadow: var(--shadow);
        }

        .sidebar-scroll::-webkit-scrollbar { width: 4px; }
        .sidebar-scroll::-webkit-scrollbar-track { background: #f8fafc; }
        .sidebar-scroll::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 10px; }

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
            background: rgba(16, 185, 129, 0.08);
            border-color: rgba(16, 185, 129, 0.35) !important;
        }

        .sidebar-sticky {
            position: sticky;
            top: 24px;
            max-height: calc(100vh - 48px);
        }

        .text-white { color: #0f172a !important; }
        .text-slate-200 { color: #1e293b !important; }
        .text-slate-300 { color: #334155 !important; }
        .text-slate-400 { color: #64748b !important; }
        .text-slate-500 { color: #64748b !important; }
        .text-emerald-100\\/70 { color: #065f46 !important; }
        .text-sky-100\\/70 { color: #0f4c81 !important; }
        .bg-white\\/2 { background: #f8fafc !important; }
        .bg-slate-900\\/40 { background: #f8fafc !important; }
        .bg-slate-950\\/30 { background: #f8fafc !important; }
        .border-white\\/5 { border-color: #e2e8f0 !important; }
        .hover\\:bg-white\\/5:hover { background: #f8fafc !important; }
        .hover\\:border-white\\/10:hover { border-color: #cbd5e1 !important; }
        .bg-emerald-500\\/5 { background: rgba(16, 185, 129, 0.06) !important; }
        .bg-sky-500\\/5 { background: rgba(14, 165, 233, 0.06) !important; }
        .text-emerald-300 { color: #059669 !important; }
        .text-sky-300 { color: #0284c7 !important; }
        .text-amber-300 { color: #d97706 !important; }
        .text-rose-300 { color: #e11d48 !important; }
        .text-emerald-400 { color: #10b981 !important; }
        .text-sky-400 { color: #0ea5e9 !important; }
        .text-amber-400 { color: #f59e0b !important; }
        .text-rose-400 { color: #f43f5e !important; }
        .text-purple-400 { color: #7c3aed !important; }
        .bg-slate-800 { background: #e2e8f0 !important; }
        .bg-emerald-500\\/10 { background: rgba(16, 185, 129, 0.12) !important; }
        .bg-rose-500\\/10 { background: rgba(244, 63, 94, 0.12) !important; }
        .bg-amber-500\\/10 { background: rgba(245, 158, 11, 0.12) !important; }
        .bg-sky-500\\/10 { background: rgba(14, 165, 233, 0.12) !important; }
        .bg-emerald-500\\/20 { background: rgba(16, 185, 129, 0.12) !important; }
        .bg-rose-500\\/20 { background: rgba(244, 63, 94, 0.12) !important; }
        table tbody tr:hover { background: #f8fafc !important; }

        .modal-overlay {
            position: fixed;
            inset: 0;
            background: rgba(15, 23, 42, 0.48);
            display: none;
            align-items: center;
            justify-content: center;
            padding: 24px;
            z-index: 1000;
        }

        .modal-overlay.is-open {
            display: flex;
        }

        .modal-panel {
            width: min(960px, 100%);
            max-height: 85vh;
            overflow: hidden;
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 1rem;
            box-shadow: 0 24px 60px rgba(15, 23, 42, 0.22);
        }

        .modal-scroll {
            max-height: calc(85vh - 88px);
            overflow-y: auto;
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
                    <h1 class="text-3xl font-extrabold tracking-tight text-slate-900">
                        Green DevOps <span class="text-emerald-400">Monitor</span>
                    </h1>
                </div>
                <p class="text-slate-600 mt-1 font-medium">
                    CI/CD sustainability, carbon, and pipeline efficiency analytics
                </p>
            </div>

            <div class="flex gap-2 flex-wrap items-center">
                <a href="{{ refresh_url }}"
                   class="inline-flex items-center gap-2 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-2 text-sm font-semibold text-emerald-700 shadow-sm hover:bg-emerald-100 hover:border-emerald-300 transition-colors">
                    <i data-lucide="refresh-cw" class="w-4 h-4"></i>
                    <span>Refresh Data</span>
                </a>
                <div class="glass-panel px-4 py-2 flex items-center gap-2">
                    <span class="status-pulse bg-emerald-500"></span>
                    <span class="text-sm font-semibold text-slate-700">{{ data_source }}</span>
                </div>
                <div class="glass-panel px-4 py-2 flex items-center gap-2">
                    <i data-lucide="hash" class="w-4 h-4 text-slate-500"></i>
                    <span class="text-sm font-semibold text-slate-700">Run: {{ selected_run }}</span>
                </div>
            </div>
        </header>

        <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">

            <aside class="lg:col-span-3 flex flex-col gap-4 sidebar-sticky">
                <div class="glass-panel p-4 flex-1 flex flex-col overflow-hidden">
                    <div class="flex items-center justify-between mb-4 px-2">
                        <h2 class="text-xs font-bold uppercase tracking-widest text-slate-500">Run History</h2>
                        <i data-lucide="history" class="w-4 h-4 text-slate-500"></i>
                    </div>

                    <p class="px-2 text-xs text-slate-500 mb-4">
                        Pick a run and keep it visible while you scroll.
                    </p>

                    <div class="sidebar-scroll overflow-y-auto space-y-2 pr-2">
                        {% for run in runs %}
                        <a href="/?run_id={{ run.run_id }}"
                           class="block p-3 rounded-xl border border-slate-200 transition-all hover:border-emerald-200 hover:bg-emerald-50/60 {% if run.run_id == selected_run %}nav-item-active{% endif %}">

                            <div class="flex justify-between items-start mb-2">
                                <span class="text-sm font-bold text-slate-800 truncate w-2/3">#{{ run.run_id }}</span>
                                <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase tracking-tighter {% if run.status == 'success' %}bg-emerald-500/20 text-emerald-400 border border-emerald-500/30{% else %}bg-rose-500/20 text-rose-400 border border-rose-500/30{% endif %}">
                                    {{ run.status }}
                                </span>
                            </div>

                            <div class="grid grid-cols-2 gap-2 text-[11px] text-slate-500 font-medium">
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
                            <p class="text-xs font-bold text-emerald-600 uppercase tracking-[0.2em] mb-2">Overall Sustainability Health</p>
                            <h2 class="text-2xl font-extrabold text-white">How sustainable was this run?</h2>
                            <p class="text-sm text-slate-600 mt-2 max-w-3xl">
                                Start here for the high-level view of score, confidence, anomalies, and baseline fit.
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
                            <div class="flex flex-wrap gap-2 mt-2">
                                <button type="button"
                                        onclick="openAlertModal('critical-alerts-modal')"
                                        class="inline-flex items-center gap-2 rounded-full border border-rose-200 bg-rose-50 px-3 py-1 text-xs font-bold uppercase tracking-wide text-rose-700 hover:bg-rose-100 transition-colors">
                                    <span>{{ anomaly_summary.critical_count }}</span>
                                    <span>Critical</span>
                                </button>
                                <button type="button"
                                        onclick="openAlertModal('warning-alerts-modal')"
                                        class="inline-flex items-center gap-2 rounded-full border border-amber-200 bg-amber-50 px-3 py-1 text-xs font-bold uppercase tracking-wide text-amber-700 hover:bg-amber-100 transition-colors">
                                    <span>{{ anomaly_summary.warning_count }}</span>
                                    <span>Warning</span>
                                </button>
                            </div>
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
                        <p class="text-xs font-bold text-emerald-600 uppercase tracking-[0.2em] mb-2">Current Run Summary</p>
                        <h2 class="text-2xl font-extrabold text-white">What happened in this run?</h2>
                        <p class="text-sm text-slate-600 mt-2">
                            The key usage and duration numbers for the selected run.
                        </p>
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
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
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4 mt-4">
                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Full Stage Duration</p>
                            <p class="text-2xl font-black text-white">{{ full_stage_duration_display }}</p>
                            <p class="text-[10px] text-slate-500 mt-2 font-medium">Jenkins/server-side stage time including orchestration overhead</p>
                        </div>

                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Workload Duration</p>
                            <p class="text-2xl font-black text-emerald-300">{{ workload_duration_display }}</p>
                            <p class="text-[10px] text-slate-500 mt-2 font-medium">Time spent executing monitored CI/CD commands</p>
                        </div>

                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Overhead %</p>
                            <p class="text-2xl font-black text-sky-300">{{ overhead_percentage_display }}</p>
                            <p class="text-[10px] text-slate-500 mt-2 font-medium">Share of stage time spent in setup, orchestration, or cleanup</p>
                        </div>
                    </div>

                    <div class="rounded-2xl border border-white/5 bg-slate-950/30 p-5 mt-4">
                        <div class="grid grid-cols-1 lg:grid-cols-3 gap-4 text-sm text-slate-500">
                            <p><span class="text-slate-200 font-semibold">Workload Duration</span> = time spent executing monitored CI/CD commands.</p>
                            <p><span class="text-slate-200 font-semibold">Full Stage Duration</span> = Jenkins/server-side execution time including orchestration overhead.</p>
                            <p><span class="text-slate-200 font-semibold">Overhead %</span> = the share of stage time spent outside the monitored workload.</p>
                        </div>
                        <p class="text-xs text-slate-500 mt-3">
                            Workload values represent monitored command execution. Full-stage values include captured CI/CD orchestration overhead where available.
                        </p>
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
                        <p class="text-xs font-bold text-emerald-300 uppercase tracking-[0.2em] mb-2">Statistical Anomaly Detection</p>
                        <h2 class="text-2xl font-extrabold text-white">Was anything unusual?</h2>
                        <p class="text-sm text-slate-600 mt-2">
                            Each metric is compared with its own historical baseline so timing, overhead, energy, and carbon stay easy to scan.
                        </p>
                    </div>

                    <div class="p-6">
                        <div class="rounded-2xl border border-white/5 bg-slate-950/30 p-4 mb-6">
                            <p class="text-sm text-slate-400">Critical and Warning signal meaningful deviation from the stage baseline.</p>
                            <p class="text-sm text-slate-400 mt-1">Z-score shows how far the current value moved from usual behavior for that metric.</p>
                        </div>

                        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                            {% for group in statistical_metric_groups %}
                            <div class="rounded-2xl border {{ group.panel_class }} p-5">
                                <div class="flex items-start justify-between gap-3">
                                    <div>
                                        <p class="text-xs font-bold text-slate-400 uppercase tracking-wider">{{ group.title }}</p>
                                        <p class="text-sm text-slate-500 mt-1">Worst stage: <span class="font-semibold text-slate-300">{{ group.worst_stage_label }}</span></p>
                                    </div>
                                    <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {{ group.severity_badge_class }}">
                                        {{ group.severity_label }}
                                    </span>
                                </div>
                                <p class="text-xl font-black {{ group.severity_text_class }} mt-4">{{ group.current_vs_usual }}</p>
                                <p class="text-sm font-semibold text-slate-300 mt-2">Change: {{ group.change_display }}</p>
                                <p class="text-sm text-slate-500 mt-3">{{ group.insight }}</p>

                                <details class="mt-4 rounded-xl border border-white/5 bg-white/2">
                                    <summary class="cursor-pointer list-none px-4 py-3 text-sm font-semibold text-slate-300 flex items-center justify-between">
                                        <span>View stage-level details</span>
                                        <span class="text-xs text-slate-500">Build / Test / Deploy</span>
                                    </summary>
                                    <div class="overflow-x-auto border-t border-white/5">
                                        <table class="w-full text-left">
                                            <thead>
                                                <tr class="text-[11px] font-bold text-slate-400 uppercase tracking-wider bg-slate-900/40">
                                                    <th class="px-4 py-3">Stage</th>
                                                    <th class="px-4 py-3">Current</th>
                                                    <th class="px-4 py-3">Usual</th>
                                                    <th class="px-4 py-3">Change</th>
                                                    <th class="px-4 py-3">Z-Score</th>
                                                    <th class="px-4 py-3 text-right">Severity</th>
                                                </tr>
                                            </thead>
                                            <tbody class="divide-y divide-white/5">
                                                {% for row in group.rows %}
                                                <tr class="hover:bg-white/5 transition-colors">
                                                    <td class="px-4 py-3 text-slate-200 font-semibold">{{ row.stage_label }}</td>
                                                    <td class="px-4 py-3 text-white">{{ row.current_display }}</td>
                                                    <td class="px-4 py-3 text-slate-300">{{ row.baseline_display }}</td>
                                                    <td class="px-4 py-3 text-slate-300">{{ row.change_display }}</td>
                                                    <td class="px-4 py-3 text-slate-300">{{ row.z_score_display }}</td>
                                                    <td class="px-4 py-3 text-right">
                                                        <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {{ row.severity_badge_class }}">
                                                            {{ row.severity_label }}
                                                        </span>
                                                    </td>
                                                </tr>
                                                {% endfor %}
                                            </tbody>
                                        </table>
                                    </div>
                                </details>
                            </div>
                            {% endfor %}
                        </div>
                    </div>
                </section>

                <section class="glass-panel overflow-hidden">
                    <div class="px-6 py-5 border-b border-white/5 bg-white/2">
                        <p class="text-xs font-bold text-emerald-300 uppercase tracking-[0.2em] mb-2">ML Anomaly Detection</p>
                        <h2 class="text-2xl font-extrabold text-white">What does the Isolation Forest model see?</h2>
                        <p class="text-sm text-slate-600 mt-2">
                            Isolation Forest learns normal pipeline behavior from historical runs and flags unusual stage patterns across duration, CPU, energy, carbon, and overhead.
                        </p>
                        <p class="text-sm text-slate-500 mt-2">
                            Statistical detection evaluates each metric independently. Isolation Forest evaluates the full multi-metric pattern, so results may differ.
                        </p>
                    </div>

                    <div class="p-6 space-y-6">
                        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
                            <div class="glass-panel p-5">
                                <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Model</p>
                                <p class="text-2xl font-black text-slate-200">{{ ml_anomaly.model }}</p>
                                <p class="text-[11px] text-slate-500 mt-2">Prototype stage-level anomaly detection model</p>
                            </div>

                            <div class="glass-panel p-5">
                                <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Status</p>
                                <p class="text-2xl font-black {% if ml_anomaly.status_color == 'rose' %}text-rose-300{% elif ml_anomaly.status_color == 'amber' %}text-amber-300{% elif ml_anomaly.status_color == 'sky' %}text-sky-300{% else %}text-emerald-300{% endif %}">{{ ml_anomaly.status }}</p>
                                <p class="text-[11px] text-slate-500 mt-2">{{ ml_anomaly.message }}</p>
                            </div>

                            <div class="glass-panel p-5">
                                <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Historical Samples Used</p>
                                <p class="text-2xl font-black text-slate-200">{{ ml_anomaly.historical_samples_used }}</p>
                                <p class="text-[11px] text-slate-500 mt-2">Historical stage records used to fit the model</p>
                            </div>

                            <div class="glass-panel p-5">
                                <p class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Predicted Stage Results</p>
                                <p class="text-2xl font-black text-slate-200">{{ ml_anomaly.results|length }}</p>
                                <p class="text-[11px] text-slate-500 mt-2">Current run stages evaluated by the model</p>
                            </div>
                        </div>

                        {% if ml_anomaly.status == 'Warming Up' %}
                        <div class="rounded-2xl border border-sky-500/20 bg-sky-500/5 p-5 flex gap-4">
                            <div class="mt-1"><i data-lucide="bot" class="w-5 h-5 text-sky-400"></i></div>
                            <div>
                                <h4 class="text-sm font-bold text-sky-300 mb-1">Model warming up</h4>
                                <p class="text-xs text-sky-100/70 leading-relaxed">
                                    Isolation Forest needs at least 10 historical stage records. Until then, the statistical anomaly model is currently the fallback.
                                </p>
                            </div>
                        </div>
                        {% endif %}

                        <div class="overflow-x-auto rounded-2xl border border-white/5">
                            <table class="w-full text-left">
                                <thead>
                                    <tr class="text-[11px] font-bold text-slate-400 uppercase tracking-wider bg-slate-900/40">
                                        <th class="px-6 py-4">Stage</th>
                                        <th class="px-6 py-4">Prediction</th>
                                        <th class="px-6 py-4">Anomaly Score</th>
                                        <th class="px-6 py-4">Severity</th>
                                        <th class="px-6 py-4">Message</th>
                                    </tr>
                                </thead>
                                <tbody class="divide-y divide-white/5">
                                    {% if ml_anomaly.results %}
                                        {% for item in ml_anomaly.results %}
                                        <tr class="hover:bg-white/5 transition-colors">
                                            <td class="px-6 py-4 text-slate-200 font-semibold">{{ item.stage_label }}</td>
                                            <td class="px-6 py-4 text-slate-300">{{ item.prediction }}</td>
                                            <td class="px-6 py-4 text-white font-mono">{{ item.anomaly_score_display }}</td>
                                            <td class="px-6 py-4">
                                                <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {% if item.severity == 'critical' %}text-rose-300 bg-rose-500/10{% elif item.severity == 'warning' %}text-amber-300 bg-amber-500/10{% else %}text-emerald-300 bg-emerald-500/10{% endif %}">
                                                    {{ item.severity }}
                                                </span>
                                            </td>
                                            <td class="px-6 py-4 text-slate-300">{{ item.message }}</td>
                                        </tr>
                                        {% endfor %}
                                    {% else %}
                                        <tr>
                                            <td colspan="5" class="px-6 py-6 text-center text-slate-500">No ML stage predictions available yet.</td>
                                        </tr>
                                    {% endif %}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </section>

                <section class="glass-panel overflow-hidden">
                    <div class="px-6 py-5 border-b border-white/5 bg-white/2">
                        <p class="text-xs font-bold text-emerald-300 uppercase tracking-[0.2em] mb-2">Baseline Comparison</p>
                        <h2 class="text-2xl font-extrabold text-white">How does this compare with normal behavior?</h2>
                        <p class="text-sm text-slate-600 mt-2">
                            Compare this run with the historical average from earlier runs.
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
                        <p class="text-sm text-slate-600 mt-2">
                            Scan the charts first, then use the table for exact stage values.
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
                                        <th class="px-6 py-4">Workload Duration</th>
                                        <th class="px-6 py-4">Full Duration</th>
                                        <th class="px-6 py-4">Overhead</th>
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

                                        <td class="px-6 py-4 text-slate-300 font-medium">{{ row.workload_duration_display }}</td>
                                        <td class="px-6 py-4 text-slate-300 font-medium">{{ row.full_duration_display }}</td>
                                        <td class="px-6 py-4 text-slate-300 font-medium">{{ row.overhead_display }}</td>

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
        const fullStageEnergy = {{ total_energy_values | safe }};
        const workloadEnergy = {{ workload_energy_values | safe }};
        const avgCpu = {{ cpu_values | safe }};

        const gridColor = "#e2e8f0";
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
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const value = Number(context.parsed.y || 0);
                            const formatted = value === 0
                                ? "0.00000000"
                                : (Math.abs(value) < 0.01 ? value.toFixed(8) : value.toFixed(4));
                            return `${context.dataset.label}: ${formatted} kWh`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    grid: { color: gridColor },
                    border: { display: false },
                    ticks: {
                        callback: function(value) {
                            const numeric = Number(value || 0);
                            return Math.abs(numeric) < 0.01 ? numeric.toFixed(8) : numeric.toFixed(4);
                        }
                    }
                },
                x: { grid: { display: false } }
            }
        };

        new Chart(document.getElementById("energyChart"), {
            type: "bar",
            data: {
                labels: stages,
                datasets: [
                    {
                        label: "Workload Energy",
                        data: workloadEnergy,
                        backgroundColor: "rgba(16, 185, 129, 0.6)",
                        borderColor: "rgba(16, 185, 129, 1)",
                        borderWidth: 1,
                        borderRadius: 6,
                        barThickness: 20
                    },
                    {
                        label: "Full Stage Estimated Energy",
                        data: fullStageEnergy,
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

        function openAlertModal(modalId) {
            const modal = document.getElementById(modalId);
            if (!modal) return;
            modal.classList.add("is-open");
            document.body.style.overflow = "hidden";
        }

        function closeAlertModal(modalId) {
            const modal = document.getElementById(modalId);
            if (!modal) return;
            modal.classList.remove("is-open");
            if (!document.querySelector(".modal-overlay.is-open")) {
                document.body.style.overflow = "";
            }
        }

        document.addEventListener("keydown", function(event) {
            if (event.key !== "Escape") return;
            document.querySelectorAll(".modal-overlay.is-open").forEach(function(modal) {
                modal.classList.remove("is-open");
            });
            document.body.style.overflow = "";
        });
    </script>

    <div id="critical-alerts-modal" class="modal-overlay" onclick="if (event.target === this) closeAlertModal('critical-alerts-modal')">
        <div class="modal-panel">
            <div class="flex items-center justify-between px-6 py-5 border-b border-slate-200">
                <div>
                    <p class="text-xs font-bold uppercase tracking-[0.2em] text-rose-600">Critical Alerts</p>
                    <h3 class="text-xl font-extrabold text-slate-900 mt-1">Critical alerts from statistical and ML models</h3>
                </div>
                <button type="button"
                        onclick="closeAlertModal('critical-alerts-modal')"
                        class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-semibold text-slate-600 hover:bg-slate-50 transition-colors">
                    Close
                </button>
            </div>
            <div class="modal-scroll px-6 py-5">
                {% if critical_alerts %}
                    <div class="space-y-4">
                        {% for alert in critical_alerts %}
                        <div class="rounded-2xl border border-rose-200 bg-rose-50 p-4">
                            <div class="flex flex-wrap items-center gap-2 mb-3">
                                <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {{ alert.severity_badge_class }}">{{ alert.severity_label }}</span>
                                <span class="text-sm font-semibold text-slate-800">{{ alert.metric_label }}</span>
                                <span class="text-sm text-slate-500">Stage: {{ alert.stage_label }}</span>
                                <span class="text-sm text-slate-500">Source: {{ alert.source }}</span>
                            </div>
                            <div class="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
                                <p class="text-slate-700"><span class="font-semibold">Current:</span> {{ alert.current_display }}</p>
                                <p class="text-slate-700"><span class="font-semibold">Baseline:</span> {{ alert.baseline_display }}</p>
                                <p class="text-slate-700"><span class="font-semibold">Change:</span> {{ alert.percentage_change_display }}</p>
                            </div>
                            <p class="text-sm text-slate-600 mt-3">{{ alert.message }}</p>
                        </div>
                        {% endfor %}
                    </div>
                {% else %}
                    <p class="text-sm text-slate-500">No alerts in this category.</p>
                {% endif %}
            </div>
        </div>
    </div>

    <div id="warning-alerts-modal" class="modal-overlay" onclick="if (event.target === this) closeAlertModal('warning-alerts-modal')">
        <div class="modal-panel">
            <div class="flex items-center justify-between px-6 py-5 border-b border-slate-200">
                <div>
                    <p class="text-xs font-bold uppercase tracking-[0.2em] text-amber-600">Warnings</p>
                    <h3 class="text-xl font-extrabold text-slate-900 mt-1">Warnings from statistical and ML models</h3>
                </div>
                <button type="button"
                        onclick="closeAlertModal('warning-alerts-modal')"
                        class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-semibold text-slate-600 hover:bg-slate-50 transition-colors">
                    Close
                </button>
            </div>
            <div class="modal-scroll px-6 py-5">
                {% if warning_alerts %}
                    <div class="space-y-4">
                        {% for alert in warning_alerts %}
                        <div class="rounded-2xl border border-amber-200 bg-amber-50 p-4">
                            <div class="flex flex-wrap items-center gap-2 mb-3">
                                <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {{ alert.severity_badge_class }}">{{ alert.severity_label }}</span>
                                <span class="text-sm font-semibold text-slate-800">{{ alert.metric_label }}</span>
                                <span class="text-sm text-slate-500">Stage: {{ alert.stage_label }}</span>
                                <span class="text-sm text-slate-500">Source: {{ alert.source }}</span>
                            </div>
                            <div class="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
                                <p class="text-slate-700"><span class="font-semibold">Current:</span> {{ alert.current_display }}</p>
                                <p class="text-slate-700"><span class="font-semibold">Baseline:</span> {{ alert.baseline_display }}</p>
                                <p class="text-slate-700"><span class="font-semibold">Change:</span> {{ alert.percentage_change_display }}</p>
                            </div>
                            <p class="text-sm text-slate-600 mt-3">{{ alert.message }}</p>
                        </div>
                        {% endfor %}
                    </div>
                {% else %}
                    <p class="text-sm text-slate-500">No alerts in this category.</p>
                {% endif %}
            </div>
        </div>
    </div>
</body>
</html>
"""


@app.route("/")
def dashboard():
    df, data_source = load_metrics()

    if df.empty:
        return """
        <div style='background:#f8fafc; color:#0f172a; height:100vh; display:flex; align-items:center; justify-content:center; font-family:sans-serif;'>
            <h2>No monitoring data found.</h2>
        </div>
        """

    df = prepare_metrics_dataframe(df)
    run_summary = build_run_summary(df)

    if run_summary.empty:
        return """
        <div style='background:#f8fafc; color:#0f172a; height:100vh; display:flex; align-items:center; justify-content:center; font-family:sans-serif;'>
            <h2>No monitoring data found.</h2>
        </div>
        """

    requested_run = request.args.get("run_id")
    available_run_ids = set(run_summary["run_id"].astype(str).tolist())
    selected_run = requested_run if requested_run in available_run_ids else str(run_summary.iloc[0]["run_id"])

    current_run_df = df[df["run_id"] == selected_run].copy()
    historical_df = df[df["run_id"] != selected_run].copy()
    ml_anomaly = detect_ml_anomalies(current_run_df, historical_df)

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
            workload_duration_seconds=("workload_duration_seconds", "sum"),
            jenkins_stage_duration_seconds=("jenkins_stage_duration_seconds", "sum"),
            infrastructure_overhead_seconds=("infrastructure_overhead_seconds", "sum"),
            overhead_percentage=("overhead_percentage", "mean"),
            jenkins_stage_duration_captured=("jenkins_stage_duration_captured", "max"),
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
            severity_rank(item.get("severity")),
            -abs(float(item.get("percentage_change") or 0.0)),
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
    workload_duration = round(float(current_run_df["workload_duration_seconds"].sum()), 2)
    jenkins_stage_duration = round(float(current_run_df["jenkins_stage_duration_seconds"].sum()), 2)
    infrastructure_overhead = round(float(current_run_df["infrastructure_overhead_seconds"].sum()), 2)
    overhead_percentage = round(
        (infrastructure_overhead / jenkins_stage_duration * 100.0) if jenkins_stage_duration > 0 else 0.0,
        2,
    )
    avg_cpu = round(float(summary["avg_cpu_percent"].mean()), 2) if not summary.empty else 0
    has_full_stage_timing = bool(current_run_df["jenkins_stage_duration_captured"].any())

    phone_charges = round(total_energy / 0.01, 3) if total_energy else 0
    led_hours = round(total_energy / 0.01, 3) if total_energy else 0
    car_meters = round(((total_carbon * 1000) / 120) * 1000, 3) if total_carbon else 0

    highest_active_stage = (
        summary.sort_values("active_energy_kwh", ascending=False).iloc[0]["stage"] if not summary.empty else "N/A"
    )
    highest_total_stage = (
        summary.sort_values("total_energy_kwh", ascending=False).iloc[0]["stage"] if not summary.empty else "N/A"
    )

    dashboard_anomalies = deduplicate_anomalies(anomalies, allowed_metrics=PP1_ANOMALY_METRICS, limit=8)
    dashboard_anomalies = [normalize_dashboard_anomaly(item) for item in dashboard_anomalies]
    major_dashboard_anomalies = [item for item in dashboard_anomalies if item.get("severity") in {"critical", "warning"}]
    formatted_statistical_alerts = [
    {
        **format_dashboard_anomaly(item),
        "source": "Statistical"
    }
    for item in dashboard_anomalies
    if item.get("severity") in {"critical", "warning"}
    ]
    formatted_ml_alerts = [
    format_ml_alert(item)
    for item in ml_anomaly.get("results", [])
    if normalize_display_severity(item.get("severity")) in {"critical", "warning"}
    ]
    formatted_alerts = formatted_statistical_alerts + formatted_ml_alerts
    critical_alerts = [
    item for item in formatted_alerts
    if item.get("severity") == "critical"
    ]
    warning_alerts = [
    item for item in formatted_alerts
    if item.get("severity") == "warning"
    ]
    combined_critical_count = len(critical_alerts)
    combined_warning_count = len(warning_alerts)

    if combined_critical_count > 0:
        anomaly_summary = {
            "critical_count": combined_critical_count,
            "warning_count": combined_warning_count,
            "overall_status": "Critical",
            "summary_message": f"{combined_critical_count} critical and {combined_warning_count} warning alert(s) detected across statistical and ML models.",
        }
    elif combined_warning_count > 0:
            anomaly_summary = {
            "critical_count": 0,
            "warning_count": combined_warning_count,
            "overall_status": "Warning",
            "summary_message": f"{combined_warning_count} warning alert(s) detected across statistical and ML models.",
        }
    else:
        anomaly_summary = {
            "critical_count": 0,
            "warning_count": 0,
            "overall_status": "Normal",
            "summary_message": "No statistical or ML anomaly alerts were detected.",
        }
    statistical_metric_groups = build_statistical_metric_groups(summary, stage_baseline_df, dashboard_anomalies)
    anomaly_teaser = (
        dashboard_anomalies[0]["message"] if dashboard_anomalies else "No major anomaly detected for this run."
    )
    pipeline_insight = (
        f"Run {selected_run} used {format_kwh(total_energy)} and emitted {format_gco2_from_kg(total_carbon)}. "
        f"Health score: {health_score['score']}/100 ({health_score['grade']}). "
        f"Workload time was {format_seconds(workload_duration)}"
        f"{f', with {format_seconds(infrastructure_overhead)} of infrastructure overhead.' if has_full_stage_timing else '.'} "
        f"{anomaly_teaser}"
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
    display_rows["workload_duration_display"] = display_rows["workload_duration_seconds"].apply(format_seconds)
    display_rows["full_duration_display"] = display_rows.apply(
        lambda row: format_seconds(row["jenkins_stage_duration_seconds"])
        if row["jenkins_stage_duration_captured"] else "Not captured",
        axis=1,
    )
    display_rows["overhead_display"] = display_rows.apply(
        lambda row: format_seconds(row["infrastructure_overhead_seconds"])
        if row["jenkins_stage_duration_captured"] else "Not captured",
        axis=1,
    )
    display_rows["overhead_percentage_display"] = display_rows.apply(
        lambda row: format_percent(row["overhead_percentage"]) if row["jenkins_stage_duration_captured"] else "Not captured",
        axis=1,
    )
    display_rows["avg_cpu_display"] = display_rows["avg_cpu_percent"].apply(format_percent)
    display_rows["total_energy_display"] = display_rows["total_energy_kwh"].apply(format_kwh)
    display_rows["total_carbon_display"] = display_rows["total_carbon_kg"].apply(format_gco2_from_kg)

    run_summary["total_energy_display"] = run_summary["total_energy_kwh"].apply(format_kwh)
    run_summary["total_carbon_display"] = run_summary["total_carbon_kg"].apply(format_gco2_from_kg)
    run_summary["duration_display"] = run_summary.apply(
        lambda row: format_seconds(row["jenkins_stage_duration_seconds"])
        if row["jenkins_stage_duration_captured"] else format_seconds(row["duration_seconds"]),
        axis=1,
    )

    formatted_ml_results = []
    for item in ml_anomaly.get("results", []):
        formatted_item = dict(item)
        formatted_item["severity"] = normalize_display_severity(item.get("severity"))
        if formatted_item["severity"] == "normal" and item.get("severity") == "info":
            formatted_item["message"] = "Stable compared with baseline."
        formatted_item["stage_label"] = str(item.get("stage", "unknown")).replace("_", " ").title()
        formatted_item["anomaly_score_display"] = format_decimal(item.get("anomaly_score", 0.0), decimals=4)
        formatted_ml_results.append(formatted_item)

    formatted_ml_anomaly = dict(ml_anomaly)
    formatted_ml_anomaly["model"] = ml_anomaly.get("model", "Isolation Forest")
    formatted_ml_anomaly["historical_samples_used"] = int(ml_anomaly.get("historical_samples_used", 0))
    formatted_ml_anomaly["status"] = (
        ml_anomaly.get("status", "Normal")
        if str(ml_anomaly.get("status", "")).strip().lower() == "warming up"
        else severity_theme(ml_anomaly.get("status", "Normal"))["label"]
    )
    formatted_ml_anomaly["status_color"] = ml_status_color(formatted_ml_anomaly["status"])
    formatted_ml_anomaly["results"] = formatted_ml_results
    refresh_url = f"/?run_id={selected_run}" if selected_run else "/"

    return render_template_string(
        HTML,
        selected_run=selected_run,
        refresh_url=refresh_url,
        data_source=data_source,
        runs=run_summary.to_dict(orient="records"),
        total_energy=total_energy,
        active_energy=active_energy,
        total_carbon=total_carbon,
        pipeline_duration=pipeline_duration,
        workload_duration=workload_duration,
        jenkins_stage_duration=jenkins_stage_duration,
        infrastructure_overhead=infrastructure_overhead,
        overhead_percentage=overhead_percentage,
        avg_cpu=avg_cpu,
        total_energy_display=format_kwh(total_energy),
        active_energy_display=format_kwh(active_energy),
        total_carbon_display=format_gco2_from_kg(total_carbon),
        pipeline_duration_display=format_seconds(pipeline_duration),
        workload_duration_display=format_seconds(workload_duration),
        full_stage_duration_display=format_seconds(jenkins_stage_duration) if has_full_stage_timing else "Not captured",
        infrastructure_overhead_display=format_seconds(infrastructure_overhead) if has_full_stage_timing else "Not captured",
        overhead_percentage_display=format_percent(overhead_percentage) if has_full_stage_timing else "Not captured",
        avg_cpu_display=format_percent(avg_cpu),
        phone_charges=phone_charges,
        led_hours=led_hours,
        car_meters=car_meters,
        phone_charges_display=format_equivalent(phone_charges, "", " charges"),
        led_hours_display=format_equivalent(led_hours, "h", "h"),
        car_meters_display=format_equivalent(car_meters, "m", "m"),
        stage_count=len(display_rows),
        pipeline_insight=pipeline_insight,
        stage_insight=stage_insight,
        rows=display_rows.to_dict(orient="records"),
        stages=json.dumps(summary["stage"].astype(str).tolist()),
        total_energy_values=json.dumps(summary["total_energy_kwh"].round(8).tolist()),
        workload_energy_values=json.dumps(summary["active_energy_kwh"].round(8).tolist()),
        cpu_values=json.dumps(summary["avg_cpu_percent"].round(2).tolist()),
        health_score=health_score,
        confidence_level=confidence_level,
        baseline_run_count=baseline_run_count,
        anomaly_summary=anomaly_summary,
        comparison_rows=comparison_rows,
        statistical_metric_groups=statistical_metric_groups,
        critical_alerts=critical_alerts,
        warning_alerts=warning_alerts,
        ml_anomaly=formatted_ml_anomaly,
        major_anomaly_count=len(major_dashboard_anomalies),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5051, debug=False)
