from flask import Flask, abort, render_template_string, request
import json
import os
import sqlite3

import os

os.environ["MONGO_URI"] = (
    "mongodb://admin:admin1234@"
    "ac-5adtlpz-shard-00-00.xxflzzs.mongodb.net:27017,"
    "ac-5adtlpz-shard-00-01.xxflzzs.mongodb.net:27017,"
    "ac-5adtlpz-shard-00-02.xxflzzs.mongodb.net:27017/"
    "?ssl=true"
    "&replicaSet=atlas-g0hboh-shard-0"
    "&authSource=admin"
    "&appName=Green-DevOps-Monitor"
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
DEPLOY_DB_PATH = "/opt/energy-profiller-hiran/deployments.db"
RELEASE_API_BUILDS_URL = "http://release-dashboard:5050/api/builds"

NUMERIC_COLS = [
    "duration_seconds",
    "workload_duration_seconds",
    "jenkins_stage_duration_seconds",
    "infrastructure_overhead_seconds",
    "overhead_percentage",
    "avg_cpu_percent",
    "peak_cpu_percent",
    "avg_memory_percent",
    "peak_memory_percent",
    "total_energy_kwh",
    "active_energy_kwh",
    "total_carbon_kg",
    "active_carbon_kg",
    "carbon_intensity_kg_per_kwh",
]

DEFAULT_STAGE_ORDER = ["release", "deploy", "operate"]
PP1_ANOMALY_METRICS = [
    "workload_duration_seconds",
    "overhead_percentage",
    "total_energy_kwh",
    "total_carbon_kg",
]

TRUE_VALUES = {"true", "1", "yes", "y", "on"}
FALSE_VALUES = {"false", "0", "no", "n", "off", ""}


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


def extract_build_number_from_run_id(run_id):
    parts = str(run_id or "").rsplit("-", 1)
    if len(parts) != 2:
        return ""
    candidate = parts[1].strip()
    return candidate if candidate.isdigit() else ""


def _release_api_url():
    return os.getenv("RELEASE_API_BUILDS_URL", RELEASE_API_BUILDS_URL)


def _normalize_release_key_part(value):
    return " ".join(str(value or "").strip().split())


def release_build_key(build):
    job_name = _normalize_release_key_part((build or {}).get("job_name"))
    build_number = _normalize_release_key_part((build or {}).get("build_number"))
    if not job_name or not build_number:
        return ""
    return f"{job_name}-{build_number}"


def load_release_builds(api_url=None, timeout_seconds=3):
    """Read Release build records from the Release dashboard API without mutating anything."""
    try:
        import requests
    except ImportError:
        print("[Release API] WARNING: requests is not installed. Continuing with Monitor data only.")
        return []

    url = api_url or _release_api_url()
    try:
        response = requests.get(url, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        print(f"[Release API] WARNING: Release builds unavailable from {url}. Continuing with Monitor data only. Error: {exc}")
        return []

    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("builds", "data", "records", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    print("[Release API] WARNING: Release builds response was not a list of records. Continuing with Monitor data only.")
    return []


def find_release_build_for_run(run_id, builds):
    monitor_key = _normalize_release_key_part(run_id)
    if not monitor_key:
        return None
    for build in builds or []:
        if release_build_key(build) == monitor_key:
            return build
    return None


def _sqlite_readonly_uri(path):
    return f"file:{os.path.abspath(path).replace(os.sep, '/')}?mode=ro"


def _row_value(row, key):
    return row[key] if row is not None and key in row.keys() else None


def load_deploy_data(pipeline_name, build_number, db_path=None):
    if not pipeline_name or not build_number:
        return None

    path = db_path or os.getenv("DEPLOY_DB_PATH", DEPLOY_DB_PATH)
    if not os.path.exists(path):
        print(f"[Deploy DB] WARNING: Deploy database not found at {path}. Continuing with Monitor data only.")
        return None

    connection = None
    try:
        connection = sqlite3.connect(_sqlite_readonly_uri(path), uri=True)
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            """
            SELECT
                d.id AS deployment_id,
                d.job_name AS deploy_job_name,
                d.build_number AS deploy_build_number,
                d.status AS deploy_status,
                d.strategy AS deploy_strategy,
                d.canary_weight AS deploy_canary_weight,
                d.carbon_profile AS deploy_carbon_profile,
                d.image AS deploy_image,
                d.start_time AS deploy_start_time,
                d.end_time AS deploy_end_time,
                d.duration_minutes AS deploy_duration_minutes,
                p.samples_collected AS deploy_samples_collected,
                p.avg_cpu AS deploy_avg_cpu,
                p.peak_cpu AS deploy_peak_cpu,
                p.min_cpu AS deploy_min_cpu,
                p.avg_memory AS deploy_avg_memory,
                p.peak_memory AS deploy_peak_memory,
                p.min_memory AS deploy_min_memory,
                c.total_g_co2 AS deploy_total_g_co2,
                c.total_kg_co2 AS deploy_total_kg_co2,
                c.total_energy_kwh AS deploy_total_energy_kwh,
                c.carbon_intensity_gco2 AS deploy_carbon_intensity_gco2,
                c.intensity_source AS deploy_intensity_source,
                c.strategy_carbon_profile AS deploy_strategy_carbon_profile,
                c.infra_multiplier AS deploy_infra_multiplier
            FROM deployments d
            LEFT JOIN profiler_results p
                ON p.deployment_id = d.id
            LEFT JOIN carbon_reports c
                ON c.deployment_id = d.id
            WHERE d.job_name = ?
                AND d.build_number = ?
            ORDER BY d.id DESC, p.id DESC, c.id DESC
            LIMIT 1
            """,
            (str(pipeline_name), str(build_number)),
        ).fetchone()

        if row is None:
            return None

        snapshot_rows = connection.execute(
            """
            SELECT
                phase,
                strategy,
                infra_multiplier,
                downtime_seconds,
                canary_weight,
                note,
                snapshot_timestamp
            FROM carbon_snapshots
            WHERE deployment_id = ?
            ORDER BY snapshot_timestamp ASC, id ASC
            """,
            (_row_value(row, "deployment_id"),),
        ).fetchall()

        snapshots = [
            {
                "phase": _row_value(snapshot, "phase"),
                "strategy": _row_value(snapshot, "strategy"),
                "infra_multiplier": _row_value(snapshot, "infra_multiplier"),
                "downtime_seconds": _row_value(snapshot, "downtime_seconds"),
                "canary_weight": _row_value(snapshot, "canary_weight"),
                "note": _row_value(snapshot, "note"),
                "snapshot_timestamp": _row_value(snapshot, "snapshot_timestamp"),
            }
            for snapshot in snapshot_rows
        ]

        return {
            "deployment_id": _row_value(row, "deployment_id"),
            "deployment": {
                "status": _row_value(row, "deploy_status"),
                "strategy": _row_value(row, "deploy_strategy"),
                "canary_weight": _row_value(row, "deploy_canary_weight"),
                "carbon_profile": _row_value(row, "deploy_carbon_profile"),
                "image": _row_value(row, "deploy_image"),
                "start_time": _row_value(row, "deploy_start_time"),
                "end_time": _row_value(row, "deploy_end_time"),
                "duration_minutes": _row_value(row, "deploy_duration_minutes"),
            },
            "profiler": {
                "avg_cpu": _row_value(row, "deploy_avg_cpu"),
                "peak_cpu": _row_value(row, "deploy_peak_cpu"),
                "min_cpu": _row_value(row, "deploy_min_cpu"),
                "avg_memory": _row_value(row, "deploy_avg_memory"),
                "peak_memory": _row_value(row, "deploy_peak_memory"),
                "min_memory": _row_value(row, "deploy_min_memory"),
                "samples_collected": _row_value(row, "deploy_samples_collected"),
            },
            "carbon": {
                "total_energy_kwh": _row_value(row, "deploy_total_energy_kwh"),
                "total_g_co2": _row_value(row, "deploy_total_g_co2"),
                "total_kg_co2": _row_value(row, "deploy_total_kg_co2"),
                "carbon_intensity_gco2": _row_value(row, "deploy_carbon_intensity_gco2"),
                "intensity_source": _row_value(row, "deploy_intensity_source"),
                "strategy_carbon_profile": _row_value(row, "deploy_strategy_carbon_profile"),
                "infra_multiplier": _row_value(row, "deploy_infra_multiplier"),
            },
            "snapshots": snapshots,
        }
    except (OSError, sqlite3.Error) as exc:
        print(f"[Deploy DB] WARNING: Deploy database lookup failed: {exc}. Continuing with Monitor data only.")
        return None
    finally:
        if connection is not None:
            connection.close()


def normalize_skipped_value(value):
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        return False
    normalized = str(value).strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    return False


def workload_analytics_dataframe(df):
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()
    if "skipped" not in df.columns:
        return df.copy()
    return df[~df["skipped"].map(normalize_skipped_value)].copy()


def prepare_metrics_dataframe(df):
    prepared = df.copy()

    if prepared.empty:
        return prepared

    if "run_id" not in prepared.columns:
        prepared["run_id"] = "run-1"
    prepared["run_id"] = prepared["run_id"].fillna("unknown-run").astype(str)

    if "stage" not in prepared.columns:
        prepared["stage"] = prepared["lifecycle_stage"] if "lifecycle_stage" in prepared.columns else "unknown"
    elif "lifecycle_stage" in prepared.columns:
        stage_values = prepared["stage"].fillna("").astype(str).str.strip()
        prepared.loc[stage_values == "", "stage"] = prepared.loc[stage_values == "", "lifecycle_stage"]
    prepared["stage"] = prepared["stage"].fillna("unknown").astype(str).str.strip().str.lower()
    prepared["stage"] = prepared["stage"].where(prepared["stage"] != "", "unknown")

    if "skipped" not in prepared.columns:
        prepared["skipped"] = False
    prepared["skipped"] = prepared["skipped"].apply(normalize_skipped_value).astype(bool)

    if "skip_reason" not in prepared.columns:
        prepared["skip_reason"] = ""
    prepared["skip_reason"] = prepared["skip_reason"].fillna("").astype(str).str.strip()

    if "strategy" not in prepared.columns:
        prepared["strategy"] = ""
    prepared["strategy"] = prepared["strategy"].fillna("").astype(str).str.strip().str.lower()

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


def format_optional_text(value):
    text = "" if value is None or pd.isna(value) else str(value).strip()
    return text if text else "Not available"


def format_optional_count(value):
    if value is None or pd.isna(value):
        return "Not available"
    return format_count(value)


def format_optional_percent(value):
    if value is None or pd.isna(value):
        return "Not available"
    return format_percent(value)


def format_optional_kwh(value):
    if value is None or pd.isna(value):
        return "Not available"
    return format_kwh(value)


def format_optional_gco2(value):
    if value is None or pd.isna(value):
        return "Not available"
    numeric = float(value)
    if numeric == 0:
        return "0.000 gCO2"
    if abs(numeric) < 1:
        return f"{numeric:.4f} gCO2"
    return f"{numeric:.2f} gCO2"


def format_optional_minutes(value):
    if value is None or pd.isna(value):
        return "Not available"
    return f"{float(value):.4f} min"


def format_optional_intensity(value):
    if value is None or pd.isna(value):
        return "Not available"
    return f"{float(value):.2f} gCO2/kWh"


def format_optional_multiplier(value):
    if value is None or pd.isna(value):
        return "Not available"
    return f"{float(value):.2f}x"


def format_release_probability(value):
    if value is None or pd.isna(value):
        return "Not available"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "Not available"
    if abs(numeric) <= 1:
        numeric *= 100
    return f"{numeric:.0f}%"


def format_release_seconds(value):
    if value is None or pd.isna(value):
        return "Not available"
    try:
        return format_seconds(value)
    except (TypeError, ValueError):
        return "Not available"


def format_release_count(value):
    if value is None or pd.isna(value):
        return "Not available"
    try:
        return format_count(value)
    except (TypeError, ValueError):
        return "Not available"


def format_release_intensity(value):
    if value is None or pd.isna(value):
        return "Not available"
    try:
        return format_optional_intensity(value)
    except (TypeError, ValueError):
        return "Not available"


def format_release_list(value):
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else "None"
    if value is None or pd.isna(value):
        return "Not available"
    text = str(value).strip()
    return text if text else "Not available"


def format_release_build_data(build):
    if not build:
        return None
    status = format_optional_text(build.get("status"))
    return {
        "status_display": status.upper() if status != "Not available" else status,
        "pipeline_type_display": format_optional_text(build.get("pipeline_type")).replace("_", " ").title(),
        "green_probability_display": format_release_probability(build.get("green_probability")),
        "scheduling_action_display": format_optional_text(build.get("scheduling_action")).replace("_", " ").title(),
        "scheduling_engine_display": format_optional_text(build.get("scheduling_engine")),
        "carbon_intensity_display": format_release_intensity(build.get("carbon_intensity")),
        "affected_modules_display": format_release_list(build.get("affected_modules")),
        "tests_executed_display": format_release_count(build.get("tests_executed")),
        "tests_skipped_display": format_release_count(build.get("tests_skipped")),
        "build_duration_display": format_release_seconds(build.get("build_duration_s")),
        "test_duration_display": format_release_seconds(build.get("test_duration_s")),
        "deploy_duration_display": format_release_seconds(build.get("deploy_duration_s")),
        "total_duration_display": format_release_seconds(build.get("total_duration_s")),
    }


def format_deploy_component_data(deploy_data):
    if not deploy_data:
        return None

    deployment = deploy_data.get("deployment") or {}
    profiler = deploy_data.get("profiler") or {}
    carbon = deploy_data.get("carbon") or {}
    snapshots = deploy_data.get("snapshots") or []

    formatted_snapshots = []
    for snapshot in snapshots:
        formatted_snapshots.append(
            {
                "phase": format_optional_text(snapshot.get("phase")),
                "strategy": format_optional_text(snapshot.get("strategy")),
                "infra_multiplier": format_optional_multiplier(snapshot.get("infra_multiplier")),
                "downtime_seconds": format_seconds(snapshot.get("downtime_seconds"))
                if snapshot.get("downtime_seconds") is not None and not pd.isna(snapshot.get("downtime_seconds"))
                else "Not available",
                "canary_weight": format_optional_text(snapshot.get("canary_weight")),
                "note": format_optional_text(snapshot.get("note")),
                "snapshot_timestamp": format_optional_text(snapshot.get("snapshot_timestamp")),
            }
        )

    return {
        **deploy_data,
        "status_display": format_optional_text(deployment.get("status")),
        "strategy_display": format_optional_text(deployment.get("strategy")).replace("_", " ").title(),
        "canary_weight_display": format_optional_text(deployment.get("canary_weight")),
        "carbon_profile_display": format_optional_text(deployment.get("carbon_profile")),
        "image_display": format_optional_text(deployment.get("image")),
        "start_time_display": format_optional_text(deployment.get("start_time")),
        "end_time_display": format_optional_text(deployment.get("end_time")),
        "duration_display": format_optional_minutes(deployment.get("duration_minutes")),
        "avg_cpu_display": format_optional_percent(profiler.get("avg_cpu")),
        "peak_cpu_display": format_optional_percent(profiler.get("peak_cpu")),
        "min_cpu_display": format_optional_percent(profiler.get("min_cpu")),
        "avg_memory_display": format_optional_percent(profiler.get("avg_memory")),
        "peak_memory_display": format_optional_percent(profiler.get("peak_memory")),
        "min_memory_display": format_optional_percent(profiler.get("min_memory")),
        "samples_collected_display": format_optional_count(profiler.get("samples_collected")),
        "total_energy_display": format_optional_kwh(carbon.get("total_energy_kwh")),
        "total_g_co2_display": format_optional_gco2(carbon.get("total_g_co2")),
        "total_kg_co2_display": format_gco2_from_kg(carbon.get("total_kg_co2"))
        if carbon.get("total_kg_co2") is not None and not pd.isna(carbon.get("total_kg_co2"))
        else "Not available",
        "carbon_intensity_display": format_optional_intensity(carbon.get("carbon_intensity_gco2")),
        "intensity_source_display": format_optional_text(carbon.get("intensity_source")),
        "strategy_carbon_profile_display": format_optional_text(carbon.get("strategy_carbon_profile")),
        "infra_multiplier_display": format_optional_multiplier(carbon.get("infra_multiplier")),
        "snapshots_display": formatted_snapshots,
    }


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
    formatted["context_scope_display"] = format_context_scope(anomaly.get("context_scope"))
    formatted["historical_samples_display"] = format_count(anomaly.get("historical_samples", 0))
    formatted["strategy_display"] = format_optional_text(anomaly.get("strategy")).replace("_", " ").title()
    formatted["fallback_reason"] = anomaly.get("fallback_reason", "")
    return formatted


def format_context_scope(scope):
    value = str(scope or "stage").replace("_", " ").strip()
    return value.title() if value else "Stage"

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


def neutralize_health_explanation(explanation):
    """Keep health-score output stage-neutral in lifecycle stage views."""
    if not explanation:
        return "Sustainability health is calculated from existing Monitor metrics for this selected run."

    text = str(explanation)
    lower_text = text.lower()
    if "build" in lower_text or "test" in lower_text:
        return (
            "Sustainability health is calculated from existing Monitor metrics for this selected run. "
            "Some prototype-stage details are hidden until the final Release, Deploy, and Operate integration data is available."
        )
    return text


def format_skip_reason(reason):
    normalized = str(reason or "").strip()
    if not normalized:
        return ""
    return normalized.replace("_", " ").title()


def summarize_stage_status(values):
    statuses = [str(value or "").strip().lower() for value in values]
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "aborted" for status in statuses):
        return "aborted"
    if any(status in {"cancelled", "canceled"} for status in statuses):
        return "cancelled"
    if any(status == "success" for status in statuses):
        return "success"
    return next((status for status in statuses if status), "unknown")


def build_stage_anomaly_summary(stat_alerts, ml_items):
    critical_count = sum(1 for alert in stat_alerts if alert.get("severity") == "critical")
    warning_count = sum(1 for alert in stat_alerts if alert.get("severity") == "warning")
    critical_count += sum(1 for item in ml_items if item.get("severity") == "critical")
    warning_count += sum(1 for item in ml_items if item.get("severity") == "warning")
    return {
        "critical_count": critical_count,
        "warning_count": warning_count,
        "overall_status": "Critical" if critical_count else ("Warning" if warning_count else "Normal"),
    }


def build_stage_baseline_context(stage_key, stat_alerts, ml_items):
    source = next((item for item in ml_items if item.get("context_scope")), None)
    if source is None:
        source = next((item for item in stat_alerts if item.get("context_scope")), None)
    if source is None:
        return {
            "label": stage_key.replace("_", " ").title(),
            "context_scope": "Stage",
            "strategy_display": "Not available",
            "historical_samples": 0,
            "historical_samples_display": "0",
            "strategy_specific": False,
            "fallback_reason": "",
        }

    strategy = format_optional_text(source.get("strategy")).replace("_", " ").title()
    scope = format_context_scope(source.get("context_scope"))
    samples = source.get("historical_samples", 0)
    label = stage_key.replace("_", " ").title()
    if source.get("strategy_specific") and strategy != "Not available" and strategy.lower() != "missing":
        label = f"{label} / {strategy}"
    return {
        "label": label,
        "context_scope": scope,
        "strategy_display": strategy,
        "historical_samples": int(samples or 0),
        "historical_samples_display": format_count(samples or 0),
        "strategy_specific": bool(source.get("strategy_specific", False)),
        "fallback_reason": source.get("fallback_reason", ""),
    }


def build_lifecycle_sections(display_rows, statistical_alerts, ml_results, deploy_component_data=None):
    """Group normalized Monitor lifecycle data under Release, Deploy, and Operate labels."""
    stage_rows = display_rows.to_dict(orient="records") if display_rows is not None and not display_rows.empty else []
    stage_lookup = {str(row.get("stage", "")).lower(): row for row in stage_rows}

    statistical_by_stage = {}
    for alert in statistical_alerts or []:
        statistical_by_stage.setdefault(str(alert.get("stage", "")).lower(), []).append(alert)

    ml_by_stage = {}
    for result in ml_results or []:
        ml_by_stage.setdefault(str(result.get("stage", "")).lower(), []).append(result)

    lifecycle_definitions = [
        {
            "key": "release",
            "label": "Release",
            "icon": "package-check",
            "source_stages": ["release"],
            "note": "Current monitor data from Release lifecycle records.",
        },
        {
            "key": "deploy",
            "label": "Deploy",
            "icon": "rocket",
            "source_stages": ["deploy"],
            "note": "Current monitor data from Deploy lifecycle records.",
        },
        {
            "key": "operate",
            "label": "Operate",
            "icon": "activity",
            "source_stages": ["operate"],
            "note": "Current monitor data from Operate observation windows.",
        },
    ]

    sections = []
    for definition in lifecycle_definitions:
        rows = [stage_lookup[stage] for stage in definition["source_stages"] if stage in stage_lookup]
        row_stage_names = {str(row.get("stage", "")).lower() for row in rows}
        stat_alerts = [
            alert
            for stage_name in row_stage_names
            for alert in statistical_by_stage.get(stage_name, [])
            if normalize_display_severity(alert.get("severity")) in {"critical", "warning"}
        ]
        ml_items = [
            item
            for stage_name in row_stage_names
            for item in ml_by_stage.get(stage_name, [])
        ]
        stage_anomaly_summary = build_stage_anomaly_summary(stat_alerts, ml_items)
        critical_count = stage_anomaly_summary["critical_count"]
        warning_count = stage_anomaly_summary["warning_count"]
        skipped_rows = [row for row in rows if normalize_skipped_value(row.get("skipped"))]
        all_skipped = bool(rows) and len(skipped_rows) == len(rows)
        total_energy = sum(float(row.get("total_energy_kwh") or 0.0) for row in rows if not normalize_skipped_value(row.get("skipped")))
        total_carbon = sum(float(row.get("total_carbon_kg") or 0.0) for row in rows if not normalize_skipped_value(row.get("skipped")))
        workload_duration = sum(float(row.get("workload_duration_seconds") or 0.0) for row in rows if not normalize_skipped_value(row.get("skipped")))
        failed = any(str(row.get("status", "")).lower() == "failed" for row in rows)
        skip_reason = next((str(row.get("skip_reason_display") or "") for row in skipped_rows if row.get("skip_reason_display")), "")
        if all_skipped:
            summary_status = "Skipped"
            summary_text = f"Reason: {skip_reason}" if skip_reason else "Lifecycle workload intentionally skipped."
        else:
            summary_status = "Failed" if failed else ("Available" if rows else "Not available")
            summary_text = (
                f"Monitor data available, {format_seconds(workload_duration)} workload duration."
                if rows
                else "Awaiting integrated Monitor data."
            )

        section_deploy_data = deploy_component_data if definition["key"] == "deploy" and not all_skipped else None
        if definition["key"] == "deploy" and not rows and section_deploy_data:
            summary_status = "Available"
            summary_text = "Deploy component data available for this Jenkins run."

        sections.append(
            {
                **definition,
                "rows": rows,
                "statistical_alerts": stat_alerts,
                "ml_results": ml_items,
                "critical_count": critical_count,
                "warning_count": warning_count,
                "anomaly_summary": stage_anomaly_summary,
                "baseline_context": build_stage_baseline_context(definition["key"], stat_alerts, ml_items),
                "has_data": bool(rows),
                "skipped": all_skipped,
                "skip_reason_display": skip_reason,
                "deploy_data": section_deploy_data,
                "deploy_data_missing": definition["key"] == "deploy" and bool(rows) and not all_skipped and not section_deploy_data,
                "summary_status": summary_status,
                "summary_text": summary_text,
                "energy_display": "Not applicable" if all_skipped else (format_kwh(total_energy) if rows else "Not available"),
                "carbon_display": "Not applicable" if all_skipped else (format_gco2_from_kg(total_carbon) if rows else "Not available"),
            }
        )

    return sections


LEGACY_HTML = """
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
                            <p class="text-[11px] text-slate-500 mt-2">{{ health_explanation_display }}</p>
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
                                        <span class="text-xs text-slate-500">Release / Deploy / Operate</span>
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
                                            <td class="px-6 py-4 text-slate-200 font-semibold">Lifecycle stage</td>
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
            color: var(--text);
            min-height: 100vh;
        }

        .glass-panel {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 1rem;
            box-shadow: var(--shadow);
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

        .sidebar-scroll::-webkit-scrollbar { width: 4px; }
        .sidebar-scroll::-webkit-scrollbar-track { background: #f8fafc; }
        .sidebar-scroll::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 10px; }

        .status-pulse {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.55); }
            70% { box-shadow: 0 0 0 10px rgba(34, 197, 94, 0); }
            100% { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0); }
        }

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

        .modal-overlay.is-open { display: flex; }

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
        <header class="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-4 mb-6">
            <div>
                <div class="flex items-center gap-3">
                    <div class="p-2 bg-emerald-100 rounded-lg">
                        <i data-lucide="leaf" class="text-emerald-600 w-8 h-8"></i>
                    </div>
                    <div>
                        <h1 class="text-3xl font-extrabold tracking-tight text-slate-900">Green DevOps Monitor</h1>
                        <p class="text-sm text-slate-600 font-medium">Centralized sustainability monitoring for the Release, Deploy, and Operate lifecycle</p>
                    </div>
                </div>
            </div>

            <div class="flex flex-wrap gap-2 items-center">
                <a href="{{ refresh_url }}" class="inline-flex items-center gap-2 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-2 text-sm font-semibold text-emerald-700 hover:bg-emerald-100">
                    <i data-lucide="refresh-cw" class="w-4 h-4"></i>
                    Refresh Data
                </a>
                <div class="glass-panel px-4 py-2 flex items-center gap-2">
                    <span class="status-pulse bg-emerald-500"></span>
                    <span class="text-sm font-semibold text-slate-700">{{ data_source }}</span>
                </div>
            </div>
        </header>

        <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">
            <aside class="lg:col-span-3 flex flex-col gap-4 sidebar-sticky">
                <nav class="glass-panel p-4">
                    <p class="text-xs font-bold uppercase tracking-[0.2em] text-slate-500 mb-3">Navigation</p>
                    <div class="space-y-2 text-sm font-semibold">
                        <a href="#home" class="flex items-center gap-2 rounded-lg px-3 py-2 text-slate-700 hover:bg-slate-50"><i data-lucide="home" class="w-4 h-4"></i>Home</a>
                        <a href="#runs" class="flex items-center gap-2 rounded-lg px-3 py-2 text-slate-700 hover:bg-slate-50"><i data-lucide="history" class="w-4 h-4"></i>Runs</a>
                        <a href="#run-overview" class="flex items-center gap-2 rounded-lg px-3 py-2 text-slate-700 hover:bg-slate-50"><i data-lucide="layout-dashboard" class="w-4 h-4"></i>Run Overview</a>
                        <a href="#lifecycle" class="flex items-center gap-2 rounded-lg px-3 py-2 text-slate-700 hover:bg-slate-50"><i data-lucide="workflow" class="w-4 h-4"></i>Release | Deploy | Operate</a>
                    </div>
                </nav>

                <section id="runs" class="glass-panel p-4 flex-1 flex flex-col overflow-hidden">
                    <div class="flex items-center justify-between mb-4">
                        <h2 class="text-xs font-bold uppercase tracking-[0.2em] text-slate-500">Recent Runs</h2>
                        <i data-lucide="history" class="w-4 h-4 text-slate-500"></i>
                    </div>
                    <div class="sidebar-scroll overflow-y-auto space-y-2 pr-2">
                        {% for run in runs %}
                        <a href="/?run_id={{ run.run_id }}" class="block p-3 rounded-xl border border-slate-200 transition-all hover:border-emerald-200 hover:bg-emerald-50/60 {% if run.run_id == selected_run %}nav-item-active{% endif %}">
                            <div class="flex justify-between items-start gap-3">
                                <span class="text-sm font-bold text-slate-800 truncate">#{{ run.run_id }}</span>
                                <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {% if run.status == 'success' %}bg-emerald-100 text-emerald-700{% else %}bg-rose-100 text-rose-700{% endif %}">{{ run.status }}</span>
                            </div>
                            <div class="grid grid-cols-2 gap-2 text-[11px] text-slate-500 font-medium mt-2">
                                <span class="flex items-center gap-1"><i data-lucide="zap" class="w-3 h-3"></i>{{ run.total_energy_display }}</span>
                                <span class="flex items-center gap-1"><i data-lucide="clock" class="w-3 h-3"></i>{{ run.duration_display }}</span>
                            </div>
                        </a>
                        {% endfor %}
                    </div>
                </section>
            </aside>

            <main class="lg:col-span-9 space-y-8">
                <section id="home" class="space-y-5">
                    <div>
                        <p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600 mb-2">Home</p>
                        <h2 class="text-2xl font-extrabold text-slate-900">System sustainability overview</h2>
                        <p class="text-sm text-slate-600 mt-2">A monitor-level view of total sustainability impact and the latest pipeline activity.</p>
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Overall Energy</p>
                            <p class="text-3xl font-black text-emerald-600 mt-2">{{ system_total_energy_display }}</p>
                        </div>
                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Overall Carbon</p>
                            <p class="text-3xl font-black text-sky-600 mt-2">{{ system_total_carbon_display }}</p>
                        </div>
                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Sustainability Health</p>
                            <p class="text-3xl font-black text-slate-900 mt-2">{{ health_score.score }}<span class="text-sm text-slate-500">/100</span></p>
                            <p class="text-sm font-semibold text-emerald-700">{{ health_score.grade }} for selected run</p>
                        </div>
                        <div class="glass-panel p-5">
                            <p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Pipeline Runs</p>
                            <p class="text-3xl font-black text-slate-900 mt-2">{{ system_run_count }}</p>
                            <p class="text-sm text-slate-500">Stored in {{ data_source }}</p>
                        </div>
                    </div>

                    <div class="grid grid-cols-1 xl:grid-cols-3 gap-6">
                        <div class="xl:col-span-2 glass-panel p-6">
                            <div class="flex items-center justify-between mb-5">
                                <h3 class="text-sm font-bold text-slate-800 uppercase tracking-wider">Sustainability Trends by Stage</h3>
                                <i data-lucide="bar-chart-3" class="w-4 h-4 text-emerald-600"></i>
                            </div>
                            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                <div class="h-[240px]"><canvas id="energyChart"></canvas></div>
                                <div class="h-[240px]"><canvas id="cpuChart"></canvas></div>
                            </div>
                        </div>

                        <div class="glass-panel p-6">
                            <h3 class="text-sm font-bold text-slate-800 uppercase tracking-wider mb-4">Latest Activity</h3>
                            <div class="space-y-3">
                                {% for run in recent_runs %}
                                <a href="/?run_id={{ run.run_id }}" class="block rounded-xl border border-slate-200 p-3 hover:bg-slate-50">
                                    <div class="flex items-center justify-between gap-3">
                                        <span class="text-sm font-bold text-slate-800 truncate">#{{ run.run_id }}</span>
                                        <span class="text-[10px] font-bold uppercase {% if run.status == 'success' %}text-emerald-700{% else %}text-rose-700{% endif %}">{{ run.status }}</span>
                                    </div>
                                    <p class="text-xs text-slate-500 mt-1">{{ run.total_energy_display }} | {{ run.total_carbon_display }}</p>
                                </a>
                                {% endfor %}
                            </div>
                        </div>
                    </div>
                </section>

                <section id="run-overview" class="glass-panel overflow-hidden">
                    <div class="px-6 py-5 border-b border-slate-200 bg-slate-50">
                        <p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600 mb-2">Run Overview</p>
                        <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
                            <div>
                                <h2 class="text-2xl font-extrabold text-slate-900">Run {{ selected_run }}</h2>
                                <p class="text-sm text-slate-600 mt-1">{{ pipeline_insight }}</p>
                            </div>
                            <span class="self-start text-xs px-3 py-1 rounded-full font-bold uppercase {% if selected_run_status == 'success' %}bg-emerald-100 text-emerald-700{% else %}bg-rose-100 text-rose-700{% endif %}">{{ selected_run_status }}</span>
                        </div>
                    </div>

                    <div class="p-6 space-y-6">
                        <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-4">
                            <div>
                                <p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Duration</p>
                                <p class="text-xl font-black text-slate-900 mt-1">{{ selected_run_duration_display }}</p>
                            </div>
                            <div>
                                <p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Total Energy</p>
                                <p class="text-xl font-black text-emerald-600 mt-1">{{ total_energy_display }}</p>
                            </div>
                            <div>
                                <p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Total Carbon</p>
                                <p class="text-xl font-black text-sky-600 mt-1">{{ total_carbon_display }}</p>
                            </div>
                            <div>
                                <p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Health Score</p>
                                <p class="text-xl font-black text-slate-900 mt-1">{{ health_score.score }}/100</p>
                            </div>
                            <div>
                                <p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Alerts</p>
                                <div class="flex flex-wrap gap-2 mt-2">
                                    <button type="button" onclick="openAlertModal('critical-alerts-modal')" class="rounded-full bg-rose-50 px-3 py-1 text-xs font-bold text-rose-700">{{ anomaly_summary.critical_count }} Critical</button>
                                    <button type="button" onclick="openAlertModal('warning-alerts-modal')" class="rounded-full bg-amber-50 px-3 py-1 text-xs font-bold text-amber-700">{{ anomaly_summary.warning_count }} Warning</button>
                                </div>
                            </div>
                        </div>

                        <div id="lifecycle" class="grid grid-cols-1 md:grid-cols-3 gap-4">
                            {% for lifecycle in lifecycle_sections %}
                            <a href="#stage-{{ lifecycle.key }}" class="rounded-xl border border-slate-200 p-4 hover:border-emerald-300 hover:bg-emerald-50/40 transition-colors">
                                <div class="flex items-center justify-between">
                                    <div class="flex items-center gap-2">
                                        <i data-lucide="{{ lifecycle.icon }}" class="w-5 h-5 text-emerald-600"></i>
                                        <h3 class="font-extrabold text-slate-900">{{ lifecycle.label }}</h3>
                                    </div>
                                    <span class="text-xs font-bold text-slate-500">{{ lifecycle.rows|length }} monitor stage{{ '' if lifecycle.rows|length == 1 else 's' }}</span>
                                </div>
                                <p class="text-xs text-slate-500 mt-3">{{ lifecycle.note }}</p>
                            </a>
                            {% endfor %}
                        </div>
                    </div>
                </section>

                <section class="space-y-5">
                    <div>
                        <p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600 mb-2">Stage Detail</p>
                        <h2 class="text-2xl font-extrabold text-slate-900">Release | Deploy | Operate</h2>
                        <p class="text-sm text-slate-600 mt-2">This phase displays only existing Monitor metrics. Component-specific PP2 data will be added later.</p>
                    </div>

                    {% for lifecycle in lifecycle_sections %}
                    <section id="stage-{{ lifecycle.key }}" class="glass-panel overflow-hidden">
                        <div class="px-6 py-5 border-b border-slate-200 bg-slate-50 flex flex-col md:flex-row md:items-center md:justify-between gap-3">
                            <div>
                                <p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600">{{ lifecycle.label }}</p>
                                <h3 class="text-xl font-extrabold text-slate-900 mt-1">{{ lifecycle.label }} stage detail</h3>
                                <p class="text-sm text-slate-600 mt-1">{{ lifecycle.note }}</p>
                            </div>
                            <div class="flex flex-wrap gap-2">
                                <span class="rounded-full bg-rose-50 px-3 py-1 text-xs font-bold text-rose-700">{{ lifecycle.critical_count }} Critical</span>
                                <span class="rounded-full bg-amber-50 px-3 py-1 text-xs font-bold text-amber-700">{{ lifecycle.warning_count }} Warning</span>
                            </div>
                        </div>

                        <div class="p-6 space-y-6">
                            {% if lifecycle.rows %}
                            <div class="overflow-x-auto border border-slate-200 rounded-xl">
                                <table class="w-full text-left">
                                    <thead>
                                        <tr class="text-[11px] font-bold text-slate-500 uppercase tracking-wider bg-slate-50">
                                            <th class="px-4 py-3">Monitor Stage</th>
                                            <th class="px-4 py-3">Status</th>
                                            <th class="px-4 py-3">Duration</th>
                                            <th class="px-4 py-3">Overhead</th>
                                            <th class="px-4 py-3">CPU</th>
                                            <th class="px-4 py-3">Energy</th>
                                            <th class="px-4 py-3 text-right">Carbon</th>
                                        </tr>
                                    </thead>
                                    <tbody class="divide-y divide-slate-200">
                                        {% for row in lifecycle.rows %}
                                        <tr>
                                            <td class="px-4 py-3 font-bold text-slate-800">{{ row.stage_label }}</td>
                                            <td class="px-4 py-3"><span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {% if row.status == 'success' %}bg-emerald-100 text-emerald-700{% else %}bg-rose-100 text-rose-700{% endif %}">{{ row.status }}</span></td>
                                            <td class="px-4 py-3 text-slate-700">{{ row.workload_duration_display }}</td>
                                            <td class="px-4 py-3 text-slate-700">{{ row.overhead_display }}</td>
                                            <td class="px-4 py-3 text-slate-700">{{ row.avg_cpu_display }}</td>
                                            <td class="px-4 py-3 font-mono text-sm text-emerald-700">{{ row.total_energy_display }}</td>
                                            <td class="px-4 py-3 text-right font-mono text-sm text-sky-700">{{ row.total_carbon_display }}</td>
                                        </tr>
                                        {% endfor %}
                                    </tbody>
                                </table>
                            </div>
                            {% else %}
                            <div class="rounded-xl border border-slate-200 bg-slate-50 p-5">
                                <p class="text-sm font-semibold text-slate-700">No existing Monitor stage records are currently mapped to {{ lifecycle.label }}.</p>
                            </div>
                            {% endif %}

                            <div class="rounded-xl border border-slate-200 p-5">
                                <div class="flex items-center justify-between gap-4 mb-4">
                                    <div>
                                        <p class="text-xs font-bold uppercase tracking-[0.2em] text-slate-500">Monitor Intelligence</p>
                                        <h4 class="text-lg font-extrabold text-slate-900 mt-1">Statistical and Isolation Forest signals</h4>
                                    </div>
                                    <span class="text-xs font-semibold text-slate-500">{{ ml_anomaly.model }}</span>
                                </div>

                                <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
                                    <div>
                                        <p class="text-sm font-bold text-slate-800 mb-3">Statistical anomaly detection</p>
                                        {% if lifecycle.statistical_alerts %}
                                            <div class="space-y-2">
                                                {% for alert in lifecycle.statistical_alerts %}
                                                <div class="rounded-lg border border-slate-200 p-3">
                                                    <div class="flex items-center justify-between gap-3">
                                                        <span class="text-sm font-semibold text-slate-800">{{ alert.stage_label }} | {{ alert.metric_label }}</span>
                                                        <span class="text-[10px] font-bold uppercase {% if alert.severity == 'critical' %}text-rose-700{% else %}text-amber-700{% endif %}">{{ alert.severity_label }}</span>
                                                    </div>
                                                    <p class="text-xs text-slate-500 mt-1">{{ alert.message }}</p>
                                                </div>
                                                {% endfor %}
                                            </div>
                                        {% else %}
                                            <p class="text-sm text-slate-500">No statistical warning or critical alerts for the mapped Monitor stages.</p>
                                        {% endif %}
                                    </div>

                                    <div>
                                        <p class="text-sm font-bold text-slate-800 mb-3">Isolation Forest</p>
                                        {% if lifecycle.ml_results %}
                                            <div class="space-y-2">
                                                {% for item in lifecycle.ml_results %}
                                                <div class="rounded-lg border border-slate-200 p-3">
                                                    <div class="flex items-center justify-between gap-3">
                                                        <span class="text-sm font-semibold text-slate-800">Stage-specific model | {{ item.prediction }}</span>
                                                        <span class="text-[10px] font-bold uppercase {% if item.severity == 'critical' %}text-rose-700{% elif item.severity == 'warning' %}text-amber-700{% else %}text-emerald-700{% endif %}">{{ item.severity }}</span>
                                                    </div>
                                                    <p class="text-xs text-slate-500 mt-1">{{ item.message }}</p>
                                                    <p class="text-xs text-slate-400 mt-1">Score: {{ item.anomaly_score_display }}</p>
                                                </div>
                                                {% endfor %}
                                            </div>
                                        {% else %}
                                            <p class="text-sm text-slate-500">No Isolation Forest result is available for the mapped Monitor stages.</p>
                                        {% endif %}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </section>
                    {% endfor %}
                </section>

                <section class="glass-panel overflow-hidden">
                    <div class="px-6 py-5 border-b border-slate-200 bg-slate-50">
                        <p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600 mb-2">Baseline Comparison</p>
                        <h2 class="text-xl font-extrabold text-slate-900">Selected run compared with historical behavior</h2>
                    </div>
                    <div class="divide-y divide-slate-200">
                        {% for item in comparison_rows %}
                        <div class="px-6 py-4 flex flex-col md:flex-row md:items-center md:justify-between gap-3">
                            <div>
                                <p class="text-sm font-bold text-slate-800">{{ item.label }}</p>
                                <p class="text-xs text-slate-500">Historical average: {% if item.baseline_display is not none %}{{ item.baseline_display }}{% else %}Unavailable{% endif %}</p>
                            </div>
                            <div class="md:text-right">
                                <p class="text-sm font-bold text-slate-900">{{ item.current_display }}</p>
                                <p class="text-xs font-semibold {% if item.change_label == 'Baseline unavailable' %}text-slate-500{% elif item.is_above %}text-amber-700{% else %}text-emerald-700{% endif %}">{{ item.change_label }}</p>
                            </div>
                        </div>
                        {% endfor %}
                    </div>
                </section>
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
            plugins: { legend: { display: true, labels: { color: tickColor } } },
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
                        label: "Workload Energy",
                        data: workloadEnergy,
                        backgroundColor: "rgba(16, 185, 129, 0.62)",
                        borderColor: "rgba(16, 185, 129, 1)",
                        borderWidth: 1,
                        borderRadius: 6
                    },
                    {
                        label: "Full Stage Estimated Energy",
                        data: fullStageEnergy,
                        backgroundColor: "rgba(14, 165, 233, 0.55)",
                        borderColor: "rgba(14, 165, 233, 1)",
                        borderWidth: 1,
                        borderRadius: 6
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
                    backgroundColor: "rgba(245, 158, 11, 0.12)",
                    fill: true,
                    tension: 0.35,
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
                <button type="button" onclick="closeAlertModal('critical-alerts-modal')" class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-semibold text-slate-600 hover:bg-slate-50">Close</button>
            </div>
            <div class="modal-scroll px-6 py-5">
                {% if critical_alerts %}
                    <div class="space-y-4">
                        {% for alert in critical_alerts %}
                        <div class="rounded-xl border border-rose-200 bg-rose-50 p-4">
                            <div class="flex flex-wrap items-center gap-2 mb-3">
                                <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {{ alert.severity_badge_class }}">{{ alert.severity_label }}</span>
                                <span class="text-sm font-semibold text-slate-800">{{ alert.metric_label }}</span>
                                <span class="text-sm text-slate-500">Stage: {{ alert.stage_label }}</span>
                                <span class="text-sm text-slate-500">Source: {{ alert.source }}</span>
                            </div>
                            <p class="text-sm text-slate-600">{{ alert.message }}</p>
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
                <button type="button" onclick="closeAlertModal('warning-alerts-modal')" class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-semibold text-slate-600 hover:bg-slate-50">Close</button>
            </div>
            <div class="modal-scroll px-6 py-5">
                {% if warning_alerts %}
                    <div class="space-y-4">
                        {% for alert in warning_alerts %}
                        <div class="rounded-xl border border-amber-200 bg-amber-50 p-4">
                            <div class="flex flex-wrap items-center gap-2 mb-3">
                                <span class="text-[10px] px-2 py-0.5 rounded-full font-bold uppercase {{ alert.severity_badge_class }}">{{ alert.severity_label }}</span>
                                <span class="text-sm font-semibold text-slate-800">{{ alert.metric_label }}</span>
                                <span class="text-sm text-slate-500">Stage: {{ alert.stage_label }}</span>
                                <span class="text-sm text-slate-500">Source: {{ alert.source }}</span>
                            </div>
                            <p class="text-sm text-slate-600">{{ alert.message }}</p>
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


APP_HTML = """
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
        body { font-family: 'Plus Jakarta Sans', sans-serif; background: radial-gradient(circle at 85% 8%, rgba(16, 185, 129, 0.12), transparent 30%), radial-gradient(circle at 12% 92%, rgba(14, 165, 233, 0.10), transparent 34%), linear-gradient(180deg, #f8fafc 0%, #eef4f1 100%); color: #0f172a; min-height: 100vh; }
        .panel { background: rgba(255, 255, 255, 0.95); border: 1px solid rgba(148, 163, 184, 0.30); border-radius: 1rem; box-shadow: 0 16px 42px rgba(15, 23, 42, 0.10); backdrop-filter: blur(10px); transition: transform .18s ease, box-shadow .18s ease, border-color .18s ease; }
        .panel:hover { border-color: rgba(16, 185, 129, 0.22); box-shadow: 0 20px 48px rgba(15, 23, 42, 0.12); }
        .stage-hero { background: linear-gradient(135deg, rgba(255,255,255,.96), rgba(240,253,244,.94)); border: 1px solid rgba(148, 163, 184, 0.30); border-radius: 1.125rem; box-shadow: 0 18px 46px rgba(15, 23, 42, 0.10); padding: 1.5rem; }
        .section-title { display: flex; align-items: center; gap: .75rem; }
        .section-icon { width: 2.25rem; height: 2.25rem; display: inline-flex; align-items: center; justify-content: center; border-radius: .8rem; background: #ecfdf5; color: #059669; }
        .console-chip { border: 1px solid rgba(148, 163, 184, 0.35); background: rgba(248, 250, 252, 0.88); }
        .deploy-kpi-grid { display: grid; grid-template-columns: repeat(1, minmax(0, 1fr)); gap: 1rem; }
        @media (min-width: 768px) { .deploy-kpi-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
        @media (min-width: 1280px) { .deploy-kpi-grid { grid-template-columns: repeat(5, minmax(0, 1fr)); } }
        .kpi-card { border: 1px solid rgba(148, 163, 184, 0.30); background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%); border-radius: .875rem; padding: 1rem; min-height: 132px; display: flex; flex-direction: column; justify-content: space-between; box-shadow: inset 0 1px 0 rgba(255,255,255,.9); }
        .kpi-icon { width: 2rem; height: 2rem; display: inline-flex; align-items: center; justify-content: center; border-radius: .75rem; }
        .kpi-label { font-size: .68rem; line-height: 1rem; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; color: #64748b; }
        .kpi-value { font-size: clamp(1.05rem, 1.45vw, 1.45rem); line-height: 1.2; font-weight: 900; color: #0f172a; overflow-wrap: anywhere; }
        .detail-card { border: 1px solid rgba(148, 163, 184, 0.30); background: rgba(255,255,255,.82); border-radius: .9rem; padding: 1rem; }
        .fact-label { font-size: .68rem; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; color: #64748b; }
        .fact-value { font-size: .9rem; font-weight: 800; color: #0f172a; overflow-wrap: anywhere; }
        .code-pill { display: inline-flex; max-width: 100%; border-radius: .65rem; background: #0f172a; color: #e2e8f0; padding: .45rem .65rem; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .75rem; overflow-wrap: anywhere; }
        .status-success { color: #047857; background: #d1fae5; }
        .status-skipped { color: #b45309; background: #fef3c7; }
        .status-failed { color: #be123c; background: #ffe4e6; }
        .status-cancelled, .status-aborted { color: #475569; background: #e2e8f0; }
        .metric-bar-track { height: .65rem; border-radius: 999px; background: #e2e8f0; overflow: hidden; box-shadow: inset 0 1px 2px rgba(15,23,42,.10); }
        .metric-bar-fill { height: 100%; border-radius: 999px; background: linear-gradient(90deg, #10b981, #0ea5e9); }
        .metric-bar-fill.cpu { background: linear-gradient(90deg, #06b6d4, #0284c7); }
        .metric-bar-fill.memory { background: linear-gradient(90deg, #10b981, #059669); }
        .timeline-line { width: 2px; min-height: 2rem; background: linear-gradient(180deg, #10b981, #38bdf8); margin-left: .42rem; }
        .status-pulse { width: 8px; height: 8px; border-radius: 50%; display: inline-block; animation: pulse 2s infinite; }
        @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(34, 197, 94, .55); } 70% { box-shadow: 0 0 0 10px rgba(34, 197, 94, 0); } 100% { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0); } }
    </style>
</head>
<body class="p-4 md:p-8">
    <div class="max-w-[1500px] mx-auto space-y-6">
        <header class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
            <a href="/" class="flex items-center gap-3">
                <span class="p-2 bg-emerald-100 rounded-lg"><i data-lucide="leaf" class="w-8 h-8 text-emerald-600"></i></span>
                <span>
                    <span class="block text-3xl font-extrabold tracking-tight text-slate-900">Green DevOps Monitor</span>
                    <span class="block text-sm text-slate-600 font-medium">Centralized sustainability intelligence for Release, Deploy, and Operate</span>
                </span>
            </a>
            <div class="flex flex-wrap items-center gap-2">
                <a href="/" class="rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50">Home</a>
                <a href="/runs" class="rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50">Runs</a>
                <div class="console-chip rounded-xl px-4 py-2 flex items-center gap-2">
                    <span class="status-pulse bg-emerald-500"></span>
                    <span class="text-sm font-semibold text-slate-700">{{ data_source }}</span>
                </div>
            </div>
        </header>

        {% if page == 'home' %}
        <main class="space-y-6">
            <section>
                <p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600">Home</p>
                <h1 class="text-3xl font-extrabold text-slate-900 mt-2">System sustainability overview</h1>
                <p class="text-sm text-slate-600 mt-1">What is happening with the Green DevOps pipeline overall?</p>
            </section>
            <section class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
                <div class="panel p-5"><p class="text-xs font-bold uppercase text-slate-500">Total Energy</p><p class="text-3xl font-black text-emerald-600 mt-2">{{ system_total_energy_display }}</p></div>
                <div class="panel p-5"><p class="text-xs font-bold uppercase text-slate-500">Total Carbon</p><p class="text-3xl font-black text-sky-600 mt-2">{{ system_total_carbon_display }}</p></div>
                <div class="panel p-5"><p class="text-xs font-bold uppercase text-slate-500">Average Sustainability Health</p><p class="text-3xl font-black text-slate-900 mt-2">{{ average_health_score }}<span class="text-sm text-slate-500">/100</span></p></div>
                <div class="panel p-5"><p class="text-xs font-bold uppercase text-slate-500">Total Pipeline Runs</p><p class="text-3xl font-black text-slate-900 mt-2">{{ system_run_count }}</p></div>
            </section>
            <section class="grid grid-cols-1 xl:grid-cols-3 gap-6">
                <div class="xl:col-span-2 panel p-6">
                    <h2 class="text-sm font-bold uppercase tracking-wider text-slate-800 mb-5">Overall Sustainability Trends</h2>
                    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        <div class="h-[250px]"><canvas id="runEnergyChart"></canvas></div>
                        <div class="h-[250px]"><canvas id="runCarbonChart"></canvas></div>
                    </div>
                </div>
                <div class="panel p-6">
                    <div class="flex items-center justify-between mb-4">
                        <h2 class="text-sm font-bold uppercase tracking-wider text-slate-800">Recent Pipeline Runs</h2>
                        <a href="/runs" class="text-xs font-bold text-emerald-700 hover:text-emerald-800">View all</a>
                    </div>
                    <div class="space-y-3">
                        {% for run in recent_runs %}
                        <a href="/run/{{ run.run_id|urlencode }}" class="block rounded-xl border border-slate-200 p-3 hover:bg-slate-50">
                            <div class="flex items-center justify-between gap-3">
                                <span class="text-sm font-bold text-slate-800 truncate">Run #{{ run.run_id }}</span>
                                <span class="text-[10px] font-bold uppercase {% if run.status == 'success' %}text-emerald-700{% else %}text-rose-700{% endif %}">{{ run.status }}</span>
                            </div>
                            <p class="text-xs text-slate-500 mt-1">{{ run.duration_display }} | {{ run.total_energy_display }} | {{ run.total_carbon_display }} | Health {{ run.health_score }}/100</p>
                        </a>
                        {% endfor %}
                    </div>
                </div>
            </section>
        </main>
        {% elif page == 'runs' %}
        <main class="space-y-6">
            <div>
                <a href="/" class="text-sm font-semibold text-emerald-700 hover:text-emerald-800">&larr; Home</a>
                <h1 class="text-3xl font-extrabold text-slate-900 mt-3">Pipeline runs</h1>
                <p class="text-sm text-slate-600 mt-1">Historical pipeline executions from the existing Monitor data source.</p>
            </div>
            <section class="panel overflow-hidden">
                <div class="overflow-x-auto">
                    <table class="w-full text-left">
                        <thead><tr class="text-[11px] uppercase tracking-wider text-slate-500 bg-slate-50">
                            <th class="px-5 py-4">Run ID</th><th class="px-5 py-4">Status</th><th class="px-5 py-4">Start</th><th class="px-5 py-4">End</th><th class="px-5 py-4">Duration</th><th class="px-5 py-4">Energy</th><th class="px-5 py-4">Carbon</th><th class="px-5 py-4">Health</th><th class="px-5 py-4">Alerts</th><th class="px-5 py-4 text-right">Action</th>
                        </tr></thead>
                        <tbody class="divide-y divide-slate-200">
                            {% for run in runs %}
                            <tr>
                                <td class="px-5 py-4 font-bold text-slate-800">#{{ run.run_id }}</td>
                                <td class="px-5 py-4"><span class="text-[10px] px-2 py-1 rounded-full font-bold uppercase {% if run.status == 'success' %}bg-emerald-100 text-emerald-700{% else %}bg-rose-100 text-rose-700{% endif %}">{{ run.status }}</span></td>
                                <td class="px-5 py-4 text-sm text-slate-600">{{ run.start_time_display }}</td>
                                <td class="px-5 py-4 text-sm text-slate-600">{{ run.end_time_display }}</td>
                                <td class="px-5 py-4 text-sm text-slate-700">{{ run.duration_display }}</td>
                                <td class="px-5 py-4 text-sm font-mono text-emerald-700">{{ run.total_energy_display }}</td>
                                <td class="px-5 py-4 text-sm font-mono text-sky-700">{{ run.total_carbon_display }}</td>
                                <td class="px-5 py-4 text-sm font-bold text-slate-800">{{ run.health_score }}/100</td>
                                <td class="px-5 py-4 text-sm text-slate-700">{{ run.alert_count }}</td>
                                <td class="px-5 py-4 text-right"><a href="/run/{{ run.run_id|urlencode }}" class="inline-flex rounded-lg bg-emerald-600 px-3 py-2 text-xs font-bold text-white hover:bg-emerald-700">View</a></td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </section>
        </main>
        {% elif page == 'run' %}
        <main class="space-y-6">
            <div>
                <a href="/runs" class="text-sm font-semibold text-emerald-700 hover:text-emerald-800">&larr; Back to Runs</a>
                <div class="flex flex-col md:flex-row md:items-start md:justify-between gap-3 mt-3">
                    <div><p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600">Run Overview</p><h1 class="text-3xl font-extrabold text-slate-900 mt-1">Run #{{ selected_run }}</h1><p class="text-sm text-slate-600 mt-1">{{ pipeline_name }}</p></div>
                    <span class="self-start text-xs px-3 py-1 rounded-full font-bold uppercase {% if selected_run_status == 'success' %}bg-emerald-100 text-emerald-700{% else %}bg-rose-100 text-rose-700{% endif %}">{{ selected_run_status }}</span>
                </div>
            </div>
            <section class="panel p-6">
                <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-5">
                    <div><p class="text-xs font-bold uppercase text-slate-500">Start Time</p><p class="text-sm font-semibold text-slate-800 mt-1">{{ selected_run_start }}</p></div>
                    <div><p class="text-xs font-bold uppercase text-slate-500">End Time</p><p class="text-sm font-semibold text-slate-800 mt-1">{{ selected_run_end }}</p></div>
                    <div><p class="text-xs font-bold uppercase text-slate-500">Duration</p><p class="text-xl font-black text-slate-900 mt-1">{{ selected_run_duration_display }}</p></div>
                    <div><p class="text-xs font-bold uppercase text-slate-500">Pipeline Status</p><p class="text-xl font-black text-slate-900 mt-1">{{ selected_run_status|title }}</p></div>
                    <div><p class="text-xs font-bold uppercase text-slate-500">Total Energy</p><p class="text-xl font-black text-emerald-600 mt-1">{{ total_energy_display }}</p></div>
                    <div><p class="text-xs font-bold uppercase text-slate-500">Total Carbon</p><p class="text-xl font-black text-sky-600 mt-1">{{ total_carbon_display }}</p></div>
                    <div><p class="text-xs font-bold uppercase text-slate-500">Warnings</p><p class="text-xl font-black text-amber-600 mt-1">{{ anomaly_summary.warning_count }}</p></div>
                    <div><p class="text-xs font-bold uppercase text-slate-500">Critical</p><p class="text-xl font-black text-rose-600 mt-1">{{ anomaly_summary.critical_count }}</p></div>
                </div>
            </section>
            <section class="panel p-6">
                <div class="section-title"><span class="section-icon"><i data-lucide="package-check" class="w-5 h-5"></i></span><div><h2 class="text-lg font-extrabold text-slate-900">Release Intelligence</h2><p class="text-sm text-slate-500">Release decision context matched to this Monitor run.</p></div></div>
                {% if release_build_data %}
                <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mt-5">
                    <div class="detail-card"><p class="fact-label">Green Probability</p><p class="text-2xl font-black text-emerald-700 mt-2">{{ release_build_data.green_probability_display }}</p></div>
                    <div class="detail-card"><p class="fact-label">Scheduling Action</p><p class="fact-value mt-2">{{ release_build_data.scheduling_action_display }}</p></div>
                    <div class="detail-card"><p class="fact-label">Scheduling Engine</p><p class="fact-value mt-2">{{ release_build_data.scheduling_engine_display }}</p></div>
                    <div class="detail-card"><p class="fact-label">Pipeline Type</p><p class="fact-value mt-2">{{ release_build_data.pipeline_type_display }}</p></div>
                    <div class="detail-card"><p class="fact-label">Release Carbon Intensity</p><p class="fact-value mt-2">{{ release_build_data.carbon_intensity_display }}</p></div>
                    <div class="detail-card"><p class="fact-label">Tests Executed</p><p class="fact-value mt-2">{{ release_build_data.tests_executed_display }}</p></div>
                    <div class="detail-card"><p class="fact-label">Tests Skipped</p><p class="fact-value mt-2">{{ release_build_data.tests_skipped_display }}</p></div>
                    <div class="detail-card"><p class="fact-label">Release Status</p><span class="mt-2 inline-flex rounded-full px-3 py-1 text-xs font-black uppercase {% if release_build_data.status_display == 'SUCCESS' %}status-success{% elif release_build_data.status_display in ['ABORTED', 'CANCELLED', 'CANCELED'] %}status-cancelled{% elif release_build_data.status_display == 'Not available' %}bg-slate-100 text-slate-600{% else %}status-failed{% endif %}">{{ release_build_data.status_display }}</span></div>
                    <div class="detail-card md:col-span-2"><p class="fact-label">Affected Modules</p><p class="fact-value mt-2">{{ release_build_data.affected_modules_display }}</p></div>
                    <div class="detail-card md:col-span-2"><p class="fact-label">Release Durations</p><p class="text-sm font-semibold text-slate-700 mt-2">Build {{ release_build_data.build_duration_display }} | Test {{ release_build_data.test_duration_display }} | Deploy {{ release_build_data.deploy_duration_display }} | Total {{ release_build_data.total_duration_display }}</p></div>
                </div>
                {% else %}
                <p class="mt-5 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-semibold text-slate-600">Release data unavailable for this run. Monitor measurements and intelligence remain available.</p>
                {% endif %}
            </section>
            <section class="panel p-6">
                <div class="section-title"><span class="section-icon"><i data-lucide="shield-check" class="w-5 h-5"></i></span><div><h2 class="text-lg font-extrabold text-slate-900">Pipeline Sustainability Health</h2><p class="text-sm text-slate-500">Calculated across the complete pipeline run.</p></div></div>
                <div class="mt-5 rounded-2xl border border-emerald-200 bg-gradient-to-br from-emerald-50 to-white p-5">
                    <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-5">
                        <div>
                            <p class="text-xs font-black uppercase tracking-[0.18em] text-emerald-700">Whole Pipeline Health</p>
                            <p class="text-5xl font-black text-slate-950 mt-3">{{ health_score.score }}<span class="text-lg text-slate-500">/100</span></p>
                            <div class="flex flex-wrap items-center gap-2 mt-3">
                                <span class="rounded-full bg-emerald-100 px-3 py-1 text-xs font-black uppercase text-emerald-700">{{ health_score.grade }}</span>
                                <span class="rounded-full px-3 py-1 text-xs font-black uppercase {% if health_score.status == 'Critical' %}status-failed{% elif health_score.status == 'Warning' %}status-skipped{% else %}status-success{% endif %}">{{ health_score.status }}</span>
                            </div>
                        </div>
                        <div class="max-w-2xl">
                            <p class="text-sm font-extrabold text-slate-900">Run-level sustainability signal</p>
                            <p class="text-sm text-slate-600 mt-2">{{ health_explanation_display }}</p>
                        </div>
                    </div>
                </div>
            </section>
            <section class="space-y-4">
                <div><p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600">Select a Stage</p><h2 class="text-2xl font-extrabold text-slate-900 mt-1">Release | Deploy | Operate</h2></div>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {% for stage in lifecycle_sections %}
                    <div class="panel p-5">
                        <div class="flex items-center justify-between">
                            <div class="flex items-center gap-2"><i data-lucide="{{ stage.icon }}" class="w-5 h-5 text-emerald-600"></i><h3 class="font-extrabold text-slate-900">{{ stage.label }}</h3></div>
                            <span class="text-xs font-semibold {% if stage.skipped %}text-amber-700{% else %}text-slate-500{% endif %}">{{ stage.summary_status }}</span>
                        </div>
                        <p class="text-xs text-slate-500 mt-3">{{ stage.summary_text }}</p>
                        <div class="grid grid-cols-2 gap-3 mt-4 text-sm">
                            <div><p class="text-xs uppercase font-bold text-slate-500">Energy</p><p class="font-bold text-slate-800">{{ stage.energy_display }}</p></div>
                            <div><p class="text-xs uppercase font-bold text-slate-500">Carbon</p><p class="font-bold text-slate-800">{{ stage.carbon_display }}</p></div>
                        </div>
                        {% if stage.key == 'deploy' and stage.deploy_data and not stage.skipped %}
                        <div class="mt-4 border-t border-slate-200 pt-4">
                            <p class="text-xs uppercase font-bold text-slate-500">Deploy Component</p>
                            <div class="grid grid-cols-2 gap-3 mt-3 text-sm">
                                <div><p class="text-xs uppercase font-bold text-slate-500">Status</p><p class="font-bold text-slate-800">{{ stage.deploy_data.status_display }}</p></div>
                                <div><p class="text-xs uppercase font-bold text-slate-500">Strategy</p><p class="font-bold text-slate-800">{{ stage.deploy_data.strategy_display }}</p></div>
                                <div><p class="text-xs uppercase font-bold text-slate-500">Carbon Profile</p><p class="font-bold text-slate-800">{{ stage.deploy_data.carbon_profile_display }}</p></div>
                                <div><p class="text-xs uppercase font-bold text-slate-500">Duration</p><p class="font-bold text-slate-800">{{ stage.deploy_data.duration_display }}</p></div>
                                <div class="col-span-2"><p class="text-xs uppercase font-bold text-slate-500">Image</p><p class="font-bold text-slate-800 break-all">{{ stage.deploy_data.image_display }}</p></div>
                            </div>
                        </div>
                        {% elif stage.key == 'deploy' and stage.deploy_data_missing %}
                        <p class="text-xs text-slate-500 mt-4">Deploy component data unavailable for this run.</p>
                        {% endif %}
                        <a href="/run/{{ selected_run|urlencode }}/{{ stage.key }}" class="mt-5 inline-flex items-center gap-2 rounded-lg bg-emerald-600 px-3 py-2 text-xs font-bold text-white hover:bg-emerald-700">View Stage <i data-lucide="arrow-right" class="w-3 h-3"></i></a>
                    </div>
                    {% endfor %}
                </div>
            </section>
        </main>
        {% elif page == 'stage' %}
        <main class="space-y-6">
            <section class="stage-hero">
                <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-5">
                    <div>
                        <a href="/run/{{ selected_run|urlencode }}" class="inline-flex items-center gap-2 text-sm font-bold text-emerald-700 hover:text-emerald-800">&larr; Back to Run #{{ selected_run }}</a>
                        <p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600 mt-5">Stage Detail</p>
                        <h1 class="text-4xl md:text-5xl font-black text-slate-950 mt-1">{{ stage_detail.label }}</h1>
                        <p class="text-sm md:text-base text-slate-600 mt-2 max-w-2xl">{{ stage_detail.note }}</p>
                    </div>
                    <div class="flex items-center gap-3 rounded-2xl border border-emerald-200 bg-white/80 px-4 py-3">
                        <span class="p-3 rounded-xl bg-emerald-100 text-emerald-700"><i data-lucide="{{ stage_detail.icon }}" class="w-6 h-6"></i></span>
                        <div>
                            <p class="text-xs font-bold uppercase tracking-wider text-slate-500">Pipeline</p>
                            <p class="text-sm font-extrabold text-slate-900">{{ pipeline_name }}</p>
                        </div>
                    </div>
                </div>
            </section>
            <section class="panel p-6">
                <div class="section-title"><span class="section-icon"><i data-lucide="gauge" class="w-5 h-5"></i></span><div><h2 class="text-lg font-extrabold text-slate-900">Stage execution information</h2><p class="text-sm text-slate-500">Monitor execution metrics for this lifecycle stage.</p></div></div>
                {% if stage_detail.key == 'deploy' and stage_detail.rows %}
                <div class="deploy-kpi-grid mt-5">
                    {% for row in stage_detail.rows %}
                    {% if (row.skipped or row.status_display in ['ABORTED', 'CANCELLED', 'CANCELED']) and row.skip_reason_display %}
                    <div class="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm font-semibold text-amber-800 md:col-span-3 xl:col-span-5">Reason: {{ row.skip_reason_display }}</div>
                    {% endif %}
                    <div class="kpi-card">
                        <span class="kpi-icon bg-emerald-100 text-emerald-700"><i data-lucide="{% if row.status_display == 'SUCCESS' %}check-circle-2{% elif row.status_display == 'SKIPPED' %}pause-circle{% elif row.status_display in ['ABORTED', 'CANCELLED', 'CANCELED'] %}circle-slash{% else %}alert-triangle{% endif %}" class="w-5 h-5"></i></span>
                        <div><p class="kpi-label">Status</p><span class="mt-2 inline-flex rounded-full px-3 py-1 text-xs font-black uppercase {% if row.status_display == 'SUCCESS' %}status-success{% elif row.status_display == 'SKIPPED' %}status-skipped{% elif row.status_display in ['ABORTED', 'CANCELLED', 'CANCELED'] %}status-cancelled{% else %}status-failed{% endif %}">{{ row.status_display }}</span></div>
                    </div>
                    <div class="kpi-card"><span class="kpi-icon bg-slate-100 text-slate-700"><i data-lucide="clock-3" class="w-5 h-5"></i></span><div><p class="kpi-label">Workload</p><p class="kpi-value mt-2">{{ row.workload_duration_display }}</p></div></div>
                    <div class="kpi-card"><span class="kpi-icon bg-cyan-100 text-cyan-700"><i data-lucide="cpu" class="w-5 h-5"></i></span><div><p class="kpi-label">Avg CPU</p><p class="kpi-value mt-2 text-cyan-800">{{ row.avg_cpu_display }}</p></div></div>
                    <div class="kpi-card"><span class="kpi-icon bg-amber-100 text-amber-700"><i data-lucide="zap" class="w-5 h-5"></i></span><div><p class="kpi-label">Energy</p><p class="kpi-value mt-2 text-emerald-700">{{ row.total_energy_display }}</p></div></div>
                    <div class="kpi-card"><span class="kpi-icon bg-emerald-100 text-emerald-700"><i data-lucide="leaf" class="w-5 h-5"></i></span><div><p class="kpi-label">Carbon</p><p class="kpi-value mt-2 text-sky-700">{{ row.total_carbon_display }}</p></div></div>
                    {% endfor %}
                </div>
                {% elif stage_detail.rows %}
                <div class="overflow-x-auto mt-4"><table class="w-full text-left"><thead><tr class="text-[11px] uppercase tracking-wider text-slate-500 bg-slate-50"><th class="px-4 py-3">Monitor Stage</th><th class="px-4 py-3">Status</th><th class="px-4 py-3">Reason</th><th class="px-4 py-3">Workload</th><th class="px-4 py-3">Full Duration</th><th class="px-4 py-3">Overhead</th><th class="px-4 py-3">CPU</th><th class="px-4 py-3">Energy</th><th class="px-4 py-3">Carbon</th></tr></thead><tbody class="divide-y divide-slate-200">{% for row in stage_detail.rows %}<tr><td class="px-4 py-3 font-bold">{{ row.stage_label }}</td><td class="px-4 py-3 font-bold {% if row.skipped %}text-amber-700{% else %}text-slate-700{% endif %}">{{ row.status_display }}</td><td class="px-4 py-3 text-sm text-slate-600">{{ row.skip_reason_display if row.skipped else '' }}</td><td class="px-4 py-3">{{ row.workload_duration_display }}</td><td class="px-4 py-3">{{ row.full_duration_display }}</td><td class="px-4 py-3">{{ row.overhead_percentage_display }}</td><td class="px-4 py-3">{{ row.avg_cpu_display }}</td><td class="px-4 py-3 font-mono text-emerald-700">{{ row.total_energy_display }}</td><td class="px-4 py-3 font-mono text-sky-700">{{ row.total_carbon_display }}</td></tr>{% endfor %}</tbody></table></div>
                {% elif stage_detail.key == 'deploy' %}
                <p class="text-sm text-slate-500 mt-3">Monitor Deploy data unavailable for this run.</p>
                {% else %}<p class="text-sm text-slate-500 mt-3">Awaiting integrated Monitor data.</p>{% endif %}
            </section>
            <section class="panel p-6">
                <div class="section-title"><span class="section-icon"><i data-lucide="rocket" class="w-5 h-5"></i></span><div><h2 class="text-lg font-extrabold text-slate-900">Component-specific information</h2><p class="text-sm text-slate-500">Deployment metadata and execution context.</p></div></div>
                {% if stage_detail.key == 'deploy' and stage_detail.skipped %}
                <p class="mt-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm font-semibold text-amber-800">Deploy component data is not applied because this Monitor Deploy lifecycle was skipped.</p>
                {% elif stage_detail.key == 'deploy' and stage_detail.deploy_data %}
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-5">
                    <div class="detail-card">
                        <div class="flex items-center justify-between gap-3">
                            <p class="text-sm font-extrabold text-slate-800">Deployment facts</p>
                            <span class="rounded-full px-3 py-1 text-[11px] font-black uppercase {% if stage_detail.deploy_data.status_display == 'SUCCESS' %}status-success{% elif stage_detail.deploy_data.status_display in ['ABORTED', 'CANCELLED', 'CANCELED'] %}status-cancelled{% elif stage_detail.deploy_data.status_display == 'Not available' %}bg-slate-100 text-slate-600{% else %}status-failed{% endif %}">{{ stage_detail.deploy_data.status_display }}</span>
                        </div>
                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 mt-4">
                            <div><p class="fact-label">Strategy</p><p class="fact-value inline-flex items-center gap-2"><i data-lucide="git-branch" class="w-4 h-4 text-slate-500"></i>{{ stage_detail.deploy_data.strategy_display }}</p></div>
                            {% if stage_detail.deploy_data.canary_weight_display != 'Not available' %}
                            <div><p class="fact-label">Canary Weight</p><p class="fact-value"><span class="rounded-full bg-amber-100 px-2 py-1 text-xs text-amber-800">{{ stage_detail.deploy_data.canary_weight_display }}</span></p></div>
                            {% endif %}
                            <div><p class="fact-label">Carbon Profile</p><p class="fact-value inline-flex items-center gap-2"><i data-lucide="sprout" class="w-4 h-4 text-emerald-600"></i>{{ stage_detail.deploy_data.carbon_profile_display }}</p></div>
                            <div><p class="fact-label">Deploy Duration</p><p class="fact-value inline-flex items-center gap-2"><i data-lucide="timer" class="w-4 h-4 text-sky-600"></i>{{ stage_detail.deploy_data.duration_display }}</p></div>
                            <div><p class="fact-label">Profiler Samples</p><p class="fact-value inline-flex items-center gap-2"><i data-lucide="list-checks" class="w-4 h-4 text-slate-500"></i>{{ stage_detail.deploy_data.samples_collected_display }}</p></div>
                            <div class="sm:col-span-2"><p class="fact-label">Image</p><p class="code-pill mt-2">{{ stage_detail.deploy_data.image_display }}</p></div>
                        </div>
                    </div>
                    {% if stage_detail.deploy_data.start_time_display != 'Not available' and stage_detail.deploy_data.end_time_display != 'Not available' %}
                    <div class="detail-card">
                        <p class="text-sm font-extrabold text-slate-800">Deployment timeline</p>
                        <div class="mt-5">
                            <div class="flex gap-3"><span class="mt-1 h-3.5 w-3.5 rounded-full bg-emerald-500 ring-4 ring-emerald-100"></span><div><p class="fact-label">Deployment started</p><p class="text-sm font-semibold text-slate-800 mt-1">{{ stage_detail.deploy_data.start_time_display }}</p></div></div>
                            <div class="timeline-line my-2"></div>
                            <div class="flex gap-3"><span class="mt-1 h-3.5 w-3.5 rounded-full bg-sky-500 ring-4 ring-sky-100"></span><div><p class="fact-label">Deployment completed</p><p class="text-sm font-semibold text-slate-800 mt-1">{{ stage_detail.deploy_data.end_time_display }}</p></div></div>
                        </div>
                    </div>
                    {% endif %}
                </div>
                {% if stage_detail.deploy_data.snapshots_display %}
                <div class="overflow-x-auto mt-4">
                    <table class="w-full text-left">
                        <thead><tr class="text-[11px] uppercase tracking-wider text-slate-500 bg-slate-50"><th class="px-4 py-3">Phase</th><th class="px-4 py-3">Strategy</th><th class="px-4 py-3">Multiplier</th><th class="px-4 py-3">Downtime</th><th class="px-4 py-3">Canary</th><th class="px-4 py-3">Note</th><th class="px-4 py-3">Snapshot Time</th></tr></thead>
                        <tbody class="divide-y divide-slate-200">{% for snapshot in stage_detail.deploy_data.snapshots_display %}<tr><td class="px-4 py-3 text-sm text-slate-700">{{ snapshot.phase }}</td><td class="px-4 py-3 text-sm text-slate-700">{{ snapshot.strategy }}</td><td class="px-4 py-3 text-sm text-slate-700">{{ snapshot.infra_multiplier }}</td><td class="px-4 py-3 text-sm text-slate-700">{{ snapshot.downtime_seconds }}</td><td class="px-4 py-3 text-sm text-slate-700">{{ snapshot.canary_weight }}</td><td class="px-4 py-3 text-sm text-slate-700">{{ snapshot.note }}</td><td class="px-4 py-3 text-sm text-slate-700">{{ snapshot.snapshot_timestamp }}</td></tr>{% endfor %}</tbody>
                    </table>
                </div>
                {% endif %}
                {% elif stage_detail.key == 'deploy' %}
                <p class="text-sm text-slate-500 mt-2">Deploy component data unavailable for this run.</p>
                {% else %}
                <p class="text-sm text-slate-500 mt-2">Component-specific results will be displayed here after integration.</p>
                {% endif %}
            </section>
            {% if stage_detail.key == 'deploy' and stage_detail.rows and not stage_detail.skipped %}
            <section class="panel p-6">
                <div class="section-title"><span class="section-icon"><i data-lucide="bar-chart-3" class="w-5 h-5"></i></span><div><h2 class="text-lg font-extrabold text-slate-900">Resource utilization</h2><p class="text-sm text-slate-500">Monitor-observed resource usage.</p></div></div>
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-5 mt-5">
                    {% for row in stage_detail.rows %}
                    <div class="detail-card">
                        <div class="flex items-center justify-between gap-3">
                            <div class="flex items-center gap-3"><span class="kpi-icon bg-cyan-100 text-cyan-700"><i data-lucide="cpu" class="w-5 h-5"></i></span><p class="text-sm font-extrabold text-slate-800">Average CPU</p></div>
                            <p class="text-lg font-black text-cyan-700">{{ row.avg_cpu_display }}</p>
                        </div>
                        <div class="metric-bar-track mt-4"><div class="metric-bar-fill cpu" style="width: {{ row.avg_cpu_bar_width }}%;"></div></div>
                    </div>
                    <div class="detail-card">
                        <div class="flex items-center justify-between gap-3">
                            <div class="flex items-center gap-3"><span class="kpi-icon bg-emerald-100 text-emerald-700"><i data-lucide="memory-stick" class="w-5 h-5"></i></span><p class="text-sm font-extrabold text-slate-800">Average Memory</p></div>
                            <p class="text-lg font-black text-emerald-700">{{ row.avg_memory_display }}</p>
                        </div>
                        <div class="metric-bar-track mt-4"><div class="metric-bar-fill memory" style="width: {{ row.avg_memory_bar_width }}%;"></div></div>
                    </div>
                    {% endfor %}
                </div>
            </section>
            {% endif %}
            {% if stage_detail.key != 'deploy' %}
            <section class="panel p-6">
                <h2 class="text-lg font-extrabold text-slate-900">Monitor sustainability information</h2>
                {% if stage_detail.rows %}<div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mt-4">{% for row in stage_detail.rows %}<div class="rounded-xl border border-slate-200 p-4"><p class="text-sm font-bold text-slate-800">Monitor data</p>{% if row.skipped %}<p class="text-xs text-amber-700 font-semibold mt-2">Skipped</p><p class="text-xs text-slate-500">Reason: {{ row.skip_reason_display }}</p><p class="text-xs text-slate-500">Energy: Not applicable</p><p class="text-xs text-slate-500">Carbon: Not applicable</p>{% else %}<p class="text-xs text-slate-500 mt-2">Peak CPU: {{ row.peak_cpu_display }}</p><p class="text-xs text-slate-500">Carbon intensity: {{ row.carbon_intensity_display }}</p><p class="text-xs text-slate-500">Infrastructure overhead: {{ row.overhead_percentage_display }}</p>{% endif %}</div>{% endfor %}</div>{% else %}<p class="text-sm text-slate-500 mt-3">Awaiting integrated Monitor data.</p>{% endif %}
            </section>
            {% endif %}
            <section class="panel p-6">
                <div class="section-title"><span class="section-icon"><i data-lucide="brain-circuit" class="w-5 h-5"></i></span><div><h2 class="text-lg font-extrabold text-slate-900">{{ stage_detail.label }} Anomaly Intelligence</h2><p class="text-sm text-slate-500">Analysis for this {{ stage_detail.label }} stage.</p></div></div>
                <div class="mt-5 rounded-2xl border border-slate-200 bg-slate-950 p-5 text-white">
                    <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
                        <div>
                            <p class="text-xs font-black uppercase tracking-[0.18em] text-emerald-300">Current stage anomaly state</p>
                            <p class="text-3xl font-black mt-2 {% if stage_detail.anomaly_summary.overall_status == 'Critical' %}text-rose-300{% elif stage_detail.anomaly_summary.overall_status == 'Warning' %}text-amber-300{% else %}text-emerald-300{% endif %}">{{ stage_detail.anomaly_summary.overall_status }}</p>
                        </div>
                        <div class="grid grid-cols-2 gap-3 text-right">
                            <div class="rounded-xl bg-white/8 px-4 py-3"><p class="text-xs uppercase font-bold text-slate-400">Warnings</p><p class="text-2xl font-black text-amber-300">{{ stage_detail.anomaly_summary.warning_count }}</p></div>
                            <div class="rounded-xl bg-white/8 px-4 py-3"><p class="text-xs uppercase font-bold text-slate-400">Critical</p><p class="text-2xl font-black text-rose-300">{{ stage_detail.anomaly_summary.critical_count }}</p></div>
                        </div>
                    </div>
                </div>
                <div class="grid grid-cols-1 xl:grid-cols-2 gap-4 mt-4">
                    <div class="detail-card xl:col-span-1">
                        <div class="flex items-center justify-between gap-3"><p class="text-sm font-extrabold text-slate-800">Statistical Detection</p><span class="kpi-icon bg-amber-100 text-amber-700"><i data-lucide="scan-search" class="w-5 h-5"></i></span></div>
                        {% if stage_detail.statistical_alerts %}
                        <div class="mt-4 space-y-3">
                            {% for alert in stage_detail.statistical_alerts %}
                            <div class="rounded-xl border border-slate-200 bg-white px-4 py-3">
                                <div class="flex items-center justify-between gap-3"><span class="text-xs font-black uppercase {{ alert.severity_badge_class }}">{{ alert.severity_label }}</span><span class="text-xs font-bold text-slate-500">{{ alert.percentage_change_display }} vs baseline</span></div>
                                <p class="text-sm font-semibold text-slate-800 mt-2">{{ alert.message }}</p>
                                <p class="text-xs text-slate-500 mt-1">{{ alert.metric_label }}: {{ alert.current_display }} vs {{ alert.baseline_display }} · {{ alert.context_scope_display }} · {{ alert.historical_samples_display }} runs</p>
                                {% if alert.fallback_reason %}<p class="text-xs text-amber-700 mt-1">{{ alert.fallback_reason }}</p>{% endif %}
                            </div>
                            {% endfor %}
                        </div>
                        {% else %}<span class="mt-4 inline-flex rounded-full bg-emerald-100 px-3 py-1 text-xs font-black uppercase text-emerald-700">Normal</span><p class="text-sm text-slate-500 mt-3">No warning or critical statistical anomalies for the selected stage data.</p>{% endif %}
                    </div>
                    <div class="detail-card xl:col-span-1">
                        <div class="flex items-center justify-between gap-3"><p class="text-sm font-extrabold text-slate-800">{{ stage_detail.label }} Isolation Forest</p><span class="kpi-icon bg-sky-100 text-sky-700"><i data-lucide="network" class="w-5 h-5"></i></span></div>
                        {% if stage_detail.ml_results %}
                        <div class="mt-4 space-y-3">
                            {% for item in stage_detail.ml_results %}
                            <div class="rounded-xl border border-slate-200 bg-white px-4 py-3">
                                <div class="flex flex-wrap items-center gap-2"><span class="rounded-full px-3 py-1 text-xs font-black uppercase {% if item.prediction == 'Anomaly' %}status-failed{% elif item.prediction == 'Warming Up' %}status-skipped{% else %}status-success{% endif %}">{{ item.prediction }}</span><span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-bold text-slate-600">{{ item.model_status }}</span></div>
                                <p class="text-sm font-semibold text-slate-800 mt-3">{{ item.message }}</p>
                                <div class="grid grid-cols-2 gap-3 mt-3 text-xs text-slate-600">
                                    <div><span class="font-black text-slate-500 uppercase">Stage</span><br>{{ item.stage_label }}</div>
                                    <div><span class="font-black text-slate-500 uppercase">Strategy</span><br>{{ item.strategy_display }}</div>
                                    <div><span class="font-black text-slate-500 uppercase">Context</span><br>{{ item.context_scope_display }}</div>
                                    <div><span class="font-black text-slate-500 uppercase">Samples</span><br>{{ item.historical_samples_display }}</div>
                                    <div><span class="font-black text-slate-500 uppercase">Score</span><br>{{ item.anomaly_score_display }}</div>
                                    <div><span class="font-black text-slate-500 uppercase">Strategy-specific</span><br>{{ 'Yes' if item.strategy_specific else 'No' }}</div>
                                </div>
                                {% if item.fallback_reason %}<p class="text-xs text-amber-700 mt-3">{{ item.fallback_reason }}</p>{% endif %}
                            </div>
                            {% endfor %}
                        </div>
                        {% else %}<p class="text-sm text-slate-500 mt-4">Awaiting integrated Monitor data.</p>{% endif %}
                    </div>
                </div>
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-4">
                    <div class="detail-card">
                        <div class="flex items-center justify-between gap-3"><p class="text-sm font-extrabold text-slate-800">Baseline Context</p><span class="kpi-icon bg-slate-100 text-slate-700"><i data-lucide="database" class="w-5 h-5"></i></span></div>
                        <p class="text-2xl font-black text-slate-950 mt-4">{{ stage_detail.baseline_context.label }}</p>
                        <p class="text-xs text-slate-500 mt-2">Scope: {{ stage_detail.baseline_context.context_scope }} | Historical runs: {{ stage_detail.baseline_context.historical_samples_display }} | Strategy-specific: {{ 'Yes' if stage_detail.baseline_context.strategy_specific else 'No' }}</p>
                        {% if stage_detail.baseline_context.fallback_reason %}<p class="text-xs text-amber-700 mt-2">{{ stage_detail.baseline_context.fallback_reason }}</p>{% endif %}
                    </div>
                </div>
            </section>
        </main>
        {% endif %}
    </div>
    <script>
        lucide.createIcons();
        {% if page == 'home' %}
        Chart.defaults.font.family = "'Plus Jakarta Sans', sans-serif";
        const runLabels = {{ run_chart_labels | safe }};
        new Chart(document.getElementById("runEnergyChart"), { type: "line", data: { labels: runLabels, datasets: [{ label: "Energy kWh", data: {{ run_energy_values | safe }}, borderColor: "rgba(16, 185, 129, 1)", backgroundColor: "rgba(16, 185, 129, .12)", fill: true, tension: .35 }] }, options: { responsive: true, maintainAspectRatio: false } });
        new Chart(document.getElementById("runCarbonChart"), { type: "line", data: { labels: runLabels, datasets: [{ label: "Carbon kgCO2e", data: {{ run_carbon_values | safe }}, borderColor: "rgba(14, 165, 233, 1)", backgroundColor: "rgba(14, 165, 233, .12)", fill: true, tension: .35 }] }, options: { responsive: true, maintainAspectRatio: false } });
        {% endif %}
    </script>
</body>
</html>
"""


def _empty_data_response():
    return """
    <div style='background:#f8fafc; color:#0f172a; height:100vh; display:flex; align-items:center; justify-content:center; font-family:sans-serif;'>
        <h2>No monitoring data found.</h2>
    </div>
    """


def load_dashboard_data():
    df, data_source = load_metrics()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), data_source
    df = prepare_metrics_dataframe(df)
    return df, build_run_summary(df), data_source


def selected_or_latest_run_id(run_summary, requested_run=None):
    available_run_ids = set(run_summary["run_id"].astype(str).tolist())
    if requested_run in available_run_ids:
        return str(requested_run)
    return str(run_summary.iloc[0]["run_id"])


def _run_times(current_run_df):
    start_time = (
        str(current_run_df["start_timestamp"].dropna().astype(str).min())
        if "start_timestamp" in current_run_df.columns and not current_run_df["start_timestamp"].dropna().empty
        else "Not available"
    )
    end_time = (
        str(current_run_df["end_timestamp"].dropna().astype(str).max())
        if "end_timestamp" in current_run_df.columns and not current_run_df["end_timestamp"].dropna().empty
        else "Not available"
    )
    return start_time, end_time


def enrich_run_summary_for_pages(df, run_summary):
    rows = run_summary.copy()
    if rows.empty:
        return rows
    rows["total_energy_display"] = rows["total_energy_kwh"].apply(format_kwh)
    rows["total_carbon_display"] = rows["total_carbon_kg"].apply(format_gco2_from_kg)
    rows["duration_display"] = rows.apply(
        lambda row: format_seconds(row["jenkins_stage_duration_seconds"])
        if row.get("jenkins_stage_duration_captured") else format_seconds(row["duration_seconds"]),
        axis=1,
    )
    health_scores, alert_counts, start_times, end_times = [], [], [], []
    for run_id in rows["run_id"].astype(str).tolist():
        current_run_df = df[df["run_id"] == run_id].copy()
        historical_df = df[df["run_id"] != run_id].copy()
        analytics_current_run_df = workload_analytics_dataframe(current_run_df)
        analytics_historical_df = workload_analytics_dataframe(historical_df)
        stage_baseline_df = calculate_stage_baselines(analytics_historical_df, analytics_current_run_df)
        anomalies = detect_stage_anomalies(analytics_current_run_df, stage_baseline_df)
        health = calculate_sustainability_score(
            analytics_current_run_df,
            calculate_pipeline_baseline(analytics_historical_df),
            anomalies,
            stage_baseline_df,
        )
        health_scores.append(int(health.get("score", 0)))
        alert_counts.append(sum(1 for item in anomalies if normalize_display_severity(item.get("severity")) in {"critical", "warning"}))
        start_time, end_time = _run_times(current_run_df)
        start_times.append(start_time)
        end_times.append(end_time)
    rows["health_score"] = health_scores
    rows["alert_count"] = alert_counts
    rows["start_time_display"] = start_times
    rows["end_time_display"] = end_times
    return rows


def build_run_context(df, run_summary, data_source, selected_run, include_release_data=True):
    current_run_df = df[df["run_id"] == selected_run].copy()
    historical_df = df[df["run_id"] != selected_run].copy()
    pipeline_name = str(current_run_df["pipeline_name"].dropna().astype(str).iloc[0]) if "pipeline_name" in current_run_df.columns and not current_run_df["pipeline_name"].dropna().empty else "Unknown pipeline"
    build_number = extract_build_number_from_run_id(selected_run)
    release_build_data = (
        format_release_build_data(find_release_build_for_run(selected_run, load_release_builds()))
        if include_release_data
        else None
    )
    deploy_component_data = format_deploy_component_data(load_deploy_data(pipeline_name, build_number))
    analytics_current_run_df = workload_analytics_dataframe(current_run_df)
    analytics_historical_df = workload_analytics_dataframe(historical_df)
    stage_baseline_df = calculate_stage_baselines(analytics_historical_df, analytics_current_run_df)
    pipeline_baseline = calculate_pipeline_baseline(analytics_historical_df)
    anomalies = detect_stage_anomalies(analytics_current_run_df, stage_baseline_df)
    health_score = calculate_sustainability_score(analytics_current_run_df, pipeline_baseline, anomalies, stage_baseline_df)
    ml_anomaly = detect_ml_anomalies(analytics_current_run_df, analytics_historical_df)

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
            peak_cpu_percent=("peak_cpu_percent", "max"),
            avg_memory_percent=("avg_memory_percent", "mean"),
            peak_memory_percent=("peak_memory_percent", "max"),
            total_energy_kwh=("total_energy_kwh", "sum"),
            active_energy_kwh=("active_energy_kwh", "sum"),
            total_carbon_kg=("total_carbon_kg", "sum"),
            carbon_intensity_kg_per_kwh=("carbon_intensity_kg_per_kwh", "mean"),
            skipped=("skipped", "all"),
            skip_reason=("skip_reason", lambda values: next((str(value) for value in values if str(value).strip()), "")),
        )
        .reset_index()
    )
    stage_status = (
        current_run_df.groupby("stage", observed=True)["status"]
        .agg(summarize_stage_status)
        .reset_index()
    )
    display_rows = summary.merge(stage_status, on="stage", how="left")
    display_rows["stage_label"] = display_rows["stage"].astype(str).str.replace("_", " ").str.title()
    display_rows["workload_duration_display"] = display_rows["workload_duration_seconds"].apply(format_seconds)
    display_rows["full_duration_display"] = display_rows.apply(lambda row: format_seconds(row["jenkins_stage_duration_seconds"]) if row["jenkins_stage_duration_captured"] else "Not captured", axis=1)
    display_rows["overhead_percentage_display"] = display_rows.apply(lambda row: format_percent(row["overhead_percentage"]) if row["jenkins_stage_duration_captured"] else "Not captured", axis=1)
    display_rows["avg_cpu_display"] = display_rows["avg_cpu_percent"].apply(format_percent)
    display_rows["peak_cpu_display"] = display_rows["peak_cpu_percent"].apply(format_percent)
    display_rows["avg_memory_display"] = display_rows["avg_memory_percent"].apply(format_percent)
    display_rows["peak_memory_display"] = display_rows["peak_memory_percent"].apply(format_percent)
    display_rows["avg_cpu_bar_width"] = display_rows["avg_cpu_percent"].clip(lower=0, upper=100)
    display_rows["avg_memory_bar_width"] = display_rows["avg_memory_percent"].clip(lower=0, upper=100)
    display_rows["carbon_intensity_display"] = display_rows["carbon_intensity_kg_per_kwh"].apply(lambda value: f"{float(value):.4f} kg/kWh")
    display_rows["total_energy_display"] = display_rows["total_energy_kwh"].apply(format_kwh)
    display_rows["total_carbon_display"] = display_rows["total_carbon_kg"].apply(format_gco2_from_kg)
    display_rows["skip_reason_display"] = display_rows["skip_reason"].apply(format_skip_reason)
    display_rows["status_display"] = display_rows.apply(
        lambda row: "SKIPPED" if normalize_skipped_value(row.get("skipped")) else str(row.get("status", "")).upper(),
        axis=1,
    )
    display_rows.loc[display_rows["skipped"], "workload_duration_display"] = "Not applicable"
    display_rows.loc[display_rows["skipped"], "full_duration_display"] = "Not applicable"
    display_rows.loc[display_rows["skipped"], "overhead_percentage_display"] = "Not applicable"
    display_rows.loc[display_rows["skipped"], "avg_cpu_display"] = "Not applicable"
    display_rows.loc[display_rows["skipped"], "peak_cpu_display"] = "Not applicable"
    display_rows.loc[display_rows["skipped"], "avg_memory_display"] = "Not applicable"
    display_rows.loc[display_rows["skipped"], "peak_memory_display"] = "Not applicable"
    display_rows.loc[display_rows["skipped"], "avg_cpu_bar_width"] = 0
    display_rows.loc[display_rows["skipped"], "avg_memory_bar_width"] = 0
    display_rows.loc[display_rows["skipped"], "carbon_intensity_display"] = "Not applicable"
    display_rows.loc[display_rows["skipped"], "total_energy_display"] = "Not applicable"
    display_rows.loc[display_rows["skipped"], "total_carbon_display"] = "Not applicable"

    dashboard_anomalies = [normalize_dashboard_anomaly(item) for item in deduplicate_anomalies(anomalies, allowed_metrics=PP1_ANOMALY_METRICS, limit=8)]
    formatted_statistical_alerts = [{**format_dashboard_anomaly(item), "source": "Statistical"} for item in dashboard_anomalies if item.get("severity") in {"critical", "warning"}]
    formatted_ml_results = []
    for item in ml_anomaly.get("results", []):
        formatted_item = dict(item)
        formatted_item["severity"] = normalize_display_severity(item.get("severity"))
        formatted_item["stage_label"] = str(item.get("stage", "unknown")).replace("_", " ").title()
        formatted_item["anomaly_score_display"] = (
            format_decimal(item.get("anomaly_score"), decimals=4)
            if item.get("anomaly_score") is not None
            else "N/A"
        )
        formatted_item["context_scope_display"] = format_context_scope(item.get("context_scope"))
        formatted_item["historical_samples_display"] = format_count(item.get("historical_samples", 0))
        formatted_item["strategy_display"] = format_optional_text(item.get("strategy")).replace("_", " ").title()
        formatted_item["fallback_reason"] = item.get("fallback_reason", "")
        formatted_ml_results.append(formatted_item)
    formatted_ml_alerts = [format_ml_alert(item) for item in ml_anomaly.get("results", []) if normalize_display_severity(item.get("severity")) in {"critical", "warning"}]
    critical_count = sum(1 for item in formatted_statistical_alerts + formatted_ml_alerts if item.get("severity") == "critical")
    warning_count = sum(1 for item in formatted_statistical_alerts + formatted_ml_alerts if item.get("severity") == "warning")
    anomaly_summary = {
        "critical_count": critical_count,
        "warning_count": warning_count,
        "overall_status": "Critical" if critical_count else ("Warning" if warning_count else "Normal"),
    }
    lifecycle_sections = build_lifecycle_sections(
        display_rows,
        formatted_statistical_alerts,
        formatted_ml_results,
        deploy_component_data=deploy_component_data,
    )

    has_full_stage_timing = bool(current_run_df["jenkins_stage_duration_captured"].any())
    workload_duration = round(float(current_run_df["workload_duration_seconds"].sum()), 2)
    jenkins_stage_duration = round(float(current_run_df["jenkins_stage_duration_seconds"].sum()), 2)
    selected_run_status = "failed" if current_run_df["status"].astype(str).str.lower().eq("failed").any() else "success"
    selected_run_start, selected_run_end = _run_times(current_run_df)

    return {
        "data_source": data_source,
        "selected_run": selected_run,
        "pipeline_name": pipeline_name,
        "selected_run_status": selected_run_status,
        "selected_run_start": selected_run_start,
        "selected_run_end": selected_run_end,
        "selected_run_duration_display": format_seconds(jenkins_stage_duration) if has_full_stage_timing else format_seconds(workload_duration),
        "total_energy_display": format_kwh(round(float(current_run_df["total_energy_kwh"].sum()), 8)),
        "total_carbon_display": format_gco2_from_kg(round(float(current_run_df["total_carbon_kg"].sum()), 8)),
        "health_score": health_score,
        "health_explanation_display": neutralize_health_explanation(health_score.get("explanation")),
        "release_build_data": release_build_data,
        "anomaly_summary": anomaly_summary,
        "lifecycle_sections": lifecycle_sections,
    }


def build_home_context(df, run_summary, data_source):
    enriched_runs = enrich_run_summary_for_pages(df, run_summary)
    chart_rows = enriched_runs.sort_values(["latest_time", "run_id"], ascending=[True, True]).tail(12)
    average_health_score = int(round(float(enriched_runs["health_score"].mean()))) if not enriched_runs.empty else 0
    return {
        "data_source": data_source,
        "system_total_energy_display": format_kwh(round(float(df["total_energy_kwh"].sum()), 8)),
        "system_total_carbon_display": format_gco2_from_kg(round(float(df["total_carbon_kg"].sum()), 8)),
        "system_run_count": int(len(enriched_runs)),
        "average_health_score": average_health_score,
        "recent_runs": enriched_runs.head(5).to_dict(orient="records"),
        "run_chart_labels": json.dumps(chart_rows["run_id"].astype(str).tolist()),
        "run_energy_values": json.dumps(chart_rows["total_energy_kwh"].round(8).tolist()),
        "run_carbon_values": json.dumps(chart_rows["total_carbon_kg"].round(8).tolist()),
    }


def _data_or_empty():
    df, data_source = load_metrics()
    if df.empty:
        return None, None, data_source
    df = prepare_metrics_dataframe(df)
    run_summary = build_run_summary(df)
    if run_summary.empty:
        return None, None, data_source
    return df, run_summary, data_source


@app.route("/")
def home():
    df, run_summary, data_source = _data_or_empty()
    if df is None:
        return _empty_data_response()
    return render_template_string(APP_HTML, page="home", **build_home_context(df, run_summary, data_source))


@app.route("/runs")
def runs_page():
    df, run_summary, data_source = _data_or_empty()
    if df is None:
        return _empty_data_response()
    runs = enrich_run_summary_for_pages(df, run_summary).to_dict(orient="records")
    return render_template_string(APP_HTML, page="runs", data_source=data_source, runs=runs)


@app.route("/run/<run_id>")
def run_overview(run_id):
    df, run_summary, data_source = _data_or_empty()
    if df is None:
        return _empty_data_response()
    if run_id not in set(run_summary["run_id"].astype(str).tolist()):
        abort(404)
    return render_template_string(APP_HTML, page="run", **build_run_context(df, run_summary, data_source, run_id))


@app.route("/run/<run_id>/<stage_key>")
def stage_detail(run_id, stage_key):
    df, run_summary, data_source = _data_or_empty()
    if df is None:
        return _empty_data_response()
    if run_id not in set(run_summary["run_id"].astype(str).tolist()):
        abort(404)
    context = build_run_context(df, run_summary, data_source, run_id, include_release_data=False)
    stage_detail_context = next((item for item in context["lifecycle_sections"] if item.get("key") == stage_key.lower()), None)
    if stage_detail_context is None:
        abort(404)
    return render_template_string(APP_HTML, page="stage", stage_detail=stage_detail_context, **context)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5051, debug=False)
