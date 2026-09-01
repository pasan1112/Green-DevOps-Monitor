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
from operate.routes import operate_bp

app = Flask(__name__)
app.register_blueprint(operate_bp)

MONGO_DB_NAME = "green_devops_monitor"
MONGO_COLLECTION_NAME = "pipeline_metrics"
CSV_FALLBACK_PATH = "data/metrics.csv"
RELEASE_CSV_PATH = "data/release_metrics.csv"
DEPLOY_DB_PATH = "/opt/energy-profiller-hiran/deployments.db"

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
    mongo_uri = "mongodb+srv://admin:admin1234@green-devops-monitor.xxflzzs.mongodb.net/"
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


def _normalize_release_key_part(value):
    return " ".join(str(value or "").strip().split())


def release_build_key(build):
    job_name = _normalize_release_key_part((build or {}).get("job_name"))
    build_number = _normalize_release_key_part((build or {}).get("build_number"))
    if not job_name or not build_number:
        return ""
    return f"{job_name}-{build_number}"


def resolve_release_csv_path(csv_path=None):
    candidates = [
        csv_path,
        os.getenv("RELEASE_CSV_PATH"),
        RELEASE_CSV_PATH,
    ]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return csv_path or os.getenv("RELEASE_CSV_PATH") or RELEASE_CSV_PATH


def load_release_builds(csv_path=None):
    """Read Release-stage telemetry written by Jenkins without mutating Monitor data."""
    path = resolve_release_csv_path(csv_path)
    if not os.path.exists(path):
        return []

    try:
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception as exc:
        print(f"[Release CSV] WARNING: Release metrics unavailable from {path}. Continuing with Monitor data only. Error: {exc}")
        return []

    return frame.to_dict(orient="records")


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


def optional_numeric(value):
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
    status = format_optional_text(build.get("release_status") or build.get("status"))
    pipeline_type = format_optional_text(build.get("pipeline_type"))
    scheduling_action = format_optional_text(build.get("scheduling_action"))
    scheduling_engine = format_optional_text(build.get("scheduling_engine"))
    optimizer_status = format_optional_text(build.get("optimizer_status"))
    optimizer_executed = format_optional_text(build.get("optimizer_executed"))
    optimizer_skip_reason = format_optional_text(build.get("optimizer_skip_reason"))
    decision_context = " ".join([pipeline_type, scheduling_action, scheduling_engine, optimizer_status, optimizer_skip_reason]).lower()
    is_full_build = any(token in decision_context for token in ("force_full_build", "full build", "full_build", "force full"))
    optimizer_bypassed = is_full_build or "bypass" in decision_context or optimizer_status.lower() == "skipped"
    optimizer_applied = optimizer_executed.lower() == "true" and optimizer_status.lower() not in {"not available", "skipped", "no_changes"}
    return {
        "status_display": status.upper() if status != "Not available" else status,
        "pipeline_type_display": pipeline_type.replace("_", " ").title() if pipeline_type != "Not available" else "Jenkins Release Telemetry",
        "green_probability_display": format_release_probability(build.get("green_probability")),
        "scheduling_action_display": scheduling_action.replace("_", " ").title(),
        "scheduling_engine_display": scheduling_engine,
        "execution_mode_display": "Full Build" if is_full_build else ("Selective / Optimized" if build.get("affected_modules") else "Not available"),
        "optimization_status_display": "Bypassed" if optimizer_bypassed else ("Applied" if optimizer_applied else optimizer_status.replace("_", " ").title()),
        "optimizer_status_display": optimizer_status.replace("_", " ").title(),
        "optimizer_duration_display": format_release_seconds(build.get("optimizer_duration_s")),
        "optimizer_executed_display": optimizer_executed,
        "optimizer_skip_reason_display": optimizer_skip_reason.replace("_", " ").title(),
        "release_duration_display": format_release_seconds(build.get("release_duration_s")),
        "release_start_display": format_optional_text(build.get("release_start_time")),
        "release_end_display": format_optional_text(build.get("release_end_time")),
        "context_note": (
            "Full build means Release optimization or scheduling was bypassed; Monitor lifecycle status still reflects actual execution."
            if optimizer_bypassed
            else "Release CSV provides Jenkins execution context; Monitor remains the source for measured sustainability results."
        ),
        "carbon_intensity_display": format_release_intensity(build.get("carbon_intensity")),
        "affected_modules_display": format_release_list(build.get("affected_modules")),
        "tests_executed_display": format_release_count(build.get("tests_executed")),
        "tests_skipped_display": format_release_count(build.get("tests_skipped")),
        "build_duration_display": format_release_seconds(build.get("build_duration_s")),
        "test_duration_display": format_release_seconds(build.get("test_duration_s")),
        "docker_build_duration_display": format_release_seconds(build.get("docker_build_duration_s") or build.get("docker_duration_s")),
        "build_command_display": format_optional_text(build.get("build_command")),
        "test_command_display": format_optional_text(build.get("test_command")),
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
    """Group normalized Monitor lifecycle data under Release and Deploy labels."""
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
                        <a href="#lifecycle" class="flex items-center gap-2 rounded-lg px-3 py-2 text-slate-700 hover:bg-slate-50"><i data-lucide="workflow" class="w-4 h-4"></i>Release | Deploy</a>
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

                        <div id="lifecycle" class="grid grid-cols-1 md:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] gap-4 md:gap-5 items-stretch">
                            {% for lifecycle in lifecycle_sections %}
                            <a href="#stage-{{ lifecycle.key }}" class="rounded-xl border border-slate-200 bg-white/80 p-5 hover:border-emerald-300 hover:bg-emerald-50/40 transition-colors min-h-[150px] flex flex-col justify-between">
                                <div class="flex items-center justify-between">
                                    <div class="flex items-center gap-2">
                                        <i data-lucide="{{ lifecycle.icon }}" class="w-5 h-5 text-emerald-600"></i>
                                        <h3 class="font-extrabold text-slate-900">{{ lifecycle.label }}</h3>
                                    </div>
                                    <span class="text-xs font-bold text-slate-500">{{ lifecycle.rows|length }} monitor stage{{ '' if lifecycle.rows|length == 1 else 's' }}</span>
                                </div>
                                <p class="text-xs text-slate-500 mt-3">{{ lifecycle.note }}</p>
                            </a>
                            {% if not loop.last %}
                            <div class="hidden md:flex items-center justify-center" aria-hidden="true">
                                <span class="inline-flex h-10 w-10 items-center justify-center rounded-full border border-emerald-200 bg-emerald-50 text-emerald-700 shadow-sm">
                                    <i data-lucide="arrow-right" class="w-5 h-5"></i>
                                </span>
                            </div>
                            {% endif %}
                            {% endfor %}
                        </div>
                    </div>
                </section>

                <section class="space-y-5">
                    <div>
                        <p class="text-xs font-bold uppercase tracking-[0.2em] text-emerald-600 mb-2">Stage Detail</p>
                        <h2 class="text-2xl font-extrabold text-slate-900">Release | Deploy</h2>
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
    <script>
        (function() {
            const storageKey = "green-devops-theme";
            let savedTheme = null;
            try { savedTheme = localStorage.getItem(storageKey); } catch (error) { savedTheme = null; }
            const preferredTheme = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
            document.documentElement.dataset.theme = savedTheme || preferredTheme || "light";
        })();
    </script>
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
        :root, [data-theme="light"] {
            --home-bg: #eff5f3;
            --home-bg-deep: #dcece8;
            --home-surface: rgba(255,255,255,.82);
            --home-surface-strong: rgba(255,255,255,.94);
            --home-surface-muted: rgba(241,245,249,.82);
            --home-border: rgba(148,163,184,.30);
            --home-border-strong: rgba(148,163,184,.46);
            --home-text: #10201d;
            --home-text-secondary: #475569;
            --home-text-muted: #7b8a9b;
            --home-accent-energy: #059669;
            --home-accent-energy-rgb: 5, 150, 105;
            --home-accent-carbon: #0284c7;
            --home-accent-carbon-rgb: 2, 132, 199;
            --home-accent-health: #00a884;
            --home-warning: #d97706;
            --home-critical: #e11d48;
            --home-shadow: 0 20px 60px rgba(15, 23, 42, .12);
            --home-shadow-soft: 0 14px 42px rgba(15, 23, 42, .08);
            --home-grid: rgba(15, 23, 42, .055);
        }
        [data-theme="dark"] {
            --home-bg: #071a17;
            --home-bg-deep: #081b2a;
            --home-surface: rgba(12, 31, 34, .74);
            --home-surface-strong: rgba(13, 38, 43, .92);
            --home-surface-muted: rgba(15, 45, 50, .62);
            --home-border: rgba(148, 163, 184, .18);
            --home-border-strong: rgba(34, 211, 238, .24);
            --home-text: #f8fafc;
            --home-text-secondary: #b7c4d4;
            --home-text-muted: #7f91a3;
            --home-accent-energy: #00c896;
            --home-accent-energy-rgb: 0, 200, 150;
            --home-accent-carbon: #22d3ee;
            --home-accent-carbon-rgb: 34, 211, 238;
            --home-accent-health: #2dd4bf;
            --home-warning: #f59e0b;
            --home-critical: #fb7185;
            --home-shadow: 0 24px 72px rgba(0, 0, 0, .34);
            --home-shadow-soft: 0 18px 50px rgba(0, 0, 0, .26);
            --home-grid: rgba(226, 232, 240, .055);
        }
        body.home-body { background: radial-gradient(ellipse at 72% 10%, rgba(var(--home-accent-energy-rgb), .18), transparent 36%), radial-gradient(ellipse at 16% 34%, rgba(var(--home-accent-carbon-rgb), .13), transparent 30%), linear-gradient(145deg, var(--home-bg) 0%, var(--home-bg-deep) 100%); color: var(--home-text); }
        body.home-body::before { content: ""; position: fixed; inset: 0; pointer-events: none; background-image: linear-gradient(var(--home-grid) 1px, transparent 1px), linear-gradient(90deg, var(--home-grid) 1px, transparent 1px); background-size: 44px 44px; mask-image: linear-gradient(180deg, rgba(0,0,0,.7), transparent 72%); }
        .home-shell { width: min(100% - 1.5rem, 1450px); margin: 0 auto; }
        .home-header { position: relative; z-index: 1; border: 1px solid var(--home-border); background: color-mix(in srgb, var(--home-surface-strong) 88%, transparent); border-radius: 1.25rem; padding: .85rem 1rem; box-shadow: var(--home-shadow-soft); backdrop-filter: blur(20px); }
        .home-brand-mark { width: 2.25rem; height: 2.25rem; border-radius: .85rem; display: inline-flex; align-items: center; justify-content: center; background: rgba(var(--home-accent-energy-rgb), .14); color: var(--home-accent-energy); border: 1px solid rgba(var(--home-accent-energy-rgb), .25); box-shadow: 0 0 24px rgba(var(--home-accent-energy-rgb), .16); }
        .home-nav-link { display: inline-flex; align-items: center; gap: .4rem; border-radius: 999px; padding: .5rem .75rem; color: var(--home-text-secondary); font-size: .875rem; font-weight: 700; transition: background .16s ease, color .16s ease; }
        .home-nav-link:hover { background: var(--home-surface-muted); color: var(--home-text); }
        .home-nav-link.active { background: rgba(var(--home-accent-energy-rgb), .16); color: var(--home-accent-energy); box-shadow: inset 0 0 0 1px rgba(var(--home-accent-energy-rgb), .22); }
        .home-source-chip { display: inline-flex; align-items: center; gap: .5rem; border-radius: 999px; border: 1px solid var(--home-border); background: var(--home-surface); padding: .5rem .8rem; color: var(--home-text-secondary); font-size: .8125rem; font-weight: 700; }
        .theme-toggle { display: inline-grid; grid-template-columns: 1fr 1fr; align-items: center; gap: .18rem; border: 1px solid var(--home-border); background: var(--home-surface); border-radius: 999px; padding: .2rem; color: var(--home-text-secondary); }
        .theme-toggle button { width: 2rem; height: 2rem; display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; transition: background .16s ease, color .16s ease, box-shadow .16s ease; }
        .theme-toggle button:focus-visible { outline: 2px solid var(--home-accent-carbon); outline-offset: 2px; }
        [data-theme="light"] .theme-toggle .theme-light, [data-theme="dark"] .theme-toggle .theme-dark { background: rgba(var(--home-accent-energy-rgb), .18); color: var(--home-accent-energy); box-shadow: inset 0 0 0 1px rgba(var(--home-accent-energy-rgb), .18); }
        .home-hero { position: relative; z-index: 1; display: grid; grid-template-columns: minmax(0, 1fr); gap: 1.4rem; align-items: center; padding: 1.55rem 0 .35rem; }
        @media (min-width: 960px) { .home-hero { grid-template-columns: minmax(0, 1.4fr) minmax(300px, 1fr); } }
        .home-eyebrow { font-size: .72rem; line-height: 1rem; font-weight: 800; letter-spacing: .18em; text-transform: uppercase; color: var(--home-accent-energy); }
        .home-title { margin-top: .65rem; max-width: 860px; font-size: clamp(2.35rem, 4.5vw, 4rem); line-height: .96; font-weight: 900; letter-spacing: 0; color: var(--home-text); }
        .home-title span { display: block; color: var(--home-accent-energy); text-shadow: 0 0 38px rgba(var(--home-accent-energy-rgb), .30); }
        .home-subtitle { margin-top: .85rem; max-width: 720px; color: var(--home-text-secondary); font-size: 1rem; line-height: 1.6; font-weight: 500; }
        .home-signal { min-height: 176px; border: 1px solid var(--home-border-strong); background: linear-gradient(145deg, rgba(var(--home-accent-energy-rgb), .14), rgba(var(--home-accent-carbon-rgb), .10) 42%, var(--home-surface)); border-radius: 1.55rem; position: relative; overflow: hidden; box-shadow: var(--home-shadow); }
        .home-signal::before { content: ""; position: absolute; inset: 0; background-image: linear-gradient(90deg, transparent 0 18%, rgba(var(--home-accent-energy-rgb), .18) 18.5%, transparent 19%), linear-gradient(135deg, transparent 0 46%, rgba(var(--home-accent-carbon-rgb), .16) 47%, transparent 49%); background-size: 68px 68px, 100% 100%; opacity: .7; }
        .home-signal-wave { position: absolute; left: 9%; right: 9%; top: 44%; height: 4px; border-radius: 999px; background: linear-gradient(90deg, transparent, var(--home-accent-energy), var(--home-accent-carbon), transparent); box-shadow: 0 0 30px rgba(var(--home-accent-energy-rgb), .38); }
        .home-signal-node { position: absolute; width: .7rem; height: .7rem; border-radius: 999px; background: var(--home-accent-energy); box-shadow: 0 0 24px rgba(var(--home-accent-energy-rgb), .72); }
        .home-signal-node.one { left: 18%; top: 34%; }
        .home-signal-node.two { left: 48%; top: 43%; background: var(--home-accent-carbon); box-shadow: 0 0 24px rgba(var(--home-accent-carbon-rgb), .62); }
        .home-signal-node.three { right: 18%; top: 55%; }
        .home-signal-caption { position: absolute; left: 1.2rem; right: 1.2rem; bottom: 1rem; display: flex; justify-content: space-between; gap: 1rem; color: var(--home-text-secondary); font-size: .75rem; font-weight: 800; }
        .home-kpi-grid { display: grid; grid-template-columns: repeat(1, minmax(0, 1fr)); gap: .9rem; }
        @media (min-width: 768px) { .home-kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
        @media (min-width: 1180px) { .home-kpi-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); } }
        .home-widget { position: relative; overflow: hidden; border-radius: 1.35rem; border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); padding: 1rem; min-height: 146px; box-shadow: var(--home-shadow-soft); transition: transform .18s ease, box-shadow .18s ease, border-color .18s ease; }
        .home-widget::after { content: ""; position: absolute; left: 1.25rem; right: 1.25rem; bottom: 1rem; height: 3px; border-radius: 999px; background: linear-gradient(90deg, rgba(var(--home-accent-energy-rgb), .80), transparent); opacity: .72; }
        .home-widget.carbon::after { background: linear-gradient(90deg, rgba(var(--home-accent-carbon-rgb), .86), transparent); }
        .home-widget.health::after { background: linear-gradient(90deg, var(--home-accent-health), transparent); }
        .home-widget.activity::after { background: linear-gradient(90deg, #64748b, transparent); }
        .home-widget:hover { transform: translateY(-3px); border-color: var(--home-border-strong); box-shadow: var(--home-shadow); }
        .home-widget-icon { width: 2.35rem; height: 2.35rem; display: inline-flex; align-items: center; justify-content: center; border-radius: .95rem; background: var(--home-surface-muted); color: var(--home-text-secondary); border: 1px solid var(--home-border); }
        .home-widget-value { margin-top: 1.25rem; font-size: clamp(1.65rem, 2.2vw, 2.35rem); line-height: 1; font-weight: 900; letter-spacing: 0; color: var(--home-text); overflow-wrap: anywhere; }
        .home-widget-label { font-size: .82rem; color: var(--home-text-secondary); font-weight: 800; letter-spacing: .08em; text-transform: uppercase; }
        .home-widget-note { margin-top: .7rem; font-size: .78rem; line-height: 1.45; color: var(--home-text-muted); font-weight: 600; }
        .home-health-ring { position: relative; width: 3.45rem; height: 3.45rem; border-radius: 999px; display: grid; place-items: center; background: conic-gradient(var(--home-accent-health) calc(var(--score) * 1%), rgba(148,163,184,.22) 0); box-shadow: 0 0 32px rgba(var(--home-accent-energy-rgb), .18); }
        .home-health-ring::before { content: ""; width: 2.55rem; height: 2.55rem; border-radius: 999px; background: var(--home-surface-strong); position: absolute; }
        .home-health-ring span { position: relative; z-index: 1; color: var(--home-text); font-size: .8rem; font-weight: 900; }
        .home-panel { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.6rem; box-shadow: var(--home-shadow-soft); backdrop-filter: blur(18px); }
        .home-section-heading { display: flex; align-items: flex-end; justify-content: space-between; gap: 1rem; padding: 1rem 1.15rem 0; }
        .home-section-title { color: var(--home-text); font-size: 1.15rem; font-weight: 850; letter-spacing: 0; }
        .home-section-subtitle { margin-top: .25rem; color: var(--home-text-secondary); font-size: .875rem; line-height: 1.5; }
        .home-chart-card { border-radius: 1.15rem; border: 1px solid var(--home-border); background: rgba(3, 10, 18, .20); padding: .9rem; }
        [data-theme="light"] .home-chart-card { background: rgba(255,255,255,.55); }
        .home-chart-title { display: flex; align-items: center; justify-content: space-between; gap: .75rem; margin-bottom: .75rem; color: var(--home-text-secondary); font-size: .82rem; font-weight: 800; }
        .home-widget-icon.energy { background: rgba(var(--home-accent-energy-rgb), .14); color: var(--home-accent-energy); box-shadow: 0 0 24px rgba(var(--home-accent-energy-rgb), .14); }
        .home-widget-icon.carbon { background: rgba(var(--home-accent-carbon-rgb), .13); color: var(--home-accent-carbon); box-shadow: 0 0 24px rgba(var(--home-accent-carbon-rgb), .12); }
        .home-widget-icon.health { background: rgba(var(--home-accent-energy-rgb), .12); color: var(--home-accent-health); }
        .home-run-list { display: grid; gap: .2rem; padding: .75rem .75rem .75rem; }
        .home-run-row { display: grid; grid-template-columns: minmax(0, 1.5fr) .7fr .8fr .7fr 1.25rem; gap: .55rem; align-items: center; border-radius: .85rem; padding: .68rem .65rem; color: var(--home-text-secondary); text-decoration: none; transition: background .16s ease, transform .16s ease, color .16s ease; }
        .home-run-row:hover { background: var(--home-surface-muted); transform: translateX(2px); }
        .home-run-head { color: var(--home-text-muted); font-size: .68rem; font-weight: 850; text-transform: uppercase; letter-spacing: .08em; }
        .home-run-id { color: var(--home-text); font-weight: 900; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .home-run-status { display: inline-flex; align-items: center; border-radius: 999px; padding: .25rem .55rem; font-size: .68rem; font-weight: 850; text-transform: uppercase; }
        .home-run-status.success { background: rgba(var(--home-accent-energy-rgb), .13); color: var(--home-accent-energy); }
        .home-run-status.skipped { background: rgba(245, 158, 11, .14); color: var(--home-warning); }
        .home-run-status.cancelled { background: rgba(148, 163, 184, .14); color: var(--home-text-muted); }
        .home-run-status.failed { background: rgba(244, 63, 94, .14); color: var(--home-critical); }
        .home-health-pill { display: inline-flex; align-items: baseline; gap: .15rem; border-radius: 999px; background: rgba(var(--home-accent-energy-rgb), .10); border: 1px solid rgba(var(--home-accent-energy-rgb), .18); padding: .35rem .6rem; font-weight: 900; color: var(--home-accent-health); }
        .home-view-all { display: inline-flex; align-items: center; gap: .35rem; color: var(--home-accent-energy); font-size: .85rem; font-weight: 800; transition: color .16s ease; }
        .home-view-all:hover { color: var(--home-accent-carbon); }
        body.home-body .home-header .text-slate-900, body.home-body .home-header .text-slate-700 { color: var(--home-text); }
        body.home-body .home-header .text-slate-600 { color: var(--home-text-secondary); }
        .home-widget.energy .home-widget-value { color: var(--home-accent-energy); }
        .home-widget.carbon .home-widget-value { color: var(--home-accent-carbon); }
        body.themed-body { background: radial-gradient(ellipse at 72% 10%, rgba(var(--home-accent-energy-rgb), .18), transparent 36%), radial-gradient(ellipse at 16% 34%, rgba(var(--home-accent-carbon-rgb), .13), transparent 30%), linear-gradient(145deg, var(--home-bg) 0%, var(--home-bg-deep) 100%); color: var(--home-text); }
        body.themed-body::before { content: ""; position: fixed; inset: 0; pointer-events: none; background-image: linear-gradient(var(--home-grid) 1px, transparent 1px), linear-gradient(90deg, var(--home-grid) 1px, transparent 1px); background-size: 44px 44px; mask-image: linear-gradient(180deg, rgba(0,0,0,.7), transparent 72%); }
        body.themed-body .home-header .text-slate-900, body.themed-body .home-header .text-slate-700 { color: var(--home-text); }
        body.themed-body .home-header .text-slate-600 { color: var(--home-text-secondary); }
        .runs-hero { position: relative; z-index: 1; display: flex; flex-direction: column; gap: .65rem; padding: 1.55rem 0 .35rem; }
        .runs-title { color: var(--home-text); font-size: clamp(2.1rem, 4vw, 3.6rem); line-height: .96; font-weight: 900; letter-spacing: 0; }
        .runs-subtitle { color: var(--home-text-secondary); font-size: 1rem; line-height: 1.6; font-weight: 500; }
        .runs-panel { position: relative; z-index: 1; border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.6rem; box-shadow: var(--home-shadow-soft); backdrop-filter: blur(18px); overflow: hidden; }
        .runs-toolbar { display: grid; grid-template-columns: minmax(0, 1fr) minmax(160px, 220px); gap: .8rem; align-items: center; padding: 1rem; border-bottom: 1px solid var(--home-border); }
        .runs-search, .runs-filter { width: 100%; border-radius: 999px; border: 1px solid var(--home-border); background: var(--home-surface-muted); color: var(--home-text); padding: .72rem .95rem; font-size: .9rem; font-weight: 650; outline: none; transition: border-color .16s ease, box-shadow .16s ease, background .16s ease; }
        .runs-search::placeholder { color: var(--home-text-muted); }
        .runs-search:focus, .runs-filter:focus { border-color: var(--home-accent-carbon); box-shadow: 0 0 0 3px rgba(var(--home-accent-carbon-rgb), .18); }
        .runs-count { color: var(--home-text-muted); font-size: .82rem; font-weight: 800; padding: 0 1rem .85rem; }
        .runs-list { display: grid; gap: .25rem; padding: .75rem; }
        .runs-row { display: grid; grid-template-columns: minmax(0, 1.55fr) .72fr .82fr .7fr 1.4rem; gap: .8rem; align-items: center; min-height: 68px; border: 1px solid transparent; border-radius: 1.05rem; padding: .75rem .85rem; color: var(--home-text-secondary); text-decoration: none; transition: background .16s ease, transform .16s ease, border-color .16s ease, box-shadow .16s ease; }
        .runs-row:hover, .runs-row:focus-visible { background: var(--home-surface-muted); border-color: var(--home-border-strong); box-shadow: 0 14px 34px rgba(var(--home-accent-energy-rgb), .10); transform: translateX(2px); outline: none; }
        .runs-head { min-height: auto; padding-block: .45rem; color: var(--home-text-muted); font-size: .68rem; font-weight: 850; text-transform: uppercase; letter-spacing: .08em; pointer-events: none; }
        .runs-id { color: var(--home-text); font-size: .98rem; font-weight: 900; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .runs-pipeline { margin-top: .12rem; color: var(--home-text-muted); font-size: .76rem; font-weight: 700; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .runs-status { display: inline-flex; align-items: center; gap: .35rem; width: fit-content; border-radius: 999px; padding: .33rem .58rem; font-size: .68rem; font-weight: 850; text-transform: uppercase; }
        .runs-status.success { background: rgba(var(--home-accent-energy-rgb), .13); color: var(--home-accent-energy); }
        .runs-status.skipped { background: rgba(245, 158, 11, .14); color: var(--home-warning); }
        .runs-status.cancelled { background: rgba(148, 163, 184, .14); color: var(--home-text-muted); }
        .runs-status.failed { background: rgba(244, 63, 94, .14); color: var(--home-critical); }
        .runs-carbon { color: var(--home-accent-carbon); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-weight: 850; }
        .runs-health { display: inline-flex; align-items: baseline; gap: .15rem; width: fit-content; border-radius: 999px; background: rgba(var(--home-accent-energy-rgb), .10); border: 1px solid rgba(var(--home-accent-energy-rgb), .18); padding: .36rem .65rem; color: var(--home-accent-health); font-weight: 900; }
        .runs-empty { display: none; margin: 0 .75rem .85rem; border: 1px solid var(--home-border); border-radius: 1.15rem; background: var(--home-surface-muted); padding: 1.35rem; color: var(--home-text-secondary); }
        .runs-reset { margin-top: .85rem; display: inline-flex; align-items: center; gap: .35rem; color: var(--home-accent-energy); font-weight: 850; font-size: .85rem; }
        .runs-reset:focus-visible { outline: 2px solid var(--home-accent-carbon); outline-offset: 3px; border-radius: .5rem; }
        .run-hero { position: relative; z-index: 1; display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(280px, .75fr); gap: 1.1rem; align-items: stretch; padding: 1.35rem 0 .15rem; }
        .run-identity { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.55rem; padding: 1.25rem; box-shadow: var(--home-shadow-soft); overflow: hidden; }
        .run-title { color: var(--home-text); font-size: clamp(2rem, 3.8vw, 3.45rem); line-height: .98; font-weight: 900; letter-spacing: 0; overflow-wrap: anywhere; }
        .run-pipeline { color: var(--home-text-secondary); font-size: .98rem; line-height: 1.55; font-weight: 700; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .run-status { display: inline-flex; align-items: center; gap: .42rem; width: fit-content; border-radius: 999px; padding: .42rem .72rem; font-size: .72rem; font-weight: 900; text-transform: uppercase; }
        .run-status.success { background: rgba(var(--home-accent-energy-rgb), .13); color: var(--home-accent-energy); }
        .run-status.failed { background: rgba(244, 63, 94, .14); color: var(--home-critical); }
        .run-timing { border: 1px solid var(--home-border); background: linear-gradient(145deg, rgba(var(--home-accent-carbon-rgb), .10), var(--home-surface)); border-radius: 1.55rem; padding: 1.1rem; box-shadow: var(--home-shadow-soft); display: grid; gap: .7rem; }
        .run-time-item { display: flex; justify-content: space-between; gap: 1rem; border-bottom: 1px solid var(--home-border); padding-bottom: .65rem; }
        .run-time-item:last-child { border-bottom: 0; padding-bottom: 0; }
        .run-label { color: var(--home-text-muted); font-size: .68rem; font-weight: 850; text-transform: uppercase; letter-spacing: .08em; }
        .run-value { color: var(--home-text); font-size: .9rem; font-weight: 850; text-align: right; overflow-wrap: anywhere; }
        .run-health-panel { position: relative; z-index: 1; display: grid; grid-template-columns: minmax(190px, .35fr) minmax(0, 1fr); gap: 1.1rem; align-items: center; border: 1px solid var(--home-border-strong); background: linear-gradient(145deg, rgba(var(--home-accent-energy-rgb), .18), var(--home-surface-strong) 48%, var(--home-surface)); border-radius: 1.65rem; padding: 1.15rem; box-shadow: var(--home-shadow); overflow: hidden; }
        .run-health-score { display: flex; align-items: center; gap: 1rem; }
        .run-health-ring { position: relative; width: 6.1rem; height: 6.1rem; border-radius: 999px; display: grid; place-items: center; background: conic-gradient(var(--home-accent-health) calc(var(--score) * 1%), rgba(148,163,184,.22) 0); box-shadow: 0 0 42px rgba(var(--home-accent-energy-rgb), .22); }
        .run-health-ring::before { content: ""; position: absolute; width: 4.55rem; height: 4.55rem; border-radius: 999px; background: var(--home-surface-strong); }
        .run-health-ring span { position: relative; z-index: 1; color: var(--home-text); font-size: 1.5rem; line-height: 1; font-weight: 900; }
        .run-health-grade { display: inline-flex; width: fit-content; border-radius: 999px; background: rgba(var(--home-accent-energy-rgb), .12); color: var(--home-accent-health); border: 1px solid rgba(var(--home-accent-energy-rgb), .20); padding: .35rem .65rem; font-size: .72rem; font-weight: 900; text-transform: uppercase; }
        .run-summary-strip { position: relative; z-index: 1; display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: .75rem; }
        .run-metric { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.15rem; padding: .9rem; min-height: 98px; box-shadow: var(--home-shadow-soft); }
        .run-metric-value { margin-top: .7rem; color: var(--home-text); font-size: clamp(1.15rem, 1.8vw, 1.65rem); line-height: 1.1; font-weight: 900; overflow-wrap: anywhere; }
        .run-metric.energy .run-metric-value { color: var(--home-accent-energy); }
        .run-metric.carbon .run-metric-value { color: var(--home-accent-carbon); }
        .run-metric.warning .run-metric-value { color: var(--home-warning); }
        .run-metric.critical .run-metric-value { color: var(--home-critical); }
        .lifecycle-flow { position: relative; z-index: 1; }
        .lifecycle-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1rem; align-items: stretch; }
        .lifecycle-card { position: relative; display: flex; flex-direction: column; border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.35rem; padding: 1.15rem; box-shadow: var(--home-shadow-soft); min-height: 216px; overflow: hidden; }
        .lifecycle-card::after { content: ""; position: absolute; left: 1rem; right: 1rem; bottom: .85rem; height: 3px; border-radius: 999px; background: linear-gradient(90deg, rgba(var(--home-accent-energy-rgb), .78), transparent); }
        .lifecycle-card.deploy::after { background: linear-gradient(90deg, rgba(var(--home-accent-carbon-rgb), .82), transparent); }
        .lifecycle-card.disabled { border-style: dashed; background: var(--home-surface-muted); opacity: .82; }
        .lifecycle-card.disabled::after { background: linear-gradient(90deg, rgba(148,163,184,.55), transparent); }
        .lifecycle-top { display: flex; align-items: center; justify-content: space-between; gap: .8rem; padding-bottom: .95rem; border-bottom: 1px solid var(--home-border); }
        .lifecycle-stage-title { display: flex; align-items: center; gap: .7rem; min-width: 0; }
        .lifecycle-stage-icon { width: 2.35rem; height: 2.35rem; display: inline-flex; align-items: center; justify-content: center; flex: 0 0 auto; border-radius: .95rem; border: 1px solid rgba(var(--home-accent-energy-rgb), .25); background: rgba(var(--home-accent-energy-rgb), .14); color: var(--home-accent-energy); box-shadow: 0 0 24px rgba(var(--home-accent-energy-rgb), .14); }
        .lifecycle-card.deploy .lifecycle-stage-icon { border-color: rgba(var(--home-accent-carbon-rgb), .25); background: rgba(var(--home-accent-carbon-rgb), .13); color: var(--home-accent-carbon); box-shadow: 0 0 24px rgba(var(--home-accent-carbon-rgb), .12); }
        .lifecycle-name { color: var(--home-text); font-size: 1.28rem; line-height: 1.15; font-weight: 900; }
        .lifecycle-status { flex: 0 0 auto; color: var(--home-text-secondary); font-size: .78rem; font-weight: 900; text-transform: uppercase; }
        .lifecycle-card.disabled .lifecycle-name, .lifecycle-card.disabled .lifecycle-status { color: var(--home-text-muted); }
        .lifecycle-metrics { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .9rem; margin-top: 1.1rem; padding-bottom: 1rem; border-bottom: 1px solid var(--home-border); }
        .lifecycle-metric-label { display: inline-flex; align-items: center; gap: .42rem; color: var(--home-text-muted); font-size: .74rem; font-weight: 850; text-transform: uppercase; letter-spacing: .08em; }
        .lifecycle-metric-label.energy i { color: var(--home-accent-energy); }
        .lifecycle-metric-label.carbon i { color: var(--home-accent-carbon); }
        .lifecycle-metric-value { margin-top: .55rem; color: var(--home-text); font-size: clamp(1.25rem, 2vw, 1.75rem); line-height: 1.12; font-weight: 900; text-align: left; overflow-wrap: anywhere; }
        .lifecycle-metric-value.energy { color: var(--home-accent-energy); }
        .lifecycle-metric-value.carbon { color: var(--home-accent-carbon); }
        .lifecycle-link { margin-top: auto; padding-top: .95rem; display: inline-flex; align-items: center; gap: .45rem; color: var(--home-accent-energy); font-size: .94rem; font-weight: 900; transition: color .16s ease; }
        .lifecycle-link:hover { color: var(--home-accent-carbon); }
        .compare-hero { position: relative; z-index: 1; display: grid; grid-template-columns: minmax(0, 1.3fr) minmax(280px, .7fr); gap: 1rem; align-items: stretch; padding-top: 1rem; }
        .compare-hero-card, .compare-panel, .compare-run-card, .compare-summary-card { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.45rem; padding: 1rem; box-shadow: var(--home-shadow-soft); backdrop-filter: blur(18px); }
        .compare-title { margin-top: .55rem; color: var(--home-text); font-size: clamp(2.35rem, 5vw, 4rem); line-height: .94; font-weight: 900; letter-spacing: 0; }
        .compare-title span { color: var(--home-accent-energy); }
        .compare-subtitle { margin-top: .8rem; max-width: 780px; color: var(--home-text-secondary); font-size: .98rem; line-height: 1.58; font-weight: 650; }
        .compare-selector-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1rem; }
        .compare-selector-card { border: 1px solid var(--home-border); background: var(--home-surface-muted); border-radius: 1.15rem; padding: .9rem; }
        .compare-input { width: 100%; margin-top: .65rem; border: 1px solid var(--home-border); background: var(--home-surface-strong); color: var(--home-text); border-radius: .9rem; padding: .78rem .85rem; font-size: .9rem; font-weight: 750; outline: none; }
        .compare-input:focus { border-color: rgba(var(--home-accent-energy-rgb), .55); box-shadow: 0 0 0 3px rgba(var(--home-accent-energy-rgb), .14); }
        .compare-actions { display: flex; flex-wrap: wrap; gap: .6rem; align-items: center; margin-top: .9rem; }
        .compare-button { display: inline-flex; align-items: center; gap: .4rem; border-radius: 999px; background: rgba(var(--home-accent-energy-rgb), .16); color: var(--home-accent-energy); border: 1px solid rgba(var(--home-accent-energy-rgb), .24); padding: .62rem .9rem; font-size: .84rem; font-weight: 900; transition: background .16s ease, color .16s ease; }
        .compare-button:hover { color: var(--home-accent-carbon); background: rgba(var(--home-accent-carbon-rgb), .14); border-color: rgba(var(--home-accent-carbon-rgb), .24); }
        .compare-run-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1rem; }
        .compare-run-card { min-height: 238px; display: flex; flex-direction: column; }
        .compare-run-head { display: flex; align-items: flex-start; justify-content: space-between; gap: .9rem; padding-bottom: .9rem; border-bottom: 1px solid var(--home-border); }
        .compare-run-title { color: var(--home-text); font-size: clamp(1.3rem, 2.3vw, 1.9rem); line-height: 1.08; font-weight: 900; overflow-wrap: anywhere; }
        .compare-run-meta { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .65rem; margin-top: 1rem; }
        .compare-mini { border: 1px solid var(--home-border); background: var(--home-surface-muted); border-radius: .95rem; padding: .72rem; min-height: 76px; }
        .compare-mini-value { margin-top: .35rem; color: var(--home-text); font-size: .92rem; line-height: 1.25; font-weight: 900; overflow-wrap: anywhere; }
        .compare-main-grid { display: grid; gap: .85rem; padding: 1rem; }
        .compare-vs-card { display: grid; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr); gap: .85rem; align-items: stretch; border: 1px solid var(--home-border); background: var(--home-surface-muted); border-radius: 1.15rem; padding: .85rem; }
        .compare-side { border: 1px solid var(--home-border); background: var(--home-surface-strong); border-radius: 1rem; padding: .95rem; min-height: 132px; display: flex; flex-direction: column; justify-content: center; }
        .compare-side.better { background: rgba(var(--home-accent-energy-rgb), .13); border-color: rgba(var(--home-accent-energy-rgb), .30); box-shadow: inset 0 0 0 1px rgba(var(--home-accent-energy-rgb), .08); }
        .compare-side.worse, .compare-side.equal, .compare-side.neutral { background: rgba(245, 158, 11, .11); border-color: rgba(245, 158, 11, .26); }
        .compare-side-label { color: var(--home-text-muted); font-size: .68rem; font-weight: 900; letter-spacing: .1em; text-transform: uppercase; }
        .compare-side-value { margin-top: .5rem; color: var(--home-text); font-size: clamp(1.65rem, 3vw, 2.55rem); line-height: 1; font-weight: 900; overflow-wrap: anywhere; }
        .compare-side.better .compare-side-value { color: var(--home-accent-energy); }
        .compare-result { margin-top: .72rem; display: inline-flex; align-items: center; gap: .38rem; width: fit-content; max-width: 100%; border-radius: 999px; padding: .34rem .58rem; font-size: .74rem; font-weight: 900; text-transform: uppercase; color: var(--home-text-muted); }
        .compare-side.better .compare-result { color: var(--home-accent-energy); background: rgba(var(--home-accent-energy-rgb), .12); }
        .compare-side.worse .compare-result, .compare-side.equal .compare-result, .compare-side.neutral .compare-result { color: var(--home-warning); background: rgba(245, 158, 11, .11); }
        .compare-center { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: .55rem; min-width: 132px; text-align: center; }
        .compare-center-label { color: var(--home-text); font-size: .78rem; line-height: 1.2; font-weight: 950; letter-spacing: .1em; text-transform: uppercase; }
        .compare-metric-icon { width: 2.1rem; height: 2.1rem; display: inline-flex; align-items: center; justify-content: center; border-radius: .78rem; background: rgba(var(--home-accent-energy-rgb), .13); color: var(--home-accent-energy); border: 1px solid rgba(var(--home-accent-energy-rgb), .22); }
        .compare-empty { border: 1px dashed var(--home-border-strong); background: var(--home-surface-muted); border-radius: 1.35rem; padding: 1.4rem; color: var(--home-text-secondary); }
        .compare-alert { border: 1px solid rgba(245, 158, 11, .28); background: rgba(245, 158, 11, .11); color: var(--home-warning); border-radius: 1rem; padding: .85rem; font-size: .86rem; font-weight: 800; }
        .release-page { position: relative; z-index: 1; }
        .release-hero { display: grid; grid-template-columns: minmax(0, 1.15fr) minmax(280px, .85fr); gap: 1rem; align-items: stretch; padding-top: 1rem; }
        .release-panel { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.45rem; padding: 1rem; box-shadow: var(--home-shadow-soft); backdrop-filter: blur(18px); overflow: hidden; }
        .release-hero-main { position: relative; min-height: 188px; }
        .release-hero-main::after { content: ""; position: absolute; right: -3rem; bottom: -4rem; width: 14rem; height: 14rem; border-radius: 999px; background: radial-gradient(circle, rgba(var(--home-accent-energy-rgb), .24), transparent 62%); pointer-events: none; }
        .release-title { color: var(--home-text); font-size: clamp(2.45rem, 5vw, 4.25rem); line-height: .92; font-weight: 900; letter-spacing: 0; }
        .release-subtitle { color: var(--home-text-secondary); font-size: .96rem; line-height: 1.55; font-weight: 700; overflow-wrap: anywhere; }
        .release-chip-row { display: flex; flex-wrap: wrap; gap: .45rem; align-items: center; }
        .release-chip { display: inline-flex; align-items: center; gap: .38rem; width: fit-content; border-radius: 999px; border: 1px solid var(--home-border); background: var(--home-surface-muted); color: var(--home-text-secondary); padding: .38rem .64rem; font-size: .7rem; font-weight: 900; text-transform: uppercase; }
        .release-chip.success, .release-chip.applied { background: rgba(var(--home-accent-energy-rgb), .13); color: var(--home-accent-energy); border-color: rgba(var(--home-accent-energy-rgb), .22); }
        .release-chip.skipped, .release-chip.bypassed { background: rgba(245, 158, 11, .14); color: var(--home-warning); border-color: rgba(245, 158, 11, .24); }
        .release-chip.failed, .release-chip.critical { background: rgba(244, 63, 94, .14); color: var(--home-critical); border-color: rgba(244, 63, 94, .24); }
        .release-split-card { display: grid; gap: .7rem; }
        .release-fact { display: flex; align-items: center; justify-content: space-between; gap: .85rem; border-bottom: 1px solid var(--home-border); padding-bottom: .68rem; }
        .release-fact:last-child { border-bottom: 0; padding-bottom: 0; }
        .release-fact-value { color: var(--home-text); font-size: .95rem; font-weight: 900; text-align: right; overflow-wrap: anywhere; }
        .release-measure-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: .75rem; }
        .release-measure { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.1rem; padding: .85rem; min-height: 110px; box-shadow: var(--home-shadow-soft); }
        .release-measure-top { display: flex; align-items: center; justify-content: space-between; gap: .5rem; }
        .release-measure-icon { width: 2.1rem; height: 2.1rem; display: inline-flex; align-items: center; justify-content: center; border-radius: .78rem; border: 1px solid var(--home-border); background: var(--home-surface-muted); color: var(--home-text-secondary); }
        .release-measure-value { margin-top: .85rem; color: var(--home-text); font-size: clamp(1.08rem, 1.7vw, 1.45rem); line-height: 1.08; font-weight: 900; overflow-wrap: anywhere; }
        .release-measure.energy .release-measure-value { color: var(--home-accent-energy); }
        .release-measure.carbon .release-measure-value { color: var(--home-accent-carbon); }
        .release-measure.cpu .release-measure-value { color: var(--home-accent-carbon); }
        .release-two-col { display: grid; grid-template-columns: minmax(0, 1fr) minmax(320px, .82fr); gap: .9rem; align-items: stretch; }
        .release-decision-primary { display: grid; grid-template-columns: minmax(140px, .55fr) minmax(0, 1fr); gap: .9rem; align-items: stretch; }
        .release-probability { min-height: 170px; display: grid; place-items: center; border: 1px solid rgba(var(--home-accent-energy-rgb), .25); background: radial-gradient(circle at 50% 38%, rgba(var(--home-accent-energy-rgb), .18), transparent 60%), var(--home-surface-muted); border-radius: 1.2rem; text-align: center; }
        .release-probability-value { color: var(--home-accent-energy); font-size: clamp(2.2rem, 4vw, 3.35rem); line-height: 1; font-weight: 900; }
        .release-decision-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .65rem; }
        .release-decision-wide { grid-column: 1 / -1; }
        .release-mini { border: 1px solid var(--home-border); background: var(--home-surface-muted); border-radius: 1rem; padding: .78rem; min-height: 86px; }
        .release-mini-value { margin-top: .45rem; color: var(--home-text); font-size: .98rem; line-height: 1.25; font-weight: 900; overflow-wrap: anywhere; }
        .release-work-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: .65rem; }
        .release-flow { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: .55rem; margin-top: .75rem; }
        .release-step { border: 1px solid var(--home-border); background: var(--home-surface-muted); border-radius: .95rem; padding: .72rem; }
        .release-alert { border: 1px solid rgba(245, 158, 11, .28); background: rgba(245, 158, 11, .11); color: var(--home-warning); border-radius: 1rem; padding: .85rem; font-size: .86rem; font-weight: 800; }
        .release-intel-head { border: 1px solid var(--home-border-strong); background: linear-gradient(145deg, rgba(var(--home-accent-carbon-rgb), .13), rgba(var(--home-accent-energy-rgb), .10), var(--home-surface)); border-radius: 1.25rem; padding: 1rem; }
        .release-intel-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .8rem; }
        .release-intel-card { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.15rem; padding: .9rem; min-height: 210px; box-shadow: var(--home-shadow-soft); }
        .release-anomaly-item { border: 1px solid var(--home-border); background: var(--home-surface-muted); border-radius: .95rem; padding: .75rem; }
        .release-muted { color: var(--home-text-muted); }
        .release-text { color: var(--home-text-secondary); }
        .release-strong { color: var(--home-text); }
        .deploy-page { position: relative; z-index: 1; }
        .deploy-hero { display: grid; grid-template-columns: minmax(0, 1.15fr) minmax(280px, .85fr); gap: 1rem; align-items: stretch; padding-top: 1rem; }
        .deploy-panel { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.45rem; padding: 1rem; box-shadow: var(--home-shadow-soft); backdrop-filter: blur(18px); overflow: hidden; }
        .deploy-hero-main { position: relative; min-height: 188px; }
        .deploy-hero-main::after { content: ""; position: absolute; right: -3rem; bottom: -4rem; width: 14rem; height: 14rem; border-radius: 999px; background: radial-gradient(circle, rgba(var(--home-accent-carbon-rgb), .24), transparent 62%); pointer-events: none; }
        .deploy-title { color: var(--home-text); font-size: clamp(2.45rem, 5vw, 4.25rem); line-height: .92; font-weight: 900; letter-spacing: 0; }
        .deploy-subtitle { color: var(--home-text-secondary); font-size: .96rem; line-height: 1.55; font-weight: 700; overflow-wrap: anywhere; }
        .deploy-chip-row { display: flex; flex-wrap: wrap; gap: .45rem; align-items: center; }
        .deploy-chip { display: inline-flex; align-items: center; gap: .38rem; width: fit-content; border-radius: 999px; border: 1px solid var(--home-border); background: var(--home-surface-muted); color: var(--home-text-secondary); padding: .38rem .64rem; font-size: .7rem; font-weight: 900; text-transform: uppercase; }
        .deploy-chip.success { background: rgba(var(--home-accent-energy-rgb), .13); color: var(--home-accent-energy); border-color: rgba(var(--home-accent-energy-rgb), .22); }
        .deploy-chip.skipped { background: rgba(245, 158, 11, .14); color: var(--home-warning); border-color: rgba(245, 158, 11, .24); }
        .deploy-chip.failed, .deploy-chip.critical { background: rgba(244, 63, 94, .14); color: var(--home-critical); border-color: rgba(244, 63, 94, .24); }
        .deploy-chip.carbon { background: rgba(var(--home-accent-carbon-rgb), .13); color: var(--home-accent-carbon); border-color: rgba(var(--home-accent-carbon-rgb), .22); }
        .deploy-fact { display: flex; align-items: center; justify-content: space-between; gap: .85rem; border-bottom: 1px solid var(--home-border); padding-bottom: .68rem; }
        .deploy-fact:last-child { border-bottom: 0; padding-bottom: 0; }
        .deploy-fact-value { color: var(--home-text); font-size: .95rem; font-weight: 900; text-align: right; overflow-wrap: anywhere; }
        .deploy-measure-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: .7rem; }
        .deploy-measure { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.05rem; padding: .78rem; min-height: 108px; box-shadow: var(--home-shadow-soft); }
        .deploy-measure-top { display: flex; align-items: center; justify-content: space-between; gap: .5rem; }
        .deploy-measure-icon { width: 2rem; height: 2rem; display: inline-flex; align-items: center; justify-content: center; border-radius: .75rem; border: 1px solid var(--home-border); background: var(--home-surface-muted); color: var(--home-text-secondary); }
        .deploy-measure-value { margin-top: .75rem; color: var(--home-text); font-size: clamp(1rem, 1.55vw, 1.32rem); line-height: 1.08; font-weight: 900; overflow-wrap: anywhere; }
        .deploy-measure.energy .deploy-measure-value { color: var(--home-accent-energy); }
        .deploy-measure.carbon .deploy-measure-value { color: var(--home-accent-carbon); }
        .deploy-measure.cpu .deploy-measure-value, .deploy-measure.memory .deploy-measure-value { color: var(--home-accent-carbon); }
        .deploy-two-col { display: grid; grid-template-columns: minmax(0, 1fr) minmax(320px, .9fr); gap: .9rem; align-items: stretch; }
        .deploy-strategy-value { color: var(--home-accent-carbon); font-size: clamp(2rem, 3.4vw, 3rem); line-height: .98; font-weight: 900; overflow-wrap: anywhere; }
        .deploy-mini-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .65rem; }
        .deploy-mini { border: 1px solid var(--home-border); background: var(--home-surface-muted); border-radius: 1rem; padding: .78rem; min-height: 86px; }
        .deploy-mini-value { margin-top: .45rem; color: var(--home-text); font-size: .98rem; line-height: 1.25; font-weight: 900; overflow-wrap: anywhere; }
        .deploy-resource-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .65rem; margin-top: .75rem; }
        .deploy-bar-track { height: .46rem; border-radius: 999px; background: rgba(148, 163, 184, .20); overflow: hidden; margin-top: .65rem; }
        .deploy-bar-fill { height: 100%; border-radius: inherit; background: linear-gradient(90deg, var(--home-accent-carbon), var(--home-accent-energy)); }
        .deploy-timeline { display: grid; gap: .7rem; }
        .deploy-event { display: grid; grid-template-columns: 2rem minmax(0, 1fr); gap: .65rem; align-items: start; }
        .deploy-dot { width: .85rem; height: .85rem; border-radius: 999px; background: var(--home-accent-carbon); box-shadow: 0 0 0 5px rgba(var(--home-accent-carbon-rgb), .13); margin: .25rem auto 0; }
        .deploy-event-body { border: 1px solid var(--home-border); background: var(--home-surface-muted); border-radius: 1rem; padding: .75rem; }
        .deploy-snapshot-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .55rem; margin-top: .55rem; }
        .deploy-alert { border: 1px solid rgba(245, 158, 11, .28); background: rgba(245, 158, 11, .11); color: var(--home-warning); border-radius: 1rem; padding: .85rem; font-size: .86rem; font-weight: 800; }
        .deploy-intel-head { border: 1px solid var(--home-border-strong); background: linear-gradient(145deg, rgba(var(--home-accent-carbon-rgb), .16), rgba(var(--home-accent-energy-rgb), .08), var(--home-surface)); border-radius: 1.25rem; padding: 1rem; }
        .deploy-intel-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .8rem; }
        .deploy-intel-card { border: 1px solid var(--home-border); background: linear-gradient(145deg, var(--home-surface-strong), var(--home-surface)); border-radius: 1.15rem; padding: .9rem; min-height: 210px; box-shadow: var(--home-shadow-soft); }
        .deploy-anomaly-item { border: 1px solid var(--home-border); background: var(--home-surface-muted); border-radius: .95rem; padding: .75rem; }
        @media (max-width: 767px) {
            .home-hero { padding-top: 2rem; }
            .home-section-heading { align-items: flex-start; flex-direction: column; }
            .home-run-row { grid-template-columns: minmax(0, 1.4fr) .78fr .82fr .72fr 1rem; gap: .45rem; padding: .85rem .55rem; }
            .home-run-head { font-size: .6rem; }
            .home-run-row { font-size: .78rem; }
            .home-run-status { font-size: .58rem; padding: .22rem .42rem; }
            .runs-toolbar { grid-template-columns: 1fr; }
            .runs-row { grid-template-columns: minmax(0, 1fr) auto; gap: .55rem; min-height: 78px; }
            .runs-head { display: none; }
            .runs-row > :nth-child(2), .runs-row > :nth-child(3), .runs-row > :nth-child(4) { grid-column: auto; }
            .runs-row > :last-child { grid-column: 2; grid-row: 1 / span 2; }
            .runs-id, .runs-pipeline { max-width: 100%; }
            .run-hero, .run-health-panel, .run-summary-strip, .lifecycle-grid, .compare-hero, .compare-selector-grid, .compare-run-grid { grid-template-columns: 1fr; }
            .compare-vs-card { grid-template-columns: 1fr; }
            .compare-center { min-width: 0; order: -1; }
            .run-pipeline { white-space: normal; }
            .release-hero, .release-measure-grid, .release-two-col, .release-decision-primary, .release-decision-grid, .release-work-grid, .release-flow, .release-intel-grid, .deploy-hero, .deploy-measure-grid, .deploy-two-col, .deploy-mini-grid, .deploy-resource-grid, .deploy-snapshot-grid, .deploy-intel-grid { grid-template-columns: 1fr; }
        }
        @media (min-width: 768px) and (max-width: 1179px) {
            .run-summary-strip { grid-template-columns: repeat(3, minmax(0, 1fr)); }
            .lifecycle-grid { grid-template-columns: 1fr; }
            .compare-hero, .compare-run-grid, .compare-selector-grid { grid-template-columns: 1fr; }
            .compare-center { min-width: 112px; }
            .release-measure-grid, .release-intel-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .release-two-col { grid-template-columns: 1fr; }
            .deploy-measure-grid, .deploy-intel-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .deploy-two-col { grid-template-columns: 1fr; }
        }
    </style>
</head>
{% set release_stage_page = page == 'stage' and stage_detail is defined and stage_detail.key == 'release' %}
{% set deploy_stage_page = page == 'stage' and stage_detail is defined and stage_detail.key == 'deploy' %}
{% set themed_page = page == 'home' or page == 'runs' or page == 'run' or page == 'compare' or release_stage_page or deploy_stage_page %}
<body class="p-4 md:p-8 {% if page == 'home' %}home-body themed-body{% elif themed_page %}themed-body{% endif %}">
    <div class="{% if themed_page %}home-shell{% else %}max-w-[1500px] mx-auto{% endif %} space-y-6">
        <header class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4 {% if themed_page %}home-header{% endif %}">
            <a href="/" class="flex items-center gap-3">
                <span class="{% if themed_page %}home-brand-mark{% else %}p-2 bg-emerald-100 rounded-lg{% endif %}"><i data-lucide="leaf" class="{% if themed_page %}w-5 h-5{% else %}w-8 h-8 text-emerald-600{% endif %}"></i></span>
                <span>
                    <span class="block {% if themed_page %}text-base font-black{% else %}text-3xl font-extrabold tracking-tight{% endif %} text-slate-900">{% if themed_page %}Green DevOps{% else %}Green DevOps Monitor{% endif %}</span>
                    <span class="block {% if themed_page %}text-xs{% else %}text-sm{% endif %} text-slate-600 font-medium">{% if themed_page %}Sustainability intelligence across the software delivery lifecycle{% else %}Centralized sustainability intelligence for Release, Deploy, and Operate{% endif %}</span>
                </span>
            </a>
            <div class="flex flex-wrap items-center gap-2">
                <a href="/" class="{% if page == 'home' %}home-nav-link active{% elif themed_page %}home-nav-link{% else %}rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50{% endif %}"><i data-lucide="house" class="{% if themed_page %}w-4 h-4{% else %}hidden{% endif %}"></i>Home</a>
                <a href="/runs" class="{% if page == 'runs' or page == 'run' or release_stage_page or deploy_stage_page %}home-nav-link active{% elif themed_page %}home-nav-link{% else %}rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50{% endif %}"><i data-lucide="list" class="{% if themed_page %}w-4 h-4{% else %}hidden{% endif %}"></i>Runs</a>
                <a href="/compare" class="{% if page == 'compare' %}home-nav-link active{% elif themed_page %}home-nav-link{% else %}rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50{% endif %}"><i data-lucide="columns-3" class="{% if themed_page %}w-4 h-4{% else %}hidden{% endif %}"></i>Compare</a>
                <a href="/operate" class="{% if themed_page %}home-nav-link{% else %}rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50{% endif %}"><i data-lucide="activity" class="{% if themed_page %}w-4 h-4{% else %}hidden{% endif %}"></i>Operate</a>
                {% if release_stage_page or deploy_stage_page %}<a href="/run/{{ selected_run|urlencode }}" class="home-nav-link"><i data-lucide="arrow-left" class="w-4 h-4"></i>Back to Run</a>{% endif %}
                {% if themed_page %}
                <div class="theme-toggle" role="group" aria-label="Color theme">
                    <button type="button" class="theme-light" data-theme-choice="light" aria-label="Use light theme"><i data-lucide="sun" class="w-4 h-4"></i></button>
                    <button type="button" class="theme-dark" data-theme-choice="dark" aria-label="Use dark theme"><i data-lucide="moon" class="w-4 h-4"></i></button>
                </div>
                {% endif %}
                <div class="{% if themed_page %}home-source-chip{% else %}console-chip rounded-xl px-4 py-2 flex items-center gap-2{% endif %}">
                    <span class="status-pulse bg-emerald-500"></span>
                    <span class="text-sm font-semibold text-slate-700">{{ data_source }}</span>
                </div>
            </div>
        </header>

        {% if page == 'home' %}
        <main class="space-y-6">
            <section class="home-hero">
                <div>
                    <p class="home-eyebrow">System Overview</p>
                    <h1 class="home-title">Green DevOps <span>Pipeline sustainability, made visible.</span></h1>
                    <p class="home-subtitle">A command center for energy use, carbon impact, health scoring, and recent delivery activity across monitored DevOps runs.</p>
                </div>
                <div class="home-signal" aria-hidden="true">
                    <div class="home-signal-wave"></div>
                    <span class="home-signal-node one"></span>
                    <span class="home-signal-node two"></span>
                    <span class="home-signal-node three"></span>
                    <div class="home-signal-caption"><span>Release</span><span>Deploy</span><span>Operate</span></div>
                </div>
            </section>
            <section class="home-kpi-grid">
                <div class="home-widget energy">
                    <div class="flex items-start justify-between gap-4">
                        <p class="home-widget-label">Total Energy</p>
                        <span class="home-widget-icon energy"><i data-lucide="zap" class="w-5 h-5"></i></span>
                    </div>
                    <p class="home-widget-value">{{ system_total_energy_display }}</p>
                    <p class="home-widget-note">Accumulated Monitor energy across all recorded pipeline runs.</p>
                </div>
                <div class="home-widget carbon">
                    <div class="flex items-start justify-between gap-4">
                        <p class="home-widget-label">Total Carbon</p>
                        <span class="home-widget-icon carbon"><i data-lucide="cloud" class="w-5 h-5"></i></span>
                    </div>
                    <p class="home-widget-value">{{ system_total_carbon_display }}</p>
                    <p class="home-widget-note">Total carbon footprint reported from Monitor measurements.</p>
                </div>
                <div class="home-widget health">
                    <div class="flex items-start justify-between gap-4">
                        <p class="home-widget-label">Average Health</p>
                        <div class="home-health-ring" style="--score: {{ average_health_score }};"><span>{{ average_health_score }}</span></div>
                    </div>
                    <p class="home-widget-value">{{ average_health_score }}<span class="text-base font-bold text-slate-400">/100</span></p>
                    <p class="home-widget-note">Average sustainability health score across completed runs.</p>
                </div>
                <div class="home-widget activity">
                    <div class="flex items-start justify-between gap-4">
                        <p class="home-widget-label">Pipeline Runs</p>
                        <span class="home-widget-icon text-slate-700"><i data-lucide="workflow" class="w-5 h-5"></i></span>
                    </div>
                    <p class="home-widget-value">{{ system_run_count }}</p>
                    <p class="home-widget-note">Runs currently available in the Monitor data source.</p>
                </div>
            </section>
            <section class="grid grid-cols-1 xl:grid-cols-[1.55fr_1fr] gap-5">
                <div class="home-panel">
                    <div class="home-section-heading">
                        <div>
                            <h2 class="home-section-title">Overall Sustainability Trends</h2>
                            <p class="home-section-subtitle">Energy and carbon movement across the latest monitored runs.</p>
                        </div>
                    </div>
                    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 p-4">
                        <div class="home-chart-card">
                            <div class="home-chart-title"><span>Energy trend</span><span class="text-emerald-700">kWh</span></div>
                            <div class="h-[220px]"><canvas id="runEnergyChart"></canvas></div>
                        </div>
                        <div class="home-chart-card">
                            <div class="home-chart-title"><span>Carbon trend</span><span class="text-sky-700">kg CO2e</span></div>
                            <div class="h-[220px]"><canvas id="runCarbonChart"></canvas></div>
                        </div>
                    </div>
                </div>
                <div class="home-panel overflow-hidden">
                    <div class="home-section-heading">
                        <div>
                            <h2 class="home-section-title">Recent Pipeline Runs</h2>
                            <p class="home-section-subtitle">Latest Monitor activity with sustainability context.</p>
                        </div>
                        <a href="/runs" class="home-view-all">View all runs <i data-lucide="chevron-right" class="w-4 h-4"></i></a>
                    </div>
                    <div class="home-run-list">
                        <div class="home-run-row home-run-head">
                            <span>Run</span><span>Status</span><span>Carbon</span><span>Health</span><span></span>
                        </div>
                        {% for run in recent_runs %}
                        <a href="/run/{{ run.run_id|urlencode }}" class="home-run-row">
                            <span class="home-run-id">#{{ run.run_id }}</span>
                            <span><span class="home-run-status {% if run.status == 'success' %}success{% elif run.status == 'skipped' %}skipped{% elif run.status in ['aborted', 'cancelled', 'canceled'] %}cancelled{% else %}failed{% endif %}">{{ run.status }}</span></span>
                            <span class="font-mono text-[var(--home-accent-carbon)]">{{ run.total_carbon_display }}</span>
                            <span><span class="home-health-pill">{{ run.health_score }}<span class="text-xs opacity-60">/100</span></span></span>
                            <span class="flex justify-end"><i data-lucide="arrow-right" class="w-4 h-4 text-[var(--home-text-muted)]"></i></span>
                        </a>
                        {% endfor %}
                    </div>
                </div>
            </section>
        </main>
        {% elif page == 'runs' %}
        <main class="space-y-6">
            <section class="runs-hero">
                <a href="/" class="home-view-all w-fit"><i data-lucide="arrow-left" class="w-4 h-4"></i> Home</a>
                <div>
                    <p class="home-eyebrow">Run Browser</p>
                    <h1 class="runs-title">Pipeline Runs</h1>
                    <p class="runs-subtitle">Browse and inspect monitored pipeline executions.</p>
                </div>
            </section>
            <section class="runs-panel">
                <div class="runs-toolbar">
                    <label class="sr-only" for="runsSearch">Search pipeline or run ID</label>
                    <input id="runsSearch" class="runs-search" type="search" placeholder="Search pipeline or run ID..." autocomplete="off">
                    <label class="sr-only" for="runsStatusFilter">Filter by status</label>
                    <select id="runsStatusFilter" class="runs-filter" aria-label="Filter by status">
                        <option value="all">All statuses</option>
                    </select>
                </div>
                <p id="runsCount" class="runs-count">{{ runs|length }} pipeline runs</p>
                <div class="runs-list" id="runsList">
                    <div class="runs-row runs-head">
                        <span>Run</span><span>Status</span><span>Carbon</span><span>Health</span><span></span>
                    </div>
                    {% for run in runs %}
                    <a href="/run/{{ run.run_id|urlencode }}" class="runs-row" data-run-row data-run-id="{{ run.run_id }}" data-pipeline-name="{{ run.pipeline_name_display }}" data-status="{{ run.status }}" title="{{ run.run_id }}">
                        <span>
                            <span class="runs-id">#{{ run.run_id }}</span>
                            {% if run.pipeline_name_display and run.pipeline_name_display != run.run_id %}
                            <span class="runs-pipeline">{{ run.pipeline_name_display }}</span>
                            {% endif %}
                        </span>
                        <span><span class="runs-status {% if run.status == 'success' %}success{% elif run.status == 'skipped' %}skipped{% elif run.status in ['aborted', 'cancelled', 'canceled'] %}cancelled{% else %}failed{% endif %}"><i data-lucide="{% if run.status == 'success' %}check-circle-2{% elif run.status == 'skipped' %}pause-circle{% elif run.status in ['aborted', 'cancelled', 'canceled'] %}circle-slash{% else %}alert-triangle{% endif %}" class="w-3 h-3"></i>{{ run.status }}</span></span>
                        <span class="runs-carbon">{{ run.total_carbon_display }}</span>
                        <span><span class="runs-health">{{ run.health_score }}<span class="text-xs opacity-60">/100</span></span></span>
                        <span class="flex justify-end"><i data-lucide="arrow-right" class="w-4 h-4 text-[var(--home-text-muted)]"></i></span>
                    </a>
                    {% endfor %}
                </div>
                <div id="runsEmpty" class="runs-empty">
                    <div class="flex gap-3">
                        <span class="home-widget-icon"><i data-lucide="search-x" class="w-5 h-5"></i></span>
                        <div>
                            <p class="font-black text-[var(--home-text)]">No matching pipeline runs</p>
                            <p class="text-sm mt-1">Adjust the search or status filter to show available monitored executions.</p>
                            <button type="button" id="runsReset" class="runs-reset">Reset filters <i data-lucide="rotate-ccw" class="w-4 h-4"></i></button>
                        </div>
                    </div>
                </div>
            </section>
        </main>
        {% elif page == 'compare' %}
        <main class="space-y-6">
            <section class="compare-hero">
                <div class="compare-hero-card">
                    <p class="home-eyebrow">Run Comparison</p>
                    <h1 class="compare-title">Green DevOps <span>Compare</span></h1>
                    <p class="compare-subtitle">Select any two historical pipeline executions. Run A is the reference, and Run B is compared against it for runtime, energy, carbon, resources, and health.</p>
                </div>
                <div class="compare-hero-card">
                    <p class="run-label">Comparison Rule</p>
                    <p class="compare-mini-value mt-2">Each metric highlights the better-performing run.</p>
                    <p class="home-widget-note">Lower is better for runtime, resource usage, energy, and carbon; higher is better for health score.</p>
                </div>
            </section>

            <section class="compare-panel">
                <div class="home-section-heading p-0">
                    <div>
                        <p class="home-eyebrow">Select Runs</p>
                        <h2 class="home-section-title">Run A and Run B</h2>
                    </div>
                </div>
                <form method="get" action="/compare" class="mt-4">
                    <div class="compare-selector-grid">
                        <div class="compare-selector-card">
                            <label for="compareRunA" class="run-label">Run A</label>
                            <input id="compareRunA" name="run_a" class="compare-input" list="compareRunOptions" value="{{ selected_run_a }}" placeholder="Search or select Run A" autocomplete="off">
                        </div>
                        <div class="compare-selector-card">
                            <label for="compareRunB" class="run-label">Run B</label>
                            <input id="compareRunB" name="run_b" class="compare-input" list="compareRunOptions" value="{{ selected_run_b }}" placeholder="Search or select Run B" autocomplete="off">
                        </div>
                    </div>
                    <datalist id="compareRunOptions">
                        {% for option in run_options %}
                        <option value="{{ option.run_id }}" label="{{ option.label }}"></option>
                        {% endfor %}
                    </datalist>
                    <div class="compare-actions">
                        <button type="submit" class="compare-button"><i data-lucide="git-compare" class="w-4 h-4"></i>Compare Runs</button>
                        <a href="/compare" class="home-view-all"><i data-lucide="rotate-ccw" class="w-4 h-4"></i>Reset</a>
                    </div>
                </form>
                {% if compare_error %}
                <div class="compare-alert mt-4">{{ compare_error }}</div>
                {% elif not selected_run_a and not selected_run_b %}
                <div class="compare-empty mt-4">
                    <p class="font-black text-[var(--home-text)]">Select two pipeline runs to compare their runtime, energy, carbon, resources, and health.</p>
                </div>
                {% elif not comparison_ready %}
                <div class="compare-empty mt-4">
                    <p class="font-black text-[var(--home-text)]">Select another run to start the comparison.</p>
                </div>
                {% endif %}
            </section>

            {% if comparison_ready %}
            <section class="compare-run-grid">
                {% for side, run in [('Run A', run_a), ('Run B', run_b)] %}
                <div class="compare-run-card">
                    <div class="compare-run-head">
                        <div>
                            <p class="home-eyebrow">{{ side }}</p>
                            <h2 class="compare-run-title">Run #{{ run.build_number if run.build_number != 'Not available' else run.run_id }}</h2>
                            <p class="home-widget-note" title="{{ run.run_id }}">{{ run.run_id }}</p>
                        </div>
                        <span class="runs-status {% if run.status == 'success' %}success{% elif run.status in ['aborted', 'cancelled', 'canceled'] %}cancelled{% else %}failed{% endif %}">{{ run.status }}</span>
                    </div>
                    <div class="compare-run-meta">
                        <div class="compare-mini"><p class="run-label">Strategy</p><p class="compare-mini-value">{{ run.strategy }}</p></div>
                        <div class="compare-mini"><p class="run-label">Optimizer</p><p class="compare-mini-value">{{ run.optimizer_status }}</p></div>
                    </div>
                </div>
                {% endfor %}
            </section>

            <section class="compare-panel">
                <div class="home-section-heading p-0">
                    <div>
                        <p class="home-eyebrow">Main Comparison</p>
                        <h2 class="home-section-title">Which run performed better?</h2>
                    </div>
                </div>
                <div class="compare-main-grid">
                    {% for metric in main_metrics %}
                    <div class="compare-vs-card">
                        <div class="compare-side {{ metric.a_class }}">
                            <p class="compare-side-label">Run A</p>
                            <p class="compare-side-value">{{ metric.a_display }}</p>
                            <span class="compare-result"><i data-lucide="{{ metric.a_icon }}" class="w-4 h-4"></i>{{ metric.a_result }}</span>
                        </div>
                        <div class="compare-center">
                            <span class="compare-metric-icon"><i data-lucide="{{ metric.icon }}" class="w-4 h-4"></i></span>
                            <p class="compare-center-label">{{ metric.label }}</p>
                        </div>
                        <div class="compare-side {{ metric.b_class }}">
                            <p class="compare-side-label">Run B</p>
                            <p class="compare-side-value">{{ metric.b_display }}</p>
                            <span class="compare-result"><i data-lucide="{{ metric.b_icon }}" class="w-4 h-4"></i>{{ metric.b_result }}</span>
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </section>
            {% endif %}
        </main>
        {% elif page == 'run' %}
        <main class="space-y-5">
            <section class="run-hero">
                <div class="run-identity">
                    <a href="/runs" class="home-view-all w-fit"><i data-lucide="arrow-left" class="w-4 h-4"></i> Pipeline Runs</a>
                    <div class="mt-4 flex flex-wrap items-center gap-2">
                        <p class="home-eyebrow">Pipeline Run</p>
                        <span class="run-status {% if selected_run_status == 'success' %}success{% else %}failed{% endif %}"><i data-lucide="{% if selected_run_status == 'success' %}check-circle-2{% else %}alert-triangle{% endif %}" class="w-4 h-4"></i>{{ selected_run_status }}</span>
                    </div>
                    <h1 class="run-title mt-3">Run #{{ selected_run }}</h1>
                    <p class="run-pipeline mt-3" title="{{ pipeline_name }}">{{ pipeline_name }}</p>
                    <p class="home-widget-note mt-4">One complete pipeline execution tracked across the Green DevOps lifecycle.</p>
                </div>
                <div class="run-timing">
                    <div class="run-time-item"><p class="run-label">Start</p><p class="run-value">{{ selected_run_start }}</p></div>
                    <div class="run-time-item"><p class="run-label">End</p><p class="run-value">{{ selected_run_end }}</p></div>
                    <div class="run-time-item"><p class="run-label">Duration</p><p class="run-value">{{ selected_run_duration_display }}</p></div>
                </div>
            </section>

            <section class="run-health-panel">
                <div class="run-health-score">
                    <div class="run-health-ring" style="--score: {{ health_score.score }};"><span>{{ health_score.score }}</span></div>
                    <div>
                        <p class="home-eyebrow">Whole Pipeline Health</p>
                        <h2 class="text-2xl font-black mt-2 text-[var(--home-text)]">Pipeline Sustainability Health</h2>
                        <div class="flex flex-wrap items-center gap-2 mt-3">
                            <span class="run-health-grade">{{ health_score.grade }}</span>
                            <span class="runs-status {% if health_score.status == 'Critical' %}failed{% elif health_score.status == 'Warning' %}skipped{% else %}success{% endif %}">{{ health_score.status }}</span>
                        </div>
                    </div>
                </div>
                <div>
                    <p class="run-label">Calculated across the complete pipeline run</p>
                    <p class="mt-3 text-sm md:text-base leading-7 text-[var(--home-text-secondary)]">{{ health_explanation_display }}</p>
                </div>
            </section>

            <section class="lifecycle-flow">
                <div class="home-section-heading">
                    <div>
                        <p class="home-eyebrow">Pipeline Summary</p>
                        <h2>Whole-run sustainability metrics</h2>
                    </div>
                </div>
                <div class="run-summary-strip mt-3">
                    <div class="run-metric"><p class="run-label">Duration</p><p class="run-metric-value">{{ selected_run_duration_display }}</p></div>
                    <div class="run-metric energy"><p class="run-label">Total Energy</p><p class="run-metric-value">{{ total_energy_display }}</p></div>
                    <div class="run-metric carbon"><p class="run-label">Total Carbon</p><p class="run-metric-value">{{ total_carbon_display }}</p></div>
                    <div class="run-metric warning"><p class="run-label">Warnings</p><p class="run-metric-value">{{ anomaly_summary.warning_count }}</p></div>
                    <div class="run-metric critical"><p class="run-label">Critical</p><p class="run-metric-value">{{ anomaly_summary.critical_count }}</p></div>
                </div>
            </section>

            <section class="lifecycle-flow">
                <div class="home-section-heading">
                    <div>
                        <p class="home-eyebrow">Lifecycle Components</p>
                    </div>
                </div>
                <div class="lifecycle-grid mt-3">
                    {% for stage in lifecycle_sections %}
                    <div class="lifecycle-card {{ stage.key }}">
                        <div class="lifecycle-top">
                            <div class="lifecycle-stage-title"><span class="lifecycle-stage-icon"><i data-lucide="{{ stage.icon }}" class="w-5 h-5"></i></span><h3 class="lifecycle-name">{{ stage.label }}</h3></div>
                            <span class="lifecycle-status" style="color: {% if stage.skipped %}var(--home-warning){% elif stage.summary_status == 'Failed' %}var(--home-critical){% else %}var(--home-text-secondary){% endif %};">{{ stage.summary_status }}</span>
                        </div>
                        <div class="lifecycle-metrics">
                            <div><p class="lifecycle-metric-label energy"><i data-lucide="zap" class="w-4 h-4"></i>Energy</p><p class="lifecycle-metric-value energy">{{ stage.energy_display }}</p></div>
                            <div><p class="lifecycle-metric-label carbon"><i data-lucide="leaf" class="w-4 h-4"></i>Carbon</p><p class="lifecycle-metric-value carbon">{{ stage.carbon_display }}</p></div>
                        </div>
                        {% if stage.skipped and stage.skip_reason_display %}<p class="home-widget-note mt-4">Reason: {{ stage.skip_reason_display }}</p>{% endif %}
                        <a href="/run/{{ selected_run|urlencode }}/{{ stage.key }}" class="lifecycle-link">View Stage <i data-lucide="arrow-right" class="w-4 h-4"></i></a>
                    </div>
                    {% endfor %}
                </div>
            </section>
        </main>
        {% elif page == 'stage' %}
        <main class="space-y-6">
            {% if stage_detail.key == 'release' %}
            {% set release_row = stage_detail.rows[0] if stage_detail.rows else none %}
            <section class="release-page space-y-5">
                <section class="release-hero">
                    <div class="release-panel release-hero-main">
                        <a href="/run/{{ selected_run|urlencode }}" class="home-view-all w-fit"><i data-lucide="arrow-left" class="w-4 h-4"></i> Back to Run #{{ selected_run }}</a>
                        <div class="release-chip-row mt-4">
                            <span class="release-chip"><i data-lucide="package-check" class="w-4 h-4"></i>Release Lifecycle</span>
                            {% if stage_detail.skipped %}
                            <span class="release-chip skipped"><i data-lucide="pause-circle" class="w-4 h-4"></i>Skipped</span>
                            {% elif release_row %}
                            <span class="release-chip {% if release_row.status_display == 'SUCCESS' %}success{% elif release_row.status_display in ['ABORTED', 'CANCELLED', 'CANCELED'] %}{% else %}failed{% endif %}"><i data-lucide="{% if release_row.status_display == 'SUCCESS' %}check-circle-2{% elif release_row.status_display in ['ABORTED', 'CANCELLED', 'CANCELED'] %}circle-slash{% else %}alert-triangle{% endif %}" class="w-4 h-4"></i>{{ release_row.status_display }}</span>
                            {% else %}
                            <span class="release-chip"><i data-lucide="circle-help" class="w-4 h-4"></i>Monitor unavailable</span>
                            {% endif %}
                            {% if stage_detail.release_data %}
                            <span class="release-chip {% if stage_detail.release_data.optimization_status_display == 'Bypassed' %}bypassed{% elif stage_detail.release_data.optimization_status_display == 'Applied' %}applied{% endif %}"><i data-lucide="route" class="w-4 h-4"></i>Optimization {{ stage_detail.release_data.optimization_status_display }}</span>
                            {% endif %}
                        </div>
                        <h1 class="release-title mt-4">Release</h1>
                        <p class="release-subtitle mt-3">Run #{{ selected_run }} - {{ pipeline_name }}</p>
                        {% if stage_detail.skipped and stage_detail.skip_reason_display %}
                        <div class="release-alert mt-4"><i data-lucide="pause-circle" class="inline w-4 h-4 mr-1"></i>Release skipped: {{ stage_detail.skip_reason_display }}</div>
                        {% elif stage_detail.release_data and stage_detail.release_data.optimization_status_display == 'Bypassed' %}
                        <div class="release-alert mt-4"><i data-lucide="git-compare-arrows" class="inline w-4 h-4 mr-1"></i>Release executed as a full build while optimization was bypassed.</div>
                        {% endif %}
                    </div>
                    <div class="release-panel release-split-card">
                        <div class="release-fact"><div><p class="run-label">Release Status</p><p class="home-widget-note">Jenkins release result</p></div><p class="release-fact-value">{{ stage_detail.release_data.status_display if stage_detail.release_data else (release_row.status_display if release_row else stage_detail.summary_status) }}</p></div>
                        <div class="release-fact"><div><p class="run-label">Execution Mode</p><p class="home-widget-note">Release component context</p></div><p class="release-fact-value">{{ stage_detail.release_data.execution_mode_display if stage_detail.release_data else 'Not available' }}</p></div>
                        <div class="release-fact"><div><p class="run-label">Optimization</p><p class="home-widget-note">Decision outcome</p></div><p class="release-fact-value">{{ stage_detail.release_data.optimization_status_display if stage_detail.release_data else 'Not available' }}</p></div>
                    </div>
                </section>

                <section class="lifecycle-flow">
                    <div class="home-section-heading">
                        <div>
                            <p class="home-eyebrow">Monitor Sustainability</p>
                            <h2>Release Metrics</h2>
                        </div>
                    </div>
                    {% if stage_detail.rows %}
                    {% for row in stage_detail.rows %}
                    <div class="release-measure-grid mt-3">
                        <div class="release-measure"><div class="release-measure-top"><p class="run-label">Duration</p><span class="release-measure-icon"><i data-lucide="timer" class="w-4 h-4"></i></span></div><p class="release-measure-value">{{ row.workload_duration_display }}</p><p class="home-widget-note">Workload time</p></div>
                        <div class="release-measure energy"><div class="release-measure-top"><p class="run-label">Energy</p><span class="release-measure-icon"><i data-lucide="zap" class="w-4 h-4"></i></span></div><p class="release-measure-value">{{ row.total_energy_display }}</p><p class="home-widget-note">Monitor total</p></div>
                        <div class="release-measure carbon"><div class="release-measure-top"><p class="run-label">Carbon</p><span class="release-measure-icon"><i data-lucide="leaf" class="w-4 h-4"></i></span></div><p class="release-measure-value">{{ row.total_carbon_display }}</p><p class="home-widget-note">Monitor total</p></div>
                        <div class="release-measure cpu"><div class="release-measure-top"><p class="run-label">CPU</p><span class="release-measure-icon"><i data-lucide="cpu" class="w-4 h-4"></i></span></div><p class="release-measure-value">{{ row.avg_cpu_display }}</p><p class="home-widget-note">Average utilization</p></div>
                    </div>
                    {% endfor %}
                    {% else %}
                    <div class="release-panel mt-3"><p class="release-text font-bold">Awaiting integrated Monitor data.</p></div>
                    {% endif %}
                </section>

                <section class="release-two-col">
                    <div class="release-panel">
                        <div class="home-section-heading !p-0">
                            <div>
                                <p class="home-eyebrow">Release Decision</p>
                                <h2>Jenkins Decision Context</h2>
                            </div>
                        </div>
                        {% if stage_detail.release_data %}
                        <div class="release-decision-primary mt-4">
                            <div class="release-probability">
                                <div>
                                    <p class="run-label">Green Probability</p>
                                    <p class="release-probability-value mt-2">{{ stage_detail.release_data.green_probability_display }}</p>
                                </div>
                            </div>
                            <div class="release-decision-grid">
                                <div class="release-mini"><p class="run-label">Scheduling Action</p><p class="release-mini-value">{{ stage_detail.release_data.scheduling_action_display }}</p></div>
                                <div class="release-mini"><p class="run-label">Optimization Status</p><p class="release-mini-value {% if stage_detail.release_data.optimization_status_display == 'Bypassed' %}text-[var(--home-warning)]{% elif stage_detail.release_data.optimization_status_display == 'Applied' %}text-[var(--home-accent-energy)]{% endif %}">{{ stage_detail.release_data.optimization_status_display }}</p></div>
                                <div class="release-mini"><p class="run-label">Execution Mode</p><p class="release-mini-value">{{ stage_detail.release_data.execution_mode_display }}</p></div>
                                <div class="release-mini"><p class="run-label">Scheduling Engine</p><p class="release-mini-value">{{ stage_detail.release_data.scheduling_engine_display }}</p></div>
                                <div class="release-mini"><p class="run-label">Release Carbon Intensity</p><p class="release-mini-value">{{ stage_detail.release_data.carbon_intensity_display }}</p></div>
                                <div class="release-mini"><p class="run-label">Optimizer Duration</p><p class="release-mini-value">{{ stage_detail.release_data.optimizer_duration_display }}</p></div>
                                <div class="release-mini release-decision-wide"><p class="run-label">Skip Reason</p><p class="release-mini-value">{{ stage_detail.release_data.optimizer_skip_reason_display }}</p></div>
                            </div>
                        </div>
                        <div class="release-alert mt-4">{{ stage_detail.release_data.context_note }}</div>
                        {% else %}
                        <div class="release-panel mt-4 bg-transparent shadow-none"><p class="release-text font-bold">Release decision data unavailable.</p><p class="home-widget-note">Monitor measurements and Release anomaly intelligence remain available.</p></div>
                        {% endif %}
                    </div>

                    <div class="release-panel">
                        <div class="home-section-heading !p-0">
                            <div>
                                <p class="home-eyebrow">What Was Processed</p>
                                <h2>Release work context</h2>
                            </div>
                        </div>
                        {% if stage_detail.release_data %}
                        <div class="release-work-grid mt-4">
                            <div class="release-mini"><p class="run-label">Affected Modules</p><p class="release-mini-value">{{ stage_detail.release_data.affected_modules_display }}</p></div>
                            <div class="release-mini"><p class="run-label">Tests Executed</p><p class="release-mini-value">{{ stage_detail.release_data.tests_executed_display }}</p></div>
                            <div class="release-mini"><p class="run-label">Tests Skipped</p><p class="release-mini-value">{{ stage_detail.release_data.tests_skipped_display }}</p></div>
                        </div>
                        <div class="release-flow">
                            <div class="release-step"><p class="run-label">Build</p><p class="release-mini-value">{{ stage_detail.release_data.build_duration_display }}</p></div>
                            <div class="release-step"><p class="run-label">Test</p><p class="release-mini-value">{{ stage_detail.release_data.test_duration_display }}</p></div>
                            <div class="release-step"><p class="run-label">Docker Build</p><p class="release-mini-value">{{ stage_detail.release_data.docker_build_duration_display }}</p></div>
                            <div class="release-step"><p class="run-label">Release Total</p><p class="release-mini-value">{{ stage_detail.release_data.release_duration_display }}</p></div>
                        </div>
                        {% else %}
                        <p class="release-text font-bold mt-4">Release work context unavailable for this run.</p>
                        {% endif %}
                    </div>
                </section>

                <section class="release-panel">
                    <div class="release-intel-head">
                        <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
                            <div>
                                <p class="home-eyebrow">Release Anomaly Intelligence</p>
                                <h2 class="text-2xl font-black mt-2 text-[var(--home-text)]">Stage-specific sustainability intelligence for this Release lifecycle</h2>
                            </div>
                            <div class="release-chip-row">
                                <span class="release-chip {% if stage_detail.anomaly_summary.overall_status == 'Critical' %}critical{% elif stage_detail.anomaly_summary.overall_status == 'Warning' %}skipped{% else %}success{% endif %}">{{ stage_detail.anomaly_summary.overall_status }}</span>
                                <span class="release-chip skipped">{{ stage_detail.anomaly_summary.warning_count }} warnings</span>
                                <span class="release-chip critical">{{ stage_detail.anomaly_summary.critical_count }} critical</span>
                            </div>
                        </div>
                    </div>
                    <div class="release-intel-grid mt-4">
                        <div class="release-intel-card">
                            <div class="flex items-center justify-between gap-3"><div><p class="run-label">Statistical Detection</p><p class="release-text text-sm font-bold mt-1">Baseline comparison</p></div><span class="release-measure-icon"><i data-lucide="scan-search" class="w-4 h-4"></i></span></div>
                            {% if stage_detail.statistical_alerts %}
                            <div class="mt-4 space-y-3">
                                {% for alert in stage_detail.statistical_alerts %}
                                <div class="release-anomaly-item">
                                    <div class="flex flex-wrap items-center justify-between gap-2"><span class="release-chip {% if alert.severity == 'critical' %}critical{% else %}skipped{% endif %}">{{ alert.severity_label }}</span><span class="release-muted text-xs font-bold">{{ alert.percentage_change_display }} vs baseline</span></div>
                                    <p class="release-strong text-sm font-extrabold mt-2">{{ alert.message }}</p>
                                    <p class="release-text text-xs mt-1">{{ alert.metric_label }}: {{ alert.current_display }} vs {{ alert.baseline_display }}</p>
                                    <p class="release-muted text-xs mt-1">{{ alert.context_scope_display }} - {{ alert.historical_samples_display }} runs</p>
                                    {% if alert.fallback_reason %}<p class="text-xs mt-1 text-[var(--home-warning)]">{{ alert.fallback_reason }}</p>{% endif %}
                                </div>
                                {% endfor %}
                            </div>
                            {% else %}
                            <span class="release-chip success mt-4">Normal</span>
                            <p class="release-text text-sm mt-3">No warning or critical statistical anomalies for Release.</p>
                            {% endif %}
                        </div>
                        <div class="release-intel-card">
                            <div class="flex items-center justify-between gap-3"><div><p class="run-label">Isolation Forest</p><p class="release-text text-sm font-bold mt-1">Multi-metric pattern check</p></div><span class="release-measure-icon"><i data-lucide="network" class="w-4 h-4"></i></span></div>
                            {% if stage_detail.ml_results %}
                            <div class="mt-4 space-y-3">
                                {% for item in stage_detail.ml_results %}
                                <div class="release-anomaly-item">
                                    <div class="release-chip-row"><span class="release-chip {% if item.prediction == 'Anomaly' %}critical{% elif item.prediction == 'Warming Up' %}skipped{% else %}success{% endif %}">{{ item.prediction }}</span><span class="release-chip">{{ item.model_status }}</span></div>
                                    <p class="release-strong text-sm font-extrabold mt-3">{{ item.message }}</p>
                                    <div class="grid grid-cols-2 gap-2 mt-3 text-xs">
                                        <div><p class="run-label">Score</p><p class="release-text font-bold">{{ item.anomaly_score_display }}</p></div>
                                        <div><p class="run-label">Samples</p><p class="release-text font-bold">{{ item.historical_samples_display }}</p></div>
                                        <div><p class="run-label">Context</p><p class="release-text font-bold">{{ item.context_scope_display }}</p></div>
                                        <div><p class="run-label">Strategy-specific</p><p class="release-text font-bold">{{ 'Yes' if item.strategy_specific else 'No' }}</p></div>
                                    </div>
                                    {% if item.fallback_reason %}<p class="text-xs mt-2 text-[var(--home-warning)]">{{ item.fallback_reason }}</p>{% endif %}
                                </div>
                                {% endfor %}
                            </div>
                            {% else %}
                            <p class="release-text text-sm mt-4">Awaiting integrated Monitor data.</p>
                            {% endif %}
                        </div>
                    </div>
                </section>
            </section>
            {% elif stage_detail.key == 'deploy' %}
            {% set deploy_row = stage_detail.rows[0] if stage_detail.rows else none %}
            <section class="deploy-page space-y-5">
                <section class="deploy-hero">
                    <div class="deploy-panel deploy-hero-main">
                        <a href="/run/{{ selected_run|urlencode }}" class="home-view-all w-fit"><i data-lucide="arrow-left" class="w-4 h-4"></i> Back to Run #{{ selected_run }}</a>
                        <div class="deploy-chip-row mt-4">
                            <span class="deploy-chip carbon"><i data-lucide="rocket" class="w-4 h-4"></i>Deploy Lifecycle</span>
                            {% if stage_detail.skipped %}
                            <span class="deploy-chip skipped"><i data-lucide="pause-circle" class="w-4 h-4"></i>Deploy Skipped</span>
                            {% elif deploy_row %}
                            <span class="deploy-chip {% if deploy_row.status_display == 'SUCCESS' %}success{% elif deploy_row.status_display in ['ABORTED', 'CANCELLED', 'CANCELED'] %}{% else %}failed{% endif %}"><i data-lucide="{% if deploy_row.status_display == 'SUCCESS' %}check-circle-2{% elif deploy_row.status_display in ['ABORTED', 'CANCELLED', 'CANCELED'] %}circle-slash{% else %}alert-triangle{% endif %}" class="w-4 h-4"></i>{{ deploy_row.status_display }}</span>
                            {% else %}
                            <span class="deploy-chip"><i data-lucide="circle-help" class="w-4 h-4"></i>Monitor unavailable</span>
                            {% endif %}
                            {% if stage_detail.deploy_data %}
                            <span class="deploy-chip carbon"><i data-lucide="git-branch" class="w-4 h-4"></i>{{ stage_detail.deploy_data.strategy_display }}</span>
                            {% endif %}
                        </div>
                        <h1 class="deploy-title mt-4">Deploy</h1>
                        <p class="deploy-subtitle mt-3">Run #{{ selected_run }} - {{ pipeline_name }}</p>
                        {% if stage_detail.skipped and stage_detail.skip_reason_display %}
                        <div class="deploy-alert mt-4"><i data-lucide="pause-circle" class="inline w-4 h-4 mr-1"></i>Deploy skipped: {{ stage_detail.skip_reason_display }}</div>
                        {% endif %}
                    </div>
                    <div class="deploy-panel">
                        <div class="deploy-fact"><div><p class="run-label">Monitor Status</p><p class="home-widget-note">Lifecycle result</p></div><p class="deploy-fact-value">{{ deploy_row.status_display if deploy_row else stage_detail.summary_status }}</p></div>
                        <div class="deploy-fact"><div><p class="run-label">Deployment Status</p><p class="home-widget-note">Deploy DB telemetry</p></div><p class="deploy-fact-value">{{ stage_detail.deploy_data.status_display if stage_detail.deploy_data else 'Not available' }}</p></div>
                        <div class="deploy-fact"><div><p class="run-label">Image</p><p class="home-widget-note">Deployment artifact</p></div><p class="deploy-fact-value">{{ stage_detail.deploy_data.image_display if stage_detail.deploy_data else 'Not available' }}</p></div>
                    </div>
                </section>

                <section class="lifecycle-flow">
                    <div class="home-section-heading">
                        <div>
                            <p class="home-eyebrow">Monitor Sustainability</p>
                            <h2>Deploy Metrics</h2>
                        </div>
                    </div>
                    {% if stage_detail.rows %}
                    {% for row in stage_detail.rows %}
                    <div class="deploy-measure-grid mt-3">
                        <div class="deploy-measure"><div class="deploy-measure-top"><p class="run-label">Duration</p><span class="deploy-measure-icon"><i data-lucide="timer" class="w-4 h-4"></i></span></div><p class="deploy-measure-value">{{ row.workload_duration_display }}</p><p class="home-widget-note">Workload time</p></div>
                        <div class="deploy-measure energy"><div class="deploy-measure-top"><p class="run-label">Energy</p><span class="deploy-measure-icon"><i data-lucide="zap" class="w-4 h-4"></i></span></div><p class="deploy-measure-value">{{ row.total_energy_display }}</p><p class="home-widget-note">Monitor total</p></div>
                        <div class="deploy-measure carbon"><div class="deploy-measure-top"><p class="run-label">Carbon</p><span class="deploy-measure-icon"><i data-lucide="leaf" class="w-4 h-4"></i></span></div><p class="deploy-measure-value">{{ row.total_carbon_display }}</p><p class="home-widget-note">Monitor total</p></div>
                        <div class="deploy-measure cpu"><div class="deploy-measure-top"><p class="run-label">Average CPU</p><span class="deploy-measure-icon"><i data-lucide="cpu" class="w-4 h-4"></i></span></div><p class="deploy-measure-value">{{ row.avg_cpu_display }}</p><p class="home-widget-note">Monitor observed</p></div>
                        <div class="deploy-measure memory"><div class="deploy-measure-top"><p class="run-label">Average Memory</p><span class="deploy-measure-icon"><i data-lucide="memory-stick" class="w-4 h-4"></i></span></div><p class="deploy-measure-value">{{ row.avg_memory_display }}</p><p class="home-widget-note">Monitor observed</p></div>
                    </div>
                    {% endfor %}
                    {% else %}
                    <div class="deploy-panel mt-3"><p class="release-text font-bold">Monitor Deploy data unavailable for this run.</p></div>
                    {% endif %}
                </section>

                <section class="deploy-two-col">
                    <div class="deploy-panel">
                        <div class="home-section-heading !p-0">
                            <div>
                                <p class="home-eyebrow">Deployment Strategy</p>
                                <h2>How the application was deployed</h2>
                            </div>
                            <p>DEPLOYMENT TELEMETRY from the Deploy component.</p>
                        </div>
                        {% if stage_detail.skipped %}
                        <div class="deploy-alert mt-4">Deployment telemetry is not applied because this Monitor Deploy lifecycle was skipped.</div>
                        {% elif stage_detail.deploy_data %}
                        <p class="deploy-strategy-value mt-4">{{ stage_detail.deploy_data.strategy_display }}</p>
                        <div class="deploy-mini-grid mt-4">
                            <div class="deploy-mini"><p class="run-label">Canary Weight</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.canary_weight_display }}</p></div>
                            <div class="deploy-mini"><p class="run-label">Carbon Profile</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.carbon_profile_display }}</p></div>
                            <div class="deploy-mini"><p class="run-label">Deployment Duration</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.duration_display }}</p></div>
                            <div class="deploy-mini"><p class="run-label">Status</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.status_display }}</p></div>
                            <div class="deploy-mini sm:col-span-2"><p class="run-label">Image</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.image_display }}</p></div>
                        </div>
                        {% else %}
                        <div class="deploy-alert mt-4">Deployment telemetry unavailable. Monitor Deploy measurements and anomaly intelligence remain available.</div>
                        {% endif %}
                    </div>

                    <div class="deploy-panel">
                        <div class="home-section-heading !p-0">
                            <div>
                                <p class="home-eyebrow">Carbon & Profiler</p>
                                <h2>Supplementary deployment telemetry</h2>
                            </div>
                        </div>
                        {% if stage_detail.skipped %}
                        <p class="release-text font-bold mt-4">No deployment telemetry applied for a skipped Deploy lifecycle.</p>
                        {% elif stage_detail.deploy_data %}
                        <div class="deploy-mini-grid mt-4">
                            <div class="deploy-mini"><p class="run-label">Carbon Intensity</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.carbon_intensity_display }}</p></div>
                            <div class="deploy-mini"><p class="run-label">Infra Multiplier</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.infra_multiplier_display }}</p></div>
                            <div class="deploy-mini"><p class="run-label">Strategy Carbon Profile</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.strategy_carbon_profile_display }}</p></div>
                            <div class="deploy-mini"><p class="run-label">Profiler Samples</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.samples_collected_display }}</p></div>
                        </div>
                        {% else %}
                        <p class="release-text font-bold mt-4">Deployment telemetry unavailable for this run.</p>
                        {% endif %}
                    </div>
                </section>

                <section class="deploy-two-col">
                    <div class="deploy-panel">
                        <div class="home-section-heading !p-0">
                            <div>
                                <p class="home-eyebrow">Deployment Timeline</p>
                                <h2>Execution and carbon snapshots</h2>
                            </div>
                        </div>
                        {% if stage_detail.deploy_data %}
                        <div class="deploy-timeline mt-4">
                            <div class="deploy-event"><span class="deploy-dot"></span><div class="deploy-event-body"><p class="run-label">Deployment Started</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.start_time_display }}</p></div></div>
                            {% for snapshot in stage_detail.deploy_data.snapshots_display %}
                            <div class="deploy-event"><span class="deploy-dot"></span><div class="deploy-event-body"><p class="run-label">{{ snapshot.phase }}</p><p class="deploy-mini-value">{{ snapshot.snapshot_timestamp }}</p><div class="deploy-snapshot-grid"><p class="release-text text-xs">Strategy: {{ snapshot.strategy }}</p><p class="release-text text-xs">Multiplier: {{ snapshot.infra_multiplier }}</p><p class="release-text text-xs">Downtime: {{ snapshot.downtime_seconds }}</p><p class="release-text text-xs">Canary: {{ snapshot.canary_weight }}</p></div><p class="release-muted text-xs mt-2">{{ snapshot.note }}</p></div></div>
                            {% endfor %}
                            <div class="deploy-event"><span class="deploy-dot"></span><div class="deploy-event-body"><p class="run-label">Deployment Completed</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.end_time_display }}</p><p class="home-widget-note">Duration: {{ stage_detail.deploy_data.duration_display }}</p></div></div>
                        </div>
                        {% else %}
                        <p class="release-text font-bold mt-4">Deployment timeline unavailable.</p>
                        {% endif %}
                    </div>

                    <div class="deploy-panel">
                        <div class="home-section-heading !p-0">
                            <div>
                                <p class="home-eyebrow">Monitor + Profiler Resource View</p>
                                <h2>CPU and memory shape</h2>
                            </div>
                        </div>
                        {% if stage_detail.rows %}
                        {% for row in stage_detail.rows %}
                        <div class="deploy-resource-grid">
                            <div class="deploy-mini"><p class="run-label">Monitor Avg CPU</p><p class="deploy-mini-value">{{ row.avg_cpu_display }}</p><div class="deploy-bar-track"><div class="deploy-bar-fill" style="width: {{ row.avg_cpu_bar_width }}%;"></div></div></div>
                            <div class="deploy-mini"><p class="run-label">Monitor Peak CPU</p><p class="deploy-mini-value">{{ row.peak_cpu_display }}</p></div>
                            <div class="deploy-mini"><p class="run-label">Monitor Avg Memory</p><p class="deploy-mini-value">{{ row.avg_memory_display }}</p><div class="deploy-bar-track"><div class="deploy-bar-fill" style="width: {{ row.avg_memory_bar_width }}%;"></div></div></div>
                            <div class="deploy-mini"><p class="run-label">Monitor Peak Memory</p><p class="deploy-mini-value">{{ row.peak_memory_display }}</p></div>
                        </div>
                        {% endfor %}
                        {% endif %}
                        {% if stage_detail.deploy_data and not stage_detail.skipped %}
                        <div class="deploy-mini-grid mt-3">
                            <div class="deploy-mini"><p class="run-label">Profiler Avg CPU</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.avg_cpu_display }}</p></div>
                            <div class="deploy-mini"><p class="run-label">Profiler Avg Memory</p><p class="deploy-mini-value">{{ stage_detail.deploy_data.avg_memory_display }}</p></div>
                        </div>
                        {% endif %}
                    </div>
                </section>

                <section class="deploy-panel">
                    <div class="deploy-intel-head">
                        <div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
                            <div>
                                <p class="home-eyebrow">Deploy Anomaly Intelligence</p>
                                <h2 class="text-2xl font-black mt-2 text-[var(--home-text)]">Stage-specific sustainability intelligence for this Deploy lifecycle</h2>
                            </div>
                            <div class="deploy-chip-row">
                                <span class="deploy-chip {% if stage_detail.anomaly_summary.overall_status == 'Critical' %}critical{% elif stage_detail.anomaly_summary.overall_status == 'Warning' %}skipped{% else %}success{% endif %}">{{ stage_detail.anomaly_summary.overall_status }}</span>
                                <span class="deploy-chip skipped">{{ stage_detail.anomaly_summary.warning_count }} warnings</span>
                                <span class="deploy-chip critical">{{ stage_detail.anomaly_summary.critical_count }} critical</span>
                            </div>
                        </div>
                    </div>
                    <div class="deploy-intel-grid mt-4">
                        <div class="deploy-intel-card">
                            <div class="flex items-center justify-between gap-3"><div><p class="run-label">Statistical Detection</p><p class="release-text text-sm font-bold mt-1">Baseline comparison</p></div><span class="deploy-measure-icon"><i data-lucide="scan-search" class="w-4 h-4"></i></span></div>
                            {% if stage_detail.statistical_alerts %}
                            <div class="mt-4 space-y-3">
                                {% for alert in stage_detail.statistical_alerts %}
                                <div class="deploy-anomaly-item"><div class="flex flex-wrap items-center justify-between gap-2"><span class="deploy-chip {% if alert.severity == 'critical' %}critical{% else %}skipped{% endif %}">{{ alert.severity_label }}</span><span class="release-muted text-xs font-bold">{{ alert.percentage_change_display }} vs baseline</span></div><p class="release-strong text-sm font-extrabold mt-2">{{ alert.message }}</p><p class="release-text text-xs mt-1">{{ alert.metric_label }}: {{ alert.current_display }} vs {{ alert.baseline_display }}</p><p class="release-muted text-xs mt-1">{{ alert.context_scope_display }} - {{ alert.historical_samples_display }} runs</p>{% if alert.fallback_reason %}<p class="text-xs mt-1 text-[var(--home-warning)]">{{ alert.fallback_reason }}</p>{% endif %}</div>
                                {% endfor %}
                            </div>
                            {% else %}
                            <span class="deploy-chip success mt-4">Normal</span>
                            <p class="release-text text-sm mt-3">No warning or critical statistical anomalies for Deploy.</p>
                            {% endif %}
                        </div>
                        <div class="deploy-intel-card">
                            <div class="flex items-center justify-between gap-3"><div><p class="run-label">Isolation Forest</p><p class="release-text text-sm font-bold mt-1">Multi-metric pattern check</p></div><span class="deploy-measure-icon"><i data-lucide="network" class="w-4 h-4"></i></span></div>
                            {% if stage_detail.ml_results %}
                            <div class="mt-4 space-y-3">
                                {% for item in stage_detail.ml_results %}
                                <div class="deploy-anomaly-item"><div class="deploy-chip-row"><span class="deploy-chip {% if item.prediction == 'Anomaly' %}critical{% elif item.prediction == 'Warming Up' %}skipped{% else %}success{% endif %}">{{ item.prediction }}</span><span class="deploy-chip">{{ item.model_status }}</span></div><p class="release-strong text-sm font-extrabold mt-3">{{ item.message }}</p><div class="grid grid-cols-2 gap-2 mt-3 text-xs"><div><p class="run-label">Score</p><p class="release-text font-bold">{{ item.anomaly_score_display }}</p></div><div><p class="run-label">Samples</p><p class="release-text font-bold">{{ item.historical_samples_display }}</p></div><div><p class="run-label">Context</p><p class="release-text font-bold">{{ item.context_scope_display }}</p></div><div><p class="run-label">Strategy-specific</p><p class="release-text font-bold">{{ 'Yes' if item.strategy_specific else 'No' }}</p></div></div>{% if item.fallback_reason %}<p class="text-xs mt-2 text-[var(--home-warning)]">{{ item.fallback_reason }}</p>{% endif %}</div>
                                {% endfor %}
                            </div>
                            {% else %}
                            <p class="release-text text-sm mt-4">Awaiting integrated Monitor data.</p>
                            {% endif %}
                        </div>
                    </div>
                </section>
            </section>
            {% endif %}
        </main>
        {% endif %}
    </div>
    <script>
        lucide.createIcons();
        const themeStorageKey = "green-devops-theme";
        const themeButtons = document.querySelectorAll("[data-theme-choice]");
        function applyTheme(theme, persist) {
            const selectedTheme = theme === "dark" ? "dark" : "light";
            document.documentElement.dataset.theme = selectedTheme;
            if (persist) {
                try { localStorage.setItem(themeStorageKey, selectedTheme); } catch (error) {}
            }
            themeButtons.forEach(function(button) {
                button.setAttribute("aria-pressed", button.dataset.themeChoice === selectedTheme ? "true" : "false");
            });
            if (window.homeCharts) updateHomeCharts();
        }
        themeButtons.forEach(function(button) {
            button.addEventListener("click", function() {
                applyTheme(button.dataset.themeChoice, true);
            });
        });
        applyTheme(document.documentElement.dataset.theme || "light", false);
        {% if page == 'home' %}
        Chart.defaults.font.family = "'Plus Jakarta Sans', sans-serif";
        const runLabels = {{ run_chart_labels | safe }};
        function homeThemeColors() {
            const styles = getComputedStyle(document.documentElement);
            return {
                energy: styles.getPropertyValue("--home-accent-energy").trim() || "#00c896",
                carbon: styles.getPropertyValue("--home-accent-carbon").trim() || "#22d3ee",
                text: styles.getPropertyValue("--home-text-secondary").trim() || "#b7c4d4",
                muted: styles.getPropertyValue("--home-text-muted").trim() || "#7f91a3",
                grid: document.documentElement.dataset.theme === "dark" ? "rgba(226,232,240,.08)" : "rgba(15,23,42,.08)",
                tooltip: document.documentElement.dataset.theme === "dark" ? "#f8fafc" : "#111827",
                tooltipText: document.documentElement.dataset.theme === "dark" ? "#111827" : "#f8fafc"
            };
        }
        function homeChartOptions() {
            const colors = homeThemeColors();
            return {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { intersect: false, mode: "index" },
                plugins: {
                    legend: { labels: { usePointStyle: true, boxWidth: 7, color: colors.text, font: { size: 12, weight: "700" } } },
                    tooltip: { backgroundColor: colors.tooltip, titleColor: colors.tooltipText, bodyColor: colors.tooltipText, padding: 12, cornerRadius: 12, titleFont: { weight: "800" }, bodyFont: { weight: "600" } }
                },
                scales: {
                    x: { grid: { display: false }, border: { display: false }, ticks: { color: colors.muted, maxRotation: 0, autoSkip: true } },
                    y: { grid: { color: colors.grid }, border: { display: false }, ticks: { color: colors.muted } }
                }
            };
        }
        function homeLineDataset(label, data, color) {
            return { label, data, borderColor: color, backgroundColor: color + "18", pointBackgroundColor: "transparent", pointBorderColor: color, pointRadius: 3, pointHoverRadius: 5, borderWidth: 2.5, fill: true, tension: .38 };
        }
        const initialHomeColors = homeThemeColors();
        window.homeCharts = [
            new Chart(document.getElementById("runEnergyChart"), { type: "line", data: { labels: runLabels, datasets: [homeLineDataset("Energy kWh", {{ run_energy_values | safe }}, initialHomeColors.energy)] }, options: homeChartOptions() }),
            new Chart(document.getElementById("runCarbonChart"), { type: "line", data: { labels: runLabels, datasets: [homeLineDataset("Carbon kgCO2e", {{ run_carbon_values | safe }}, initialHomeColors.carbon)] }, options: homeChartOptions() })
        ];
        function updateHomeCharts() {
            const colors = homeThemeColors();
            window.homeCharts[0].data.datasets[0].borderColor = colors.energy;
            window.homeCharts[0].data.datasets[0].backgroundColor = colors.energy + "18";
            window.homeCharts[0].data.datasets[0].pointBorderColor = colors.energy;
            window.homeCharts[1].data.datasets[0].borderColor = colors.carbon;
            window.homeCharts[1].data.datasets[0].backgroundColor = colors.carbon + "18";
            window.homeCharts[1].data.datasets[0].pointBorderColor = colors.carbon;
            window.homeCharts.forEach(function(chart) {
                chart.options = homeChartOptions();
                chart.update();
            });
        }
        {% endif %}
        {% if page == 'runs' %}
        const runRows = Array.from(document.querySelectorAll("[data-run-row]"));
        const runsSearch = document.getElementById("runsSearch");
        const runsStatusFilter = document.getElementById("runsStatusFilter");
        const runsCount = document.getElementById("runsCount");
        const runsEmpty = document.getElementById("runsEmpty");
        const runsReset = document.getElementById("runsReset");

        const statusLabels = {
            success: "Success",
            failed: "Failed",
            skipped: "Skipped",
            aborted: "Aborted",
            cancelled: "Cancelled",
            canceled: "Cancelled"
        };
        Array.from(new Set(runRows.map(function(row) {
            return (row.dataset.status || "").toLowerCase();
        }).filter(Boolean))).sort().forEach(function(status) {
            const option = document.createElement("option");
            option.value = status;
            option.textContent = statusLabels[status] || status.charAt(0).toUpperCase() + status.slice(1);
            runsStatusFilter.appendChild(option);
        });

        function updateRunsList() {
            const query = (runsSearch.value || "").trim().toLowerCase();
            const selectedStatus = runsStatusFilter.value;
            let visibleCount = 0;
            runRows.forEach(function(row) {
                const haystack = ((row.dataset.runId || "") + " " + (row.dataset.pipelineName || "")).toLowerCase();
                const status = (row.dataset.status || "").toLowerCase();
                const matchesSearch = !query || haystack.includes(query);
                const matchesStatus = selectedStatus === "all" || status === selectedStatus;
                const isVisible = matchesSearch && matchesStatus;
                row.hidden = !isVisible;
                if (isVisible) visibleCount += 1;
            });
            const totalCount = runRows.length;
            runsCount.textContent = visibleCount === totalCount ? `${totalCount} pipeline runs` : `Showing ${visibleCount} of ${totalCount} runs`;
            runsEmpty.style.display = visibleCount ? "none" : "block";
        }

        runsSearch.addEventListener("input", updateRunsList);
        runsStatusFilter.addEventListener("change", updateRunsList);
        runsReset.addEventListener("click", function() {
            runsSearch.value = "";
            runsStatusFilter.value = "all";
            updateRunsList();
            runsSearch.focus();
        });
        updateRunsList();
        {% endif %}
    </script>
</body>
</html>
"""


def _empty_data_response():
    return """
    <div style='background:#f6f7f9; color:#111827; min-height:100vh; display:flex; align-items:center; justify-content:center; font-family:Plus Jakarta Sans, system-ui, -apple-system, BlinkMacSystemFont, sans-serif; padding:24px;'>
        <div style='max-width:520px; width:100%; border:1px solid #e5e7eb; border-radius:24px; background:#fff; box-shadow:0 18px 54px rgba(15,23,42,.08); padding:32px; text-align:center;'>
            <div style='width:44px; height:44px; border-radius:16px; background:#ecfdf5; color:#047857; display:inline-flex; align-items:center; justify-content:center; margin-bottom:16px;'>&bull;</div>
            <h2 style='font-size:28px; line-height:1.1; margin:0; font-weight:800;'>No monitoring data yet</h2>
            <p style='margin:12px 0 0; color:#64748b; line-height:1.6;'>Run a monitored pipeline to begin building your sustainability history.</p>
        </div>
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
    health_scores, alert_counts, start_times, end_times, pipeline_names = [], [], [], [], []
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
        pipeline_name = (
            str(current_run_df["pipeline_name"].dropna().astype(str).iloc[0])
            if "pipeline_name" in current_run_df.columns and not current_run_df["pipeline_name"].dropna().empty
            else ""
        )
        pipeline_names.append(pipeline_name)
    rows["health_score"] = health_scores
    rows["alert_count"] = alert_counts
    rows["start_time_display"] = start_times
    rows["end_time_display"] = end_times
    rows["pipeline_name_display"] = pipeline_names
    return rows


def build_run_context(df, run_summary, data_source, selected_run, include_release_data=False):
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


def _winner_compare_metric(label, icon, run_a_value, run_b_value, formatter, lower_is_better=True):
    a_numeric = optional_numeric(run_a_value)
    b_numeric = optional_numeric(run_b_value)
    metric = {
        "label": label,
        "icon": icon,
        "a_display": formatter(a_numeric) if a_numeric is not None else "N/A",
        "b_display": formatter(b_numeric) if b_numeric is not None else "N/A",
        "a_class": "neutral",
        "b_class": "neutral",
        "a_icon": "circle",
        "b_icon": "circle",
        "a_result": "N/A",
        "b_result": "N/A",
    }
    if a_numeric is None or b_numeric is None:
        return metric

    if a_numeric == b_numeric:
        metric.update({
            "a_class": "equal",
            "b_class": "equal",
            "a_icon": "circle-alert",
            "b_icon": "circle-alert",
            "a_result": "Equal",
            "b_result": "Equal",
        })
        return metric

    a_is_better = a_numeric < b_numeric if lower_is_better else a_numeric > b_numeric
    better_value = a_numeric if a_is_better else b_numeric
    worse_value = b_numeric if a_is_better else a_numeric
    better_key = "a" if a_is_better else "b"
    worse_key = "b" if a_is_better else "a"
    metric[f"{better_key}_class"] = "better"
    metric[f"{worse_key}_class"] = "worse"
    metric[f"{better_key}_icon"] = "circle-check"
    metric[f"{worse_key}_icon"] = "circle-alert"

    if lower_is_better:
        if worse_value == 0:
            result = "N/A"
        else:
            improvement = ((worse_value - better_value) / worse_value) * 100.0
            suffix = "faster" if "Runtime" in label else "lower"
            result = f"{improvement:.1f}% {suffix}"
    else:
        result = f"{abs(better_value - worse_value):.0f} points higher"

    metric[f"{better_key}_result"] = result
    metric[f"{worse_key}_result"] = ""
    return metric


def _compare_run_details(df, run_id, release_builds, enriched_run):
    run_rows = df[df["run_id"] == run_id].copy()
    analytics_rows = workload_analytics_dataframe(run_rows)
    source_rows = analytics_rows if not analytics_rows.empty else run_rows
    uses_jenkins_timing = bool(source_rows["jenkins_stage_duration_captured"].any()) if "jenkins_stage_duration_captured" in source_rows else False
    duration_column = "jenkins_stage_duration_seconds" if uses_jenkins_timing else "workload_duration_seconds"
    total_duration = optional_numeric(source_rows[duration_column].sum()) if duration_column in source_rows else None
    release_rows = source_rows[source_rows["stage"].astype(str).str.lower() == "release"] if "stage" in source_rows else pd.DataFrame()
    deploy_rows = source_rows[source_rows["stage"].astype(str).str.lower() == "deploy"] if "stage" in source_rows else pd.DataFrame()
    release_duration = optional_numeric(release_rows[duration_column].sum()) if duration_column in release_rows and not release_rows.empty else None
    deploy_duration = optional_numeric(deploy_rows[duration_column].sum()) if duration_column in deploy_rows and not deploy_rows.empty else None

    pipeline_name = format_optional_text(source_rows["pipeline_name"].dropna().astype(str).iloc[0]) if "pipeline_name" in source_rows and not source_rows["pipeline_name"].dropna().empty else "Not available"
    build_number = extract_build_number_from_run_id(run_id)
    release_data = format_release_build_data(find_release_build_for_run(run_id, release_builds))
    deploy_data = format_deploy_component_data(load_deploy_data(pipeline_name, build_number))
    strategies = []
    if deploy_data:
        strategies.append(deploy_data.get("strategy_display"))
    if "strategy" in source_rows:
        strategies.extend(source_rows["strategy"].dropna().astype(str).tolist())
    strategy_display = next((format_optional_text(item).replace("_", " ").title() for item in strategies if format_optional_text(item).lower() not in {"not available", "missing"}), "Not available")

    optimizer_status = "Not available"
    if release_data:
        optimizer_status = release_data.get("optimizer_status_display", "Not available")
    elif "optimizer_status" in source_rows:
        optimizer_status = format_optional_text(source_rows["optimizer_status"].dropna().astype(str).iloc[0]).replace("_", " ").title() if not source_rows["optimizer_status"].dropna().empty else "Not available"

    start_time, end_time = _run_times(run_rows)
    selected_run_status = "failed" if run_rows["status"].astype(str).str.lower().eq("failed").any() else str(enriched_run.get("status", "success"))
    return {
        "run_id": run_id,
        "build_number": build_number or "Not available",
        "pipeline": pipeline_name,
        "status": selected_run_status,
        "strategy": strategy_display,
        "optimizer_status": optimizer_status,
        "timestamp": end_time if end_time != "Not captured" else start_time,
        "start": start_time,
        "end": end_time,
        "health_score": optional_numeric(enriched_run.get("health_score")),
        "metrics": {
            "release_duration": release_duration,
            "deploy_duration": deploy_duration,
            "duration": total_duration,
            "avg_cpu": optional_numeric(source_rows["avg_cpu_percent"].mean()) if "avg_cpu_percent" in source_rows else None,
            "avg_memory": optional_numeric(source_rows["avg_memory_percent"].mean()) if "avg_memory_percent" in source_rows else None,
            "energy": optional_numeric(source_rows["total_energy_kwh"].sum()) if "total_energy_kwh" in source_rows else None,
            "carbon": optional_numeric(source_rows["total_carbon_kg"].sum()) if "total_carbon_kg" in source_rows else None,
        },
    }


def build_compare_context(df, run_summary, data_source, run_a_id=None, run_b_id=None):
    enriched_runs = enrich_run_summary_for_pages(df, run_summary)
    enriched_lookup = {str(run.get("run_id", "")): run for run in enriched_runs.to_dict(orient="records")}
    run_options = []
    for run in enriched_lookup.values():
        run_id = str(run.get("run_id", ""))
        run_options.append(
            {
                "run_id": run_id,
                "label": f"Run #{extract_build_number_from_run_id(run_id) or run_id} | Pipeline: {run.get('pipeline_name_display') or 'Not available'} | Status: {run.get('status')} | Time: {run.get('end_time_display') or run.get('latest_time') or 'Not captured'}",
                "status": run.get("status"),
                "pipeline": run.get("pipeline_name_display"),
            }
        )

    available_ids = {item["run_id"] for item in run_options}
    selected_a = str(run_a_id or "")
    selected_b = str(run_b_id or "")
    run_a = run_b = None
    comparison_ready = False
    compare_error = ""
    if selected_a and selected_a not in available_ids:
        compare_error = "Run A was not found in the available Monitor history."
        selected_a = ""
    if selected_b and selected_b not in available_ids:
        compare_error = "Run B was not found in the available Monitor history."
        selected_b = ""
    if selected_a and selected_b and selected_a == selected_b:
        compare_error = "Choose two different pipeline runs to compare."
    elif selected_a and selected_b:
        release_builds = load_release_builds()
        run_a = _compare_run_details(df, selected_a, release_builds, enriched_lookup.get(selected_a, {}))
        run_b = _compare_run_details(df, selected_b, release_builds, enriched_lookup.get(selected_b, {}))
        comparison_ready = True

    main_metrics = []
    if comparison_ready:
        main_metrics = [
            _winner_compare_metric("Release Runtime", "package-check", run_a["metrics"]["release_duration"], run_b["metrics"]["release_duration"], format_seconds),
            _winner_compare_metric("Deploy Runtime", "rocket", run_a["metrics"]["deploy_duration"], run_b["metrics"]["deploy_duration"], format_seconds),
            _winner_compare_metric("Total Runtime", "timer", run_a["metrics"]["duration"], run_b["metrics"]["duration"], format_seconds),
            _winner_compare_metric("Average CPU", "cpu", run_a["metrics"]["avg_cpu"], run_b["metrics"]["avg_cpu"], format_percent),
            _winner_compare_metric("Average Memory", "memory-stick", run_a["metrics"]["avg_memory"], run_b["metrics"]["avg_memory"], format_percent),
            _winner_compare_metric("Total Energy", "zap", run_a["metrics"]["energy"], run_b["metrics"]["energy"], format_kwh),
            _winner_compare_metric("Total Carbon", "leaf", run_a["metrics"]["carbon"], run_b["metrics"]["carbon"], format_gco2_from_kg),
            _winner_compare_metric("Health Score", "heart-pulse", run_a["health_score"], run_b["health_score"], lambda value: f"{int(round(value))}/100", lower_is_better=False),
        ]

    return {
        "data_source": data_source,
        "run_options": run_options,
        "selected_run_a": selected_a,
        "selected_run_b": selected_b,
        "run_a": run_a,
        "run_b": run_b,
        "comparison_ready": comparison_ready,
        "compare_error": compare_error,
        "main_metrics": main_metrics,
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


@app.route("/compare")
def compare_page():
    df, run_summary, data_source = _data_or_empty()
    if df is None:
        return _empty_data_response()
    return render_template_string(
        APP_HTML,
        page="compare",
        **build_compare_context(
            df,
            run_summary,
            data_source,
            run_a_id=request.args.get("run_a"),
            run_b_id=request.args.get("run_b"),
        ),
    )


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
    normalized_stage_key = stage_key.lower()
    if normalized_stage_key == "operate":
        abort(404)
    context = build_run_context(df, run_summary, data_source, run_id, include_release_data=normalized_stage_key == "release")
    stage_detail_context = next((item for item in context["lifecycle_sections"] if item.get("key") == normalized_stage_key), None)
    if stage_detail_context is None:
        abort(404)
    if normalized_stage_key == "release":
        stage_detail_context = {**stage_detail_context, "release_data": context.get("release_build_data")}
    return render_template_string(APP_HTML, page="stage", stage_detail=stage_detail_context, **context)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5051, debug=False)
