"""Isolation Forest prototype for stage-level anomaly detection."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

from .baseline_model import LIFECYCLE_STAGES, MIN_CONTEXT_SAMPLES, normalize_lifecycle_stage, normalize_strategy, select_historical_context

try:
    from sklearn.ensemble import IsolationForest
except ImportError:  # pragma: no cover - depends on local environment setup
    IsolationForest = None


ML_FEATURE_COLUMNS = [
    "workload_duration_seconds",
    "jenkins_stage_duration_seconds",
    "overhead_percentage",
    "avg_cpu_percent",
    "peak_cpu_percent",
    "total_energy_kwh",
    "active_energy_kwh",
    "total_carbon_kg",
    "active_carbon_kg",
    "carbon_intensity_kg_per_kwh",
]

STAGE_SPECIFIC_MODELS = LIFECYCLE_STAGES
MIN_TRAINING_SAMPLES = MIN_CONTEXT_SAMPLES

WARMING_UP_RESPONSE = {
    "enabled": False,
    "status": "Warming Up",
    "message": "Stage-specific Isolation Forest models require more historical runs for reliable detection.",
    "results": [],
}


def prepare_ml_features(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare numeric ML features from stage-level pipeline data."""
    stage_records = _build_stage_records(df)
    if stage_records.empty:
        return pd.DataFrame(columns=ML_FEATURE_COLUMNS)

    prepared = stage_records.copy()

    for column in ML_FEATURE_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = 0.0
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce").fillna(0.0)

    return prepared[ML_FEATURE_COLUMNS]


def detect_ml_anomalies(current_run_df: pd.DataFrame, historical_df: pd.DataFrame) -> Dict[str, object]:
    """Detect stage anomalies using independent stage-specific Isolation Forest models."""
    historical_df = _filter_historical_context(current_run_df, historical_df)
    historical_stage_records = _build_stage_records(historical_df)
    current_stage_records = _build_stage_records(current_run_df)

    if current_stage_records.empty:
        return {
            "enabled": False,
            "status": "Normal",
            "message": "No current stage data is available for ML anomaly detection.",
            "results": [],
            "historical_samples_used": int(len(historical_stage_records)),
            "model": "Stage-specific Isolation Forest",
            "stage_models": _build_stage_model_summary(historical_stage_records, current_stage_records, active_stages=set()),
        }

    if IsolationForest is None:
        return {
            "enabled": False,
            "status": "Warming Up",
            "message": "scikit-learn is not installed yet. Install project requirements to enable Isolation Forest detection.",
            "results": _build_dependency_warming_results(historical_stage_records, current_stage_records),
            "historical_samples_used": int(len(historical_stage_records)),
            "model": "Stage-specific Isolation Forest",
            "stage_models": _build_stage_model_summary(historical_stage_records, current_stage_records, active_stages=set()),
        }

    results: List[Dict[str, object]] = []
    active_stages = set()

    for stage in STAGE_SPECIFIC_MODELS:
        current_stage_df = current_stage_records[current_stage_records["stage"] == stage].copy()
        current_context = _current_stage_context(current_stage_df, stage)
        historical_stage_df, context = select_historical_context(
            historical_stage_records,
            stage,
            pipeline_name=current_context.get("pipeline_name"),
            strategy=current_context.get("strategy"),
            min_samples=MIN_TRAINING_SAMPLES,
        )
        historical_samples = int(context.get("historical_samples", len(historical_stage_df)))

        if historical_samples < MIN_TRAINING_SAMPLES:
            results.append(_build_warming_up_result(stage, historical_samples, context))
            continue

        active_stages.add(stage)
        if current_stage_df.empty:
            results.append(_build_no_current_stage_result(stage, historical_samples, context))
            continue

        historical_features = _prepare_stage_features(historical_stage_df)
        current_features = _prepare_stage_features(current_stage_df)
        if historical_features.empty or current_features.empty:
            results.append(_build_no_current_stage_result(stage, historical_samples, context))
            continue

        model = IsolationForest(contamination=0.15, random_state=42)
        model.fit(historical_features)

        prediction = int(model.predict(current_features)[0])
        score = float(model.decision_function(current_features)[0])
        is_anomaly = prediction == -1
        severity = _ml_severity(is_anomaly, score)

        results.append(
            {
                "stage": stage,
                "model_status": "active",
                "historical_samples": historical_samples,
                **_context_result_fields(context),
                "is_anomaly": is_anomaly,
                "prediction": "Anomaly" if is_anomaly else "Normal",
                "anomaly_score": round(score, 6),
                "severity": severity,
                "message": _ml_message(stage, is_anomaly, score, context),
            }
        )

    status = _overall_ml_status(results)
    if any(item["severity"] == "critical" for item in results):
        status = "Critical"
    elif any(item["severity"] == "warning" for item in results):
        status = "Warning"

    return {
        "enabled": bool(active_stages),
        "status": status,
        "message": _overall_ml_message(results),
        "results": results,
        "historical_samples_used": int(len(historical_stage_records)),
        "model": "Stage-specific Isolation Forest",
        "stage_models": _build_stage_model_summary(historical_stage_records, current_stage_records, active_stages=active_stages),
    }


def _prepare_stage_features(stage_records: pd.DataFrame) -> pd.DataFrame:
    if stage_records is None or stage_records.empty:
        return pd.DataFrame(columns=ML_FEATURE_COLUMNS)

    prepared = stage_records.copy()
    for column in ML_FEATURE_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = 0.0
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce").fillna(0.0)

    return prepared[ML_FEATURE_COLUMNS]


def _filter_historical_context(current_run_df: pd.DataFrame, historical_df: pd.DataFrame) -> pd.DataFrame:
    filtered = historical_df.copy() if historical_df is not None else pd.DataFrame()
    current = current_run_df.copy() if current_run_df is not None else pd.DataFrame()
    if filtered.empty or current.empty:
        return filtered

    if "run_id" in filtered.columns and "run_id" in current.columns:
        current_run_ids = set(current["run_id"].dropna().astype(str).tolist())
        filtered = filtered[~filtered["run_id"].astype(str).isin(current_run_ids)].copy()

    if "pipeline_name" in filtered.columns and "pipeline_name" in current.columns:
        current_pipelines = current["pipeline_name"].dropna().astype(str).unique().tolist()
        if current_pipelines:
            filtered = filtered[filtered["pipeline_name"].astype(str).isin(current_pipelines)].copy()

    return filtered


def _current_stage_context(current_stage_df: pd.DataFrame, stage: str) -> Dict[str, str]:
    if current_stage_df is None or current_stage_df.empty:
        return {"stage": stage, "pipeline_name": "", "strategy": ""}
    row = current_stage_df.iloc[0]
    return {
        "stage": stage,
        "pipeline_name": str(row.get("pipeline_name") or ""),
        "strategy": normalize_strategy(row.get("strategy")),
    }


def _build_dependency_warming_results(
    historical_stage_records: pd.DataFrame,
    current_stage_records: pd.DataFrame,
) -> List[Dict[str, object]]:
    results = []
    for stage in STAGE_SPECIFIC_MODELS:
        current_context = _current_stage_context(
            current_stage_records[current_stage_records["stage"] == stage] if current_stage_records is not None and not current_stage_records.empty else pd.DataFrame(),
            stage,
        )
        _, context = select_historical_context(
            historical_stage_records,
            stage,
            pipeline_name=current_context.get("pipeline_name"),
            strategy=current_context.get("strategy"),
            min_samples=MIN_TRAINING_SAMPLES,
        )
        results.append(_build_warming_up_result(stage, int(context.get("historical_samples", 0)), context))
    return results


def _context_result_fields(context: Dict[str, object]) -> Dict[str, object]:
    return {
        "context_scope": context.get("context_scope", "insufficient"),
        "pipeline_name": context.get("pipeline_name", ""),
        "strategy": context.get("strategy", ""),
        "strategy_specific": bool(context.get("strategy_specific", False)),
        "fallback_occurred": bool(context.get("fallback_occurred", False)),
        "fallback_reason": context.get("fallback_reason", ""),
        "minimum_training_samples": int(context.get("minimum_training_samples", MIN_TRAINING_SAMPLES)),
    }


def _build_warming_up_result(stage: str, historical_samples: int, context: Dict[str, object] | None = None) -> Dict[str, object]:
    context = context or {}
    return {
        "stage": stage,
        "model_status": "warming_up",
        "historical_samples": historical_samples,
        **_context_result_fields(context),
        "is_anomaly": False,
        "prediction": "Warming Up",
        "anomaly_score": None,
        "severity": "normal",
        "message": (
            f"{stage.replace('_', ' ').title()} Isolation Forest is warming up "
            f"({historical_samples}/{MIN_TRAINING_SAMPLES} historical stage records)."
        ),
    }


def _build_no_current_stage_result(stage: str, historical_samples: int, context: Dict[str, object] | None = None) -> Dict[str, object]:
    context = context or {}
    return {
        "stage": stage,
        "model_status": "active",
        "historical_samples": historical_samples,
        **_context_result_fields(context),
        "is_anomaly": False,
        "prediction": "Not Available",
        "anomaly_score": None,
        "severity": "normal",
        "message": f"No current {stage.replace('_', ' ').title()} stage record is available for ML anomaly detection.",
    }


def _stage_sample_count(stage_records: pd.DataFrame, stage: str) -> int:
    if stage_records is None or stage_records.empty or "stage" not in stage_records.columns:
        return 0
    return int(len(stage_records[stage_records["stage"] == stage]))


def _build_stage_model_summary(
    stage_records: pd.DataFrame,
    current_stage_records: pd.DataFrame | None = None,
    active_stages: set[str] | None = None,
) -> Dict[str, Dict[str, object]]:
    active_stages = active_stages or set()
    summary = {}
    for stage in STAGE_SPECIFIC_MODELS:
        current_context = _current_stage_context(
            current_stage_records[current_stage_records["stage"] == stage] if current_stage_records is not None and not current_stage_records.empty else pd.DataFrame(),
            stage,
        )
        _, context = select_historical_context(
            stage_records,
            stage,
            pipeline_name=current_context.get("pipeline_name"),
            strategy=current_context.get("strategy"),
            min_samples=MIN_TRAINING_SAMPLES,
        )
        historical_samples = int(context.get("historical_samples", _stage_sample_count(stage_records, stage)))
        summary[stage] = {
            "model_status": "active" if stage in active_stages else "warming_up",
            "historical_samples": historical_samples,
            "minimum_training_samples": MIN_TRAINING_SAMPLES,
            **_context_result_fields(context),
        }
    return summary


def _overall_ml_status(results: List[Dict[str, object]]) -> str:
    if not results:
        return "Normal"
    active_count = sum(1 for item in results if item.get("model_status") == "active")
    if active_count == 0:
        return "Warming Up"
    if active_count < len(STAGE_SPECIFIC_MODELS):
        return "Partial Active"
    return "Active"


def _overall_ml_message(results: List[Dict[str, object]]) -> str:
    active_count = sum(1 for item in results if item.get("model_status") == "active")
    if active_count == 0:
        return "All stage-specific Isolation Forest models are warming up; statistical anomaly detection remains active."
    if active_count < len(STAGE_SPECIFIC_MODELS):
        return (
            f"{active_count} of {len(STAGE_SPECIFIC_MODELS)} stage-specific Isolation Forest models are active; "
            "warming stages continue to rely on statistical anomaly detection."
        )
    return (
        "Stage-specific Isolation Forest models learn normal behavior separately for Release, Deploy, and Operate "
        "across duration, CPU, energy, carbon, and overhead."
    )


def _build_stage_records(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "run_id",
                "pipeline_name",
                "stage",
                "strategy",
                "workload_duration_seconds",
                "jenkins_stage_duration_seconds",
                "overhead_percentage",
                "avg_cpu_percent",
                "peak_cpu_percent",
                "total_energy_kwh",
                "active_energy_kwh",
                "total_carbon_kg",
                "active_carbon_kg",
                "carbon_intensity_kg_per_kwh",
            ]
        )

    prepared = df.copy()
    if "run_id" not in prepared.columns:
        prepared["run_id"] = "unknown-run"
    prepared["run_id"] = prepared["run_id"].fillna("unknown-run").astype(str)

    if "pipeline_name" not in prepared.columns:
        prepared["pipeline_name"] = ""
    prepared["pipeline_name"] = prepared["pipeline_name"].fillna("").astype(str)

    if "stage" not in prepared.columns:
        prepared["stage"] = "unknown"
    prepared["stage"] = prepared["stage"].map(normalize_lifecycle_stage)
    prepared.loc[prepared["stage"] == "", "stage"] = "unknown"

    if "strategy" not in prepared.columns:
        prepared["strategy"] = ""
    prepared["strategy"] = prepared["strategy"].map(normalize_strategy)

    numeric_defaults = {
        "workload_duration_seconds": 0.0,
        "jenkins_stage_duration_seconds": 0.0,
        "overhead_percentage": 0.0,
        "avg_cpu_percent": 0.0,
        "peak_cpu_percent": 0.0,
        "total_energy_kwh": 0.0,
        "active_energy_kwh": 0.0,
        "total_carbon_kg": 0.0,
        "active_carbon_kg": 0.0,
        "carbon_intensity_kg_per_kwh": 0.0,
    }
    for column, default in numeric_defaults.items():
        if column not in prepared.columns:
            prepared[column] = default
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce").fillna(default)

    if "workload_duration_seconds" not in df.columns and "duration_seconds" in prepared.columns:
        prepared["workload_duration_seconds"] = pd.to_numeric(
            prepared["duration_seconds"], errors="coerce"
        ).fillna(0.0)

    if "jenkins_stage_duration_seconds" not in df.columns:
        prepared["jenkins_stage_duration_seconds"] = prepared["workload_duration_seconds"]

    if "overhead_percentage" not in df.columns:
        prepared["overhead_percentage"] = 0.0
        non_zero_duration = prepared["jenkins_stage_duration_seconds"] > 0
        prepared.loc[non_zero_duration, "overhead_percentage"] = (
            (
                prepared.loc[non_zero_duration, "jenkins_stage_duration_seconds"]
                - prepared.loc[non_zero_duration, "workload_duration_seconds"]
            ).clip(lower=0.0)
            / prepared.loc[non_zero_duration, "jenkins_stage_duration_seconds"]
            * 100.0
        )

    aggregated = (
        prepared.groupby(["run_id", "pipeline_name", "stage", "strategy"], dropna=False)
        .agg(
            workload_duration_seconds=("workload_duration_seconds", "sum"),
            jenkins_stage_duration_seconds=("jenkins_stage_duration_seconds", "sum"),
            overhead_percentage=("overhead_percentage", "mean"),
            avg_cpu_percent=("avg_cpu_percent", "mean"),
            peak_cpu_percent=("peak_cpu_percent", "max"),
            total_energy_kwh=("total_energy_kwh", "sum"),
            active_energy_kwh=("active_energy_kwh", "sum"),
            total_carbon_kg=("total_carbon_kg", "sum"),
            active_carbon_kg=("active_carbon_kg", "sum"),
            carbon_intensity_kg_per_kwh=("carbon_intensity_kg_per_kwh", "mean"),
        )
        .reset_index()
    )

    for column in aggregated.columns:
        if column not in {"run_id", "pipeline_name", "stage", "strategy"}:
            aggregated[column] = pd.to_numeric(aggregated[column], errors="coerce").fillna(0.0)

    return aggregated


def _ml_severity(is_anomaly: bool, score: float) -> str:
    if not is_anomaly:
        return "normal"
    if score < -0.10:
        return "critical"
    return "warning"


def _ml_message(stage: str, is_anomaly: bool, score: float, context: Dict[str, object] | None = None) -> str:
    stage_label = stage.replace("_", " ").title()
    context = context or {}
    context_label = str(context.get("context_scope", "stage")).replace("_", " ")
    if not is_anomaly:
        return f"{stage_label} is within the learned normal pattern for the {context_label} context."
    if score < -0.10:
        return f"{stage_label} shows a strongly unusual stage pattern compared with the {context_label} history."
    return f"{stage_label} shows a mild unusual stage pattern compared with the {context_label} history."
