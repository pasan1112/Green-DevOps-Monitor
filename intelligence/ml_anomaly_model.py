"""Isolation Forest prototype for stage-level anomaly detection."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

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
    "stage_encoded",
]

STAGE_ENCODING = {
    "build": 0,
    "test": 1,
    "deploy": 2,
    "unknown": 3,
}

WARMING_UP_RESPONSE = {
    "enabled": False,
    "status": "Warming Up",
    "message": "Isolation Forest requires more historical runs for reliable detection.",
    "results": [],
}


def prepare_ml_features(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare numeric ML features from stage-level pipeline data."""
    stage_records = _build_stage_records(df)
    if stage_records.empty:
        return pd.DataFrame(columns=ML_FEATURE_COLUMNS)

    prepared = stage_records.copy()
    prepared["stage_encoded"] = prepared["stage"].apply(_encode_stage)

    for column in ML_FEATURE_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = 0.0
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce").fillna(0.0)

    return prepared[ML_FEATURE_COLUMNS]


def detect_ml_anomalies(current_run_df: pd.DataFrame, historical_df: pd.DataFrame) -> Dict[str, object]:
    """Detect stage anomalies using an Isolation Forest model."""
    historical_stage_records = _build_stage_records(historical_df)
    historical_features = prepare_ml_features(historical_df)

    if IsolationForest is None:
        return {
            "enabled": False,
            "status": "Warming Up",
            "message": "scikit-learn is not installed yet. Install project requirements to enable Isolation Forest detection.",
            "results": [],
            "historical_samples_used": int(len(historical_stage_records)),
            "model": "Isolation Forest",
        }

    if len(historical_features) < 10:
        return {
            **WARMING_UP_RESPONSE,
            "historical_samples_used": int(len(historical_stage_records)),
            "model": "Isolation Forest",
        }

    current_stage_records = _build_stage_records(current_run_df)
    current_features = prepare_ml_features(current_run_df)

    if current_stage_records.empty or current_features.empty:
        return {
            "enabled": True,
            "status": "Normal",
            "message": "No current stage data is available for ML anomaly detection.",
            "results": [],
            "historical_samples_used": int(len(historical_stage_records)),
            "model": "Isolation Forest",
        }

    model = IsolationForest(contamination=0.15, random_state=42)
    model.fit(historical_features)

    predictions = model.predict(current_features)
    anomaly_scores = model.decision_function(current_features)

    results: List[Dict[str, object]] = []
    for row, prediction, score in zip(
        current_stage_records.to_dict(orient="records"),
        predictions.tolist(),
        anomaly_scores.tolist(),
    ):
        stage = str(row.get("stage", "unknown"))
        is_anomaly = int(prediction) == -1
        severity = _ml_severity(is_anomaly, float(score))
        message = _ml_message(stage, is_anomaly, float(score))
        results.append(
            {
                "stage": stage,
                "prediction": "Anomaly" if is_anomaly else "Normal",
                "anomaly_score": round(float(score), 6),
                "severity": severity,
                "message": message,
            }
        )

    status = "Normal"
    if any(item["severity"] == "critical" for item in results):
        status = "Critical"
    elif any(item["severity"] == "warning" for item in results):
        status = "Warning"

    return {
        "enabled": True,
        "status": status,
        "message": (
            "Isolation Forest learns normal pipeline behavior from historical runs and flags unusual stage patterns "
            "across duration, CPU, energy, carbon, and overhead."
        ),
        "results": results,
        "historical_samples_used": int(len(historical_stage_records)),
        "model": "Isolation Forest",
    }


def _build_stage_records(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "run_id",
                "stage",
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

    if "stage" not in prepared.columns:
        prepared["stage"] = "unknown"
    prepared["stage"] = prepared["stage"].fillna("unknown").astype(str).str.strip().str.lower()
    prepared.loc[prepared["stage"] == "", "stage"] = "unknown"

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
        prepared.groupby(["run_id", "stage"], dropna=False)
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
        if column not in {"run_id", "stage"}:
            aggregated[column] = pd.to_numeric(aggregated[column], errors="coerce").fillna(0.0)

    return aggregated


def _encode_stage(stage: object) -> int:
    normalized = str(stage).strip().lower() if stage is not None else "unknown"
    return STAGE_ENCODING.get(normalized, STAGE_ENCODING["unknown"])


def _ml_severity(is_anomaly: bool, score: float) -> str:
    if not is_anomaly:
        return "normal"
    if score < -0.10:
        return "critical"
    return "warning"


def _ml_message(stage: str, is_anomaly: bool, score: float) -> str:
    stage_label = stage.replace("_", " ").title()
    if not is_anomaly:
        return f"{stage_label} is within the learned normal pipeline pattern."
    if score < -0.10:
        return f"{stage_label} shows a strongly unusual stage pattern compared with historical runs."
    return f"{stage_label} shows a mild unusual stage pattern compared with historical runs."
