"""Explainable anomaly detection for stage-level sustainability metrics."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd


ANOMALY_METRICS = [
    "duration_seconds",
    "workload_duration_seconds",
    "jenkins_stage_duration_seconds",
    "infrastructure_overhead_seconds",
    "overhead_percentage",
    "total_energy_kwh",
    "active_energy_kwh",
    "total_carbon_kg",
    "avg_cpu_percent",
]


def detect_stage_anomalies(current_run_df: pd.DataFrame, baseline_df: pd.DataFrame) -> List[Dict]:
    """Compare the current run against historical stage baselines."""
    if current_run_df is None or current_run_df.empty or baseline_df is None or baseline_df.empty:
        return []

    current = current_run_df.copy()
    if "stage" not in current.columns or "stage" not in baseline_df.columns:
        return []

    for metric in ANOMALY_METRICS:
        if metric not in current.columns:
            current[metric] = 0.0
        current[metric] = pd.to_numeric(current[metric], errors="coerce").fillna(0.0)

    current_stage_values = (
        current.groupby("stage", dropna=False)
        .agg(
            duration_seconds=("duration_seconds", "sum"),
            workload_duration_seconds=("workload_duration_seconds", "sum"),
            jenkins_stage_duration_seconds=("jenkins_stage_duration_seconds", "sum"),
            infrastructure_overhead_seconds=("infrastructure_overhead_seconds", "sum"),
            overhead_percentage=("overhead_percentage", "mean"),
            total_energy_kwh=("total_energy_kwh", "sum"),
            active_energy_kwh=("active_energy_kwh", "sum"),
            total_carbon_kg=("total_carbon_kg", "sum"),
            avg_cpu_percent=("avg_cpu_percent", "mean"),
        )
        .reset_index()
    )

    anomalies: List[Dict] = []
    for _, current_row in current_stage_values.iterrows():
        stage = current_row["stage"]
        baseline_rows = baseline_df[baseline_df["stage"] == stage]
        if baseline_rows.empty:
            continue

        baseline_row = baseline_rows.iloc[0]
        if _to_float(baseline_row.get("historical_samples")) is not None and int(baseline_row.get("historical_samples") or 0) < int(baseline_row.get("minimum_training_samples") or 0):
            continue
        for metric in ANOMALY_METRICS:
            anomaly = _build_anomaly(stage, metric, current_row.get(metric), baseline_row)
            if anomaly is not None:
                anomalies.append(anomaly)

    return anomalies


def summarize_anomalies(anomalies: List[Dict]) -> Dict[str, object]:
    critical_count = sum(1 for item in anomalies if item.get("severity") == "critical")
    warning_count = sum(1 for item in anomalies if item.get("severity") == "warning")

    if critical_count > 0:
        overall_status = "Critical"
        summary_message = f"{critical_count} critical anomaly(s) detected across monitored stages."
    elif warning_count > 0:
        overall_status = "Warning"
        summary_message = f"{warning_count} warning anomaly(s) detected across monitored stages."
    else:
        overall_status = "Normal"
        summary_message = "No stage-level sustainability anomalies were detected."

    return {
        "critical_count": critical_count,
        "warning_count": warning_count,
        "overall_status": overall_status,
        "summary_message": summary_message,
    }


def _build_anomaly(stage, metric, current_value, baseline_row):
    mean = _to_float(baseline_row.get(f"{metric}_mean"))
    std = _to_float(baseline_row.get(f"{metric}_std"))
    current_value = _to_float(current_value) or 0.0

    if mean is None:
        return None

    percentage_change = _percentage_change(current_value, mean)
    absolute_difference = abs(current_value - mean)
    z_score = None
    if std is not None and std > 0:
        z_score = (current_value - mean) / std

    severity = _determine_severity(metric, current_value, mean, percentage_change, absolute_difference, z_score)
    if severity == "normal":
        return None

    stage_label = str(stage).replace("_", " ").title()
    metric_label = _metric_label(metric)
    message = _build_message(stage_label, metric, metric_label, current_value, percentage_change, severity)

    return {
        "stage": stage,
        "metric": metric,
        "current_value": round(current_value, 8),
        "baseline_mean": round(mean, 8),
        "percentage_change": None if percentage_change is None else round(percentage_change, 2),
        "z_score": None if z_score is None else round(z_score, 2),
        "severity": severity,
        "message": message,
        "context_scope": baseline_row.get("context_scope", "stage"),
        "pipeline_name": baseline_row.get("pipeline_name", ""),
        "strategy": baseline_row.get("strategy", ""),
        "historical_samples": int(baseline_row.get("historical_samples") or 0),
        "strategy_specific": bool(baseline_row.get("strategy_specific", False)),
        "fallback_occurred": bool(baseline_row.get("fallback_occurred", False)),
        "fallback_reason": baseline_row.get("fallback_reason", ""),
    }


def _determine_severity(metric, current_value, baseline_mean, percentage_change, absolute_difference, z_score):
    if metric == "workload_duration_seconds" and absolute_difference < 1.0:
        return "normal"

    if metric == "overhead_percentage":
        if current_value >= 85 and (percentage_change is not None and percentage_change >= 20):
            return "critical"
        if current_value >= 60 and (percentage_change is not None and percentage_change >= 10):
            return "warning"
        if current_value >= 60:
            return "info"
        return "normal"

    if baseline_mean is None or current_value <= baseline_mean:
        return "normal"

    if percentage_change is None:
        return "normal"

    if percentage_change >= 75:
        return "critical"
    if percentage_change >= 30:
        return "warning"
    if z_score is not None and percentage_change >= 20 and z_score >= 2.5:
        return "critical"
    if z_score is not None and percentage_change >= 10 and z_score >= 1.5:
        return "warning"
    return "normal"


def _percentage_change(current_value, baseline_mean):
    if baseline_mean is None or baseline_mean == 0:
        return None
    return ((current_value - baseline_mean) / baseline_mean) * 100.0


def _format_percentage_for_message(percentage_change):
    if percentage_change is None:
        return "materially"
    return f"{round(percentage_change)}%"


def _build_message(stage_label, metric, metric_label, current_value, percentage_change, severity):
    if metric == "overhead_percentage" and severity == "info":
        return f"{stage_label} overhead is high, but consistent with previous runs."
    if severity == "info":
        return f"{stage_label} {metric_label} is stable compared with baseline."
    if percentage_change is None:
        return f"{stage_label} {metric_label} is above baseline."

    rounded_change = round(percentage_change)
    if metric == "overhead_percentage":
        return f"{stage_label} overhead is {rounded_change}% above baseline."
    if metric == "total_energy_kwh":
        return f"{stage_label} energy is {rounded_change}% above baseline."
    if metric == "total_carbon_kg":
        return f"{stage_label} carbon footprint is {rounded_change}% above baseline."
    if metric == "workload_duration_seconds":
        return f"{stage_label} workload duration is {rounded_change}% above baseline."
    return f"{stage_label} {metric_label} is {rounded_change}% above baseline."


def _metric_label(metric):
    labels = {
        "duration_seconds": "workload duration",
        "workload_duration_seconds": "workload duration",
        "jenkins_stage_duration_seconds": "full stage duration",
        "infrastructure_overhead_seconds": "infrastructure overhead",
        "overhead_percentage": "overhead percentage",
        "total_energy_kwh": "total energy",
        "active_energy_kwh": "active energy",
        "total_carbon_kg": "carbon footprint",
        "avg_cpu_percent": "average CPU load",
    }
    return labels.get(metric, metric.replace("_", " "))


def _to_float(value):
    if value is None or pd.isna(value):
        return None
    return float(value)
