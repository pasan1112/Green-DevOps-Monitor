"""Historical baseline calculations for monitored pipeline metrics."""

from __future__ import annotations

from typing import Dict

import pandas as pd


STAGE_BASELINE_METRICS = [
    "duration_seconds",
    "workload_duration_seconds",
    "jenkins_stage_duration_seconds",
    "infrastructure_overhead_seconds",
    "overhead_percentage",
    "avg_cpu_percent",
    "total_energy_kwh",
    "active_energy_kwh",
    "total_carbon_kg",
    "active_carbon_kg",
]

PIPELINE_BASELINE_METRICS = [
    "duration_seconds",
    "workload_duration_seconds",
    "jenkins_stage_duration_seconds",
    "infrastructure_overhead_seconds",
    "overhead_percentage",
    "total_energy_kwh",
    "total_carbon_kg",
]


def _prepare_dataframe(
    df: pd.DataFrame,
    required_columns: list[str],
    numeric_columns: list[str] | None = None,
) -> pd.DataFrame:
    prepared = df.copy() if df is not None else pd.DataFrame()
    numeric_columns = numeric_columns or []

    if prepared.empty:
        for column in required_columns:
            if column in numeric_columns:
                prepared[column] = pd.Series(dtype="float64")
            else:
                prepared[column] = pd.Series(dtype="object")
        return prepared

    for column in required_columns:
        if column not in prepared.columns:
            prepared[column] = 0.0 if column in numeric_columns else ""
        if column in numeric_columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
        else:
            prepared[column] = prepared[column].fillna("").astype(str)

    return prepared


def calculate_stage_baselines(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-stage mean/std baselines for monitoring metrics."""
    required_columns = ["stage", *STAGE_BASELINE_METRICS]
    prepared = _prepare_dataframe(df, required_columns, STAGE_BASELINE_METRICS)

    if prepared.empty or "stage" not in prepared.columns:
        columns = ["stage"]
        for metric in STAGE_BASELINE_METRICS:
            columns.extend([f"{metric}_mean", f"{metric}_std"])
        return pd.DataFrame(columns=columns)

    grouped = (
        prepared.groupby("stage", dropna=False)[STAGE_BASELINE_METRICS]
        .agg(["mean", "std"])
        .reset_index()
    )
    flattened_columns = []
    for column in grouped.columns.to_flat_index():
        if isinstance(column, tuple):
            if column[0] == "stage":
                flattened_columns.append("stage")
            else:
                flattened_columns.append(f"{column[0]}_{column[1]}")
        else:
            flattened_columns.append(column)
    grouped.columns = flattened_columns

    return grouped


def calculate_pipeline_baseline(df: pd.DataFrame) -> Dict[str, float]:
    """Return overall run-level mean/std baselines for timing, energy, carbon, and overhead."""
    required_columns = ["run_id", *PIPELINE_BASELINE_METRICS]
    prepared = _prepare_dataframe(df, required_columns, PIPELINE_BASELINE_METRICS)

    if prepared.empty or "run_id" not in prepared.columns:
        return {
            "run_count": 0,
            "duration_seconds_mean": None,
            "duration_seconds_std": None,
            "workload_duration_seconds_mean": None,
            "workload_duration_seconds_std": None,
            "jenkins_stage_duration_seconds_mean": None,
            "jenkins_stage_duration_seconds_std": None,
            "infrastructure_overhead_seconds_mean": None,
            "infrastructure_overhead_seconds_std": None,
            "overhead_percentage_mean": None,
            "overhead_percentage_std": None,
            "total_energy_kwh_mean": None,
            "total_energy_kwh_std": None,
            "total_carbon_kg_mean": None,
            "total_carbon_kg_std": None,
        }

    aggregation_map = {
        "duration_seconds": "sum",
        "workload_duration_seconds": "sum",
        "jenkins_stage_duration_seconds": "sum",
        "infrastructure_overhead_seconds": "sum",
        "overhead_percentage": "mean",
        "total_energy_kwh": "sum",
        "total_carbon_kg": "sum",
    }
    pipeline_totals = prepared.groupby("run_id", dropna=False).agg(aggregation_map).reset_index()

    baseline: Dict[str, float] = {"run_count": int(len(pipeline_totals))}
    for metric in PIPELINE_BASELINE_METRICS:
        baseline[f"{metric}_mean"] = _safe_float(pipeline_totals[metric].mean())
        baseline[f"{metric}_std"] = _safe_float(pipeline_totals[metric].std())

    return baseline


def _safe_float(value):
    if pd.isna(value):
        return None
    return float(value)
