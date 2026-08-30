"""Historical baseline calculations for monitored pipeline metrics."""

from __future__ import annotations

from typing import Dict

import pandas as pd


LIFECYCLE_STAGES = ["release", "deploy", "operate"]
MIN_CONTEXT_SAMPLES = 10
MISSING_STRATEGY_LABEL = "missing"

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


def normalize_lifecycle_stage(stage) -> str:
    return str(stage or "").strip().lower()


def normalize_strategy(strategy) -> str:
    value = str(strategy or "").strip().lower()
    return value if value else MISSING_STRATEGY_LABEL


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


def select_historical_context(
    historical_df: pd.DataFrame,
    stage: str,
    pipeline_name: str | None = None,
    strategy: str | None = None,
    min_samples: int = MIN_CONTEXT_SAMPLES,
) -> tuple[pd.DataFrame, Dict[str, object]]:
    """Select historical rows using pipeline/stage/strategy hierarchy."""
    required_columns = ["run_id", "pipeline_name", "stage", "strategy", *STAGE_BASELINE_METRICS]
    prepared = _prepare_dataframe(historical_df, required_columns, STAGE_BASELINE_METRICS)
    if prepared.empty:
        return prepared, _context_metadata(
            stage=stage,
            pipeline_name=pipeline_name,
            strategy=strategy,
            context_scope="insufficient",
            samples=0,
            strategy_specific=False,
            fallback_reason="No historical records are available.",
            min_samples=min_samples,
        )

    prepared["stage"] = prepared["stage"].map(normalize_lifecycle_stage)
    prepared["pipeline_name"] = prepared["pipeline_name"].fillna("").astype(str)
    prepared["strategy"] = prepared["strategy"].map(normalize_strategy)

    stage = normalize_lifecycle_stage(stage)
    strategy = normalize_strategy(strategy)
    pipeline_name = str(pipeline_name or "").strip()
    stage_df = prepared[prepared["stage"] == stage].copy()

    if stage_df.empty:
        return stage_df, _context_metadata(
            stage=stage,
            pipeline_name=pipeline_name,
            strategy=strategy,
            context_scope="insufficient",
            samples=0,
            strategy_specific=False,
            fallback_reason=f"No historical {stage.title()} records are available.",
            min_samples=min_samples,
        )

    if pipeline_name:
        pipeline_stage_df = stage_df[stage_df["pipeline_name"] == pipeline_name].copy()
        if not pipeline_stage_df.empty:
            strategy_stage_df = pipeline_stage_df[pipeline_stage_df["strategy"] == strategy].copy()
            if strategy != MISSING_STRATEGY_LABEL and len(strategy_stage_df) >= min_samples:
                return strategy_stage_df, _context_metadata(
                    stage=stage,
                    pipeline_name=pipeline_name,
                    strategy=strategy,
                    context_scope="pipeline_stage_strategy",
                    samples=len(strategy_stage_df),
                    strategy_specific=True,
                    fallback_reason="",
                    min_samples=min_samples,
                )
            if len(pipeline_stage_df) >= min_samples:
                reason = ""
                if strategy != MISSING_STRATEGY_LABEL:
                    reason = (
                        f"Only {len(strategy_stage_df)} historical {stage.title()}/{strategy.title()} records; "
                        f"using pipeline-level {stage.title()} history."
                    )
                return pipeline_stage_df, _context_metadata(
                    stage=stage,
                    pipeline_name=pipeline_name,
                    strategy=strategy,
                    context_scope="pipeline_stage",
                    samples=len(pipeline_stage_df),
                    strategy_specific=False,
                    fallback_reason=reason,
                    min_samples=min_samples,
                )

    if len(stage_df) >= min_samples:
        reason = (
            f"Only {len(stage_df[stage_df['pipeline_name'] == pipeline_name])} historical "
            f"{pipeline_name}/{stage.title()} records; using all {stage.title()} history."
        ) if pipeline_name else ""
        return stage_df, _context_metadata(
            stage=stage,
            pipeline_name="",
            strategy=strategy,
            context_scope="stage",
            samples=len(stage_df),
            strategy_specific=False,
            fallback_reason=reason,
            min_samples=min_samples,
        )

    return stage_df, _context_metadata(
        stage=stage,
        pipeline_name=pipeline_name,
        strategy=strategy,
        context_scope="insufficient",
        samples=len(stage_df),
        strategy_specific=False,
        fallback_reason=(
            f"Only {len(stage_df)} historical {stage.title()} records are available; "
            f"{min_samples} are required."
        ),
        min_samples=min_samples,
    )


def calculate_stage_baselines(
    df: pd.DataFrame,
    current_run_df: pd.DataFrame | None = None,
    min_samples: int = MIN_CONTEXT_SAMPLES,
) -> pd.DataFrame:
    """Return contextual per-stage mean/std baselines for monitoring metrics."""
    required_columns = ["stage", "pipeline_name", "strategy", *STAGE_BASELINE_METRICS]
    prepared = _prepare_dataframe(df, required_columns, STAGE_BASELINE_METRICS)

    if prepared.empty or "stage" not in prepared.columns:
        columns = _baseline_columns()
        return pd.DataFrame(columns=columns)

    prepared["stage"] = prepared["stage"].map(normalize_lifecycle_stage)
    prepared["strategy"] = prepared["strategy"].map(normalize_strategy)

    if current_run_df is None or current_run_df.empty:
        grouped = _aggregate_baseline(prepared)
        grouped["context_scope"] = "stage"
        grouped["pipeline_name"] = ""
        grouped["strategy"] = MISSING_STRATEGY_LABEL
        grouped["historical_samples"] = grouped["sample_count"]
        grouped["minimum_training_samples"] = min_samples
        grouped["strategy_specific"] = False
        grouped["fallback_occurred"] = False
        grouped["fallback_reason"] = ""
        return grouped[_baseline_columns()]

    current = _prepare_dataframe(current_run_df, ["stage", "pipeline_name", "strategy"], [])
    current["stage"] = current["stage"].map(normalize_lifecycle_stage)
    current["strategy"] = current["strategy"].map(normalize_strategy)
    current["pipeline_name"] = current["pipeline_name"].fillna("").astype(str)

    rows = []
    for _, current_row in current.drop_duplicates(["stage", "pipeline_name", "strategy"]).iterrows():
        stage = current_row.get("stage")
        if stage not in LIFECYCLE_STAGES:
            continue
        context_df, metadata = select_historical_context(
            prepared,
            stage,
            pipeline_name=current_row.get("pipeline_name"),
            strategy=current_row.get("strategy"),
            min_samples=min_samples,
        )
        if context_df.empty or len(context_df) < min_samples:
            baseline = _empty_baseline_row(stage, metadata)
        else:
            baseline = _aggregate_baseline(context_df).iloc[0].to_dict()
            baseline.update(metadata)
        rows.append(baseline)

    if not rows:
        return pd.DataFrame(columns=_baseline_columns())
    return pd.DataFrame(rows)[_baseline_columns()]


def _aggregate_baseline(prepared: pd.DataFrame) -> pd.DataFrame:
    grouped = prepared.groupby("stage", dropna=False)[STAGE_BASELINE_METRICS].agg(["mean", "std", "count"]).reset_index()
    flattened_columns = []
    for column in grouped.columns.to_flat_index():
        if isinstance(column, tuple):
            if column[0] == "stage":
                flattened_columns.append("stage")
            elif column[1] == "count":
                flattened_columns.append(f"{column[0]}_count")
            else:
                flattened_columns.append(f"{column[0]}_{column[1]}")
        else:
            flattened_columns.append(column)
    grouped.columns = flattened_columns
    grouped["sample_count"] = grouped.get(f"{STAGE_BASELINE_METRICS[0]}_count", 0).astype(int)
    for metric in STAGE_BASELINE_METRICS:
        count_column = f"{metric}_count"
        if count_column in grouped.columns:
            grouped = grouped.drop(columns=[count_column])
    return grouped


def _baseline_columns() -> list[str]:
    columns = [
        "stage",
        "context_scope",
        "pipeline_name",
        "strategy",
        "historical_samples",
        "sample_count",
        "minimum_training_samples",
        "strategy_specific",
        "fallback_occurred",
        "fallback_reason",
    ]
    for metric in STAGE_BASELINE_METRICS:
        columns.extend([f"{metric}_mean", f"{metric}_std"])
    return columns


def _empty_baseline_row(stage: str, metadata: Dict[str, object]) -> Dict[str, object]:
    row = {"stage": stage, "sample_count": metadata.get("historical_samples", 0)}
    row.update(metadata)
    for metric in STAGE_BASELINE_METRICS:
        row[f"{metric}_mean"] = None
        row[f"{metric}_std"] = None
    return row


def _context_metadata(
    *,
    stage: str,
    pipeline_name: str | None,
    strategy: str | None,
    context_scope: str,
    samples: int,
    strategy_specific: bool,
    fallback_reason: str,
    min_samples: int,
) -> Dict[str, object]:
    return {
        "context_scope": context_scope,
        "pipeline_name": pipeline_name or "",
        "stage": normalize_lifecycle_stage(stage),
        "strategy": normalize_strategy(strategy),
        "historical_samples": int(samples),
        "sample_count": int(samples),
        "minimum_training_samples": int(min_samples),
        "strategy_specific": bool(strategy_specific),
        "fallback_occurred": bool(fallback_reason),
        "fallback_reason": fallback_reason,
    }


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
