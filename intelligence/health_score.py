"""Rule-based sustainability health scoring for monitored pipeline runs."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

from .anomaly_model import summarize_anomalies


def calculate_sustainability_score(
    current_run_df: pd.DataFrame,
    baseline_df: Dict[str, float],
    anomalies: List[Dict],
    stage_baseline_df: pd.DataFrame | None = None,
) -> Dict[str, object]:
    anomalies = anomalies or []
    current = current_run_df.copy() if current_run_df is not None else pd.DataFrame()
    for column in [
        "duration_seconds",
        "total_energy_kwh",
        "total_carbon_kg",
        "overhead_percentage",
        "infrastructure_overhead_seconds",
    ]:
        if column not in current.columns:
            current[column] = 0.0
        current[column] = pd.to_numeric(current[column], errors="coerce").fillna(0.0)

    current_totals = {
        "duration_seconds": float(current["duration_seconds"].sum()) if not current.empty else 0.0,
        "total_energy_kwh": float(current["total_energy_kwh"].sum()) if not current.empty else 0.0,
        "total_carbon_kg": float(current["total_carbon_kg"].sum()) if not current.empty else 0.0,
    }

    explanation_bits = []

    contextual_baseline = _contextual_baseline_totals(stage_baseline_df)
    comparison_baseline = contextual_baseline or baseline_df
    baseline_context = _baseline_context(stage_baseline_df, comparison_baseline)

    if current.empty:
        score = 50
        explanation = "This run has insufficient lifecycle telemetry for a confident sustainability score."
        return {
            "score": score,
            "grade": _grade_from_score(score),
            "status": "Warning",
            "explanation": explanation,
            "baseline_context": baseline_context,
        }

    component_scores = {
        "total_energy_kwh": _baseline_component_score(
            current_totals["total_energy_kwh"],
            comparison_baseline.get("total_energy_kwh_mean"),
        ),
        "total_carbon_kg": _baseline_component_score(
            current_totals["total_carbon_kg"],
            comparison_baseline.get("total_carbon_kg_mean"),
        ),
        "duration_seconds": _baseline_component_score(
            current_totals["duration_seconds"],
            comparison_baseline.get("duration_seconds_mean"),
        ),
    }

    score = _weighted_sustainability_score(component_scores)

    if _is_worse_than_baseline(current_totals["total_energy_kwh"], comparison_baseline.get("total_energy_kwh_mean")):
        explanation_bits.append("total pipeline energy is above baseline")
    if _is_worse_than_baseline(current_totals["total_carbon_kg"], comparison_baseline.get("total_carbon_kg_mean")):
        explanation_bits.append("total pipeline carbon is above baseline")
    if _is_worse_than_baseline(current_totals["duration_seconds"], comparison_baseline.get("duration_seconds_mean")):
        explanation_bits.append("pipeline duration is above baseline")

    critical_count = sum(1 for item in anomalies if item.get("severity") == "critical")
    warning_count = sum(1 for item in anomalies if item.get("severity") == "warning")
    score -= critical_count * 15
    score -= warning_count * 7

    max_overhead_percentage = float(current["overhead_percentage"].max()) if not current.empty else 0.0
    if max_overhead_percentage > 85:
        score -= 12
        explanation_bits.append("infrastructure overhead is extremely high")
    elif max_overhead_percentage > 60:
        score -= 5
        explanation_bits.append("infrastructure overhead is above the preferred range")

    failed_stage = False
    if "status" in current.columns:
        failed_stage = current["status"].astype(str).str.lower().eq("failed").any()
    if failed_stage:
        score -= 20
        explanation_bits.append("at least one stage failed")

    score = max(0, min(100, score))

    anomaly_summary = summarize_anomalies(anomalies)
    if failed_stage and anomaly_summary["overall_status"] != "Critical":
        status = "Critical"
    elif any(bit in explanation_bits for bit in ["total pipeline energy is above baseline", "total pipeline carbon is above baseline"]):
        status = "Critical" if anomaly_summary["overall_status"] == "Critical" else "Warning"
    elif "pipeline duration is above baseline" in explanation_bits and anomaly_summary["overall_status"] == "Normal":
        status = "Warning"
    else:
        status = anomaly_summary["overall_status"]

    explanation = _build_explanation(score, explanation_bits, anomalies, comparison_baseline)

    return {
        "score": int(round(score)),
        "grade": _grade_from_score(score),
        "status": status,
        "explanation": explanation,
        "baseline_context": baseline_context,
    }


def _baseline_component_score(current_value, baseline_mean):
    baseline = _safe_positive_float(baseline_mean)
    if baseline is None:
        return 100.0

    current = _safe_non_negative_float(current_value)
    ratio = current / baseline
    points = [
        (1.00, 100.0),
        (1.10, 90.0),
        (1.20, 80.0),
        (1.30, 65.0),
        (1.50, 40.0),
        (1.75, 20.0),
        (2.00, 0.0),
    ]

    if ratio <= points[0][0]:
        return points[0][1]
    if ratio >= points[-1][0]:
        return points[-1][1]

    for (left_ratio, left_score), (right_ratio, right_score) in zip(points, points[1:]):
        if left_ratio <= ratio <= right_ratio:
            distance = (ratio - left_ratio) / (right_ratio - left_ratio)
            return left_score + distance * (right_score - left_score)
    return 0.0


def _weighted_sustainability_score(component_scores):
    weights = {
        "total_energy_kwh": 35.0,
        "total_carbon_kg": 35.0,
        "duration_seconds": 20.0,
    }
    total_weight = sum(weights.values())
    return sum(component_scores[metric] * (weight / total_weight) for metric, weight in weights.items())


def _is_worse_than_baseline(current_value, baseline_mean):
    baseline = _safe_positive_float(baseline_mean)
    if baseline is None:
        return False
    return _safe_non_negative_float(current_value) > baseline


def _safe_positive_float(value):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric) or numeric <= 0:
        return None
    return numeric


def _safe_non_negative_float(value):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(numeric):
        return 0.0
    return max(0.0, numeric)


def _grade_from_score(score):
    if score >= 85:
        return "Excellent"
    if score >= 70:
        return "Good"
    if score >= 50:
        return "Moderate"
    return "Poor"


def _build_explanation(score, explanation_bits, anomalies, baseline_df):
    if baseline_df.get("run_count", 0) == 0:
        base_message = "This run has limited historical context, so the score is based mainly on current-stage behavior."
    elif score >= 85:
        base_message = "This run is sustainable compared with historical behavior."
    elif score >= 70:
        base_message = "This run is reasonably sustainable compared with historical behavior."
    elif score >= 50:
        base_message = "This run shows moderate sustainability risk compared with historical behavior."
    else:
        base_message = "This run shows significant sustainability risk compared with historical behavior."

    if anomalies:
        top_anomaly = anomalies[0]["message"][0].lower() + anomalies[0]["message"][1:]
        return f"{base_message} However, {top_anomaly}"

    if explanation_bits:
        return f"{base_message} Key issue: {', '.join(explanation_bits)}."

    return base_message


def _contextual_baseline_totals(stage_baseline_df: pd.DataFrame | None) -> Dict[str, float] | None:
    if stage_baseline_df is None or stage_baseline_df.empty:
        return None
    usable = stage_baseline_df.copy()
    if "historical_samples" in usable.columns and "minimum_training_samples" in usable.columns:
        usable = usable[
            pd.to_numeric(usable["historical_samples"], errors="coerce").fillna(0)
            >= pd.to_numeric(usable["minimum_training_samples"], errors="coerce").fillna(0)
        ].copy()
    if usable.empty:
        return None

    baseline = {"run_count": int(pd.to_numeric(usable.get("historical_samples", 0), errors="coerce").fillna(0).max())}
    for metric in ["duration_seconds", "total_energy_kwh", "total_carbon_kg"]:
        column = f"{metric}_mean"
        baseline[column] = (
            float(pd.to_numeric(usable[column], errors="coerce").fillna(0).sum())
            if column in usable.columns
            else None
        )
    return baseline


def _baseline_context(stage_baseline_df: pd.DataFrame | None, comparison_baseline: Dict[str, float]) -> Dict[str, object]:
    if stage_baseline_df is None or stage_baseline_df.empty:
        return {
            "context_scope": "pipeline",
            "label": "Pipeline baseline",
            "historical_samples": int(comparison_baseline.get("run_count") or 0),
            "strategy_specific": False,
            "fallback_reason": "",
        }

    rows = stage_baseline_df.to_dict(orient="records")
    usable_rows = [
        row for row in rows
        if int(row.get("historical_samples") or 0) >= int(row.get("minimum_training_samples") or 0)
    ]
    source_rows = usable_rows or rows
    scopes = sorted({str(row.get("context_scope", "insufficient")) for row in source_rows})
    stages = sorted({str(row.get("stage", "")).title() for row in source_rows if row.get("stage")})
    strategies = sorted({
        str(row.get("strategy", "")).title()
        for row in source_rows
        if row.get("strategy") and str(row.get("strategy")) != "missing"
    })
    fallback_reasons = [str(row.get("fallback_reason") or "") for row in source_rows if row.get("fallback_reason")]
    sample_counts = [int(row.get("historical_samples") or 0) for row in source_rows]
    strategy_specific = any(bool(row.get("strategy_specific")) for row in source_rows)
    stage_label = " + ".join(stages) if stages else "Lifecycle"
    strategy_label = f" / {' + '.join(strategies)}" if strategies and strategy_specific else ""
    return {
        "context_scope": ", ".join(scopes),
        "label": f"{stage_label}{strategy_label}",
        "historical_samples": min(sample_counts) if sample_counts else 0,
        "strategy_specific": strategy_specific,
        "fallback_reason": fallback_reasons[0] if fallback_reasons else "",
    }
