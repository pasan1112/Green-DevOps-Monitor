"""Rule-based sustainability health scoring for monitored pipeline runs."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

from .anomaly_model import summarize_anomalies


def calculate_sustainability_score(
    current_run_df: pd.DataFrame,
    baseline_df: Dict[str, float],
    anomalies: List[Dict],
) -> Dict[str, object]:
    score = 100

    critical_count = sum(1 for item in anomalies if item.get("severity") == "critical")
    warning_count = sum(1 for item in anomalies if item.get("severity") == "warning")
    score -= critical_count * 15
    score -= warning_count * 7

    current = current_run_df.copy() if current_run_df is not None else pd.DataFrame()
    for column in ["duration_seconds", "total_energy_kwh", "total_carbon_kg"]:
        if column not in current.columns:
            current[column] = 0.0
        current[column] = pd.to_numeric(current[column], errors="coerce").fillna(0.0)

    current_totals = {
        "duration_seconds": float(current["duration_seconds"].sum()) if not current.empty else 0.0,
        "total_energy_kwh": float(current["total_energy_kwh"].sum()) if not current.empty else 0.0,
        "total_carbon_kg": float(current["total_carbon_kg"].sum()) if not current.empty else 0.0,
    }

    explanation_bits = []

    if _is_above_baseline(current_totals["total_energy_kwh"], baseline_df.get("total_energy_kwh_mean"), 30):
        score -= 10
        explanation_bits.append("total pipeline energy is above baseline")

    if _is_above_baseline(current_totals["total_carbon_kg"], baseline_df.get("total_carbon_kg_mean"), 30):
        score -= 10
        explanation_bits.append("total pipeline carbon is above baseline")

    if _is_above_baseline(current_totals["duration_seconds"], baseline_df.get("duration_seconds_mean"), 30):
        score -= 8
        explanation_bits.append("pipeline duration is above baseline")

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

    explanation = _build_explanation(score, explanation_bits, anomalies, baseline_df)

    return {
        "score": int(round(score)),
        "grade": _grade_from_score(score),
        "status": status,
        "explanation": explanation,
    }


def _is_above_baseline(current_value, baseline_mean, threshold_percent):
    if baseline_mean is None or baseline_mean <= 0:
        return False
    return current_value > baseline_mean * (1 + threshold_percent / 100.0)


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
