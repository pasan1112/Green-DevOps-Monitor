import os
from datetime import datetime, timezone

import requests
from flask import Blueprint, jsonify, render_template


operate_bp = Blueprint(
    "operate",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/operate/static",
)

DEFAULT_OPERATION_API_URL = "http://localhost:8000"
DEFAULT_PROMETHEUS_URL = "http://localhost:9090"
REQUEST_TIMEOUT_SECONDS = 3
CPU_QUERY = (
    'rate(container_cpu_usage_seconds_total{namespace="green-devops",'
    'container="green-release-app"}[1m]) * 100'
)


def _base_url(env_name, default):
    return os.getenv(env_name, default).rstrip("/")


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _json_error(message, status_code=503, detail=None):
    payload = {
        "available": False,
        "error": message,
        "timestamp": _utc_now_iso(),
    }
    if detail:
        payload["detail"] = str(detail)
    return jsonify(payload), status_code


@operate_bp.route("/operate")
def operate_page():
    return render_template("operate.html")


@operate_bp.route("/api/operate/status")
def operate_status():
    operation_url = _base_url("GREEN_DEVOPS_API_URL", DEFAULT_OPERATION_API_URL)
    try:
        response = requests.get(
            f"{operation_url}/status",
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        return _json_error("Operation API unavailable", detail=exc)
    except ValueError as exc:
        return _json_error("Operation API returned invalid JSON", detail=exc)

    if not isinstance(payload, dict):
        return _json_error("Operation API returned an unexpected payload", status_code=502)

    return jsonify(
        {
            "available": True,
            "source": operation_url,
            "received_at": _utc_now_iso(),
            "data": payload,
        }
    )


@operate_bp.route("/api/operate/cpu")
def operate_cpu():
    prometheus_url = _base_url("GREEN_DEVOPS_PROMETHEUS_URL", DEFAULT_PROMETHEUS_URL)
    try:
        response = requests.get(
            f"{prometheus_url}/api/v1/query",
            params={"query": CPU_QUERY},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        return _json_error("Prometheus unavailable", detail=exc)
    except ValueError as exc:
        return _json_error("Prometheus returned invalid JSON", detail=exc)

    if payload.get("status") != "success":
        return _json_error(
            "Prometheus query failed",
            status_code=502,
            detail=payload.get("error") or payload.get("errorType"),
        )

    results = ((payload.get("data") or {}).get("result") or [])
    if not results:
        return jsonify(
            {
                "available": False,
                "cpu_percent": None,
                "timestamp": _utc_now_iso(),
                "source": prometheus_url,
                "message": "Current CPU unavailable",
            }
        )

    try:
        value = results[0].get("value") or []
        sample_timestamp = value[0]
        cpu_percent = float(value[1])
    except (IndexError, TypeError, ValueError, AttributeError) as exc:
        return _json_error("Prometheus CPU response was not usable", status_code=502, detail=exc)

    return jsonify(
        {
            "available": True,
            "cpu_percent": cpu_percent,
            "timestamp": sample_timestamp,
            "received_at": _utc_now_iso(),
            "source": prometheus_url,
        }
    )
