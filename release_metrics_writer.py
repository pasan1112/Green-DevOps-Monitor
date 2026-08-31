import csv
import json
import sys
from pathlib import Path


CSV_COLUMNS = [
    "job_name",
    "build_number",
    "commit_sha",
    "commit_message",
    "release_status",
    "release_start_time",
    "release_end_time",
    "release_duration_s",
    "optimizer_status",
    "optimizer_duration_s",
    "optimizer_executed",
    "optimizer_skip_reason",
    "affected_modules",
    "build_duration_s",
    "test_duration_s",
    "docker_build_duration_s",
    "tests_executed",
    "tests_skipped",
    "build_command",
    "test_command",
    "carbon_intensity",
    "green_probability",
    "scheduling_action",
    "scheduling_engine",
]


def load_rows(csv_path):
    if not csv_path.is_file() or csv_path.stat().st_size == 0:
        return []

    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def normalize_record(record):
    return {
        column: record.get(column, "") if record.get(column, "") is not None else ""
        for column in CSV_COLUMNS
    }


def upsert_release_record(csv_path, record):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    record_key = (str(record.get("job_name", "")), str(record.get("build_number", "")))
    rows = [
        row
        for row in load_rows(csv_path)
        if (str(row.get("job_name", "")), str(row.get("build_number", ""))) != record_key
    ]
    rows.append(normalize_record(record))

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main(argv):
    if len(argv) != 3:
        print("Usage: release_metrics_writer.py <csv-path> <payload-json>", file=sys.stderr)
        return 2

    csv_path = Path(argv[1])
    payload_path = Path(argv[2])
    record = json.loads(payload_path.read_text(encoding="utf-8"))
    upsert_release_record(csv_path, record)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
