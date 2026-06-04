import argparse
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DEFAULT_VALIDATION_REPORT_PATH = "/data/landing/_validation/report"
DEFAULT_QUALITY_REPORT_PATH = "/data/features/_quality/report"


def _escape_label(value):
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _metric(name, labels, value):
    label_text = ",".join(
        f'{key}="{_escape_label(val)}"' for key, val in sorted(labels.items())
    )
    return f"{name}{{{label_text}}} {value}"


def _report_files(path):
    report_path = Path(path)
    if not report_path.exists():
        return []
    if report_path.is_file():
        return [report_path]
    return sorted(
        file_path
        for file_path in report_path.rglob("*.json")
        if file_path.name.startswith("part-") or file_path.name.endswith(".json")
    )


def _load_records(path):
    records = []
    for file_path in _report_files(path):
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                loaded = json.loads(line)
                if isinstance(loaded, list):
                    records.extend(loaded)
                else:
                    records.append(loaded)
    return records


def _records_to_metrics(report_name, path):
    lines = []
    records = _load_records(path)
    lines.append(
        _metric(
            "data_quality_report_available",
            {"report": report_name, "path": path},
            1 if records else 0,
        )
    )
    lines.append(
        _metric(
            "data_quality_report_records",
            {"report": report_name, "path": path},
            len(records),
        )
    )

    for record in records:
        dataset = record.get("dataset", "unknown")
        layer = record.get("layer", "landing" if report_name == "validation" else "unknown")
        labels = {"report": report_name, "layer": layer, "dataset": dataset}
        rows = int(record.get("rows") or 0)
        duplicate_keys = int(record.get("duplicate_keys") or 0)
        has_error = 1 if record.get("error") else 0

        lines.append(_metric("data_quality_dataset_rows", labels, rows))
        lines.append(_metric("data_quality_duplicate_keys", labels, duplicate_keys))
        lines.append(_metric("data_quality_report_error", labels, has_error))

        if rows > 0:
            lines.append(
                _metric(
                    "data_quality_duplicate_key_ratio",
                    labels,
                    duplicate_keys / rows,
                )
            )

        nulls = record.get("nulls") or {}
        total_nulls = 0
        for column, value in nulls.items():
            null_count = int(value or 0)
            total_nulls += null_count
            column_labels = {**labels, "column": column}
            lines.append(_metric("data_quality_nulls", column_labels, null_count))
            if rows > 0:
                lines.append(
                    _metric("data_quality_null_ratio", column_labels, null_count / rows)
                )
        lines.append(_metric("data_quality_total_nulls", labels, total_nulls))

    return lines


def collect_metrics(validation_report_path, quality_report_path):
    lines = [
        "# HELP data_quality_report_available Whether a Spark report was found and parsed.",
        "# TYPE data_quality_report_available gauge",
        "# HELP data_quality_report_records Number of records in a Spark report.",
        "# TYPE data_quality_report_records gauge",
        "# HELP data_quality_dataset_rows Row count by report, layer, and dataset.",
        "# TYPE data_quality_dataset_rows gauge",
        "# HELP data_quality_duplicate_keys Duplicate key count by report, layer, and dataset.",
        "# TYPE data_quality_duplicate_keys gauge",
        "# HELP data_quality_duplicate_key_ratio Duplicate key ratio by report, layer, and dataset.",
        "# TYPE data_quality_duplicate_key_ratio gauge",
        "# HELP data_quality_nulls Null count by report, layer, dataset, and column.",
        "# TYPE data_quality_nulls gauge",
        "# HELP data_quality_null_ratio Null ratio by report, layer, dataset, and column.",
        "# TYPE data_quality_null_ratio gauge",
        "# HELP data_quality_total_nulls Total null count by report, layer, and dataset.",
        "# TYPE data_quality_total_nulls gauge",
        "# HELP data_quality_report_error Dataset-level report error flag.",
        "# TYPE data_quality_report_error gauge",
    ]
    try:
        lines.extend(_records_to_metrics("validation", validation_report_path))
        lines.extend(_records_to_metrics("quality", quality_report_path))
    except Exception as exc:
        labels = {"report": "exporter", "layer": "exporter", "dataset": "exporter"}
        lines.append(_metric("data_quality_report_error", labels, 1))
        lines.append(f'# exporter_error="{_escape_label(exc)}"')
    lines.append(f"data_quality_exporter_last_scrape_timestamp_seconds {time.time()}")
    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    validation_report_path = DEFAULT_VALIDATION_REPORT_PATH
    quality_report_path = DEFAULT_QUALITY_REPORT_PATH

    def do_GET(self):
        if self.path not in {"/metrics", "/"}:
            self.send_response(404)
            self.end_headers()
            return

        body = collect_metrics(
            self.validation_report_path,
            self.quality_report_path,
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def parse_args():
    parser = argparse.ArgumentParser(description="Expose Spark data quality reports as Prometheus metrics.")
    parser.add_argument("--host", default=os.getenv("EXPORTER_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("EXPORTER_PORT", "9108")))
    parser.add_argument(
        "--validation-report-path",
        default=os.getenv("VALIDATION_REPORT_PATH", DEFAULT_VALIDATION_REPORT_PATH),
    )
    parser.add_argument(
        "--quality-report-path",
        default=os.getenv("QUALITY_REPORT_PATH", DEFAULT_QUALITY_REPORT_PATH),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    MetricsHandler.validation_report_path = args.validation_report_path
    MetricsHandler.quality_report_path = args.quality_report_path
    server = ThreadingHTTPServer((args.host, args.port), MetricsHandler)
    print(f"Data quality exporter listening on {args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
