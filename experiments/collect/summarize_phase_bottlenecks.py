#!/usr/bin/env python3

import argparse
import csv
import sys
from pathlib import Path


RELEVANT_PHASE_OPERATIONS = {
    "PUT": {"PUT", "STORE"},
    "GET": {"GET"},
}


def parse_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def read_csv(path):
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def latency_p99_by_run(rows, operation):
    p99 = {}
    op = operation.upper()
    for row in rows:
        if row.get("operation", "").upper() != op:
            continue
        scenario = row.get("scenario", "")
        run_id = row.get("run_id", "")
        if not scenario or not run_id:
            continue
        p99[(scenario, run_id)] = parse_float(row.get("p99_ms"))
    return p99


def phase_group(component, event, phase):
    phase = phase.lower()
    event = event.lower()
    if "metadata" in phase:
        return "metadata"
    if phase in {"write_commit", "replica_write_quorum"}:
        return "replica_quorum"
    if phase in {"durable_write", "file_write", "fsync"}:
        return "storage_write"
    if phase in {"queue_wait", "store_wait"}:
        return "storage_queue"
    if phase == "body_read":
        return "request_body"
    if phase == "read":
        return "read_path"
    if event:
        return event
    return component


def phase_row_is_relevant(row, operation, include_total):
    phase = row.get("phase", "")
    if not include_total and phase == "total":
        return False
    phase_op = row.get("operation", "").upper()
    relevant_ops = RELEVANT_PHASE_OPERATIONS.get(operation.upper(), {operation.upper()})
    return phase_op in relevant_ops


def summarize(phase_rows, latency_rows, operation, top_n, include_total):
    end_to_end_p99 = latency_p99_by_run(latency_rows, operation)
    grouped = {}
    for row in phase_rows:
        if not phase_row_is_relevant(row, operation, include_total):
            continue
        scenario = row.get("scenario", "")
        run_id = row.get("run_id", "")
        if not scenario or not run_id:
            continue
        p99 = parse_float(row.get("p99_ms"))
        if p99 <= 0:
            continue
        grouped.setdefault((scenario, run_id), []).append(row)

    output = []
    for key in sorted(grouped):
        scenario, run_id = key
        final_p99 = end_to_end_p99.get(key, 0.0)
        rows = sorted(grouped[key], key=lambda r: parse_float(r.get("p99_ms")), reverse=True)
        for rank, row in enumerate(rows[:top_n], start=1):
            p99 = parse_float(row.get("p99_ms"))
            ratio = p99 / final_p99 if final_p99 > 0 else 0.0
            component = row.get("component", "")
            event = row.get("event", "")
            phase = row.get("phase", "")
            output.append({
                "scenario": scenario,
                "run_id": run_id,
                "operation": operation.upper(),
                "end_to_end_p99_ms": final_p99,
                "rank": rank,
                "bottleneck_group": phase_group(component, event, phase),
                "component": component,
                "event": event,
                "phase": phase,
                "phase_operation": row.get("operation", ""),
                "write_class": row.get("write_class", ""),
                "count": parse_int(row.get("count")),
                "p99_ms": p99,
                "p99_ratio": ratio,
            })
    return output


def write_csv(path, rows):
    fields = [
        "scenario",
        "run_id",
        "operation",
        "end_to_end_p99_ms",
        "rank",
        "bottleneck_group",
        "component",
        "event",
        "phase",
        "phase_operation",
        "write_class",
        "count",
        "p99_ms",
        "p99_ratio",
    ]
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        f = path.open("w", encoding="utf-8", newline="")
    else:
        f = sys.stdout
    try:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: f"{value:.6f}" if isinstance(value, float) else value
                for key, value in row.items()
            })
    finally:
        if path:
            f.close()


def main():
    parser = argparse.ArgumentParser(description="Rank likely phase-level contributors to end-to-end tail latency.")
    parser.add_argument("--phase-csv", type=Path, required=True, help="Phase latency summary CSV.")
    parser.add_argument("--latency-csv", type=Path, required=True, help="Latency comparison or summary CSV.")
    parser.add_argument("--operation", default="PUT", help="End-to-end operation to diagnose, default: PUT.")
    parser.add_argument("--top", type=int, default=6, help="Number of phase rows to emit per run.")
    parser.add_argument("--include-total", action="store_true", help="Include phase rows named total.")
    parser.add_argument("--out", type=Path, help="Output CSV. Defaults to stdout.")
    args = parser.parse_args()

    if args.top < 1:
        parser.error("--top must be >= 1")
    phase_rows = read_csv(args.phase_csv)
    latency_rows = read_csv(args.latency_csv)
    rows = summarize(phase_rows, latency_rows, args.operation, args.top, args.include_total)
    write_csv(args.out, rows)


if __name__ == "__main__":
    main()
