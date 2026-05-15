#!/usr/bin/env python3

import argparse
import csv
import sys
from pathlib import Path


FIELDNAMES = [
    "scenario",
    "run_id",
    "duration_sec",
    "final_due_total",
    "final_due_ready",
    "final_repl_pending",
    "final_repl_running",
    "final_repl_retry_wait",
    "final_repl_done",
    "final_repl_failed",
    "final_gc_pending",
    "final_gc_done",
    "max_due_ready",
    "max_repl_pending",
    "max_repl_running",
    "final_backlog",
    "drain_progress",
    "repl_done_per_min",
    "first_repl_activity_offset_sec",
    "first_repl_done_offset_sec",
    "last_repl_done_offset_sec",
    "result_dir",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Summarize migration progress from metrics.csv files.")
    parser.add_argument(
        "--result-root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "results",
        help="Root directory containing experiments/results/<scenario>/<run_id>/metrics.csv",
    )
    parser.add_argument(
        "--run-id-root",
        default="",
        help="Only include run IDs that start with this value, useful for run_matrix_local.sh",
    )
    parser.add_argument(
        "--latest-per-scenario",
        action="store_true",
        help="When --run-id-root is not set, compare only the newest run for each scenario.",
    )
    parser.add_argument("--out", type=Path, help="Optional output CSV path.")
    return parser.parse_args()


def int_field(row, name):
    try:
        return int(float(row.get(name, 0) or 0))
    except (TypeError, ValueError):
        return 0


def metrics_paths(result_root):
    if not result_root.exists():
        return []
    return sorted(result_root.glob("*/*/metrics.csv"))


def select_latest_per_scenario(paths):
    latest = {}
    for path in paths:
        scenario = path.parent.parent.name
        current = latest.get(scenario)
        if current is None or path.parent.stat().st_mtime > current.parent.stat().st_mtime:
            latest[scenario] = path
    return sorted(latest.values(), key=lambda p: p.parent.parent.name)


def offset(value):
    return "" if value is None else str(value)


def summarize(path):
    scenario = path.parent.parent.name
    run_id = path.parent.name
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None

    timestamps = [int_field(row, "timestamp_unix") for row in rows if int_field(row, "timestamp_unix") > 0]
    first_ts = min(timestamps) if timestamps else 0
    last_ts = max(timestamps) if timestamps else first_ts
    duration_sec = max(last_ts - first_ts, 0)

    final = rows[-1]
    final_due_total = int_field(final, "due_total")
    final_due_ready = int_field(final, "due_ready")
    final_repl_pending = int_field(final, "repl_pending")
    final_repl_running = int_field(final, "repl_running")
    final_repl_retry_wait = int_field(final, "repl_retry_wait")
    final_repl_done = int_field(final, "repl_done")
    final_repl_failed = int_field(final, "repl_failed")
    final_gc_pending = int_field(final, "gc_pending")
    final_gc_done = int_field(final, "gc_done")

    max_due_ready = max(int_field(row, "due_ready") for row in rows)
    max_repl_pending = max(int_field(row, "repl_pending") for row in rows)
    max_repl_running = max(int_field(row, "repl_running") for row in rows)

    first_activity_ts = None
    first_done_ts = None
    last_done_ts = None
    previous_done = 0
    for row in rows:
        ts = int_field(row, "timestamp_unix")
        pending = int_field(row, "repl_pending")
        running = int_field(row, "repl_running")
        done = int_field(row, "repl_done")
        retry = int_field(row, "repl_retry_wait")
        if first_activity_ts is None and (pending > 0 or running > 0 or retry > 0 or done > 0):
            first_activity_ts = ts
        if done > 0 and first_done_ts is None:
            first_done_ts = ts
        if done > previous_done:
            last_done_ts = ts
            previous_done = done

    final_backlog = final_due_ready + final_repl_pending + final_repl_running + final_repl_retry_wait
    denominator = final_backlog + final_repl_done + final_repl_failed
    drain_progress = (final_repl_done / denominator) if denominator else 0.0
    repl_done_per_min = (final_repl_done / (duration_sec / 60.0)) if duration_sec > 0 else 0.0

    return {
        "scenario": scenario,
        "run_id": run_id,
        "duration_sec": duration_sec,
        "final_due_total": final_due_total,
        "final_due_ready": final_due_ready,
        "final_repl_pending": final_repl_pending,
        "final_repl_running": final_repl_running,
        "final_repl_retry_wait": final_repl_retry_wait,
        "final_repl_done": final_repl_done,
        "final_repl_failed": final_repl_failed,
        "final_gc_pending": final_gc_pending,
        "final_gc_done": final_gc_done,
        "max_due_ready": max_due_ready,
        "max_repl_pending": max_repl_pending,
        "max_repl_running": max_repl_running,
        "final_backlog": final_backlog,
        "drain_progress": f"{drain_progress:.6f}",
        "repl_done_per_min": f"{repl_done_per_min:.6f}",
        "first_repl_activity_offset_sec": offset(None if first_activity_ts is None else first_activity_ts - first_ts),
        "first_repl_done_offset_sec": offset(None if first_done_ts is None else first_done_ts - first_ts),
        "last_repl_done_offset_sec": offset(None if last_done_ts is None else last_done_ts - first_ts),
        "result_dir": str(path.parent),
    }


def sort_key(row):
    scenario_order = {
        "baseline_no_migration": 0,
        "strategy_a_age_based": 1,
        "strategy_b_throttled": 2,
        "strategy_c_pressure_aware": 3,
    }
    return (scenario_order.get(row["scenario"], 99), row["scenario"], row["run_id"])


def main():
    args = parse_args()
    paths = metrics_paths(args.result_root)
    if args.run_id_root:
        paths = [p for p in paths if p.parent.name.startswith(args.run_id_root)]
    elif args.latest_per_scenario:
        paths = select_latest_per_scenario(paths)

    rows = [row for path in paths if (row := summarize(path)) is not None]
    rows.sort(key=sort_key)

    output = sys.stdout
    close_output = False
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        output = args.out.open("w", encoding="utf-8", newline="")
        close_output = True

    try:
        writer = csv.DictWriter(output, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    finally:
        if close_output:
            output.close()

    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
