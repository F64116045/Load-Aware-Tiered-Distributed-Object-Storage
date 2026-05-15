#!/usr/bin/env python3

import argparse
import csv
import sys
from pathlib import Path


FIELDNAMES = [
    "scenario",
    "run_id",
    "operation",
    "count",
    "errors",
    "error_rate",
    "throughput_rps",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "result_dir",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Compare experiment summary.csv files.")
    parser.add_argument(
        "--result-root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "results",
        help="Root directory containing experiments/results/<scenario>/<run_id>/summary.csv",
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


def summary_paths(result_root):
    if not result_root.exists():
        return []
    return sorted(result_root.glob("*/*/summary.csv"))


def select_latest_per_scenario(paths):
    latest = {}
    for path in paths:
        scenario = path.parent.parent.name
        current = latest.get(scenario)
        if current is None or path.parent.stat().st_mtime > current.parent.stat().st_mtime:
            latest[scenario] = path
    return sorted(latest.values(), key=lambda p: p.parent.parent.name)


def load_rows(path):
    scenario = path.parent.parent.name
    run_id = path.parent.name
    rows = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(
                {
                    "scenario": scenario,
                    "run_id": run_id,
                    "operation": row.get("operation", ""),
                    "count": row.get("count", ""),
                    "errors": row.get("errors", ""),
                    "error_rate": row.get("error_rate", ""),
                    "throughput_rps": row.get("throughput_rps", ""),
                    "p50_ms": row.get("p50_ms", ""),
                    "p95_ms": row.get("p95_ms", ""),
                    "p99_ms": row.get("p99_ms", ""),
                    "max_ms": row.get("max_ms", ""),
                    "result_dir": str(path.parent),
                }
            )
    return rows


def sort_key(row):
    scenario_order = {
        "baseline_no_migration": 0,
        "strategy_a_age_based": 1,
        "strategy_b_throttled": 2,
        "strategy_c_pressure_aware": 3,
    }
    op_order = {"ALL": 0, "GET": 1, "PUT": 2}
    return (
        scenario_order.get(row["scenario"], 99),
        row["scenario"],
        op_order.get(row["operation"], 99),
        row["operation"],
        row["run_id"],
    )


def main():
    args = parse_args()
    paths = summary_paths(args.result_root)
    if args.run_id_root:
        paths = [p for p in paths if p.parent.name.startswith(args.run_id_root)]
    elif args.latest_per_scenario:
        paths = select_latest_per_scenario(paths)

    rows = []
    for path in paths:
        rows.extend(load_rows(path))
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
