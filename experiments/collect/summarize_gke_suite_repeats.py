#!/usr/bin/env python3
"""Summarize repeated GKE suite runs without printing huge phase CSVs."""

from __future__ import annotations

import argparse
import csv
import re
import statistics
from pathlib import Path
from typing import Iterable, List


POLICY_LABELS = {
    "baseline_no_migration": "baseline",
    "strategy_a_age_based": "A",
    "strategy_b_throttled": "B",
    "strategy_c_pressure_aware": "C",
}


def policy_label(scenario: str) -> str:
    return POLICY_LABELS.get(scenario, scenario)


def median(values: Iterable[float]) -> float:
    items = list(values)
    if not items:
        return 0.0
    return float(statistics.median(items))


def suite_root_from_latency(path: Path) -> str:
    name = path.name
    if not name.startswith("suite-") or not name.endswith("-latency.csv"):
        raise ValueError(f"not a suite latency CSV: {path}")
    return name[len("suite-") : -len("-latency.csv")]


def find_latency_files(result_root: Path, selector: str, latest: int) -> List[Path]:
    files = [
        p
        for p in result_root.glob("suite-*-latency.csv")
        if not p.name.endswith("-phase-latency.csv") and selector in p.name
    ]
    files = sorted(files, key=lambda p: p.stat().st_mtime)
    if latest > 0:
        files = files[-latest:]
    return files


def load_csv(path: Path) -> List[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def run_label(run_id: str) -> str:
    match = re.search(r"(20\d+T\d+Z-gke-[^-]+-[^-]+-r\d+)", run_id)
    if match:
        return match.group(1)
    parts = run_id.split("-io-")
    return parts[0] if parts else run_id


def collect_placement(result_root: Path, suite_root: str) -> List[dict[str, str]]:
    records: List[dict[str, str]] = []
    for path in sorted(result_root.glob(f"*/*{suite_root}*/logs/k8s-pressure/*-placement.txt")):
        values: dict[str, str] = {"path": str(path)}
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if "=" in line:
                    key, value = line.split("=", 1)
                    values[key] = value
        records.append(values)
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-root", default="experiments/results")
    parser.add_argument("--selector", default="gke-fixed-io-r")
    parser.add_argument("--latest", type=int, default=3)
    args = parser.parse_args()

    result_root = Path(args.result_root)
    lat_files = find_latency_files(result_root, args.selector, args.latest)
    if not lat_files:
        print(f"ERROR: no suite latency CSVs found under {result_root} matching {args.selector}", flush=True)
        return 1

    migration_files = [Path(str(p).replace("-latency.csv", "-migration.csv")) for p in lat_files]
    missing = [str(p) for p in migration_files if not p.exists()]
    if missing:
        print("ERROR: missing migration CSVs:")
        for path in missing:
            print(path)
        return 1

    print("=== files ===")
    for path in lat_files:
        print(path)
    for path in migration_files:
        print(path)

    latency_rows: List[dict[str, str]] = []
    for path in lat_files:
        for row in load_csv(path):
            if row.get("operation") in {"GET", "PUT"}:
                row["_suite_root"] = suite_root_from_latency(path)
                latency_rows.append(row)

    migration_rows: List[dict[str, str]] = []
    for path in migration_files:
        for row in load_csv(path):
            row["_suite_root"] = suite_root_from_latency(Path(str(path).replace("-migration.csv", "-latency.csv")))
            migration_rows.append(row)

    print("\n=== per-run latency p99 ===")
    print("run,policy,op,p99_ms,p95_ms,p50_ms,throughput,errors")
    for row in latency_rows:
        print(
            ",".join(
                [
                    run_label(row["run_id"]),
                    policy_label(row["scenario"]),
                    row["operation"],
                    f"{float(row['p99_ms']):.1f}",
                    f"{float(row['p95_ms']):.1f}",
                    f"{float(row['p50_ms']):.1f}",
                    f"{float(row['throughput_rps']):.2f}",
                    row["errors"],
                ]
            )
        )

    print("\n=== median latency p99 over selected repeats ===")
    print("policy,op,median_p99_ms,median_p95_ms,median_p50_ms,median_throughput,total_errors")
    for policy in ["baseline", "A", "B", "C"]:
      for operation in ["PUT", "GET"]:
        rows = [
            row
            for row in latency_rows
            if policy_label(row["scenario"]) == policy and row["operation"] == operation
        ]
        if not rows:
            continue
        total_errors = sum(int(float(row["errors"])) for row in rows)
        print(
            ",".join(
                [
                    policy,
                    operation,
                    f"{median(float(row['p99_ms']) for row in rows):.1f}",
                    f"{median(float(row['p95_ms']) for row in rows):.1f}",
                    f"{median(float(row['p50_ms']) for row in rows):.1f}",
                    f"{median(float(row['throughput_rps']) for row in rows):.2f}",
                    str(total_errors),
                ]
            )
        )

    print("\n=== per-run migration ===")
    print("run,policy,repl_done,final_backlog,drain_progress,repl_done_per_min")
    for row in migration_rows:
        print(
            ",".join(
                [
                    run_label(row["run_id"]),
                    policy_label(row["scenario"]),
                    row["final_repl_done"],
                    row["final_backlog"],
                    row["drain_progress"],
                    row["repl_done_per_min"],
                ]
            )
        )

    print("\n=== median migration over selected repeats ===")
    print("policy,median_repl_done,median_final_backlog,median_drain_progress")
    for policy in ["baseline", "A", "B", "C"]:
        rows = [row for row in migration_rows if policy_label(row["scenario"]) == policy]
        if not rows:
            continue
        print(
            ",".join(
                [
                    policy,
                    f"{median(float(row['final_repl_done']) for row in rows):.1f}",
                    f"{median(float(row['final_backlog']) for row in rows):.1f}",
                    f"{median(float(row['drain_progress']) for row in rows):.3f}",
                ]
            )
        )

    print("\n=== pressure placement ===")
    placement_ok = True
    for path in lat_files:
        suite_root = suite_root_from_latency(path)
        records = collect_placement(result_root, suite_root)
        for record in records:
            node = record.get("node", "")
            target = record.get("target_node", "")
            targets = record.get("target_nodes", "")
            scenario_hint = Path(record["path"]).parts[-4] if len(Path(record["path"]).parts) >= 4 else ""
            print(f"{suite_root},{scenario_hint},target_node={target},node={node},target_nodes={targets}")
            if not target or target != node:
                placement_ok = False
    print(f"placement_ok={str(placement_ok).lower()}")

    return 0 if placement_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
