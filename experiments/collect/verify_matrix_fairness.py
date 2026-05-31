#!/usr/bin/env python3
"""Validate that a local experiment matrix used the same non-policy parameters."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterable, List


INVARIANT_KEYS = [
    "API_BASE",
    "COMPOSE_FILES",
    "K8S_DISCOVER_API_BASE",
    "K8S_API_SERVICE_NAME",
    "K8S_API_SERVICE_PORT",
    "OBJECT_COUNT",
    "OBJECT_SIZE_BYTES",
    "PRELOAD_OBJECTS",
    "PRELOAD_AGE_WAIT_SEC",
    "WORKLOAD_DURATION_SEC",
    "WORKLOAD_CONCURRENCY",
    "GET_PERCENT",
    "AGE_THRESHOLD_SEC",
    "PRESSURE_PROFILE",
    "PRESSURE_CPUS",
    "PRESSURE_DURATION_SEC",
    "PRESSURE_DELAY_SEC",
    "PRESSURE_WARMUP_SEC",
    "HDD_WORKERS",
    "HDD_BYTES",
    "METRICS_INTERVAL_SEC",
    "STORAGE_DURABILITY_MODE",
    "STORAGE_GROUP_SYNC_INTERVAL_MS",
    "STORAGE_GROUP_SYNC_MAX_BATCH",
]

EXPECTED_SCENARIOS = [
    "baseline_no_migration",
    "strategy_a_age_based",
    "strategy_b_throttled",
    "strategy_c_pressure_aware",
]


def parse_run_env(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key] = value
    return values


def find_runs(result_root: Path, run_id_root: str) -> List[Path]:
    runs: List[Path] = []
    for scenario in EXPECTED_SCENARIOS:
        scenario_dir = result_root / scenario
        if not scenario_dir.exists():
            continue
        runs.extend(sorted(scenario_dir.glob(f"{run_id_root}-*")))
    return runs


def format_row(values: Iterable[str]) -> str:
    return ",".join(values)


def uses_discovered_k8s_endpoint(records: List[tuple[Path, Dict[str, str]]]) -> bool:
    return bool(records) and all(
        values.get("K8S_DISCOVER_API_BASE", "") == "true" for _, values in records
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-root", default="experiments/results")
    parser.add_argument("--run-id-root", required=True)
    parser.add_argument("--out")
    args = parser.parse_args()

    result_root = Path(args.result_root)
    runs = find_runs(result_root, args.run_id_root)
    lines: List[str] = []

    if not runs:
        lines.append(f"FAIL no runs found for run_id_root={args.run_id_root}")
        text = "\n".join(lines) + "\n"
        if args.out:
            Path(args.out).write_text(text, encoding="utf-8")
        else:
            print(text, end="")
        return 1

    records = []
    for run_dir in runs:
        env_file = run_dir / "run.env"
        if not env_file.exists():
            lines.append(f"FAIL missing run.env: {run_dir}")
            continue
        values = parse_run_env(env_file)
        records.append((run_dir, values))

    lines.append("scenario,run_id," + ",".join(INVARIANT_KEYS))
    for _, values in records:
        row = [
            values.get("SCENARIO", ""),
            values.get("RUN_ID", ""),
            *[values.get(key, "") for key in INVARIANT_KEYS],
        ]
        lines.append(format_row(row))

    failures = []
    ignored_differences = []
    ignore_api_base = uses_discovered_k8s_endpoint(records)
    for key in INVARIANT_KEYS:
        observed = {values.get(key, "") for _, values in records}
        if len(observed) > 1:
            if key == "API_BASE" and ignore_api_base:
                ignored_differences.append(
                    f"{key} differs because each Kubernetes reset may allocate a new LoadBalancer endpoint: {sorted(observed)}"
                )
                continue
            failures.append(f"{key} differs: {sorted(observed)}")

    scenarios = {values.get("SCENARIO", "") for _, values in records}
    missing = [scenario for scenario in EXPECTED_SCENARIOS if scenario not in scenarios]
    if missing:
        failures.append(f"missing scenarios: {missing}")

    if failures:
        lines.append("FAIL non-policy parameters are not uniform")
        lines.extend(failures)
        exit_code = 1
    else:
        lines.append("PASS non-policy parameters are uniform across compared scenarios")
        exit_code = 0
    if ignored_differences:
        lines.append("INFO ignored derived endpoint differences")
        lines.extend(ignored_differences)

    text = "\n".join(lines) + "\n"
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
