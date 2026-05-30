#!/usr/bin/env python3

import argparse
import csv
import math
import re
import sys
from collections import defaultdict
from pathlib import Path


PHASE_MARKERS = [
    ("[API Phase]", "api", "request"),
    ("[WriteReplication Phase]", "write_service", "replication_commit"),
    ("[Storage Route Phase]", "storage_node", "store_route"),
    ("[Storage Phase]", "storage_node", "durable_write"),
]

KV_RE = re.compile(r"([A-Za-z0-9_]+)=([^\s]+)")


def percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (rank - lo)


def parse_run_env(result_dir):
    env = {}
    env_file = result_dir / "run.env"
    if not env_file.exists():
        return env
    for raw in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def parse_key_values(line):
    return {match.group(1): match.group(2) for match in KV_RE.finditer(line)}


def marker_for_line(line):
    for marker, component, event in PHASE_MARKERS:
        if marker in line:
            return component, event
    return None, None


def clean_phase_name(key):
    if key.endswith("_ms"):
        return key[:-3]
    return key


def operation_for(component, event, values):
    op = values.get("op")
    if op:
        return op
    if component == "write_service" and event == "replication_commit":
        return "PUT"
    if component == "storage_node":
        return "STORE"
    return "UNKNOWN"


def parse_phase_events(result_dir):
    env = parse_run_env(result_dir)
    scenario = env.get("SCENARIO") or result_dir.parent.name
    run_id = env.get("RUN_ID") or result_dir.name
    events = []

    for log_file in sorted(result_dir.rglob("*.log")):
        try:
            lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            component, event = marker_for_line(line)
            if not component:
                continue
            values = parse_key_values(line)
            operation = operation_for(component, event, values)
            identity = values.get("object") or values.get("key") or ""
            size_bytes = values.get("size_bytes", "")
            for key, value in values.items():
                if not key.endswith("_ms"):
                    continue
                try:
                    value_ms = float(value)
                except ValueError:
                    continue
                events.append({
                    "scenario": scenario,
                    "run_id": run_id,
                    "component": component,
                    "event": event,
                    "operation": operation,
                    "phase": clean_phase_name(key),
                    "value_ms": value_ms,
                    "object_or_key": identity,
                    "size_bytes": size_bytes,
                    "source_file": str(log_file.relative_to(result_dir)),
                    "result_dir": str(result_dir),
                })
    return events


def summarize_events(events):
    buckets = defaultdict(list)
    for event in events:
        key = (
            event["scenario"],
            event["run_id"],
            event["component"],
            event["event"],
            event["operation"],
            event["phase"],
            event["result_dir"],
        )
        buckets[key].append(event["value_ms"])

    rows = []
    for (
        scenario,
        run_id,
        component,
        event,
        operation,
        phase,
        result_dir,
    ), values in sorted(buckets.items()):
        rows.append({
            "scenario": scenario,
            "run_id": run_id,
            "component": component,
            "event": event,
            "operation": operation,
            "phase": phase,
            "count": len(values),
            "p50_ms": percentile(values, 50),
            "p95_ms": percentile(values, 95),
            "p99_ms": percentile(values, 99),
            "max_ms": max(values) if values else 0.0,
            "result_dir": result_dir,
        })
    return rows


def discover_result_dirs(result_root, run_id_root):
    dirs = []
    for scenario_dir in sorted(result_root.iterdir()):
        if not scenario_dir.is_dir():
            continue
        for run_dir in sorted(scenario_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            if run_id_root and not run_dir.name.startswith(run_id_root):
                continue
            dirs.append(run_dir)
    return dirs


def write_csv(path, fieldnames, rows):
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        f = path.open("w", encoding="utf-8", newline="")
    else:
        f = sys.stdout
    try:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
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
    parser = argparse.ArgumentParser(description="Summarize structured phase latency logs from experiment results.")
    parser.add_argument("--result-dir", type=Path, help="Single scenario result directory.")
    parser.add_argument("--result-root", type=Path, help="Experiment result root containing scenario/run directories.")
    parser.add_argument("--run-id-root", help="Run id prefix used by a matrix run.")
    parser.add_argument("--out", type=Path, help="Output summary CSV.")
    parser.add_argument("--events-out", type=Path, help="Optional raw parsed phase event CSV.")
    args = parser.parse_args()

    if args.result_dir:
        result_dirs = [args.result_dir]
    elif args.result_root:
        result_dirs = discover_result_dirs(args.result_root, args.run_id_root)
    else:
        parser.error("one of --result-dir or --result-root is required")

    events = []
    for result_dir in result_dirs:
        if result_dir.is_dir():
            events.extend(parse_phase_events(result_dir))

    summary_rows = summarize_events(events)
    summary_fields = [
        "scenario",
        "run_id",
        "component",
        "event",
        "operation",
        "phase",
        "count",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
        "result_dir",
    ]
    write_csv(args.out, summary_fields, summary_rows)

    if args.events_out:
        event_fields = [
            "scenario",
            "run_id",
            "component",
            "event",
            "operation",
            "phase",
            "value_ms",
            "object_or_key",
            "size_bytes",
            "source_file",
            "result_dir",
        ]
        write_csv(args.events_out, event_fields, events)


if __name__ == "__main__":
    main()
