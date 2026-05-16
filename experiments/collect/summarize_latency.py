#!/usr/bin/env python3

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path


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


def status_ok(code):
    return code.startswith("2")


def summarize(rows, operation):
    latencies = []
    errors = 0
    first_ts = None
    last_ts = None
    for row in rows:
        if operation != "ALL" and row["operation"] != operation:
            continue
        try:
            latency = float(row["latency_ms"])
            ts = int(row["timestamp_unix_ms"])
        except (KeyError, TypeError, ValueError):
            continue
        latencies.append(latency)
        first_ts = ts if first_ts is None else min(first_ts, ts)
        last_ts = ts if last_ts is None else max(last_ts, ts)
        if not status_ok(row.get("http_code", "")):
            errors += 1

    count = len(latencies)
    duration_sec = 0.0
    if first_ts is not None and last_ts is not None and last_ts >= first_ts:
        duration_sec = max((last_ts - first_ts) / 1000.0, 0.001)
    return {
        "operation": operation,
        "count": count,
        "errors": errors,
        "error_rate": (errors / count) if count else 0.0,
        "throughput_rps": (count / duration_sec) if duration_sec else 0.0,
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "max_ms": max(latencies) if latencies else 0.0,
    }


def main():
    parser = argparse.ArgumentParser(description="Summarize experiment latency CSV.")
    parser.add_argument("latency_csv", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    with args.latency_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    summaries = [summarize(rows, op) for op in ("ALL", "GET", "PUT")]
    fieldnames = [
        "operation",
        "count",
        "errors",
        "error_rate",
        "throughput_rps",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
    ]

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(summaries)

    writer = csv.DictWriter(__import__("sys").stdout, fieldnames=fieldnames)
    writer.writeheader()
    for row in summaries:
        writer.writerow({
            key: f"{value:.6f}" if isinstance(value, float) else value
            for key, value in row.items()
        })


if __name__ == "__main__":
    main()
