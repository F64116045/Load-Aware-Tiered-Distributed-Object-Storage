#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"

require_python3
ensure_result_dir >/dev/null

DURATION_SEC="${DURATION_SEC:-180}"
INTERVAL_SEC="${INTERVAL_SEC:-5}"
OUT_FILE="${OUT_FILE:-${RESULT_DIR}/metrics.csv}"
RAW_FILE="${RAW_FILE:-${RESULT_DIR}/admin_samples.jsonl}"

printf 'timestamp_unix,scenario,run_id,node_count,live_nodes,stale_nodes,avg_cpu_pct,max_cpu_pct,avg_iowait_pct,max_iowait_pct,avg_memory_pct,max_memory_pct,max_queue_depth,due_total,due_ready,repl_pending,repl_running,repl_retry_wait,repl_done,repl_failed,gc_pending,gc_running,gc_done,gc_failed,leader_id,scanner_status\n' >"${OUT_FILE}"
: >"${RAW_FILE}"

deadline=$((SECONDS + DURATION_SEC))
exp_log "Collect admin metrics: duration=${DURATION_SEC}s interval=${INTERVAL_SEC}s"

while (( SECONDS < deadline )); do
  ts="$(date +%s)"
  tmp_dir="$(mktemp -d)"
  nodes_json="${tmp_dir}/nodes.json"
  metrics_json="${tmp_dir}/metrics.json"
  repl_json="${tmp_dir}/repl_tasks.json"
  gc_json="${tmp_dir}/gc_tasks.json"

  curl -sS "${API_BASE}/v2/admin/nodes?limit=1000" -o "${nodes_json}" || printf '{}' >"${nodes_json}"
  curl -sS "${API_BASE}/v2/admin/metrics-snapshot" -o "${metrics_json}" || printf '{}' >"${metrics_json}"
  curl -sS "${API_BASE}/v2/admin/tasks?task_type=REPL_TO_EC&limit=1000" -o "${repl_json}" || printf '{}' >"${repl_json}"
  curl -sS "${API_BASE}/v2/admin/tasks?task_type=GC&limit=1000" -o "${gc_json}" || printf '{}' >"${gc_json}"

  python3 - "$ts" "$SCENARIO" "$RUN_ID" "$nodes_json" "$metrics_json" "$repl_json" "$gc_json" >>"${OUT_FILE}" <<'PY'
import csv
import json
import sys

ts, scenario, run_id = sys.argv[1:4]
paths = sys.argv[4:]

def load(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

nodes_body, metrics_body, repl_body, gc_body = [load(p) for p in paths]
nodes = nodes_body.get("nodes") or []
live = [n for n in nodes if n.get("status") == "UP" and not n.get("is_stale")]

def fnum(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default

def avg(values):
    return sum(values) / len(values) if values else 0.0

cpu = [fnum(n.get("cpu_load")) * 100.0 for n in live]
iowait = [fnum(n.get("disk_iowait_pct")) for n in live]
memory = [fnum(n.get("memory_used_pct")) for n in live]
queues = [int(fnum(n.get("io_queue_depth"))) for n in live]
due = metrics_body.get("tiering_due_index") or {}
leader = metrics_body.get("tiering_leader") or {}

def counts(body):
    c = body.get("state_counts") or {}
    return {
        "PENDING": int(c.get("PENDING", 0) or 0),
        "RUNNING": int(c.get("RUNNING", 0) or 0),
        "RETRY_WAIT": int(c.get("RETRY_WAIT", 0) or 0),
        "DONE": int(c.get("DONE", 0) or 0),
        "FAILED": int(c.get("FAILED", 0) or 0),
    }

repl = counts(repl_body)
gc = counts(gc_body)
row = [
    ts,
    scenario,
    run_id,
    len(nodes),
    len(live),
    len(nodes) - len(live),
    f"{avg(cpu):.3f}",
    f"{max(cpu) if cpu else 0.0:.3f}",
    f"{avg(iowait):.3f}",
    f"{max(iowait) if iowait else 0.0:.3f}",
    f"{avg(memory):.3f}",
    f"{max(memory) if memory else 0.0:.3f}",
    max(queues) if queues else 0,
    int(due.get("due_total", 0) or 0),
    int(due.get("due_ready", 0) or 0),
    repl["PENDING"],
    repl["RUNNING"],
    repl["RETRY_WAIT"],
    repl["DONE"],
    repl["FAILED"],
    gc["PENDING"],
    gc["RUNNING"],
    gc["DONE"],
    gc["FAILED"],
    leader.get("leader_id", ""),
    leader.get("scanner_status", ""),
]
csv.writer(sys.stdout).writerow(row)
PY

  python3 - "$ts" "$nodes_json" "$metrics_json" "$repl_json" "$gc_json" >>"${RAW_FILE}" <<'PY'
import json
import sys

ts = sys.argv[1]
labels = ["nodes", "metrics", "repl_tasks", "gc_tasks"]
payload = {"timestamp_unix": int(ts)}
for label, path in zip(labels, sys.argv[2:]):
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload[label] = json.load(f)
    except Exception as exc:
        payload[label] = {"error": str(exc)}
print(json.dumps(payload, separators=(",", ":")))
PY

  rm -rf "${tmp_dir}"
  sleep "${INTERVAL_SEC}"
done

exp_log "Metrics CSV: ${OUT_FILE}"
