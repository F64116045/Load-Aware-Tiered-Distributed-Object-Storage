#!/usr/bin/env bash

set -euo pipefail

NAMESPACE="${NAMESPACE:-rec-store}"
TIMEOUT="${TIMEOUT:-300s}"

kubectl -n "${NAMESPACE}" rollout status statefulset/pd --timeout="${TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status statefulset/tikv --timeout="${TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status deployment/meta-service --timeout="${TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status statefulset/storage-node --timeout="${TIMEOUT}"
kubectl -n "${NAMESPACE}" rollout status deployment/api --timeout="${TIMEOUT}"
kubectl -n "${NAMESPACE}" wait --for=condition=ready pod -l app=storage-node --timeout="${TIMEOUT}"
