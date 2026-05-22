# Run AWS k3s Experiments

This guide prepares the local Docker Compose experiment flow for an AWS k3s
cluster. The goal is not production Kubernetes hardening; it is a repeatable
research environment that can run the same baseline/A/B/C matrix with cleaner
CPU, I/O, and network conditions than WSL/Docker Desktop.

## Target Topology

Use this topology for the first cloud run:

```text
1 control-plane / runner EC2
6 worker EC2 instances for storage-node pods
1 k3s namespace: rec-store
1 PD pod + 1 TiKV pod for metadata
3 meta-service pods behind a ClusterIP service
2 API pods behind a NodePort service on 30080
6 storage-node StatefulSet pods with one PVC each
0 or 1 tiering-worker pod, controlled by the experiment script
```

The manifests use k3s `local-path` volumes by default. That is acceptable for a
prototype experiment because each storage-node pod gets its own PVC and the
namespace is reset between scenarios. For a longer-running or failure-tolerant
deployment, replace this with EBS CSI backed storage classes.

## Build and Push the Image

Push one immutable tag per experiment batch:

```bash
export IMAGE=<registry>/<repo>:aws-exp-001
./deploy/k3s/scripts/build-and-push-image.sh
```

For ECR, log in to the registry first with the AWS CLI. For a quick private
Docker Hub or GHCR run, use the equivalent registry login.

## Deploy the Stack

From a shell with `kubectl` configured for the k3s cluster:

```bash
IMAGE=<registry>/<repo>:aws-exp-001 \
RESET_NAMESPACE=true \
./deploy/k3s/scripts/deploy.sh
```

Wait or re-check readiness:

```bash
./deploy/k3s/scripts/wait-ready.sh
kubectl -n rec-store get pods -o wide
```

The API service is exposed as NodePort `30080`. For a first AWS run, allow only
your client IP to access TCP `30080` in the EC2 security group, then set:

```bash
export API_BASE=http://<control-plane-public-ip>:30080
curl -fsS "${API_BASE}/health"
curl -fsS "${API_BASE}/v2/admin/nodes?limit=1000"
```

The node list should show six live storage nodes before running experiments.

## Run One Fair Matrix

The k3s matrix resets the namespace before each scenario, redeploys the same
image, preloads objects, ages the preload set, runs the foreground workload, and
then writes the same comparison CSVs as the local runner.

No injected pressure:

```bash
IMAGE=<registry>/<repo>:aws-exp-001 \
API_BASE=http://<control-plane-public-ip>:30080 \
MATRIX_PRESSURE_PROFILE=none \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_k3s.sh
```

CPU pressure:

```bash
IMAGE=<registry>/<repo>:aws-exp-001 \
API_BASE=http://<control-plane-public-ip>:30080 \
MATRIX_PRESSURE_PROFILE=cpu MATRIX_PRESSURE_CPUS=2 \
MATRIX_PRESSURE_DURATION_SEC=60 MATRIX_PRESSURE_WARMUP_SEC=10 \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_k3s.sh
```

I/O pressure:

```bash
IMAGE=<registry>/<repo>:aws-exp-001 \
API_BASE=http://<control-plane-public-ip>:30080 \
MATRIX_PRESSURE_PROFILE=io MATRIX_HDD_WORKERS=2 MATRIX_HDD_BYTES=512M \
MATRIX_PRESSURE_DURATION_SEC=60 MATRIX_PRESSURE_WARMUP_SEC=10 \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_k3s.sh
```

Outputs:

```text
experiments/results/matrix-<run_id_root>-fairness.txt
experiments/results/matrix-<run_id_root>-comparison.csv
experiments/results/matrix-<run_id_root>-migration.csv
```

## Notes and Limits

- The k3s pressure jobs are cluster-level stress pods, not precise per-storage
  node targeting. They are good enough for the first AWS comparison, but
  local-aware pressure placement should be added if the paper claims per-node
  avoidance.
- The tiering worker starts at zero replicas in the base manifest. The matrix
  script scales it only for A/B/C scenarios, keeping the baseline clean.
- Kubernetes service names use hyphens. Storage nodes advertise URLs like
  `http://storage-node-0.storage-node:8001`; do not use the Docker Compose
  underscore names in k3s.
- Run at least three matrices per pressure profile and report median P99 or
  mean P99 with raw CSVs retained.
