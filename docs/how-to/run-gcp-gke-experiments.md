# Run GCP GKE Experiments

This guide runs the same baseline/A/B/C experiment matrix on Google Kubernetes
Engine. It uses the shared Kubernetes base under `deploy/k3s/base` plus the GKE
overlay under `deploy/gke/standard`.

## Target Topology

Use one GKE Standard cluster:

```text
region/zone: asia-east1 / asia-east1-a
node pool: 6 nodes for the free-trial quota-friendly profile
machine type: e2-standard-2 for first cloud runs
disk: 30 GiB pd-balanced per node
stateful workload PVCs: 45 GiB total by default
namespace: rec-store
```

Inside Kubernetes:

```text
1 PD pod + 1 TiKV pod for metadata
3 meta-service pods
2 API pods behind a LoadBalancer service
6 storage-node StatefulSet pods, each with one PVC
0 or 1 tiering-worker pod, controlled by the matrix runner
```

The GKE overlay changes the API service from NodePort to LoadBalancer and adds a
required anti-affinity rule for storage-node pods so the six storage pods are
spread one per Kubernetes node. This keeps the storage tier distributed even on
the quota-friendly six-node profile. It also shrinks lab PVC requests to fit the
common free-trial `SSD_TOTAL_GB=250` quota:

```text
PD PVC: 5 GiB
TiKV PVC: 10 GiB
storage-node PVCs: 6 * 5 GiB
total workload PVCs: 45 GiB
```

## One-Time GCP Setup

Install and initialize the Google Cloud CLI on your local machine or on a runner
VM:

```bash
gcloud init
gcloud config set project <project-id>
gcloud config set compute/region asia-east1
gcloud config set compute/zone asia-east1-a
```

Enable the required APIs:

```bash
gcloud services enable \
  artifactregistry.googleapis.com \
  container.googleapis.com \
  compute.googleapis.com
```

Create an Artifact Registry Docker repository:

```bash
gcloud artifacts repositories create rec-store \
  --repository-format=docker \
  --location=asia-east1 \
  --description="REC object store experiment images"
```

Allow Docker to push to Artifact Registry:

```bash
gcloud auth configure-docker asia-east1-docker.pkg.dev
```

## Build and Push the Image

From the repository root:

```bash
export GCP_PROJECT_ID="$(gcloud config get-value project)"
export IMAGE="asia-east1-docker.pkg.dev/${GCP_PROJECT_ID}/rec-store/rec-store:gke-exp-001"

IMAGE="${IMAGE}" ./deploy/gke/scripts/build-and-push-image.sh
```

The script is registry-neutral. It works with Artifact Registry after
`gcloud auth configure-docker`.

## Create the GKE Cluster

Create a Standard cluster:

```bash
gcloud container clusters create rec-store-exp \
  --zone asia-east1-a \
  --num-nodes 6 \
  --machine-type e2-standard-2 \
  --disk-type pd-balanced \
  --disk-size 30 \
  --enable-ip-alias
```

Fetch kubeconfig:

```bash
gcloud container clusters get-credentials rec-store-exp --zone asia-east1-a
kubectl get nodes -o wide
```

You should see six ready nodes before deploying the storage system. This profile
uses `6 * 2 vCPU = 12 vCPU` and `6 * 30 GiB = 180 GiB` of node boot disk. The
GKE overlay adds about 45 GiB of workload PVCs, keeping the expected persistent
disk total near 225 GiB and under the common new-project 250 GiB SSD quota.

## Deploy the Store

Deploy with the GKE overlay:

```bash
IMAGE="${IMAGE}" \
RESET_NAMESPACE=true \
./deploy/gke/scripts/deploy.sh
```

The GKE deploy wrapper uses the GKE overlay and grants the GKE node service
account `roles/artifactregistry.reader` on the Artifact Registry repository by
default. Set `GRANT_ARTIFACT_READER=false` only if this permission is managed
elsewhere.

Check pods and the API service:

```bash
kubectl -n rec-store get pods -o wide
kubectl -n rec-store get svc api -w
```

When the `api` service has an external IP:

```bash
export API_BASE="http://<api-external-ip>:8000"
curl -fsS "${API_BASE}/health"
curl -fsS "${API_BASE}/v2/admin/nodes?limit=1000"
```

The admin node list should show six live storage nodes.

## Run the Experiment Matrix

The GKE matrix runner resets and redeploys the namespace for each scenario, so
it discovers the `api` LoadBalancer endpoint after every redeploy. You can keep
`API_BASE` for manual smoke checks, but do not need to pass it to
`run_matrix_gke.sh`.

Smoke run:

```bash
IMAGE="${IMAGE}" \
MATRIX_PRESSURE_PROFILE=none \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=20 OBJECT_SIZE_BYTES=262144 \
WORKLOAD_DURATION_SEC=20 WORKLOAD_CONCURRENCY=2 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_gke.sh
```

No-pressure matrix:

```bash
IMAGE="${IMAGE}" \
MATRIX_PRESSURE_PROFILE=none \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_gke.sh
```

CPU-pressure matrix:

```bash
IMAGE="${IMAGE}" \
MATRIX_PRESSURE_PROFILE=cpu MATRIX_PRESSURE_CPUS=2 \
MATRIX_PRESSURE_DURATION_SEC=60 MATRIX_PRESSURE_WARMUP_SEC=10 \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_gke.sh
```

I/O-pressure matrix:

```bash
IMAGE="${IMAGE}" \
MATRIX_PRESSURE_PROFILE=io MATRIX_HDD_WORKERS=2 MATRIX_HDD_BYTES=512M \
MATRIX_PRESSURE_DURATION_SEC=60 MATRIX_PRESSURE_WARMUP_SEC=10 \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=70 \
OBJECT_COUNT=200 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=45 WORKLOAD_CONCURRENCY=8 GET_PERCENT=70 \
./experiments/scenarios/run_matrix_gke.sh
```

Outputs:

```text
experiments/results/matrix-<run_id_root>-fairness.txt
experiments/results/matrix-<run_id_root>-comparison.csv
experiments/results/matrix-<run_id_root>-migration.csv
```

Use at least three matrix runs per pressure profile for report-quality numbers.

## Cleanup

Delete the experiment namespace between ad-hoc runs:

```bash
kubectl delete namespace rec-store
```

Delete the cluster when you are done:

```bash
gcloud container clusters delete rec-store-exp --zone asia-east1-a
```

Also check Artifact Registry and persistent disks if you created extra resources
while debugging.
