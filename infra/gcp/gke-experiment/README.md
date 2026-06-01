# GKE Experiment Infrastructure

This Terraform root module creates a fixed-size GKE Standard cluster for the REC object-store experiments.

It is compatible with Terraform `1.5.7`, which is commonly available in Google Cloud Shell.

It manages only the cloud infrastructure:

- GKE Standard cluster and fixed worker node pool
- Dedicated VPC and subnet with secondary Pod/Service ranges
- GKE node service account
- Minimum GKE node IAM plus Artifact Registry pull permission for the node service account
- Optional Artifact Registry Docker repository creation

It does not run the experiment suite. Keep deployment and workload control in the existing scripts under `deploy/gke/` and `experiments/scenarios/`.

## Why Terraform Here

The goal is to make the cloud environment repeatable. For final measurements, avoid ad hoc `gcloud container clusters create ...` commands because the machine type, disk size, network, IAM, and cleanup behavior should be visible in versioned code.

The default profile uses 6 `n2-standard-4` nodes as a pilot profile. For final runs, switch `machine_type` to a stronger fixed-size shape such as `n2-standard-8` or `n4-standard-8`, then rerun the same experiment matrix multiple times.

Note: 6 `n2-standard-4` nodes require 24 vCPUs in the selected region. If the project quota is still 12 vCPUs, request a quota increase before treating the result as final. A smaller temporary profile can be used for smoke testing, but it should not be presented as the final cloud experiment.

## First Setup

```bash
cd infra/gcp/gke-experiment
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

- Set `project_id` to your GCP project.
- Keep `create_artifact_registry_repository = false` if `rec-store` already exists.
- Set `create_artifact_registry_repository = true` only for a fresh project without the repository.

Then create the cluster:

```bash
terraform init
terraform plan
terraform apply
```

## Use The Cluster

From the repository root:

```bash
cd infra/gcp/gke-experiment
eval "$(terraform output -raw get_credentials_command)"

cd ../../..
export GCP_PROJECT_ID="$(terraform -chdir=infra/gcp/gke-experiment output -raw project_id)"
export GCP_REGION="$(terraform -chdir=infra/gcp/gke-experiment output -raw region)"
export GCP_ZONE="$(terraform -chdir=infra/gcp/gke-experiment output -raw zone)"
export CLUSTER_NAME="$(terraform -chdir=infra/gcp/gke-experiment output -raw cluster_name)"
export IMAGE="$(terraform -chdir=infra/gcp/gke-experiment output -raw image_example)"

gcloud auth configure-docker "${GCP_REGION}-docker.pkg.dev"
./deploy/gke/scripts/build-and-push-image.sh "$IMAGE"
IMAGE="$IMAGE" RESET_NAMESPACE=true ./deploy/gke/scripts/deploy.sh
```

After the deployment is healthy, run the GKE suite as usual:

```bash
IMAGE="$IMAGE" \
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=90 \
OBJECT_COUNT=50 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=60 WORKLOAD_CONCURRENCY=2 GET_PERCENT=70 \
./experiments/scenarios/run_gke_experiment_suite.sh
```

## Cost Cleanup

Destroy the Terraform-managed cluster before leaving:

```bash
terraform -chdir=infra/gcp/gke-experiment destroy
```

Then verify that no leftover disks or forwarding rules remain:

```bash
gcloud compute disks list --filter="zone:asia-east1-a" --format="table(name,sizeGb,type,users)"
gcloud compute forwarding-rules list --format="table(name,region.basename(),IPAddress,ports,target)"
```

If the Kubernetes namespace is still alive before destroy, remove it first so PVC-created disks are released:

```bash
kubectl delete namespace rec-store --ignore-not-found
kubectl wait --for=delete namespace/rec-store --timeout=180s || true
```
