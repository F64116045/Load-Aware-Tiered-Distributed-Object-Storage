# GKE Experiment Infrastructure

This Terraform root module creates a fixed-size GKE Standard cluster for the REC object-store experiments.

It is compatible with Terraform `1.5.7`, which is commonly available in Google Cloud Shell.

It manages only the cloud infrastructure:

- GKE Standard cluster with separate system and storage node pools
- Dedicated VPC and subnet with secondary Pod/Service ranges
- GKE node service account
- Minimum GKE node IAM plus Artifact Registry pull permission for the node service account
- Cloud Build service account permissions needed to read submitted source archives and push images
- Optional Artifact Registry Docker repository creation

It does not run the experiment suite. Keep deployment and workload control in the existing scripts under `deploy/gke/` and `experiments/scenarios/`.

## Why Terraform Here

The goal is to make the cloud environment repeatable. For final measurements, avoid ad hoc `gcloud container clusters create ...` commands because the machine type, disk size, network, IAM, and cleanup behavior should be visible in versioned code.

The default profile uses 2 system nodes and 6 storage nodes. This makes pod placement part of the experiment design instead of relying on the default Kubernetes scheduler. System pods run on nodes labeled `rec-store-role=system`; `storage-node` pods run on nodes labeled `rec-store-role=storage` with required anti-affinity so each storage replica lands on a different worker.

The standard example uses `n2-standard-4` for both pools and therefore needs 32 vCPUs. The constrained example uses `n2-standard-2` for both pools and needs 16 vCPUs. If the project quota is still lower than that, request a quota increase before treating the result as final. A smaller temporary profile can be used for smoke testing, but it should not be presented as the final cloud experiment.

## First Setup

```bash
cd infra/gcp/gke-experiment
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

- Set `project_id` to your GCP project.
- Keep `create_artifact_registry_repository = false` if `rec-store` already exists.
- Set `create_artifact_registry_repository = true` only for a fresh project without the repository.

If the project is limited by GCP quota and `CPUs (all regions)` is fixed at 16 vCPUs, use the constrained profile instead:

```bash
cp terraform.free-trial.tfvars.example terraform.tfvars
```

That profile uses 2 system plus 6 storage `n2-standard-2` nodes. It preserves the 6-storage-node topology while avoiding metadata/API co-location with storage pressure.

Then create the cluster:

```bash
terraform init
terraform plan
terraform apply
```

If Terraform fails while creating project IAM bindings with a `Cloud Resource Manager API ... disabled` error, enable the API once and rerun the plan:

```bash
gcloud services enable cloudresourcemanager.googleapis.com iam.googleapis.com
terraform plan -out=tfplan
terraform apply tfplan
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

If Cloud Shell Docker networking cannot push to Artifact Registry, use Cloud Build instead:

```bash
cat > /tmp/cloudbuild-rec-store.yaml <<'EOF'
steps:
  - name: gcr.io/cloud-builders/docker
    args:
      - build
      - --build-arg
      - GOPROXY=https://goproxy.io,direct
      - -t
      - ${_IMAGE}
      - .
images:
  - ${_IMAGE}
EOF

gcloud builds submit \
  --config=/tmp/cloudbuild-rec-store.yaml \
  --substitutions=_IMAGE="${IMAGE}" \
  .
```

After the deployment is healthy, run the formal experiment suite:

```bash
AGE_THRESHOLD_SEC=60 PRELOAD_AGE_WAIT_SEC=90 \
OBJECT_COUNT=50 OBJECT_SIZE_BYTES=1048576 \
WORKLOAD_DURATION_SEC=60 WORKLOAD_CONCURRENCY=2 GET_PERCENT=70 \
./experiments/scenarios/run_gke_formal_experiment.sh
```

This runs three repeats for `none`, `cpu`, and `io`. For CPU and I/O pressure,
the runner targets two storage-only nodes after each Kubernetes namespace reset.

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
