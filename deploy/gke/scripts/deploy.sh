#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

IMAGE="${IMAGE:-}"
GCP_REGION="${GCP_REGION:-asia-east1}"
GCP_ZONE="${GCP_ZONE:-asia-east1-a}"
CLUSTER_NAME="${CLUSTER_NAME:-rec-store-exp}"
GRANT_ARTIFACT_READER="${GRANT_ARTIFACT_READER:-true}"

if [[ -z "${IMAGE}" ]]; then
  echo "ERROR: IMAGE is required, for example IMAGE=asia-east1-docker.pkg.dev/<project>/rec-store/rec-store:gke-exp-001" >&2
  exit 1
fi

parse_artifact_registry_image() {
  local image_no_tag host project repo
  image_no_tag="${IMAGE%%:*}"
  host="${image_no_tag%%/*}"
  if [[ "${host}" != *-docker.pkg.dev ]]; then
    return 1
  fi
  GCP_REGION="${host%-docker.pkg.dev}"
  image_no_tag="${image_no_tag#*/}"
  project="${image_no_tag%%/*}"
  image_no_tag="${image_no_tag#*/}"
  repo="${image_no_tag%%/*}"
  if [[ -n "${project}" && -n "${repo}" ]]; then
    GCP_PROJECT_ID="${GCP_PROJECT_ID:-${project}}"
    AR_REPO="${AR_REPO:-${repo}}"
    return 0
  fi
  return 1
}

grant_artifact_registry_reader() {
  if [[ "${GRANT_ARTIFACT_READER}" != "true" ]]; then
    return 0
  fi
  if ! command -v gcloud >/dev/null 2>&1; then
    echo "WARN: gcloud not found; skip Artifact Registry reader grant" >&2
    return 0
  fi
  parse_artifact_registry_image || {
    echo "WARN: IMAGE is not an Artifact Registry image; skip Artifact Registry reader grant" >&2
    return 0
  }

  local project_number node_sa
  project_number="$(gcloud projects describe "${GCP_PROJECT_ID}" --format='value(projectNumber)')"
  node_sa="$(gcloud container clusters describe "${CLUSTER_NAME}" --zone "${GCP_ZONE}" --project "${GCP_PROJECT_ID}" --format='value(nodeConfig.serviceAccount)' 2>/dev/null || true)"
  if [[ -z "${node_sa}" || "${node_sa}" == "default" ]]; then
    node_sa="${project_number}-compute@developer.gserviceaccount.com"
  fi

  echo "Grant Artifact Registry reader to GKE node service account: ${node_sa}" >&2
  gcloud artifacts repositories add-iam-policy-binding "${AR_REPO}" \
    --project "${GCP_PROJECT_ID}" \
    --location "${GCP_REGION}" \
    --member "serviceAccount:${node_sa}" \
    --role "roles/artifactregistry.reader" >/dev/null
}

grant_artifact_registry_reader

IMAGE="${IMAGE}" \
KUSTOMIZE_DIR=deploy/gke/standard \
"${REPO_ROOT}/deploy/k3s/scripts/deploy.sh"
