#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

IMAGE="${IMAGE:-}"
NAMESPACE="${NAMESPACE:-rec-store}"
RESET_NAMESPACE="${RESET_NAMESPACE:-false}"
WAIT="${WAIT:-true}"
KUSTOMIZE_DIR="${KUSTOMIZE_DIR:-deploy/k3s/base}"

if [[ -z "${IMAGE}" ]]; then
  echo "ERROR: IMAGE is required, for example IMAGE=asia-east1-docker.pkg.dev/<project>/<repo>/rec-store:gke-exp-001" >&2
  exit 1
fi

if [[ "${NAMESPACE}" != "rec-store" ]]; then
  echo "ERROR: current base manifests expect NAMESPACE=rec-store" >&2
  exit 1
fi

image_name="${IMAGE%:*}"
image_tag="${IMAGE##*:}"
if [[ "${image_name}" == "${IMAGE}" ]]; then
  image_name="${IMAGE}"
  image_tag="latest"
fi

if [[ "${KUSTOMIZE_DIR}" = /* ]]; then
  echo "ERROR: KUSTOMIZE_DIR must be relative to the repository root" >&2
  exit 1
fi
if [[ "${KUSTOMIZE_DIR}" != deploy/* ]]; then
  echo "ERROR: KUSTOMIZE_DIR must point under deploy/, got: ${KUSTOMIZE_DIR}" >&2
  exit 1
fi
if [[ ! -d "${REPO_ROOT}/${KUSTOMIZE_DIR}" ]]; then
  echo "ERROR: KUSTOMIZE_DIR does not exist: ${KUSTOMIZE_DIR}" >&2
  exit 1
fi

if [[ "${RESET_NAMESPACE}" == "true" ]]; then
  kubectl delete namespace "${NAMESPACE}" --ignore-not-found
  kubectl wait --for=delete "namespace/${NAMESPACE}" --timeout=180s >/dev/null 2>&1 || true
fi

tmp_dir="$(mktemp -d "${REPO_ROOT}/deploy/.tmp-kustomize.XXXXXX")"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

resource_path="../${KUSTOMIZE_DIR#deploy/}"

cat >"${tmp_dir}/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ${resource_path}
images:
  - name: rec-store-image
    newName: ${image_name}
    newTag: ${image_tag}
  - name: localhost/rec-store
    newName: ${image_name}
    newTag: ${image_tag}
EOF

kubectl apply -k "${tmp_dir}"

if [[ "${WAIT}" == "true" ]]; then
  "${SCRIPT_DIR}/wait-ready.sh"
fi
