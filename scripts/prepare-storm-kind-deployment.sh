#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?Usage: prepare-storm-kind-deployment.sh <run-id> [namespace]}"
NAMESPACE="${2:-storm}"
POD_SELECTOR="${3:-app=storm-webdav}"

POD_NAME="$(
  kubectl get pods -n "${NAMESPACE}" -l "${POD_SELECTOR}" \
    -o jsonpath='{.items[0].metadata.name}'
)"

BASE_PATH="/data/sa/gfal-tests/${RUN_ID}"
WRITABLE_PATH="${BASE_PATH}/writable"
DENIED_PATH="${BASE_PATH}/denied"
SERVICE_HOST="storm-webdav.${NAMESPACE}.svc.cluster.local"

kubectl wait -n "${NAMESPACE}" --for=condition=Ready "pod/${POD_NAME}" --timeout=10m >/dev/null

kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- sh -lc "
  set -eu
  mkdir -p '${WRITABLE_PATH}' '${DENIED_PATH}'
  chmod 0777 '${WRITABLE_PATH}'
  chmod 0555 '${DENIED_PATH}'
"

cat <<EOF
GFAL_DEPLOYMENT_NAME=storm-kind
GFAL_DEPLOYMENT_VERIFY_SSL=0
GFAL_DEPLOYMENT_SUPPORTS_LISTING=1
GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE=http://${SERVICE_HOST}:8085/sa/gfal-tests/${RUN_ID}/writable
GFAL_DEPLOYMENT_HTTP_DENIED_BASE=http://${SERVICE_HOST}:8085/sa/gfal-tests/${RUN_ID}/denied
EOF
