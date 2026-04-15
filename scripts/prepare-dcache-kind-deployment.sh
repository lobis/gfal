#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:?Usage: prepare-dcache-kind-deployment.sh <run-id> [release] [namespace]}"
RELEASE="${2:-store}"
NAMESPACE="${3:-default}"

DOOR_POD="${RELEASE}-dcache-door-0"
SERVICE_HOST="${RELEASE}-door-svc.${NAMESPACE}.svc.cluster.local"
BASE_PATH="/data/gfal-tests/${RUN_ID}"
WRITABLE_PATH="${BASE_PATH}/writable"
DENIED_PATH="${BASE_PATH}/denied"

dcache_chimera() {
    kubectl exec -n "${NAMESPACE}" "${DOOR_POD}" -- /opt/dcache/bin/chimera "$@"
}

kubectl wait -n "${NAMESPACE}" --for=condition=Ready "pod/${DOOR_POD}" --timeout=10m

dcache_chimera mkdir /data/gfal-tests || true
dcache_chimera chmod 0777 /data/gfal-tests || true
dcache_chimera mkdir "${BASE_PATH}" || true
dcache_chimera chmod 0755 "${BASE_PATH}" || true
dcache_chimera mkdir "${WRITABLE_PATH}" || true
dcache_chimera chmod 0777 "${WRITABLE_PATH}" || true
dcache_chimera mkdir "${DENIED_PATH}" || true
dcache_chimera chmod 0555 "${DENIED_PATH}" || true

cat <<EOF
GFAL_DEPLOYMENT_NAME=dcache-kind
GFAL_DEPLOYMENT_VERIFY_SSL=0
GFAL_DEPLOYMENT_SUPPORTS_LISTING=1
GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE=https://${SERVICE_HOST}:8083${WRITABLE_PATH}
GFAL_DEPLOYMENT_HTTP_DENIED_BASE=https://${SERVICE_HOST}:8083${DENIED_PATH}
GFAL_DEPLOYMENT_ROOT_WRITABLE_BASE=root://${SERVICE_HOST}:1094/${WRITABLE_PATH}
GFAL_DEPLOYMENT_ROOT_DENIED_BASE=root://${SERVICE_HOST}:1094/${DENIED_PATH}
EOF
