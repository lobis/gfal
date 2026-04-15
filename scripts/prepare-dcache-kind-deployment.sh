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

run_chimera_allow_fail() {
    local output
    if output=$(dcache_chimera "$@" 2>&1); then
        if [ -n "${output}" ]; then
            echo "${output}" >&2
        fi
        return 0
    fi

    local status=$?
    echo "Warning: dcache_chimera $* failed with exit ${status}" >&2
    if [ -n "${output}" ]; then
        echo "${output}" >&2
    fi
    return 0
}

kubectl wait -n "${NAMESPACE}" --for=condition=Ready "pod/${DOOR_POD}" --timeout=10m >/dev/null

run_chimera_allow_fail mkdir /data/gfal-tests
run_chimera_allow_fail chmod 0777 /data/gfal-tests
run_chimera_allow_fail mkdir "${BASE_PATH}"
run_chimera_allow_fail chmod 0755 "${BASE_PATH}"
run_chimera_allow_fail mkdir "${WRITABLE_PATH}"
run_chimera_allow_fail chmod 0777 "${WRITABLE_PATH}"
run_chimera_allow_fail mkdir "${DENIED_PATH}"
run_chimera_allow_fail chmod 0555 "${DENIED_PATH}"

cat <<EOF
GFAL_DEPLOYMENT_NAME=dcache-kind
GFAL_DEPLOYMENT_VERIFY_SSL=0
GFAL_DEPLOYMENT_SUPPORTS_LISTING=1
GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE=https://${SERVICE_HOST}:8083${WRITABLE_PATH}
GFAL_DEPLOYMENT_HTTP_DENIED_BASE=https://${SERVICE_HOST}:8083${DENIED_PATH}
GFAL_DEPLOYMENT_ROOT_WRITABLE_BASE=root://${SERVICE_HOST}:1094/${WRITABLE_PATH}
GFAL_DEPLOYMENT_ROOT_DENIED_BASE=root://${SERVICE_HOST}:1094/${DENIED_PATH}
EOF
