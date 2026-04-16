#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:-manual}"
BASE_PATH="/eos/dev/gfal-tests/${RUN_ID}"
WRITABLE_PATH="${BASE_PATH}/writable"
DENIED_PATH="${BASE_PATH}/denied"
HTTP_HOST="${EOS_TEST_HTTP_HOST:-eos-mgm}"
HTTP_PORT="${EOS_TEST_HTTP_PORT:-8443}"
ROOT_HOST="${EOS_TEST_ROOT_HOST:-${HTTP_HOST}}"
ROOT_PORT="${EOS_TEST_ROOT_PORT:-1094}"

if [[ -n "${EOS_TEST_SSH_TARGET:-}" ]]; then
    ssh "${EOS_TEST_SSH_TARGET}" "
      set -euo pipefail
      eos mkdir -p '${WRITABLE_PATH}'
      eos mkdir -p '${DENIED_PATH}'
      eos chmod 777 '${WRITABLE_PATH}'
      eos chmod 755 '${DENIED_PATH}'
    " >/dev/null
elif [[ -n "${EOS_TEST_KUBECTL_TARGET:-}" ]]; then
    KUBECTL_ARGS=()
    if [[ -n "${EOS_TEST_KUBECTL_NAMESPACE:-}" ]]; then
        KUBECTL_ARGS+=(-n "${EOS_TEST_KUBECTL_NAMESPACE}")
    fi
    if [[ -n "${EOS_TEST_KUBECTL_CONTAINER:-}" ]]; then
        KUBECTL_ARGS+=(-c "${EOS_TEST_KUBECTL_CONTAINER}")
    fi

    kubectl exec "${KUBECTL_ARGS[@]}" "${EOS_TEST_KUBECTL_TARGET}" -- bash -lc "
      set -euo pipefail
      eos mkdir -p '${WRITABLE_PATH}'
      eos mkdir -p '${DENIED_PATH}'
      eos chmod 777 '${WRITABLE_PATH}'
      eos chmod 755 '${DENIED_PATH}'
    " >/dev/null
else
    echo "Either EOS_TEST_SSH_TARGET or EOS_TEST_KUBECTL_TARGET must be set" >&2
    exit 1
fi

cat <<EOF
GFAL_DEPLOYMENT_NAME=eos-kind
GFAL_DEPLOYMENT_VERIFY_SSL=0
GFAL_DEPLOYMENT_SUPPORTS_LISTING=1
GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE=https://${HTTP_HOST}:${HTTP_PORT}${WRITABLE_PATH}
GFAL_DEPLOYMENT_HTTP_DENIED_BASE=https://${HTTP_HOST}:${HTTP_PORT}${DENIED_PATH}
GFAL_DEPLOYMENT_ROOT_WRITABLE_BASE=root://${ROOT_HOST}:${ROOT_PORT}/${WRITABLE_PATH}
GFAL_DEPLOYMENT_ROOT_DENIED_BASE=root://${ROOT_HOST}:${ROOT_PORT}/${DENIED_PATH}
EOF
