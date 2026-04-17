#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${GFAL_BENCH_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
FILE_COUNT="${GFAL_BENCH_FILE_COUNT:-100}"
FILE_SIZE_MB="${GFAL_BENCH_FILE_SIZE_MB:-1}"
MOUNT_PATH="${GFAL_BENCH_MOUNT_PATH:-$HOME/eos}"
FUSE_BASE="${GFAL_BENCH_FUSE_BASE:-$MOUNT_PATH/pilot/opstest/dteam/python3-gfal/tmp}"
EOS_BASE="${GFAL_BENCH_EOS_BASE:-/eos/pilot/opstest/dteam/python3-gfal/tmp}"
EOS_HOST="${GFAL_BENCH_EOS_HOST:-eospilot.cern.ch}"
LOCAL_ROOT="${GFAL_BENCH_LOCAL_ROOT:-/tmp/gfal-eospilot-bench-${RUN_ID}}"
CLEANUP="${GFAL_BENCH_CLEANUP:-0}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [--run-id ID] [--file-count N] [--file-size-mb N] [--cleanup]

Benchmark three ways of copying a 100x1 MiB small-file dataset to EOSPilot from lxplus:
  1. rsync to the eosxd FUSE mount
  2. gfal cp -r over HTTPS
  3. gfal cp -r over XRootD

Defaults:
  run id:       ${RUN_ID}
  file count:   ${FILE_COUNT}
  file size MiB:${FILE_SIZE_MB}
  mount path:   ${MOUNT_PATH}
  fuse base:    ${FUSE_BASE}
  EOS base:     ${EOS_BASE}
  local root:   ${LOCAL_ROOT}

Environment overrides:
  GFAL_BENCH_RUN_ID
  GFAL_BENCH_FILE_COUNT
  GFAL_BENCH_FILE_SIZE_MB
  GFAL_BENCH_MOUNT_PATH
  GFAL_BENCH_FUSE_BASE
  GFAL_BENCH_EOS_BASE
  GFAL_BENCH_EOS_HOST
  GFAL_BENCH_LOCAL_ROOT
  GFAL_BENCH_CLEANUP=1

Examples:
  $(basename "$0")
  $(basename "$0") --run-id manual-1 --cleanup
  GFAL_BENCH_FILE_COUNT=500 $(basename "$0")
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-id)
            RUN_ID="$2"
            LOCAL_ROOT="${GFAL_BENCH_LOCAL_ROOT:-/tmp/gfal-eospilot-bench-${RUN_ID}}"
            shift 2
            ;;
        --file-count)
            FILE_COUNT="$2"
            shift 2
            ;;
        --file-size-mb)
            FILE_SIZE_MB="$2"
            shift 2
            ;;
        --cleanup)
            CLEANUP=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: required command not found: $1" >&2
        exit 1
    fi
}

now_seconds() {
    python3 - <<'PY'
import time
print(f"{time.perf_counter():.9f}")
PY
}

format_float() {
    python3 - "$1" <<'PY'
import sys
print(f"{float(sys.argv[1]):.3f}")
PY
}

div_float() {
    python3 - "$1" "$2" <<'PY'
import sys
num = float(sys.argv[1])
den = float(sys.argv[2])
print("0.000" if den == 0 else f"{num / den:.3f}")
PY
}

to_https_url() {
    local eos_path="$1"
    printf 'https://%s//%s' "$EOS_HOST" "${eos_path#/}"
}

to_root_url() {
    local eos_path="$1"
    printf 'root://%s//%s' "$EOS_HOST" "${eos_path#/}"
}

ensure_lxplus() {
    local host
    host="$(hostname -f 2>/dev/null || hostname)"
    if [[ "$host" != lxplus* ]]; then
        echo "warning: this script is intended for lxplus, current host is '$host'" >&2
    fi
}

ensure_proxy_hint() {
    local default_proxy
    default_proxy="/tmp/x509up_u$(id -u)"

    if [[ -n "${X509_USER_PROXY:-}" && -f "${X509_USER_PROXY}" ]]; then
        return
    fi

    if [[ -f "$default_proxy" ]]; then
        export X509_USER_PROXY="$default_proxy"
        return
    fi

    echo "warning: no X509 proxy found in X509_USER_PROXY or $default_proxy" >&2
    echo "warning: gfal HTTPS/XRootD copies may fail unless lxplus auth is already set up" >&2
}

ensure_fuse_mount() {
    mkdir -p "$MOUNT_PATH"
    if mountpoint -q "$MOUNT_PATH"; then
        return
    fi

    echo "Mounting EOSPilot FUSE at $MOUNT_PATH"
    eosxd -ofsname=eospilot.cern.ch:/eos/ "$MOUNT_PATH"

    for _ in $(seq 1 15); do
        if mountpoint -q "$MOUNT_PATH"; then
            return
        fi
        sleep 1
    done

    echo "error: eosxd mount did not become ready at $MOUNT_PATH" >&2
    exit 1
}

create_dataset() {
    local src_dir="$1"
    mkdir -p "$src_dir"

    python3 - "$src_dir" "$FILE_COUNT" "$FILE_SIZE_MB" <<'PY'
from pathlib import Path
import sys

src_dir = Path(sys.argv[1])
file_count = int(sys.argv[2])
file_size_mb = int(sys.argv[3])
file_size = file_size_mb * 1024 * 1024

for index in range(file_count):
    payload = (f"file-{index:04d}\n".encode("ascii") * ((file_size // 10) + 1))[:file_size]
    (src_dir / f"bench-{index:04d}.bin").write_bytes(payload)
PY
}

count_files() {
    find "$1" -maxdepth 1 -type f | wc -l | tr -d ' '
}

run_benchmark() {
    local label="$1"
    local dest_dir="$2"
    shift 2

    local start end duration file_count rate_mib rate_files
    start="$(now_seconds)"
    "$@"
    end="$(now_seconds)"
    duration="$(python3 - "$start" "$end" <<'PY'
import sys
print(f"{float(sys.argv[2]) - float(sys.argv[1]):.9f}")
PY
)"

    file_count="$(count_files "$dest_dir")"
    if [[ "$file_count" != "$FILE_COUNT" ]]; then
        echo "error: ${label} copied ${file_count} files, expected ${FILE_COUNT}" >&2
        exit 1
    fi

    rate_mib="$(div_float "$TOTAL_MIB" "$duration")"
    rate_files="$(div_float "$FILE_COUNT" "$duration")"

    RESULTS_LABELS+=("$label")
    RESULTS_SECONDS+=("$(format_float "$duration")")
    RESULTS_FILES_PER_SEC+=("$rate_files")
    RESULTS_MIB_PER_SEC+=("$rate_mib")
    RESULTS_DESTINATIONS+=("$dest_dir")
}

cleanup_paths() {
    rm -rf "$LOCAL_ROOT" "$FUSE_RUN_ROOT"
}

need_cmd eosxd
need_cmd gfal
need_cmd python3
need_cmd rsync
need_cmd mountpoint

ensure_lxplus
ensure_proxy_hint
ensure_fuse_mount

TOTAL_MIB="$((FILE_COUNT * FILE_SIZE_MB))"
SRC_DIR="${LOCAL_ROOT}/src"
SRC_BASENAME="$(basename "$SRC_DIR")"
FUSE_RUN_ROOT="${FUSE_BASE%/}/bench-${RUN_ID}"
EOS_RUN_ROOT="${EOS_BASE%/}/bench-${RUN_ID}"

RSYNC_PARENT="${FUSE_RUN_ROOT}/rsync"
HTTPS_PARENT="${FUSE_RUN_ROOT}/https"
XROOTD_PARENT="${FUSE_RUN_ROOT}/xrootd"

RSYNC_DEST="${RSYNC_PARENT}/${SRC_BASENAME}"
HTTPS_DEST="${HTTPS_PARENT}/${SRC_BASENAME}"
XROOTD_DEST="${XROOTD_PARENT}/${SRC_BASENAME}"

HTTPS_PARENT_URL="$(to_https_url "${EOS_RUN_ROOT}/https")"
XROOTD_PARENT_URL="$(to_root_url "${EOS_RUN_ROOT}/xrootd")"
SUMMARY_TSV="${LOCAL_ROOT}/summary.tsv"

if [[ -e "$FUSE_RUN_ROOT" || -e "$LOCAL_ROOT" ]]; then
    echo "error: benchmark paths already exist, pick a new --run-id or remove them first" >&2
    echo "  local: $LOCAL_ROOT" >&2
    echo "  remote: $FUSE_RUN_ROOT" >&2
    exit 1
fi

echo "Preparing dataset under $SRC_DIR"
create_dataset "$SRC_DIR"

mkdir -p "$RSYNC_PARENT" "$HTTPS_PARENT" "$XROOTD_PARENT"

echo
echo "Benchmark configuration"
echo "  run id:           $RUN_ID"
echo "  files:            $FILE_COUNT"
echo "  size per file:    ${FILE_SIZE_MB} MiB"
echo "  total payload:    ${TOTAL_MIB} MiB"
echo "  source dir:       $SRC_DIR"
echo "  fuse run root:    $FUSE_RUN_ROOT"
echo "  https parent url: $HTTPS_PARENT_URL"
echo "  xrootd parent url: $XROOTD_PARENT_URL"
echo "  summary file:     $SUMMARY_TSV"

declare -a RESULTS_LABELS=()
declare -a RESULTS_SECONDS=()
declare -a RESULTS_FILES_PER_SEC=()
declare -a RESULTS_MIB_PER_SEC=()
declare -a RESULTS_DESTINATIONS=()

echo
echo "Running rsync -> FUSE"
run_benchmark "rsync-fuse" "$RSYNC_DEST" rsync -a "$SRC_DIR" "${RSYNC_PARENT}/"

echo "Running gfal cp -r -> HTTPS"
run_benchmark "gfal-https" "$HTTPS_DEST" gfal cp -r "$SRC_DIR" "$HTTPS_PARENT_URL"

echo "Running gfal cp -r -> XRootD"
run_benchmark "gfal-xrootd" "$XROOTD_DEST" gfal cp -r "$SRC_DIR" "$XROOTD_PARENT_URL"

echo
printf '%-14s %10s %12s %12s %s\n' "method" "seconds" "files/s" "MiB/s" "destination"
{
    printf 'method\tseconds\tfiles_per_second\tmib_per_second\tdestination\n'
    for index in "${!RESULTS_LABELS[@]}"; do
        printf '%s\t%s\t%s\t%s\t%s\n' \
            "${RESULTS_LABELS[$index]}" \
            "${RESULTS_SECONDS[$index]}" \
            "${RESULTS_FILES_PER_SEC[$index]}" \
            "${RESULTS_MIB_PER_SEC[$index]}" \
            "${RESULTS_DESTINATIONS[$index]}"
    done
} > "$SUMMARY_TSV"

for index in "${!RESULTS_LABELS[@]}"; do
    printf '%-14s %10s %12s %12s %s\n' \
        "${RESULTS_LABELS[$index]}" \
        "${RESULTS_SECONDS[$index]}" \
        "${RESULTS_FILES_PER_SEC[$index]}" \
        "${RESULTS_MIB_PER_SEC[$index]}" \
        "${RESULTS_DESTINATIONS[$index]}"
done

if [[ "$CLEANUP" == "1" ]]; then
    echo
    echo "Cleaning up benchmark directories"
    cleanup_paths
else
    echo
    echo "Left benchmark data in place for inspection:"
    echo "  local:  $LOCAL_ROOT"
    echo "  remote: $FUSE_RUN_ROOT"
    echo "  summary: $SUMMARY_TSV"
fi
