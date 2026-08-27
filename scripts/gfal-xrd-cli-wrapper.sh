#!/usr/bin/env bash
# gfal xrd-cli development wrapper

set -euo pipefail

xrootd_prefix=${GFAL_XROOTD_PREFIX:-/opt/xrootd-xrd-cli}
xrdfs=$xrootd_prefix/usr/bin/xrdfs
xrdcp=$xrootd_prefix/usr/bin/xrdcp

if [[ ! -x $xrdfs || ! -x $xrdcp ]]; then
    echo "gfal: compatible XRootD client not found under $xrootd_prefix" >&2
    exit 127
fi

export GFAL_XRDFS=$xrdfs
export GFAL_XRDCP=$xrdcp
export LD_LIBRARY_PATH="$xrootd_prefix/usr/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

exec /usr/bin/gfal "$@"
