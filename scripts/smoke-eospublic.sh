#!/usr/bin/env bash

set -euo pipefail

gfal_command=${GFAL:-gfal}
base_url=root://eospublic.cern.ch//eos/opendata
source_url=$base_url/atlas/documentation/ATLAS-Data-Policy-2021.pdf
smoke_dir=$(mktemp -d /tmp/gfal-smoke.XXXXXX)

cleanup() {
    rm -rf -- "$smoke_dir"
}
trap cleanup EXIT

echo "Testing $($gfal_command --version) against eospublic.cern.ch"

"$gfal_command" stat "$base_url/" >/dev/null
"$gfal_command" ls "$base_url/" | grep -Fx atlas >/dev/null

remote_checksum=$("$gfal_command" sum "$source_url" ADLER32 | awk '{print $NF}')
"$gfal_command" cp -f "$source_url" "file://$smoke_dir/policy.pdf"
local_checksum=$(xrdadler32 "$smoke_dir/policy.pdf" | awk '{print $1}')

if [[ $remote_checksum != "$local_checksum" ]]; then
    echo "Checksum mismatch: remote=$remote_checksum local=$local_checksum" >&2
    exit 1
fi

echo "EOS smoke test passed (Adler-32: $remote_checksum)"
