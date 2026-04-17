#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
P12_INPUT="${GFAL_SERVICE_ACCOUNT_P12:-$ROOT_DIR/gfal_service_account.p12}"
P12_B64_INPUT="${GFAL_SERVICE_ACCOUNT_P12_B64:-$ROOT_DIR/.gfal_ci_secret}"
WORK_DIR="${GFAL_PROXY_TMPDIR:-/tmp}"
P12_PATH="${GFAL_PROXY_P12_PATH:-$WORK_DIR/sa.p12}"
CERT_PATH="${GFAL_PROXY_CERT_PATH:-$WORK_DIR/usercert.pem}"
KEY_PATH="${GFAL_PROXY_KEY_PATH:-$WORK_DIR/userkey.pem}"
PROXY_PATH="${GFAL_PROXY_PATH:-$WORK_DIR/x509proxy}"
VALIDITY="${GFAL_PROXY_VALIDITY:-12:00}"

if [[ -f "$P12_INPUT" ]]; then
  cp "$P12_INPUT" "$P12_PATH"
elif [[ -f "$P12_B64_INPUT" ]]; then
  base64 -d "$P12_B64_INPUT" > "$P12_PATH"
else
  echo "No service-account PKCS#12 source found." >&2
  echo "Checked: $P12_INPUT and $P12_B64_INPUT" >&2
  exit 1
fi

openssl pkcs12 -in "$P12_PATH" -clcerts -nokeys -passin pass: -out "$CERT_PATH"
openssl pkcs12 -in "$P12_PATH" -nocerts -nodes -passin pass: -out "$KEY_PATH"
chmod 600 "$CERT_PATH" "$KEY_PATH"

rm -f "$PROXY_PATH"

if command -v grid-proxy-init >/dev/null 2>&1; then
  grid-proxy-init -cert "$CERT_PATH" -key "$KEY_PATH" -out "$PROXY_PATH" -valid "$VALIDITY" -q
elif command -v xrdgsiproxy >/dev/null 2>&1; then
  xrdgsiproxy init -cert "$CERT_PATH" -key "$KEY_PATH" -f "$PROXY_PATH" -valid "$VALIDITY"
else
  echo "Neither grid-proxy-init nor xrdgsiproxy is installed." >&2
  exit 1
fi

PROXY_EXPIRY="$(openssl x509 -in "$PROXY_PATH" -noout -enddate | cut -d= -f2)"
echo "Generated proxy at $PROXY_PATH"
echo "Proxy expires: $PROXY_EXPIRY"
echo "Export with:"
echo "export X509_USER_PROXY=$PROXY_PATH"
