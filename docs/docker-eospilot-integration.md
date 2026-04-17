# Docker + EOSPilot integration test workflow

This document describes how to run the integration tests safely with the Alma-based Docker image and the EOSPilot service-account proxy.

## Safety rules

- **Only run write/delete integration tests under the EOSPilot tmp scratch path**:
  - `https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp`
  - `root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp`
- Do **not** point mutating tests at any non-`tmp` EOSPilot path.
- Be conservative with cleanup:
  - use unique file/dir names
  - prefer deleting the exact file/dir created by the test
  - avoid broad recursive deletes unless the test created that exact subtree

## Prerequisites

- Docker image built locally:

```bash
docker build --platform linux/amd64 -t xrootd-cern-test -f docker/Dockerfile.xrootd-cern-test .
```

- Python virtualenv with dev dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
```

## Generate a short-lived proxy from `.gfal_ci_secret`

The repo may contain an untracked `.gfal_ci_secret` file holding the base64-encoded `.p12` for the service account.

Generate a short-lived proxy like this:

```bash
rm -f /tmp/sa.p12 /tmp/usercert.pem /tmp/userkey.pem /tmp/gfal-tmp-proxy
base64 -d .gfal_ci_secret > /tmp/sa.p12
openssl pkcs12 -in /tmp/sa.p12 -clcerts -nokeys -passin pass: -out /tmp/usercert.pem
openssl pkcs12 -in /tmp/sa.p12 -nocerts -nodes -passin pass: -out /tmp/userkey.pem
chmod 600 /tmp/userkey.pem

docker run --rm --platform linux/amd64 \
  -v /tmp/usercert.pem:/tmp/usercert.pem:ro \
  -v /tmp/userkey.pem:/tmp/userkey.pem:ro \
  -v /tmp:/proxies \
  xrootd-cern-test \
  sh -lc 'grid-proxy-init -cert /tmp/usercert.pem -key /tmp/userkey.pem -out /proxies/gfal-tmp-proxy -valid 12:00 -q'

export X509_USER_PROXY=/tmp/gfal-tmp-proxy
```

Optional verification:

```bash
openssl x509 -in "$X509_USER_PROXY" -noout -subject -enddate
```

## Run all integration tests with Docker/proxy available

With the proxy exported, run:

```bash
X509_USER_PROXY=/tmp/gfal-tmp-proxy ./.venv/bin/python -m pytest tests -m integration -v --tb=short
```

In the current test layout this gives Docker-backed coverage where needed:
- the compare suite in `tests/test_integration_compare_gfal2.py`
- XRootD Docker fallback paths in `tests/test_integration_eospilot.py` on environments without native GSI setup

## Run only the legacy-vs-new comparison suite

```bash
X509_USER_PROXY=/tmp/gfal-tmp-proxy ./.venv/bin/python -m pytest tests/test_integration_compare_gfal2.py -v --tb=short -rxXs
```

Expected current behavior in the Alma Docker image:
- new `gfal` CLI runs successfully
- legacy `gfal2-utils` are usable for the comparison tests in this image
- the compare suite should exercise both command families directly rather than `xfail`ing

## How the proxy reaches Docker

`tests/helpers.py` already supports this flow.

When `X509_USER_PROXY` points to a real file, Docker-backed helpers mount it into the container as:

- `/tmp/x509proxy`

and set:

- `X509_USER_PROXY=/tmp/x509proxy`

That means exporting `X509_USER_PROXY` in the host shell is enough for:
- `run_gfal_docker(...)`
- `run_gfal2_docker(...)`

## Troubleshooting on `lobis-eos-dev`

For the CERN dev VM, connect with:

```bash
ssh -J lxplus lobis-eos-dev
```

Direct EOS/XRootD access with the service-account proxy is known to work on that
host, but `eosxd` FUSE mounts are currently unreliable.

Observed behavior:
- `eosxd -ofsname=eospilot.cern.ch:/eos/ /root/eos-pilot` can create a mount, but
  the mounted tree may still be inaccessible and resolve as `nobody`.
- `eosxd get eos.identityparent /root/eos-pilot` may report `nobody,secret=` even
  when the same proxy works with `xrdfs` and `eos`.

Important host prerequisite:
- the VM must have a hashed grid CA directory at `/etc/grid-security/certificates`
  or GSI auth can fail before EOS/XRootD clients ever use the proxy
- on `lobis-eos-dev`, building that directory from the system CA bundle fixed
  direct GSI access for commands such as:
  - `X509_USER_PROXY=/tmp/x509up_u0 XrdSecPROTOCOL=gsi xrdfs root://eospilot.cern.ch stat /eos/pilot/opstest/dteam/python3-gfal/tmp`
  - `X509_USER_PROXY=/tmp/x509up_u0 XrdSecPROTOCOL=gsi eos root://eospilot.cern.ch whoami`

Current status:
- direct `xrdfs`/`eos` access with the robot proxy works after the CA-dir fix
- root/shared `eosxd` mounts on this VM still did not bind the X.509 identity
  correctly during investigation, so prefer direct clients over FUSE there until
  the `eosxd` credential-binding path is understood

## Current known-good local result

With `X509_USER_PROXY=/tmp/gfal-tmp-proxy` exported:

```text
the compare suite should run real legacy-vs-new command checks in the Docker image
```
