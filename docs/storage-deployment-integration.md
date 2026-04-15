# Storage Deployment Integration

This document describes the deployment-backed integration contract used by
`tests/test_integration_storage_deployment.py`.

The goal is to let the same `gfal` integration suite run against:

- EOS deployments that we stand up ourselves in CI
- dCache deployments run by the dCache team in their GitLab/Kubernetes setup
- local or ad-hoc deployment environments

## Why This Contract Exists

The dCache team offered three realistic integration paths for us:

- run our test suite as a container inside their GitLab + Kubernetes CI
- consume their Helm chart in a Kubernetes-backed test deployment
- eventually use their planned docker-compose test environment for local runs

That means our side needs to provide two things:

- a deployment-agnostic test suite
- a container image that can run that suite when a deployment is already available

For EOS we also want our own CI path, so the repo includes an EOS-specific
job in the main CI workflow that follows the same general pattern used in
`eos-tui`: deploy EOS via the official Helm chart on a local `kind` cluster,
expose the storage endpoints, then run the generic deployment suite.

For dCache the repo now also includes a dedicated CI job in the main workflow.
It deploys PostgreSQL, Zookeeper, Kafka, and dCache itself on a local `kind`
cluster, prepares writable and denied paths with `chimera`, and runs the same
generic deployment suite from a dedicated in-cluster test runner pod.

## Environment Contract

`tests/test_integration_storage_deployment.py` is enabled when at least one of
the following is set:

- `GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE`
- `GFAL_DEPLOYMENT_ROOT_WRITABLE_BASE`

Optional variables:

- `GFAL_DEPLOYMENT_NAME`
- `GFAL_DEPLOYMENT_HTTP_DENIED_BASE`
- `GFAL_DEPLOYMENT_ROOT_DENIED_BASE`
- `GFAL_DEPLOYMENT_VERIFY_SSL`
- `GFAL_DEPLOYMENT_CERT`
- `GFAL_DEPLOYMENT_KEY`
- `GFAL_DEPLOYMENT_PROXY`
- `GFAL_DEPLOYMENT_SUPPORTS_LISTING`

The contract expects writable base URLs rather than just hostnames so each
deployment can decide where the allowed and denied paths live.

Examples:

```bash
export GFAL_DEPLOYMENT_NAME=eos-kind
export GFAL_DEPLOYMENT_VERIFY_SSL=0
export GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE=https://127.0.0.1:8443/eos/dev/gfal-tests/run-123/writable
export GFAL_DEPLOYMENT_HTTP_DENIED_BASE=https://127.0.0.1:8443/eos/dev/gfal-tests/run-123/denied
export GFAL_DEPLOYMENT_ROOT_WRITABLE_BASE=root://127.0.0.1:1094//eos/dev/gfal-tests/run-123/writable
export GFAL_DEPLOYMENT_ROOT_DENIED_BASE=root://127.0.0.1:1094//eos/dev/gfal-tests/run-123/denied
```

```bash
export GFAL_DEPLOYMENT_NAME=dcache-k8s
export GFAL_DEPLOYMENT_VERIFY_SSL=0
export GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE=https://dcache-door.example.org:8083/pnfs/example.org/data/gfal-tests/writable
export GFAL_DEPLOYMENT_HTTP_DENIED_BASE=https://dcache-door.example.org:8083/pnfs/example.org/data/gfal-tests/denied
export GFAL_DEPLOYMENT_ROOT_WRITABLE_BASE=root://dcache-door.example.org:1094//pnfs/example.org/data/gfal-tests/writable
export GFAL_DEPLOYMENT_ROOT_DENIED_BASE=root://dcache-door.example.org:1094//pnfs/example.org/data/gfal-tests/denied
export GFAL_DEPLOYMENT_PROXY=/tmp/x509proxy
```

## Generic Test Coverage

The deployment suite currently checks:

- local <-> HTTPS/WebDAV copy round trips
- local <-> XRootD copy round trips
- HTTPS <-> XRootD bridge copies when both protocols are exposed
- remote directory listing after upload
- denied-path write failures on both HTTP and XRootD when configured

This keeps the contract focused on the common EOS/dCache behavior that matters
for `gfal`, without baking in provider-specific path assumptions like
`/eos/...` or `/pnfs/...`.

## Container Runner

The repo includes `docker/Dockerfile.integration-runner`.

Build it with:

```bash
docker build -t gfal-integration-runner -f docker/Dockerfile.integration-runner .
```

Run it by passing the deployment contract as environment variables:

```bash
docker run --rm \
  -e GFAL_DEPLOYMENT_NAME=dcache-k8s \
  -e GFAL_DEPLOYMENT_VERIFY_SSL=0 \
  -e GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE=... \
  -e GFAL_DEPLOYMENT_ROOT_WRITABLE_BASE=... \
  -e GFAL_DEPLOYMENT_PROXY=/tmp/x509proxy \
  -v /tmp/x509proxy:/tmp/x509proxy:ro \
  gfal-integration-runner
```

This is the expected hand-off point for the dCache team’s GitLab/Kubernetes
pipeline.

## EOS Workflow

For EOS we provide `scripts/prepare-eos-kind-deployment.sh`, which assumes:

- an EOS cluster is already running
- the runner has SSH access to the MGM pod
- ports `1094` and `8443` are port-forwarded to `127.0.0.1`

The script:

- creates writable and denied test directories
- sets permissive ACLs on the writable path and stricter ones on the denied path
- prints the `GFAL_DEPLOYMENT_*` environment variables consumed by the generic suite

Example:

```bash
export EOS_TEST_SSH_TARGET=eos-mgm
./scripts/prepare-eos-kind-deployment.sh run-123
```

## dCache Workflow

For dCache we provide `ci/dcache-values.yaml` and
`scripts/prepare-dcache-kind-deployment.sh`.

The CI job:

- deploys the recommended backing services from the dCache Kubernetes how-to
- pins the Zookeeper and Kafka dependency charts to `bitnamilegacy/*` images,
  because the historical default `bitnami/*` tags referenced by those chart
  versions are no longer published on Docker Hub
- opts into those legacy images via the charts' `allowInsecureImages` gate so
  the deployment stays reproducible even after the upstream Bitnami registry
  cleanup
- installs the dCache Helm chart with a minimal door/pool profile
- disables the chart's NFS-based probes and the NFS door itself, since this
  CI job validates WebDAV and XRootD behavior rather than NFS readiness
- enables anonymous WebDAV and XRootD writes for the dedicated test area
- creates writable and denied directories under `/data/gfal-tests/<run-id>`
- runs `tests/test_integration_storage_deployment.py` from an in-cluster pod

The dCache preparation script prints the same `GFAL_DEPLOYMENT_*` contract as
the EOS one, but it first waits for the dCache door pod to become ready so it
fails cleanly on a broken deployment instead of exporting unusable endpoints.
The resulting URLs target the in-cluster dCache service DNS names instead of
localhost port-forwards.

## dCache Collaboration Path

The dCache team indicated the following preferred integration path:

- express the test suite as a container so they can run it in GitLab + Kubernetes
- optionally adapt their Helm chart for our test use case
- later also support a docker-compose-based local setup

The generic deployment suite plus `docker/Dockerfile.integration-runner` is the
artifact intended for that collaboration.
