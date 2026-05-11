# XRootD Native Backend Transition

This transition moved XRootD access behind an internal native adapter and
removed the runtime dependency on `fsspec-xrootd`.

## Current state

The current behavior is:

- `root://` and `xroot://` use the native adapter in
  `gfal.core.xrootd_native`.
- `http://` and `https://` use the aiohttp/WebDAV backend by default.
- `fsspec` remains a dependency for local/generic filesystem integration and
  optional non-XRootD protocol backends.
- `fsspec-xrootd` is no longer required.

One opt-in selector exists for HTTPS transition testing:

```bash
GFAL_HTTPS_BACKEND=xrootd
```

`GFAL_HTTPS_BACKEND=xrootd` first probes `https://` URLs through the same
adapter. This is intentionally opt-in because XRootD HTTPS support depends on
the installed XRootD client build and available plugins. If the probe reports
that HTTP(S) is not supported, `gfal` falls back to the aiohttp/WebDAV backend
and emits a one-time warning for that URL.

## Implementation notes

The native adapter wraps the XRootD Python bindings directly and exposes the
small fsspec-shaped surface used by the rest of gfal:

- `info`
- `ls`
- `open`
- `mkdir` / `makedirs`
- `rm` / `rmdir`
- `mv`
- `chmod`
- `checksum`

This lets `GfalClient` and the CLI keep using `url_to_fs()` while the backend is
migrated incrementally.

## Dependency tradeoff

Keeping `fsspec` is still useful because it provides the generic filesystem
shape used by local files and optional non-XRootD backends such as S3 and SFTP.
It also keeps the fallback path for protocols that are not implemented directly
by gfal.

Dropping `fsspec-xrootd` removes an extra adapter layer that gfal was already
partially bypassing for metadata, directory listing, rename, and TPC behavior.
The native adapter gives gfal direct control over XRootD status handling, path
normalization, and gfal2 compatibility details.

The main cost is that gfal now owns more of the fsspec-shaped XRootD surface:
file objects, directory traversal, checksums, and error mapping. Those paths are
covered by the local XRootD fixture tests and should be kept in sync with future
XRootD binding changes.

## First smoke-test result

The native `root://` path passes the local XRootD fixture tests with:

```bash
.venv/bin/python -m pytest tests/test_xrootd.py -q
```

In the current local environment, direct XRootD Python binding access to
`https://eospublic.cern.ch/...` returns:

```text
[ERROR] Operation not supported
```

for both `FileSystem.stat()` and `File.open()`. The experimental HTTPS selector
therefore performs a capability probe before returning the native filesystem. If
that probe sees the unsupported-protocol error, the command continues through
the existing HTTP/WebDAV backend.

The next step is to compare this fallback behavior against gfal2 traces and
then decide whether `GFAL_HTTPS_BACKEND=auto` should try native HTTPS first for
known-supported deployments.
