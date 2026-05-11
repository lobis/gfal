# XRootD Native Backend Transition

This transition moves XRootD access behind an internal native adapter so the
project can reduce its reliance on `fsspec-xrootd` over time.

## Current state

The default behavior is unchanged:

- `root://` and `xroot://` still use `fsspec-xrootd`.
- `http://` and `https://` still use the aiohttp/WebDAV backend.

Two opt-in selectors exist for transition testing:

```bash
GFAL_XROOTD_BACKEND=native
GFAL_HTTPS_BACKEND=xrootd
```

`GFAL_XROOTD_BACKEND=native` routes `root://` and `xroot://` URLs through the
native adapter in `gfal.core.xrootd_native`.

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

## First smoke-test result

The native `root://` path passes the local XRootD fixture tests with:

```bash
GFAL_XROOTD_BACKEND=native .venv/bin/python -m pytest tests/test_xrootd.py -q
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
