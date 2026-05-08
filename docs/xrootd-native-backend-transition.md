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

`GFAL_HTTPS_BACKEND=xrootd` routes `https://` URLs through the same adapter. This
is intentionally opt-in because XRootD HTTPS support depends on the installed
XRootD client build and available plugins.

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

for both `FileSystem.stat()` and `File.open()`. That means the HTTPS migration
should not be a blind default flip. The next step should add capability detection
and fallback, then compare the resulting traces against gfal2 before enabling
native HTTPS by default.
