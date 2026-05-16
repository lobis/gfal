# XRootD Native Backend Transition

This transition moved XRootD access behind native XRootD Python bindings and
removed the runtime dependency on `fsspec-xrootd`.

## Current state

The current behavior is:

- `root://` and `xroot://` use XRootD's native fsspec implementation from
  `XRootD.fsspec`.
- `http://` and `https://` use the aiohttp/WebDAV backend by default.
- `fsspec` remains a dependency for local/generic filesystem integration and
  optional non-XRootD protocol backends.
- `fsspec-xrootd` is no longer required.
- During the upstream trial, `pyproject.toml` points at the XRootD PR branch:
  `git+https://github.com/lobis/xrootd.git@codex/native-fsspec-root`.

One opt-in selector exists for HTTPS transition testing:

```bash
GFAL_HTTPS_BACKEND=xrootd
```

`GFAL_HTTPS_BACKEND=xrootd` still uses gfal's small internal adapter because the
current XRootD fsspec PR only registers `root`, `xroot`, `roots`, and `xroots`.
This is intentionally opt-in because XRootD HTTPS support depends on the
installed XRootD client build and available plugins. If the probe reports that
HTTP(S) is not supported, `gfal` falls back to the aiohttp/WebDAV backend and
emits a one-time warning for that URL.

## Implementation notes

The native XRootD fsspec filesystem provides the filesystem surface used by the
rest of gfal:

- `info`
- `ls`
- `open`
- `mkdir` / `makedirs`
- `rm` / `rmdir`
- `mv`
- `chmod`
- `checksum`

This lets `GfalClient` and the CLI keep using `url_to_fs()` while the backend is
migrated incrementally. gfal registers the XRootD fsspec implementation
explicitly before instantiating it, which allows local development wheels to work
even before entry-point metadata is refreshed.

## Dependency tradeoff

Keeping `fsspec` is still useful because it provides the generic filesystem
shape used by local files and optional non-XRootD backends such as S3 and SFTP.
It also keeps the fallback path for protocols that are not implemented directly
by gfal.

Dropping `fsspec-xrootd` removes an extra adapter layer that gfal was already
partially bypassing for metadata, directory listing, rename, and TPC behavior.
The native implementation now provides stat-rich `info()` and `ls()` results, so
gfal no longer needs its extra `_myclient` metadata enrichment or rename
special-case.

The main remaining cost is that `GFAL_HTTPS_BACKEND=xrootd` still depends on
gfal's internal HTTPS-capability probe until XRootD's fsspec implementation
supports HTTPS schemes directly.

## Upstream PR findings

Two packaging/API issues showed up during early testing of
`xrootd/xrootd#2789`:

- Installing from the `python/` subdirectory alone can build the Python bindings
  against an unrelated XRootD client library. The branch now uses the repository
  root direct reference so the Python bindings and bundled client library come
  from the same commit.
- `fsspec.url_to_fs("root://...")` originally passed `protocol` twice because
  fsspec uses the URL scheme as its own argument and the PR's
  `_get_kwargs_from_urls()` also returned `protocol`. The upstream branch now
  returns `xrootd_protocol` for the XRootD-specific constructor argument.

gfal still instantiates `XRootD.fsspec.XRootDFileSystem` directly for `root://`
URLs so the trial does not depend on entry-point ordering when other `root`
implementations are installed.

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
