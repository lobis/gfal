# xrdfs CLI refactor

The `xrdfs` branch is moving the command-line implementation from Python
protocol adapters to the operating system's XRootD client. The public command
surface remains a single executable:

```text
gfal ls
gfal cat
gfal stat
gfal sum
gfal xattr
```

No `gfal-*` executables are installed.

For complete ROOT, HTTP, and DAV URLs, these five commands invoke `xrdfs`
directly and do not import `fsspec`, `aiohttp`, or the Python XRootD bindings.
Local paths, `file://` URLs, unported commands, and the public Python API still
use the previous implementation during the transition. This fallback is
intentionally lazy so it can be removed command-by-command.
Options that only exist in the previous frontend, such as `--no-verify`,
`--authz-token`, and the extended `ls` sorting flags, also select that fallback
until equivalent XRootD client controls are available. Invocations using the
`EOSAUTHZ` or `GFAL_AUTHZ_TOKEN` environment variables take the same path so a
credential is not copied into an `xrdfs` process argument implicitly. Tokens
already present in user-supplied URLs are passed through, but redacted from
labels and diagnostics.

## Client requirement

`xrdfs` is an operating-system dependency; `pip` cannot install it. RPM builds
will depend on `xrootd-client`, and HTTP/WebDAV URLs additionally need the
`xrdcl-http` plugin.

The wrapper currently needs the command-first and JSON metadata interface being
developed in [xrootd pull request #2868](https://github.com/xrootd/xrootd/pull/2868).
It checks the local `xrdfs --help` output before contacting a server and exits
with a clear compatibility error when the required interface is absent.
Consequently, this branch is not ready for PyPI or EPEL publication until those
changes are released or backported into the packaged XRootD client.

For development, select a locally built client explicitly:

```bash
export GFAL_XRDFS=/path/to/xrootd/build/bin/xrdfs
gfal stat root://host.example//path/to/file
```

The supported URL schemes are `root`, `roots`, `xroot`, `xroots`, `http`,
`https`, `dav`, and `davs`. A complete URL, including the authority, is
required.

## Packaging direction

The first migration slice keeps the old Python dependencies because unported
commands and the Python API remain reachable. They must not be bundled into a
future EPEL submission. Once the remaining public surface has either migrated
or been retired, the base package can drop those dependencies atomically and
depend only on Python's standard library plus the packaged XRootD client.
