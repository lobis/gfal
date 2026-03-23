# gfal — Grid File Access Library

> **`AGENTS.md` is a symlink to this file.**  Claude Code reads `CLAUDE.md`;
> other AI coding agents (Codex, Copilot Workspace, Cursor, …) read `AGENTS.md`.
> Edit only `CLAUDE.md` — changes are automatically visible through both names.

**gfal** stands for **Grid File Access Library**. This is a pip-installable **Python-only** rewrite of the [gfal2-util](https://github.com/lobis/gfal2-util) CLI tools — no C library required. Built on [fsspec](https://filesystem-spec.readthedocs.io/). Supports **HTTP/HTTPS** and **XRootD** only (via [fsspec-xrootd](https://github.com/scikit-hep/fsspec-xrootd)).

GitHub: [github.com/lobis/gfal](https://github.com/lobis/gfal)

The original gfal2-util implementation lives in `gfal2-util/` (gitignored, clone separately for reference) and is the reference for CLI compatibility.

## CLI compatibility reference

**[`docs/gfal2-util-help-reference.md`](docs/gfal2-util-help-reference.md)** is the canonical
reference for this project.  It contains the full `--help` output for every command captured
from a live `lxplus.cern.ch` node.  **Always consult this file** when implementing or auditing
CLI flags — do not guess flag names or behaviours from memory.

The reference document also contains a summary table at the bottom showing which flags are:
- ✅ fully supported
- ⚠️ accepted but ignored (with a warning)
- documented as intentionally omitted (gfal2/GridFTP-specific)

## Development environment

A virtualenv lives at `.venv/` in the project root. Always use it:

```bash
source .venv/bin/activate   # macOS/Linux
# or directly:
.venv/bin/python -m pytest tests/
.venv/bin/pip install -e .
```

Never use the system `python` / `python3` / `pip` for this project. Always activate the venv or call `.venv/bin/python` explicitly.

**IMPORTANT — editable install:** The package must be installed with `pip install -e .` (editable). A non-editable install caches a snapshot in site-packages; source changes are silently ignored and tests run against stale code. After any change, if unsure whether the install is editable, re-run `.venv/bin/pip install -e .`.

To verify the install is editable (source changes are picked up), check that the module file points into `src/`:

```bash
.venv/bin/python -c "from gfal_cli import commands; print(commands.__file__)"
# Good: /path/to/gfal-cli/src/gfal_cli/commands.py
# Bad:  /path/to/gfal-cli/.venv/lib/pythonX.Y/site-packages/gfal_cli/commands.py
```

## Installation

```bash
pip install -e .
```

This registers all `gfal-*` executables as console scripts. Reinstall after changes to `pyproject.toml` (new entry points). Source edits in `src/` are picked up immediately without reinstalling.

## Project layout

```
src/gfal_cli/
  shell.py      Entry point + dispatcher (all executables share this)
  base.py       CommandBase class, @arg decorator, surl() type, common args
  fs.py         fsspec integration: url_to_fs(), StatInfo wrapper, helpers
  commands.py   mkdir, save, cat, stat, rename, chmod, sum, xattr
  ls.py         gfal ls (CommandLs)
  copy.py       gfal cp (CommandCopy)
  rm.py         gfal rm (CommandRm)
  tpc.py        Third-party copy backends (HTTP WebDAV COPY, XRootD CopyProcess)
  utils.py      file_type_str(), file_mode_str() — pure helpers, no fsspec
  progress.py   Terminal progress bar for copy operations
```

## How dispatch works

Every `gfal-*` executable calls the same `shell.main()`. It reads `sys.argv[0]`, strips the `gfal-` prefix, resolves any aliases (`cp` → `copy`), then finds the `CommandBase` subclass that has an `execute_<cmd>` method. That method is decorated with `@arg(...)` to declare its argparse arguments.

To add a new command:
1. Add an `execute_<name>(self)` method to an existing or new `CommandBase` subclass.
2. Add a `gfal-<name> = "gfal_cli.shell:main"` entry point in `pyproject.toml` and reinstall.
3. Import the module in `shell.py` so the subclass is registered.

## fsspec integration (`fs.py`)

- `url_to_fs(url, storage_options)` — normalises URLs (bare paths → `file://`, `dav://` → `http://`), returns `(AbstractFileSystem, path)`.
- `StatInfo(info_dict)` — wraps an fsspec `info()` dict into a POSIX stat-like object. Synthesises `st_mode` when the filesystem doesn't provide one (e.g. HTTP returns no mode, uid, gid).
- `build_storage_options(params)` — extracts `client_cert`/`client_key` from parsed CLI params for HTTP auth. XRootD auth is handled via `X509_USER_*` environment variables (set in `base.py:execute()`).

### Known fsspec quirks

- `LocalFileSystem.mkdir(path)` raises `FileExistsError` unconditionally if the path exists, even when `create_parents=True`. Use `makedirs(path, exist_ok=True)` for the `-p` flag — already handled in `execute_mkdir`.
- HTTP directory listing, mkdir, and rm now work via WebDAV (`webdav.py`). `ls()` sends `PROPFIND` Depth:1 and parses the `DAV:` XML response. Servers that don't support WebDAV will return 405; `info()` falls back to a plain HEAD request for compatibility with non-WebDAV HTTP servers.
- HTTP `info()` returns very few fields (no mode, uid, gid, timestamps). `StatInfo` fills in sensible defaults so the rest of the code doesn't need to guard every access.
- For XRootD the `info()` dict contains a `mode` integer; rely on that rather than synthesising it.
- XRootD via `fsspec.filesystem("root")` fails — use `fsspec.url_to_fs(url)` instead so fsspec extracts the `hostid` from the URL and passes it to `XRootDFileSystem.__init__()`.
- XRootD URL paths use **double-slash** for absolute paths: `root://host//abs/path`. A single slash (`root://host/path`) is treated as a relative path and rejected by servers configured with `oss.localroot`.
- `XRootDFileSystem.mv()` is inherited from `AbstractFileSystem` and calls `copy()` + `rm()`. `_cp_file` raises `NotImplementedError`, producing a silent empty error (`str(NotImplementedError()) == ""`). Always use `fso._myclient.mv(src_path, dst_path)` for XRootD renames (already done in `execute_rename`).
- `XRootDFileSystem.ls(file_path)` raises `OSError("not a directory")` when called on a file path instead of returning a single-entry list. Already handled in `ls.py` with a try/except fallback to `[info]`.
- `XRootDFileSystem._myclient` is an `XRootD.client.FileSystem` instance. Its `mv(src, dst)` returns `(XRootDStatus, None)`. Check `status.ok` for success; `status.errno` is 0 even on failure for some errors — use `status.message` for the human-readable description.

### HTTP error messages

fsspec/aiohttp raise `ClientResponseError` (not an `OSError`) for HTTP errors. `CommandBase._format_error()` maps HTTP status codes to POSIX-style descriptions (403 → "Permission denied", 404 → "No such file or directory") and also handles fsspec-style `FileNotFoundError` instances that carry no `strerror`.

### HTTP/WebDAV layer (`webdav.py`)

`url_to_fs()` for `http://` and `https://` (including `dav://`/`davs://`) returns a
`WebDAVFileSystem` (from `webdav.py`) instead of fsspec's plain `HTTPFileSystem`.
`WebDAVFileSystem` adds:
- `ls()` — WebDAV `PROPFIND` Depth:1 (parses `DAV:` XML response)
- `info()` — `PROPFIND` Depth:0 with HEAD fallback for non-WebDAV servers
- `mkdir()` / `makedirs()` — WebDAV `MKCOL`
- `rm()` / `rmdir()` — HTTP `DELETE`
- `mv()` — WebDAV `MOVE`
- `open()` — delegated to fsspec's `HTTPFileSystem` (GET/PUT unchanged)
- `chmod()` — no-op (HTTP has no permission model)

Tests for `WebDAVFileSystem` live in `tests/test_webdav.py` and use an in-process
mock WebDAV server (no external network needed).

### EOS HTTPS endpoint (eospublic.cern.ch:8444)

- File stat/cat/copy works via HTTPS.
- Directory listing returns **403 Forbidden** — EOS does not support HTTP directory listing. Use XRootD (`root://`) for directory operations.
- The server uses the CERN Root CA 2 certificate. Without it installed locally, all HTTPS requests fail with `SSLCertVerificationError`. Use `--no-verify` to skip, or install the CA:
  ```bash
  # macOS
  curl -O https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt
  openssl x509 -inform DER -in "CERN Root Certification Authority 2.crt" -out /tmp/cern-root-ca-2.pem
  sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain /tmp/cern-root-ca-2.pem
  ```

### XRootD on macOS (pip-installed)

The `xrootd` pip package embeds Linux-style `$ORIGIN` RPATHs in its `.dylib` files. macOS dyld does not expand `$ORIGIN`, so the XRootD security plugins (GSI, kerberos, …) fail to load with "Could not load authentication handler" unless the `pyxrootd` directory is in `DYLD_LIBRARY_PATH` at process startup.

**This is handled automatically.** `shell.main()` calls `_ensure_xrootd_dylib_path()` which re-execs the process with `DYLD_LIBRARY_PATH` set before any XRootD code loads. The re-exec only happens when invoked as a real binary on disk (`os.path.isfile(sys.argv[0])`) to avoid interfering with tests or `-c` invocations. Linux is unaffected.

### X509 proxy auto-detection

If `X509_USER_PROXY` is not set and no `--cert` flag is given, `base.py:execute()` automatically looks for a proxy at `/tmp/x509up_u<uid>` (the standard location written by `voms-proxy-init`). No environment setup needed for typical CERN workflows.

## Packaging

- `pyproject.toml` is the **source of truth** for versioning and dependencies.
- `Makefile`: Use `make dist` to build Python distributions and `make rpm` for RPMs (requires Linux/`rpmbuild`).
- `gfal-cli.spec`: RPM spec file (project PyPI name is now `gfal`).
  - > [!IMPORTANT]
  - > Always keep the `Requires:` and `BuildRequires:` in the `.spec` file in sync with the `dependencies` in `pyproject.toml`.
- **GitHub Actions**: The `publish.yml` workflow automatically builds and publishes PyPI packages, RPMs, and DEBs to both PyPI and GitHub Releases when a new `v*` tag is pushed.

### DEB packaging (Ubuntu/Debian)

The DEB is built in `.github/workflows/ci.yml`. It uses `pip install --target` to bundle dependencies into `/usr/lib/python3/dist-packages/`. This works but **will break whenever a new bundled package conflicts with a system `python3-*` package**.

**Symptom:** `dpkg: error processing archive ... trying to overwrite '/usr/lib/python3/dist-packages/<pkg>/__init__.py', which is also in package python3-<pkg>`

**Fix — two steps, both required:**

1. **Add the conflicting package to `Depends:`** in the `control` block so `apt` installs the system version.
2. **Prune the bundled copy** with `rm -rf` immediately after the `pip install` step — before `dpkg-deb --build`.

**Currently pruned packages** (system packages that must not be re-bundled):

| Pruned path pattern | Ubuntu system package |
|--------------------|-----------------------|
| `rich*` | `python3-rich` |
| `markdown_it*` | `python3-markdown-it` |
| `mdurl*` | `python3-mdurl` |
| `mdit_py_plugins*` | (dep of markdown-it, no separate deb) |
| `pygments*` | `python3-pygments` |
| `click*` | `python3-click` |

When adding a new Python dependency to `pyproject.toml`, check whether Ubuntu 24.04 already ships it as a `python3-<pkg>` package. If it does, add it to the prune list and `Depends` in `ci.yml`. To check:
```bash
docker run --rm ubuntu:24.04 apt-cache show python3-<pkg> 2>/dev/null | grep Version
```

## Common args (every command)

`-v / --verbose`, `-t / --timeout`, `-E / --cert`, `--key`, `--log-file`

These are added automatically by `CommandBase.parse()`. Do not redeclare them in individual commands.

## After every code change — mandatory checklist

Before considering any task done, **both** of the following must pass:

```bash
# 1. Lint, format, and spell-check
.venv/bin/pre-commit run --files <changed files>

# 2. Full unit test suite
.venv/bin/python -m pytest tests/ -x -q
```

Integration tests (require network) are excluded by default; run them separately when relevant:

```bash
.venv/bin/python -m pytest tests/ -m integration -q
```

Do not mark a task complete if either pre-commit or pytest reports failures.

## Code style

After making any code change, run pre-commit on the modified files before considering the task done:

```bash
.venv/bin/pre-commit run --files <file1> <file2> ...
```

pre-commit runs (in order): trailing-whitespace, end-of-file-fixer, YAML/TOML checks, debug-statement detection, **ruff** (lint + auto-fix), **ruff-format**, and **codespell**. Running it directly catches everything the CI gate checks, including spelling mistakes.

To run against every file at once (e.g. after a large refactor):

```bash
.venv/bin/pre-commit run --all-files
```

Whenever a **new file** is created inside the package (`src/gfal_cli/`) or tests (`tests/`), immediately run `git add <file>`. Hatchling (the build backend) only packages git-tracked files; untracked files are silently excluded from the wheel, causing `ImportError` at runtime even though the file exists in the working tree.

The ruff configuration enforces the `PTH` rule family: always use `pathlib.Path` methods instead of `os.path` equivalents. Key mappings:

| `os.path` | `Path` equivalent |
|-----------|-------------------|
| `os.path.exists(p)` | `Path(p).exists()` |
| `os.path.isfile(p)` | `Path(p).is_file()` |
| `os.path.isdir(p)` | `Path(p).is_dir()` |
| `os.path.dirname(p)` | `Path(p).parent` |
| `os.path.basename(p)` | `Path(p).name` |
| `os.path.join(a, b)` | `Path(a) / b` |
| `os.listdir(p)` | `list(Path(p).iterdir())` |

When a `str` is required (e.g. for `os.environ`, `ctypes.CDLL`, or third-party APIs that don't accept `Path`), use `str(Path(...))` or call `.parent` then `str()`.

### Exception handling (SIM105)

When intentionally ignoring exceptions, always use `contextlib.suppress(...)` instead of `try-except-pass` blocks for better readability and to satisfy the `SIM105` lint rule.

```python
import contextlib

with contextlib.suppress(ValueError, OSError):
    # This is preferred over try-except-pass
    do_something()
```

## Error handling

`CommandBase._executor()` catches all exceptions in the worker thread and maps them to exit codes. The exception's `errno` attribute is used when present; otherwise exit 1. Broken pipe (EPIPE) is silently swallowed. Tracebacks are never printed to the user.

`CommandBase._format_error(e)` converts exceptions to user-friendly strings. It handles three cases: real OS errors (already have `strerror` in `str(e)`), fsspec-style `OSError` subclasses with no `strerror` (appends POSIX description from the type), and aiohttp `ClientResponseError` with an HTTP `status` code (maps to POSIX description).

**Debugging tip:** If `gfal-<cmd>:` shows an empty error message, the exception is likely `NotImplementedError` or another exception type whose `str()` is `""`. These are easy to miss because the output looks like a blank error rather than a crash.

## Third-party copy (`tpc.py`)

`gfal-cp` supports TPC via `--tpc` (attempt TPC, fall back to streaming) and `--tpc-only` (require TPC). The dispatch in `copy.py:_do_copy` calls `tpc.do_tpc()` before falling through to `_copy_file`.

**HTTP/HTTPS TPC** — WebDAV `COPY` method:
- `--tpc-mode pull` (default): client sends `COPY <dst>` with `Source: <src>` — destination pulls.
- `--tpc-mode push`: client sends `COPY <src>` with `Destination: <dst>` — source pushes.
- Server may respond `202 Accepted` and stream WLCG performance markers; `_parse_tpc_body` reads them until `success:` / `failure:`.
- `--scitag N`: appended as `SciTag: N` header (WLCG network monitoring).
- `NotImplementedError` is raised on HTTP 405/501 so the caller can fall back.

**XRootD TPC** — `root://` to `root://` only, via pyxrootd `CopyProcess(thirdparty=True, force=True)`. Raises `NotImplementedError` when pyxrootd is not installed.

**Fallback logic**: `NotImplementedError` from `do_tpc` is caught in `_do_copy`; unless `--tpc-only` was set the copy continues with client-side streaming. Any other exception propagates as a real error.

## SSH / remote command policy

The reference system for gfal2-util CLI compatibility is `lxplus.cern.ch`.

**Always prompt the user before running any SSH command** (even read-only ones). The
canonical help output for every command has already been captured in
[`docs/gfal2-util-help-reference.md`](docs/gfal2-util-help-reference.md) —
**consult that file first** before connecting live.

When a live connection is genuinely needed (e.g. to observe actual output format,
not just flags), only run commands that:
- Are completely read-only (e.g. `--help`, `gfal stat` on a public file, `gfal ls` on a public path).
- Cannot leave any persistent side-effects (no writes, no staging requests, no token requests).

Use:
```bash
ssh lxplus.cern.ch '<command>'
```

Never run `gfal bringonline`, `gfal archivepoll`, `gfal evict`, `gfal token`,
`gfal cp`, `gfal rm`, `gfal mkdir`, `gfal chmod`, `gfal save`, `gfal rename`
without explicit user confirmation, even with `--dry-run`.

## Intentionally omitted / stubbed

The following are not functionally implemented (require native gfal2 C library or
are protocol-specific), but their **CLI interface is fully preserved** for backwards
compatibility.  Each stub prints a clear "not supported" message and exits 1.

| Command / flag | Reason | Status |
|----------------|--------|--------|
| `gfal-bringonline` | Requires gfal2 tape/SRM support | CLI stub in `tape.py` |
| `gfal-archivepoll` | Requires gfal2 tape/SRM support | CLI stub in `tape.py` |
| `gfal-evict` | Requires gfal2 tape/SRM support | CLI stub in `tape.py` |
| `gfal-token` | Requires gfal2 macaroon/token support | CLI stub in `tape.py` |
| `gfal-legacy-*` | Legacy LFC commands; no active users | Not implemented |
| `-D`/`--definition` | gfal2 parameter override; no gfal2 | Accepted, ignored (common args) |
| `-C`/`--client-info` | gfal2 client metadata; no gfal2 | Accepted, ignored (common args) |
| `-4`/`-6` | IPv4/IPv6 preference (GridFTP only) | Accepted, ignored (common args) |
| `-n`/`--nbstreams` | Parallel streams (GridFTP only) | Accepted, warned+ignored (`copy.py`) |
| `--tcp-buffersize` | TCP buffer tuning (GridFTP only) | Accepted, warned+ignored (`copy.py`) |
| `-s`/`--src-spacetoken` | SRM space tokens | Accepted, warned+ignored (`copy.py`) |
| `-S`/`--dst-spacetoken` | SRM space tokens | Accepted, warned+ignored (`copy.py`) |
| `--evict` (copy flag) | Post-transfer source eviction | Accepted, no-op (`copy.py`) |
| `--no-delegation` | Disable proxy delegation (TPC) | Accepted, no-op (`copy.py`) |
| `--disable-cleanup` | Keep partial dst on failure | Accepted, no-op (`copy.py`) |

## Testing

### Cross-platform path handling

CI runs on Linux, macOS, **and Windows**. Never hardcode forward-slash path
comparisons (e.g. `assert "/tmp" in str(path)`) — on Windows `Path("/tmp")`
becomes `D:\tmp`. Use `Path` objects or `PurePosixPath` for comparisons, or
compare individual path components via `.parts` / `.name`. Common patterns:

```python
# BAD — breaks on Windows:
assert "/tmp" in str(tree.path)

# GOOD — platform-independent:
assert Path(str(tree.path)).parts[-1] == "tmp"
```

Similarly, never use `str | None` type unions in test files — use
`Optional[str]` from `typing` instead, since CI still tests Python 3.9
where PEP 604 unions are not supported at runtime.

### Windows subprocess encoding

CI also runs on Windows. When using `subprocess.run(..., text=True)`, always
specify `encoding="utf-8"` explicitly — otherwise Python uses the system
default (cp1252 on Windows) which cannot decode rich-click's Unicode box-drawing
characters. Failure to do so causes stdout to be `None` and subsequent string
operations to crash with `TypeError`.

```python
# BAD — breaks on Windows when output contains Unicode:
proc = subprocess.run([...], capture_output=True, text=True)

# GOOD — works cross-platform:
proc = subprocess.run([...], capture_output=True, text=True, encoding="utf-8")

# For rich/help output that may contain unusual characters, also add errors="replace":
proc = subprocess.run([...], capture_output=True, text=True, encoding="utf-8", errors="replace")
```

pytest test suite lives in `tests/`. Run with:

```bash
pytest tests/                                    # all unit tests
pytest tests/ -m integration                     # integration tests (need network)
pytest tests/ -m xrootd                          # XRootD server tests (auto-skip if xrootd not installed)
pytest tests/test_integration_eospublic.py       # EOS public endpoint tests
```

### XRootD integration tests (`tests/test_xrootd.py`)

The `xrootd_server` fixture (in `conftest.py`) starts a real local XRootD daemon automatically. Tests are skipped if the `xrootd` binary or `fsspec-xrootd` package is not installed.

The fixture serves a temp directory over `root://` and optionally `https://` (XrdHttp plugin, requires `openssl`). Minimal server config used:

```
xrd.port <PORT>
oss.localroot <data_dir>
xrd.protocol xrootd *
xrootd.export /
sec.protbind * none      # no authentication (test only)
```

For XrdHttp (HTTPS): `xrd.protocol http:<PORT> /opt/homebrew/lib/libXrdHttp-5.so` — the plugin path is macOS/Homebrew-specific.

The `helpers.py` `_subprocess_env()` sets `DYLD_LIBRARY_PATH` to the `pyxrootd` directory for macOS, since test subprocesses run as `python -c "..."` (not real binaries) and bypass the re-exec guard in `shell.main()`.

Manual smoke tests against local files:

```bash
echo "hello" > /tmp/test.txt

gfal stat /tmp/test.txt
gfal ls -l /tmp/
gfal cp /tmp/test.txt /tmp/test_copy.txt
gfal sum /tmp/test.txt ADLER32
gfal cat /tmp/test.txt
gfal mkdir /tmp/test_dir
gfal cp -r /tmp/test_dir /tmp/test_dir2   # recursive copy
gfal rm -r /tmp/test_dir /tmp/test_dir2
gfal rm /tmp/test_copy.txt
rm /tmp/test.txt
```

For XRootD and HTTP, substitute `root://` and `https://` URLs respectively. XRootD auth uses the proxy at `$X509_USER_PROXY` or cert/key via `-E`/`--key`.
