# gfal-cli

A pip-installable Python rewrite of the [gfal2-util](https://github.com/lobis/gfal2-util) CLI tools, built on [fsspec](https://filesystem-spec.readthedocs.io/). Supports **HTTP/HTTPS** and **XRootD** only (via [fsspec-xrootd](https://github.com/scikit-hep/fsspec-xrootd)).

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
  ls.py         gfal-ls (CommandLs)
  copy.py       gfal-cp / gfal-copy (CommandCopy)
  rm.py         gfal-rm (CommandRm)
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
- HTTP `info()` always returns `type='file'` — it cannot distinguish files from directories (just does a HEAD request). `gfal-ls` therefore always calls `fso.ls()` directly and infers type from the result rather than relying on `info()['type']`.
- HTTP `info()` returns very few fields (no mode, uid, gid, timestamps). `StatInfo` fills in sensible defaults so the rest of the code doesn't need to guard every access.
- For XRootD the `info()` dict contains a `mode` integer; rely on that rather than synthesising it.
- XRootD via `fsspec.filesystem("root")` fails — use `fsspec.url_to_fs(url)` instead so fsspec extracts the `hostid` from the URL and passes it to `XRootDFileSystem.__init__()`.

### HTTP error messages

fsspec/aiohttp raise `ClientResponseError` (not an `OSError`) for HTTP errors. `CommandBase._format_error()` maps HTTP status codes to POSIX-style descriptions (403 → "Permission denied", 404 → "No such file or directory") and also handles fsspec-style `FileNotFoundError` instances that carry no `strerror`.

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

## Common args (every command)

`-v / --verbose`, `-t / --timeout`, `-E / --cert`, `--key`, `--log-file`

These are added automatically by `CommandBase.parse()`. Do not redeclare them in individual commands.

## Running tests

**MANDATORY**: After any code change, run the full test suite before considering the task done:

```bash
.venv/bin/python -m pytest tests/ -x -q
```

Integration tests (require network) are excluded by default; run them separately:

```bash
.venv/bin/python -m pytest tests/ -m integration -q
```

All unit tests must pass. Do not mark a task complete if there are test failures.

## Code style

After making any code change, run ruff on the modified files before considering the task done.

Whenever a **new file** is created inside the package (`src/gfal_cli/`) or tests (`tests/`), immediately run `git add <file>`. Hatchling (the build backend) only packages git-tracked files; untracked files are silently excluded from the wheel, causing `ImportError` at runtime even though the file exists in the working tree.

After making any code change, run ruff on the modified files before considering the task done:

```bash
ruff check <file1> <file2> ...
```

ruff is configured as a pre-commit hook and enforces the `PTH` rule family: always use `pathlib.Path` methods instead of `os.path` equivalents. Key mappings:

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

## Error handling

`CommandBase._executor()` catches all exceptions in the worker thread and maps them to exit codes. The exception's `errno` attribute is used when present; otherwise exit 1. Broken pipe (EPIPE) is silently swallowed. Tracebacks are never printed to the user.

`CommandBase._format_error(e)` converts exceptions to user-friendly strings. It handles three cases: real OS errors (already have `strerror` in `str(e)`), fsspec-style `OSError` subclasses with no `strerror` (appends POSIX description from the type), and aiohttp `ClientResponseError` with an HTTP `status` code (maps to POSIX description).

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
- Are completely read-only (e.g. `--help`, `gfal-stat` on a public file, `gfal-ls` on a public path).
- Cannot leave any persistent side-effects (no writes, no staging requests, no token requests).

Use:
```bash
ssh lxplus.cern.ch '<command>'
```

Never run `gfal-bringonline`, `gfal-archivepoll`, `gfal-evict`, `gfal-token`,
`gfal-cp`, `gfal-rm`, `gfal-mkdir`, `gfal-chmod`, `gfal-save`, `gfal-rename`
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

pytest test suite lives in `tests/`. Run with:

```bash
pytest tests/                                    # all unit tests
pytest tests/ -m integration                     # integration tests (need network)
pytest tests/test_integration_eospublic.py       # EOS public endpoint tests
```

Manual smoke tests against local files:

```bash
echo "hello" > /tmp/test.txt

gfal-stat /tmp/test.txt
gfal-ls -l /tmp/
gfal-cp /tmp/test.txt /tmp/test_copy.txt
gfal-sum /tmp/test.txt ADLER32
gfal-cat /tmp/test.txt
gfal-mkdir /tmp/test_dir
gfal-cp -r /tmp/test_dir /tmp/test_dir2   # recursive copy
gfal-rm -r /tmp/test_dir /tmp/test_dir2
gfal-rm /tmp/test_copy.txt
rm /tmp/test.txt
```

For XRootD and HTTP, substitute `root://` and `https://` URLs respectively. XRootD auth uses the proxy at `$X509_USER_PROXY` or cert/key via `-E`/`--key`.
