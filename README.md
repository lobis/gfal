# gfal

[![CI](https://github.com/lobis/gfal/actions/workflows/ci.yml/badge.svg)](https://github.com/lobis/gfal/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/lobis/gfal/branch/main/graph/badge.svg)](https://codecov.io/gh/lobis/gfal)
[![PyPI](https://img.shields.io/pypi/v/gfal)](https://pypi.org/project/gfal/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/gfal)](https://pypi.org/project/gfal/)
[![Conda](https://img.shields.io/conda/vn/lobis/gfal)](https://anaconda.org/lobis/gfal)
[![Python](https://img.shields.io/pypi/pyversions/gfal)](https://pypi.org/project/gfal/)
[![License](https://img.shields.io/pypi/l/gfal)](https://github.com/lobis/gfal/blob/main/LICENSE)
[![Lines of code](https://img.shields.io/badge/lines%20of%20code-7.4k-blue)](https://github.com/lobis/gfal/tree/main/src/gfal)
[![Lines of tests](https://img.shields.io/badge/lines%20of%20tests-22k-blue)](https://github.com/lobis/gfal/tree/main/tests)
[![Tests](https://img.shields.io/badge/tests-1500%2B-green)](https://github.com/lobis/gfal/tree/main/tests)
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

**Grid File Access Library — Python rewrite of gfal2**

**Documentation: [lobis.github.io/gfal](https://lobis.github.io/gfal/)**

A pip-installable **Python-only** rewrite of the [gfal2-util](https://github.com/lobis/gfal2-util) CLI tools, built on [fsspec](https://filesystem-spec.readthedocs.io/) — no C library required. Supports **HTTP/HTTPS** out of the box, with **XRootD** support via [fsspec-xrootd](https://github.com/scikit-hep/fsspec-xrootd) when XRootD bindings are available.

`gfal` is both a **Python library** (sync + async) and a **command-line tool**. Use it to stat, list, copy, checksum, and manage files on local, HTTP/WebDAV, and XRootD storage from Python or the terminal.

## Installation

```bash
pip install gfal
```

This installs the CLI and the Python library with local-file and HTTP/HTTPS support. For XRootD (`root://`) support, you additionally need XRootD bindings:

```bash
# Conda (recommended)
conda install -c conda-forge xrootd

# Or install the full bundle from the lobis channel
conda install -c lobis -c conda-forge gfal
```

See the [installation docs](https://lobis.github.io/gfal/installation/) for RPM packages, native repositories, and CERN CA certificate setup.

## Python library

`import gfal` gives you an **async-first** client and a **synchronous** wrapper — same methods, same API:

### Synchronous

```python
import gfal

client = gfal.GfalClient()

# Stat
info = client.stat("/tmp/data.txt")
print(f"size={info.size}, is_file={info.is_file()}")

# List a directory
for entry in client.ls("/tmp/mydir"):
    print(f"{entry.size:>10}  {entry.info['name']}")

# Copy with checksum verification
client.copy(
    "https://example.com/data.root",
    "file:///tmp/data.root",
    options=gfal.CopyOptions(
        overwrite=True,
        checksum=gfal.ChecksumPolicy("ADLER32"),
    ),
)

# Checksum
print(client.checksum("/tmp/data.root", "MD5"))
```

### Asynchronous

```python
import asyncio
import gfal


async def main():
    client = gfal.AsyncGfalClient()

    info = await client.stat("/tmp/data.txt")
    print(f"size={info.size}")

    entries = await client.ls("/tmp/mydir")
    for entry in entries:
        print(entry.info["name"])

    await client.copy(
        "root://server//eos/data/file.root",
        "file:///tmp/file.root",
    )


asyncio.run(main())
```

### Key features

| Method | Description |
|--------|-------------|
| `stat(url)` | POSIX-style metadata (`StatResult`) |
| `exists(url)` | Check existence |
| `ls(url)` | List directory (returns `StatResult` list or names) |
| `copy(src, dst)` | Copy files/directories with optional checksum, TPC, dry-run |
| `start_copy(src, dst)` | Background copy returning a `TransferHandle` |
| `checksum(url, algo)` | Compute `ADLER32`, `MD5`, `SHA256`, etc. |
| `open(url, mode)` | Open remote file for reading/writing |
| `mkdir(url)` | Create directories (with `parents=True` for `-p`) |
| `rm(url)` | Remove files/directories |
| `rename(src, dst)` | Rename within same filesystem |
| `chmod(url, mode)` | Change permissions |
| `getxattr` / `setxattr` / `listxattr` / `xattrs` | Extended attributes |

### Error handling

All operations raise typed exceptions inheriting from `GfalError` (`OSError`):

```python
try:
    client.stat("file:///nonexistent")
except gfal.GfalFileNotFoundError:
    print("File not found")
except gfal.GfalPermissionError:
    print("Permission denied")
except gfal.GfalError as e:
    print(f"Error (errno={e.errno}): {e}")
```

Available exceptions: `GfalFileNotFoundError`, `GfalPermissionError`, `GfalFileExistsError`, `GfalNotADirectoryError`, `GfalIsADirectoryError`, `GfalTimeoutError`.

For the full Python API reference, see the [Python API documentation](https://lobis.github.io/gfal/python-api/).

## CLI quick start

After installation, the `gfal` command is available on your `PATH`:

```bash
# Stat a remote file (HTTPS)
gfal stat https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C

# List a directory (XRootD)
gfal ls -l root://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/

# Compute a checksum
gfal sum https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C MD5

# Download a file
gfal cp https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C file:///tmp/single_cluster_r5.C

# Peek at the contents
gfal cat https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C | head -n 5
```

Local paths work as bare paths or `file://` URIs. See [EOS public examples](docs/eospublic-examples.md) for more.

## CLI reference

### Commands

| Command | Description |
|---------|-------------|
| `gfal ls` | List directory contents |
| `gfal cp` | Copy files or directories |
| `gfal rm` | Remove files or directories |
| `gfal stat` | Display file status |
| `gfal mkdir` | Create directories |
| `gfal cat` | Print file contents to stdout |
| `gfal save` | Write stdin to a remote file |
| `gfal rename` | Rename / move a file |
| `gfal chmod` | Change file permissions |
| `gfal sum` | Compute file checksums |
| `gfal xattr` | Get or set extended attributes |
| `gfal completion` | Generate shell completions (bash, zsh, fish) |

### Common options (all commands)

| Option | Description |
|--------|-------------|
| `-v` / `--verbose` | Verbose output (stackable: `-vv`, `-vvv`) |
| `-t N` / `--timeout N` | Global timeout in seconds (default: 1800) |
| `-E CERT` / `--cert CERT` | Path to client certificate (PEM) |
| `--key KEY` | Path to client key (PEM) |
| `--no-verify` | Disable TLS certificate verification |
| `--log-file FILE` | Write log output to a file |

### `gfal ls`

```bash
gfal ls [OPTIONS] URI [URI ...]
```

| Option | Description |
|--------|-------------|
| `-l` | Long listing (permissions, size, date) |
| `-a` / `--all` | Show hidden files |
| `-d` / `--directory` | List the entry itself, not its contents |
| `-H` / `--human-readable` | Human-readable sizes (e.g. `1.2M`) |
| `-r` / `--reverse` | Reverse sort order |
| `--sort` | Sort by: `name` (default), `size`, `time`, `extension`, `version`, `none` |
| `-S` | Sort by size (shorthand for `--sort=size`) |
| `--time-style` | Timestamp format: `locale`, `iso`, `long-iso`, `full-iso` |
| `--full-time` | Equivalent to `--time-style=full-iso` |
| `--color` | Colorise output: `auto` (default), `always`, `never` |
| `--xattr ATTR` | Show an extended attribute column |

```bash
gfal ls -lH root://server//eos/data/
gfal ls -la /tmp/mydir/
gfal ls -l --sort=size --reverse root://server//eos/data/
```

### `gfal cp`

```bash
gfal cp [OPTIONS] SRC [SRC ...] DST
```

| Option | Description |
|--------|-------------|
| `-f` / `--force` | Overwrite destination if it exists |
| `-r` / `-R` / `--recursive` | Copy directories recursively |
| `-p` / `--parent` | Create parent directories at destination |
| `-K ALG` / `--checksum ALG` | Verify checksum (`ADLER32`, `MD5`, `SHA256`, …); use `ALG:value` to supply expected hash |
| `--checksum-mode` | `both` (default), `source`, `target` |
| `--compare` | Skip if destination matches: `size`, `size_mtime`, `checksum`, `none` |
| `--parallel N` | Concurrent transfers during recursive copy |
| `--preserve-times` | Preserve modification timestamps (default) |
| `--no-preserve-times` | Don't preserve timestamps |
| `--dry-run` | Show what would be copied without copying |
| `--from-file FILE` | Read source URIs from a file (one per line) |
| `--abort-on-failure` | Stop after the first failed transfer |
| `-T N` / `--transfer-timeout N` | Per-file timeout in seconds |
| `--tpc` | Attempt third-party copy, fall back to streaming |
| `--tpc-only` | Require third-party copy (fail if unsupported) |
| `--tpc-mode` | TPC direction: `pull` (default) or `push` |
| `--scitag N` | WLCG SciTag flow identifier (65–65535) |

```bash
# Simple copy
gfal cp file:///tmp/src.txt https://server/dst.txt

# Force overwrite with ADLER32 verification
gfal cp -f -K ADLER32 root://server//path/file.root file:///tmp/file.root

# Recursive copy, create parents, with parallel transfers
gfal cp -r -p --parallel 4 root://server//eos/srcdir/ file:///tmp/dstdir/

# Skip if destination already has same size
gfal cp --compare size root://server//eos/data/ file:///tmp/data/

# Third-party copy between two servers
gfal cp --tpc root://src-server//path/file root://dst-server//path/file

# Dry-run preview
gfal cp -r --dry-run root://server//eos/data/ file:///tmp/backup/
```

### `gfal rm`

```bash
gfal rm [OPTIONS] URI [URI ...]
```

| Option | Description |
|--------|-------------|
| `-r` / `-R` / `--recursive` | Remove directories and their contents |
| `--dry-run` | Show what would be deleted |
| `--from-file FILE` | Read URIs to delete from a file |
| `--just-delete` | Skip the stat check and delete directly |

```bash
gfal rm file:///tmp/old.txt
gfal rm -r root://server//eos/old_dir/
gfal rm --dry-run root://server//eos/dir/
```

### `gfal stat`

```bash
gfal stat URI [URI ...]
```

Prints POSIX-style stat information (size, permissions, timestamps):

```
  File: 'root://server//eos/data/file.root'
  Size: 1048576        regular file
Access: (0644/-rw-r--r--)      Uid: 1000   Gid: 1000
Access: 2025-06-01 12:34:56.000000
Modify: 2025-06-01 12:34:56.000000
Change: 2025-06-01 12:34:56.000000
```

### `gfal mkdir`

```bash
gfal mkdir [OPTIONS] URI [URI ...]
```

| Option | Description |
|--------|-------------|
| `-p` / `--parents` | Create intermediate directories; no error if exists |
| `-m MODE` | Permissions in octal (default: `755`) |

```bash
gfal mkdir root://server//eos/user/j/jdoe/newdir
gfal mkdir -p root://server//eos/user/j/jdoe/a/b/c
```

### `gfal sum`

```bash
gfal sum URI ALGORITHM
```

Supported: `ADLER32`, `CRC32`, `CRC32C`, `MD5`, `SHA1`, `SHA256`, `SHA512`.

```bash
gfal sum file:///tmp/file.root ADLER32
# file:///tmp/file.root 0a1b2c3d
```

### `gfal cat`

```bash
gfal cat URI [URI ...]
```

Prints file contents to stdout. Multiple files are concatenated.

### `gfal save`

```bash
echo "hello" | gfal save root://server//eos/user/j/jdoe/hello.txt
```

Reads from stdin and writes to the given URI.

### `gfal rename`

```bash
gfal rename SOURCE DESTINATION
```

### `gfal chmod`

```bash
gfal chmod MODE URI [URI ...]
```

MODE is an octal permission string, e.g. `0644` or `755`.

### `gfal xattr`

```bash
# Get an attribute
gfal xattr root://server//eos/data/file.root xroot.checksum

# Set an attribute
gfal xattr root://server//eos/data/file.root user.tag=important
```

### Shell completion

Generate shell completion scripts:

```bash
# Bash
gfal completion bash >> ~/.bashrc

# Zsh
gfal completion zsh >> ~/.zshrc

# Fish
gfal completion fish > ~/.config/fish/completions/gfal.fish
```

## Authentication

**X.509 proxy (XRootD / HTTPS):** If `X509_USER_PROXY` is set or a proxy exists at `/tmp/x509up_u<uid>`, it is used automatically. Override with `-E`/`--key`.

**HTTPS client certificates:** Pass `--cert` and `--key` for mutual TLS authentication.

## Supported protocols

| Scheme | Description | Requirements |
|--------|-------------|--------------|
| `file://` or bare path | Local filesystem | Built-in |
| `http://` / `https://` | HTTP/WebDAV | Built-in |
| `dav://` / `davs://` | WebDAV (converted to HTTP) | Built-in |
| `root://` | XRootD | `xrootd` + `fsspec-xrootd` |

## Development

```bash
git clone https://github.com/lobis/gfal.git
cd gfal

python -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"
pytest tests/
```

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/lobis"><img src="https://avatars.githubusercontent.com/u/35803280?v=4?s=100" width="100px;" alt="Luis Antonio Obis Aparicio"/><br /><sub><b>Luis Antonio Obis Aparicio</b></sub></a><br /><a href="https://github.com/lobis/gfal/commits?author=lobis" title="Code">💻</a> <a href="https://github.com/lobis/gfal/commits?author=lobis" title="Documentation">📖</a> <a href="#maintenance-lobis" title="Maintenance">🚧</a> <a href="#infra-lobis" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="#ideas-lobis" title="Ideas, Planning, & Feedback">🤔</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!
