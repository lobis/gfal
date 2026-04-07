# gfal
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

**Grid File Access Library — Python rewrite of gfal2**

**Documentation: [lobis.github.io/gfal](https://lobis.github.io/gfal/)**

A pip-installable Python-only rewrite of the [gfal2-util](https://github.com/lobis/gfal2-util) CLI tools, built on [fsspec](https://filesystem-spec.readthedocs.io/) — no C library required. Supports **HTTP/HTTPS** out of the box, with optional **XRootD** support via [fsspec-xrootd](https://github.com/scikit-hep/fsspec-xrootd).

## Installation

### From PyPI

```bash
pip install gfal
```

This installs the base package with local-file and HTTP/HTTPS support.

### From PyPI with XRootD support

```bash
pip install 'gfal[xrootd]'
```

This installs the optional XRootD dependencies: `xrootd` and `fsspec-xrootd`.

### From Native Repository (Recommended for Updates)

Enable the repository to receive automatic updates via `dnf update`.

#### YUM (AlmaLinux / RHEL)

```bash
dnf install -y epel-release
dnf config-manager --set-enabled crb
curl -sL -o /etc/yum.repos.d/gfal.repo https://lobis.github.io/gfal/rpm/gfal.repo
dnf install -y python3-gfal
```

The RPM build is currently **HTTP/HTTPS-only**. XRootD support is not bundled in
the EPEL package because `fsspec-xrootd` is not available there yet.

After installation the `gfal` command is available on your `PATH`. Run `gfal --help` to see all subcommands:

| Subcommand | Description |
|-----------|-------------|
| `gfal ls` | List directory contents |
| `gfal cp` | Copy files |
| `gfal rm` | Remove files or directories |
| `gfal mkdir` | Create directories |
| `gfal stat` | Display file status |
| `gfal cat` | Print file contents to stdout |
| `gfal save` | Write stdin to a remote file |
| `gfal rename` | Rename / move a file |
| `gfal chmod` | Change file permissions |
| `gfal sum` | Compute file checksums |
| `gfal xattr` | Get or set extended attributes |

## Quick start

All commands accept any URL that fsspec understands. Local paths must be given as `file://` URIs.
If you installed only `pip install gfal`, start with the `https://` examples below.
The `root://` examples require `pip install 'gfal[xrootd]'`.

```bash
# Stat a real EOS public file (HTTPS)
gfal stat https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C

# List a real EOS public directory (XRootD)
gfal ls root://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/

# Compute a checksum for the same file
gfal sum https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C MD5

# Copy the file to /tmp
gfal cp https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C file:///tmp/single_cluster_r5.C

# Peek at the first few lines
gfal cat https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C | head -n 5
```

For more copy-pasteable public CERN examples, see [docs/eospublic-examples.md](docs/eospublic-examples.md).

## Command reference

### `gfal ls`

```
gfal ls [OPTIONS] URI [URI ...]
```

| Option | Description |
|--------|-------------|
| `-l` | Long listing (permissions, size, date) |
| `-a` / `--all` | Show hidden files (names starting with `.`) |
| `-d` / `--directory` | List the entry itself, not its contents |
| `-H` / `--human-readable` | Human-readable sizes with `-l` (e.g. `1.2M`) |
| `-r` / `--reverse` | Reverse sort order |
| `--time-style` | Timestamp format: `locale` (default), `iso`, `long-iso`, `full-iso` |
| `--full-time` | Equivalent to `--time-style=full-iso` |
| `--color` | Colorise output: `auto` (default), `always`, `never` |

```bash
gfal ls -lH root://server//eos/data/
gfal ls -la file:///tmp/mydir/
gfal ls -l --time-style=full-iso https://example.com/files/
```

### `gfal cp`

```
gfal cp [OPTIONS] SRC [SRC ...] DST
```

| Option | Description |
|--------|-------------|
| `-f` / `--force` | Overwrite destination if it exists |
| `-r` / `-R` / `--recursive` | Copy directories recursively |
| `-p` / `--parent` | Create parent directories at destination as needed |
| `-K ALG` / `--checksum ALG` | Verify checksum after copy (`ADLER32`, `MD5`, `SHA256`, …) |
| `--checksum-mode` | `both` (default), `source`, `target` |
| `--dry-run` | Show what would be copied without copying |
| `--from-file FILE` | Read source URIs from a file (one per line) |
| `--abort-on-failure` | Stop after the first failed transfer |
| `--transfer-timeout N` | Per-file timeout in seconds (0 = no timeout) |
| `--tpc` | Attempt third-party copy, fall back to streaming |
| `--tpc-only` | Require third-party copy (fail if unsupported) |

```bash
# Simple copy
gfal cp file:///tmp/src.txt https://server/dst.txt

# Force overwrite with ADLER32 verification
gfal cp -f -K ADLER32 root://server//path/file.root file:///tmp/file.root

# Recursive copy, create parents
gfal cp -r -p root://server//eos/srcdir/ file:///tmp/dstdir/

# Third-party copy between two XRootD servers
gfal cp --tpc root://src-server//path/file root://dst-server//path/file
```

### `gfal rm`

```
gfal rm [OPTIONS] URI [URI ...]
```

| Option | Description |
|--------|-------------|
| `-r` / `-R` / `--recursive` | Remove directories and their contents |
| `--dry-run` | Show what would be deleted without deleting |
| `--from-file FILE` | Read URIs to delete from a file |
| `--just-delete` | Skip the stat check and delete directly |

```bash
gfal rm file:///tmp/old.txt
gfal rm -r root://server//eos/old_dir/
gfal rm --dry-run root://server//eos/dir/     # preview only
```

### `gfal stat`

```
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

```
gfal mkdir [OPTIONS] URI [URI ...]
```

| Option | Description |
|--------|-------------|
| `-p` / `--parents` | Create intermediate parent directories; no error if already exists |
| `-m MODE` | Permissions in octal (default: `755`) |

```bash
gfal mkdir root://server//eos/user/j/jdoe/newdir
gfal mkdir -p root://server//eos/user/j/jdoe/a/b/c
```

### `gfal sum`

```
gfal sum URI ALGORITHM
```

Supported algorithms: `ADLER32`, `CRC32`, `CRC32C`, `MD5`, `SHA1`, `SHA256`, `SHA512`.

```bash
gfal sum file:///tmp/file.root ADLER32
# file:///tmp/file.root 0a1b2c3d
```

### `gfal cat`

```
gfal cat URI [URI ...]
```

Prints file contents to stdout. Multiple files are concatenated.

### `gfal save`

```
gfal save URI
```

Reads from stdin and writes to the given URI.

```bash
echo "hello" | gfal save root://server//eos/user/j/jdoe/hello.txt
```

### `gfal rename`

```
gfal rename SOURCE DESTINATION
```

### `gfal chmod`

```
gfal chmod MODE URI [URI ...]
```

MODE is an octal permission string, e.g. `0644` or `755`.

## Common options

Every command accepts these global flags:

| Option | Description |
|--------|-------------|
| `-v` / `--verbose` | Enable verbose output |
| `-t N` / `--timeout N` | Global timeout in seconds |
| `-E CERT` / `--cert CERT` | Path to client certificate (PEM) |
| `--key KEY` | Path to client key (PEM) |
| `--no-verify` | Disable TLS certificate verification |
| `--log-file FILE` | Write log output to a file |

## Authentication

**X.509 proxy (XRootD / HTTPS):** If `X509_USER_PROXY` is set or a proxy exists at `/tmp/x509up_u<uid>`, it is used automatically. Override with `-E`/`--key`.

**HTTPS client certificates:** Pass `--cert` and `--key` for mutual TLS authentication.

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
