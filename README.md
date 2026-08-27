# gfal

[![CI](https://github.com/lobis/gfal/actions/workflows/ci.yml/badge.svg)](https://github.com/lobis/gfal/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/gfal)](https://pypi.org/project/gfal/)
[![License](https://img.shields.io/pypi/l/gfal)](LICENSE)

`gfal` is a GFAL-compatible command-line interface backed by the system
`xrdfs` and `xrdcp` programs. It is intended as a lightweight replacement for
the commonly used commands from `gfal2-util` without shipping another storage
protocol stack in Python.

Only the aggregate command is installed:

```text
gfal ls ...
gfal cat ...
gfal stat ...
gfal sum ...
gfal cp ...
```

The standalone `gfal-ls`, `gfal-cat`, and similar executable aliases are not
installed.

## Runtime model

The Python package has no third-party runtime dependencies. It uses only the
Python standard library and delegates storage operations to XRootD:

- `xrdfs` handles metadata, namespace, checksum, token, and tape operations.
- `xrdcp` handles copies and streaming uploads.
- `xrdcl-http` provides HTTP/WebDAV transport support for XRootD.

The current development version requires the command-first JSON and tape
interfaces developed on the XRootD `xrd-cli` branch. An older `xrdfs` is
detected before an operation and produces a clear incompatibility error rather
than silently switching to another backend.

## Installation

For a Python environment:

```bash
python3 -m pip install gfal
```

`pip` installs only the Python router. `xrdfs` and `xrdcp` must also be
available on `PATH`.

For EL9 and EL10, build the RPM with packages from BaseOS, CRB, and EPEL:

```bash
dnf install epel-release dnf-plugins-core
dnf config-manager --set-enabled crb
dnf install \
  bash-completion make pyproject-rpm-macros python3-build python3-devel \
  python3-hatch-vcs python3-hatchling rpm-build xrdcl-http xrootd-client
make rpm
```

The resulting `python3-gfal` RPM is `noarch` and requires `xrootd-client` plus
either the integrated XRootD 6 HTTP plugin or the EPEL 5 `xrdcl-http` package.
It does not bundle
dependencies, disable RPM dependency generation, or download packages during
the RPM build.

## Commands

The native backend currently exposes:

```text
archivepoll  bringonline  cat    chmod  cp     evict
ls           mkdir        rename rm     save   stat
sum          token        xattr
```

Run `gfal COMMAND --help` for exact options. Options or protocols that cannot
yet preserve the gfal2 contract fail explicitly with `ENOTSUP`.

Examples:

```bash
gfal stat root://eospublic.cern.ch//eos/opendata/
gfal ls -l root://eospublic.cern.ch//eos/opendata/
gfal sum root://eospublic.cern.ch//eos/opendata/example ADLER32
gfal cp root://server.example//data/source file:///tmp/source
```

## Development

Build and verify the Python distributions:

```bash
make dist
python3 scripts/verify_distribution.py
```

Run the dependency-free test suite:

```bash
pytest -q -o addopts='' \
  tests/test_cp_native.py \
  tests/test_local_cli.py \
  tests/test_packaging_metadata.py \
  tests/test_save_native.py \
  tests/test_xrdfs_cli.py
```

CI repeats the RPM build and clean installation in AlmaLinux 9 and AlmaLinux
10 containers.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
