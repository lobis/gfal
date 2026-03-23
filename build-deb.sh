#!/bin/bash
# Build a Debian package for gfal.
# Intended to run inside an ubuntu:24.04 (or later) container with the
# repository root mounted at /workspace (the working directory).
#
# Usage (matching the Makefile 'deb' target):
#   bash build-deb.sh [ARCH]
#
# ARCH defaults to the output of `dpkg --print-architecture` (e.g. amd64).
# The resulting .deb is written to the repository root as:
#   python3-gfal-<VERSION>-<ARCH>.deb

set -euo pipefail

ARCH="${1:-$(dpkg --print-architecture)}"

# Install build dependencies.
apt-get update -q
DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
    python3-pip git python3-all binutils dpkg-dev

# Allow git to read the repository even when owned by a different UID
# (common inside Docker when the workspace is mounted from the host).
git config --global --add safe.directory '*'

# Install Python build tools.
python3 -m pip install --quiet --break-system-packages build hatchling hatch-vcs

# Determine the package version (strip the local part: +gXXXXXXX…).
FULL_VERSION=$(python3 -m hatchling version)
VERSION=$(echo "${FULL_VERSION}" | sed 's/+.*//')

echo "Building python3-gfal ${VERSION} for ${ARCH}"

# Build the wheel.
python3 -m build --no-isolation

# Assemble the package tree.
mkdir -p pkg/DEBIAN pkg/usr/bin pkg/usr/lib/python3/dist-packages

cat > pkg/DEBIAN/control << EOF
Package: python3-gfal
Version: ${VERSION}
Section: utils
Priority: optional
Architecture: ${ARCH}
Maintainer: Luis Antonio Obis Aparicio <luis.obis@cern.ch>
Depends: python3, python3-fsspec, python3-xrootd, python3-aiohttp, python3-requests, python3-rich, python3-markdown-it, python3-mdurl, python3-pygments, python3-click
Description: Grid File Access Library — Python rewrite of gfal2, based on fsspec
EOF

# Bundle the wheel and fsspec-xrootd (no transitive deps; let the system
# packages satisfy everything else listed in Depends above).
python3 -m pip install \
    fsspec-xrootd "dist/gfal-${FULL_VERSION}-py3-none-any.whl" \
    --no-deps --ignore-installed --break-system-packages \
    --target pkg/usr/lib/python3/dist-packages

# Bundle textual and rich-click (not yet in Ubuntu repos as of 22.04/24.04).
python3 -m pip install textual rich-click \
    --ignore-installed --break-system-packages \
    --target pkg/usr/lib/python3/dist-packages

# Prune packages that conflict with official Ubuntu system packages.
# These are declared as Depends above so apt will install the system versions.
# Failing to prune them causes dpkg errors about overwriting files.
# Currently pruned (provided by system): rich, markdown_it, mdurl,
# mdit_py_plugins, pygments, click.
rm -rf pkg/usr/lib/python3/dist-packages/rich*
rm -rf pkg/usr/lib/python3/dist-packages/markdown_it*
rm -rf pkg/usr/lib/python3/dist-packages/mdurl*
rm -rf pkg/usr/lib/python3/dist-packages/mdit_py_plugins*
rm -rf pkg/usr/lib/python3/dist-packages/pygments*
rm -rf pkg/usr/lib/python3/dist-packages/click*

# Move console-script entry points to /usr/bin.
if [ -d pkg/usr/lib/python3/dist-packages/bin ]; then
    mv pkg/usr/lib/python3/dist-packages/bin/* pkg/usr/bin/
    rm -rf pkg/usr/lib/python3/dist-packages/bin
fi

# Fix shebangs to point at the system Python 3 interpreter.
sed -i '1s|^.*$|#!/usr/bin/python3|' pkg/usr/bin/gfal*

# Build the .deb.
DEB_FILE="python3-gfal-${VERSION}-${ARCH}.deb"
dpkg-deb --build pkg "${DEB_FILE}"
echo "Built: ${DEB_FILE}"
