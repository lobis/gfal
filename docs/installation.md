# Installation

## PyPI (Stable)

The easiest way to install `gfal` is via `pip`:

```bash
pip install gfal
```

This installs the base package with local-file and HTTP/HTTPS support.

### PyPI with XRootD support

For XRootD support, install the optional extra:

```bash
pip install "gfal[xrootd]"
```

This installs both `fsspec-xrootd` and the PyPI `xrootd` bindings.

On grid systems where XRootD Python bindings are already available and centrally
managed, prefer the site package manager or conda for those bindings and keep
`gfal` itself lean. In that case, install `gfal` normally and add the fsspec
adapter:

```bash
pip install gfal
pip install fsspec-xrootd
```

In conda environments, install XRootD from conda-forge:

```bash
conda install -c conda-forge xrootd
```

### Conda with XRootD support

For conda environments, install `gfal` from the `lobis` channel with
`conda-forge` enabled for dependencies:

```bash
conda install -c lobis -c conda-forge gfal
```

The base conda package intentionally does **not** pull in every optional
fsspec backend. Install protocol-specific dependencies separately when needed:

```bash
# S3 / S3-compatible endpoints
conda install -c conda-forge s3fs boto3

# SSH / SFTP endpoints
conda install -c conda-forge paramiko sshfs
```

For pip users, the equivalent extras are:

```bash
pip install "gfal[xrootd]"
pip install "gfal[s3]"
pip install "gfal[ssh]"
```

For pip users, the `xrootd` extra is optional to avoid making every base
installation download the full XRootD client bundle.

---

## Native Repositories (Auto-updates)

We provide a native YUM repository hosted on GitHub Pages. This is the recommended way to stay updated on Linux systems.

### YUM (AlmaLinux / RHEL / Fedora)

Install the repository configuration:

```bash
dnf install -y epel-release
if [ "$(rpm -E '%{rhel}')" = "9" ]; then dnf config-manager --set-enabled crb; fi
curl -sL -o /etc/yum.repos.d/gfal.repo https://lobis.github.io/gfal/rpm/gfal.repo
dnf install -y python3-gfal
```

The RPM build is currently **HTTP/HTTPS-only**. XRootD support is not bundled in
the EPEL package because `fsspec-xrootd` is not available in EPEL yet.

---

## Direct Download (RPM)

You can also download individual packages from the [GitHub Releases](https://github.com/lobis/gfal/releases) page.

**AlmaLinux 9/10**:
```bash
dnf install -y epel-release
dnf install -y https://github.com/lobis/gfal/releases/latest/download/python3-gfal-0.1.50-1.el$(rpm -E '%{rhel}').noarch.rpm
```

If you use direct-download installs, update the version in the filename when a
new release comes out. If you want the latest version automatically, use the
repository configuration above instead.

This RPM has the same support profile as the repository package: HTTP/HTTPS by
default, without bundled XRootD support.

## CERN CA Certificates

To access CERN resources via HTTPS (like `eospublic.cern.ch:8444`) without the `--no-verify` flag, you must install the CERN Root CA 2 certificate.

### Linux (RHEL / AlmaLinux / Fedora)

```bash
sudo curl -L "https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt" -o /etc/pki/ca-trust/source/anchors/CERN-Root-CA-2.crt
sudo update-ca-trust
```

### Linux (Ubuntu / Debian)

```bash
# requires openssl
curl -sL "https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt" -o /tmp/cern.crt
openssl x509 -inform DER -in /tmp/cern.crt -out /tmp/cern.pem
sudo mv /tmp/cern.pem /usr/local/share/ca-certificates/cern-root-ca-2.crt
sudo update-ca-certificates
```

### macOS

```bash
# requires openssl
curl -sL "https://cafiles.cern.ch/cafiles/certificates/CERN%20Root%20Certification%20Authority%202.crt" -o /tmp/cern.crt
openssl x509 -inform DER -in /tmp/cern.crt -out /tmp/cern.pem
sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain /tmp/cern.pem
```
