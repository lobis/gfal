# Installation

## PyPI (Stable)

The easiest way to install `gfal` is via `pip`:

```bash
pip install gfal
```

This installs the base package with local-file, HTTP/HTTPS, and XRootD support.
The PyPI package depends on `xrootd>=6.0.1`.

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
pip install "gfal[s3]"
pip install "gfal[ssh]"
```

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

The RPM build bundles the PyPI XRootD bindings because EPEL currently provides
XRootD 5.x while `gfal` requires the Python bindings starting at 6.0.1.

---

## Direct Download (RPM)

You can also download individual packages from the [GitHub Releases](https://github.com/lobis/gfal/releases) page.

**AlmaLinux 9/10**:
```bash
dnf install -y epel-release
dnf install -y https://github.com/lobis/gfal/releases/latest/download/python3-gfal-0.1.50-1.el$(rpm -E '%{rhel}').$(uname -m).rpm
```

If you use direct-download installs, update the version in the filename when a
new release comes out. If you want the latest version automatically, use the
repository configuration above instead.

This RPM has the same support profile as the repository package, including
bundled XRootD Python bindings.

## CERN HTTPS and CERN CA Certificates

Many CERN HTTPS endpoints are not signed by a CA that is present in every
minimal OS or Python installation. If you see an SSL certificate verification
error for hosts such as `eospublic.cern.ch` or `eospilot.cern.ch`, choose one
of these approaches:

- Prefer `root://...` URLs when HTTPS is not required. This avoids HTTPS
  certificate validation entirely and uses XRootD instead, for example:

  ```bash
  gfal stat root://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C
  ```

  XRootD support is included in normal `pip install gfal` and
  `conda install -c lobis -c conda-forge gfal` installs.

- For quick tests against trusted CERN endpoints, pass `--no-verify` to disable
  TLS certificate verification:

  ```bash
  gfal stat --no-verify https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C
  ```

  This is intentionally insecure: it confirms the connection can be made, but
  it does not verify the server identity.

- For normal verified HTTPS usage, install the CERN Root CA 2 certificate into
  the system trust store. After this, plain `https://...` commands should work
  without `--no-verify`.

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
