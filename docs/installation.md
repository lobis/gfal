# Installation

## PyPI (Stable)

The easiest way to install `gfal-cli` is via `pip`:

```bash
pip install gfal-cli
```

---

## Native Repositories (Auto-updates)

We provide native YUM and APT repositories hosted on GitHub Pages. This is the recommended way to stay updated on Linux systems.

### YUM (AlmaLinux / RHEL / Fedora)

Install the repository configuration:

```bash
sudo curl -sL -o /etc/yum.repos.d/gfal-cli.repo https://lobis.github.io/gfal-cli/rpm/gfal-cli.repo
sudo dnf install -y python3-gfal-cli
```

### APT (Ubuntu / Debian)

Add the repository:

```bash
echo "deb [trusted=yes] https://lobis.github.io/gfal-cli/deb/ stable main" | sudo tee /etc/apt/sources.list.d/gfal-cli.list
sudo apt-get update
sudo apt-get install -y python3-gfal-cli
```

---

## Direct Download (RPM / DEB)

You can also download individual packages from the [GitHub Releases](https://github.com/lobis/gfal-cli/releases) page.

**AlmaLinux 9/10**:
```bash
dnf install -y https://github.com/lobis/gfal-cli/releases/latest/download/python3-gfal-cli-latest-el9.rpm
```

**Ubuntu**:
```bash
curl -L -O https://github.com/lobis/gfal-cli/releases/latest/download/python3-gfal-cli-latest.deb

---

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
