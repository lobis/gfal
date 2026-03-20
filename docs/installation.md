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
sudo apt-get update && sudo apt-get install -y ./python3-gfal-cli-latest.deb
```
