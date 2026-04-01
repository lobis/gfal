%{!?__python3: %global __python3 /usr/bin/python3}
%{!?python3_sitelib: %global python3_sitelib %(%{__python3} -c "import sysconfig; print(sysconfig.get_path('purelib'))")}

%define base_name gfal
%define dist_name gfal

Name: python3-%{base_name}
Version: %{pkg_version}
Release: %{pkg_release}%{?dist}
Summary: Grid File Access Library — Python rewrite of gfal2, based on fsspec (HTTP/HTTPS and XRootD)
License: BSD-3-Clause
URL: https://github.com/lobis/gfal
Source0: %{dist_name}-%{version}-py3-none-any.whl

BuildArch: noarch
BuildRequires: python3-devel
BuildRequires: python3-pip

# Runtime dependencies — all available in EPEL / base RHEL
Requires: python3-fsspec
Requires: python3-aiohttp
Requires: python3-requests
Requires: python3-rich
Requires: python3-click
Requires: python3-xrootd
# Note: fsspec-xrootd (XRootD fsspec plugin) is not packaged in EPEL.
# Install it separately with: pip install fsspec-xrootd

AutoReq: no

%description
gfal (Grid File Access Library) is a pip-installable Python-only rewrite of the
gfal2-util CLI tools, built on fsspec — no C library required.
Supports HTTP/HTTPS and XRootD. XRootD support requires python3-xrootd from EPEL.

%prep
# Nothing to prep for a pre-built wheel

%build
# Nothing to build for a pre-built wheel

%install
# Install the wheel into the system Python site-packages (no venv, EPEL-compliant)
%{__python3} -m pip install \
    --root %{buildroot} \
    --prefix /usr \
    --no-deps \
    --no-build-isolation \
    --no-cache-dir \
    %{_sourcedir}/%{dist_name}-%{version}-py3-none-any.whl

%files
%{python3_sitelib}/gfal/
%{python3_sitelib}/gfal-*.dist-info/
%{_bindir}/gfal*

%changelog -f %{_sourcedir}/CHANGELOG
