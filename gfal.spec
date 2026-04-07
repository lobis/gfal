%{!?__python3: %global __python3 /usr/bin/python3}
%{!?python3_sitelib: %global python3_sitelib %(%{__python3} -c "import sysconfig; print(sysconfig.get_path('purelib'))")}

%define base_name gfal
%define dist_name gfal

Name: python3-%{base_name}
Version: %{pkg_version}
Release: %{pkg_release}%{?dist}
Summary: Grid File Access Library — Python rewrite of gfal2, based on fsspec (HTTP/HTTPS by default, optional XRootD)
License: BSD-3-Clause
URL: https://github.com/lobis/gfal
Source0: %{dist_name}-%{version}-py3-none-any.whl

BuildArch: noarch
BuildRequires: python3-devel
BuildRequires: python3-pip

# Runtime dependencies available in EPEL / base RHEL
Requires: python3-aiohttp
Requires: python3-requests
Requires: python3-rich
Requires: python3-click
# python3-fsspec is available in EPEL 10+ but not EPEL 9 — handled conditionally below
%if 0%{?rhel} >= 10
Requires: python3-fsspec
%endif
# Note: python3-truststore is not packaged in EPEL; it is an optional TLS
#       enhancement and is omitted here (no functional impact).
# Note: XRootD support needs both the xrootd Python bindings and the
#       fsspec-xrootd plugin. The latter is not currently packaged in EPEL,
#       so the RPM is published as HTTP/HTTPS-only by default.

AutoReq: no

%description
gfal (Grid File Access Library) is a pip-installable Python-only rewrite of the
gfal2-util CLI tools, built on fsspec — no C library required.
Supports HTTP/HTTPS out of the box.

XRootD support is optional and requires both the xrootd Python bindings and the
fsspec-xrootd plugin. Since fsspec-xrootd is not currently packaged in EPEL,
the RPM is intentionally published without XRootD runtime dependencies.

%prep
# Nothing to prep for a pre-built wheel

%build
# Nothing to build for a pre-built wheel

%install
# Install gfal itself (no deps — all declared as Requires or bundled below)
%{__python3} -m pip install \
    --root %{buildroot} \
    --prefix /usr \
    --no-deps \
    --no-build-isolation \
    --no-cache-dir \
    %{_sourcedir}/%{dist_name}-%{version}-py3-none-any.whl

%if 0%{?rhel} < 10
# Bundle fsspec on RHEL/EL9 where it is not available in EPEL
%{__python3} -m pip install \
    --root %{buildroot} \
    --prefix /usr \
    --no-build-isolation \
    --no-cache-dir \
    "fsspec>=2023.1.0"
%endif

%files
%{python3_sitelib}/gfal/
%{python3_sitelib}/gfal-*.dist-info/
%if 0%{?rhel} < 10
%{python3_sitelib}/fsspec/
%{python3_sitelib}/fsspec-*.dist-info/
%endif
%{_bindir}/gfal*

%changelog -f %{_sourcedir}/CHANGELOG
