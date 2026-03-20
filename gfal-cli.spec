%{!?python3_sitelib: %global python3_sitelib %(python3 -c "import sysconfig; print(sysconfig.get_path('purelib'))" 2>/dev/null || echo /usr/lib/python3/site-packages)}
%{!?__python3: %global __python3 /usr/bin/python3}

%define base_name gfal-cli
%define dist_name gfal_cli

Name: python3-%{base_name}
Version: %{pkg_version}
Release: %{pkg_release}%{?dist}
Summary: GFAL2-compatible CLI tools based on fsspec (HTTP/HTTPS and XRootD)
License: MIT
URL: https://github.com/lobis/gfal-cli
Source0: %{dist_name}-%{version}-py3-none-any.whl

BuildArch: noarch
BuildRequires: python3-devel
BuildRequires: python3-pip
BuildRequires: python3-setuptools
BuildRequires: python3-wheel

Requires: python3-fsspec
Requires: python3-xrootd
Requires: python3-aiohttp
Requires: python3-requests

%description
A pip-installable Python rewrite of the gfal2-util CLI tools, built on fsspec.
Supports HTTP/HTTPS and XRootD only (via fsspec-xrootd).

%prep
# Nothing to prep for wheel

%build
# Nothing to build for wheel

%install
mkdir -p %{buildroot}%{python3_sitelib}

# 1. Pull the missing dependency from PyPI and bundle it in the RPM buildroot
%{__python3} -m pip install fsspec-xrootd --no-deps --ignore-installed --root %{buildroot} --prefix %{_prefix}

# 2. Install the gfal-cli wheel into the RPM buildroot
%{__python3} -m pip install --no-deps --ignore-installed --root %{buildroot} --prefix %{_prefix} %{_sourcedir}/%{dist_name}-%{version}-py3-none-any.whl

%files
%defattr(-,root,root,-)
%{_bindir}/gfal*
%{python3_sitelib}/gfal_cli*
%{python3_sitelib}/fsspec_xrootd*

%changelog -f CHANGELOG
