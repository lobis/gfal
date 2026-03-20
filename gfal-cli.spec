%define base_name gfal-cli
%define dist_name gfal_cli

Name: python3-%{base_name}
Version: %{version}
Release: %{release}%{?dist}
Summary: GFAL2-compatible CLI tools based on fsspec (HTTP/HTTPS and XRootD)
License: MIT
URL: https://github.com/lobis/gfal-cli
Source0: %{base_name}-%{version}.tar.gz

BuildArch: noarch
BuildRequires: python3-devel
BuildRequires: python3-pip
BuildRequires: python3-setuptools
BuildRequires: python3-wheel

Requires: python3-fsspec
Requires: python3-fsspec-xrootd
Requires: python3-aiohttp
Requires: python3-requests

%description
A pip-installable Python rewrite of the gfal2-util CLI tools, built on fsspec.
Supports HTTP/HTTPS and XRootD only (via fsspec-xrootd).

%prep
%autosetup -n %{dist_name}-%{version}

%build
# Nothing to build for pure python

%install
mkdir -p %{buildroot}%{python3_sitelib}
%{__python3} -m pip install --no-deps --no-binary :all: --root %{buildroot} --prefix %{_prefix} %{SOURCE0}

%files
%defattr(-,root,root,-)
%{_bindir}/gfal*
%{python3_sitelib}/gfal_cli*

%changelog
* Fri Mar 20 2026 Luis Antonio Obis Aparicio <luis.obis@cern.ch> - %{version}-%{release}
- Initial package building with hatchling and pip
