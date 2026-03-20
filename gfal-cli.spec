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

# Only require the base C++ XRootD library from the OS.
Requires: python3-xrootd

# Stop RPM from auto-generating strict python3.Xdist() requirements
AutoReq: no

%description
A pip-installable Python rewrite of the gfal2-util CLI tools, built on fsspec.
Supports HTTP/HTTPS and XRootD only (via fsspec-xrootd).

%prep
# Nothing to prep for wheel

%build
# Nothing to build for wheel

%install
mkdir -p %{buildroot}%{python3_sitelib}

# Install the app AND all its dependencies into the RPM buildroot
%{__python3} -m pip install fsspec-xrootd fsspec aiohttp requests --no-deps --ignore-installed --root %{buildroot} --prefix %{_prefix}
%{__python3} -m pip install --no-deps --ignore-installed --root %{buildroot} --prefix %{_prefix} %{_sourcedir}/%{dist_name}-%{version}-py3-none-any.whl

# Because we used --no-deps to avoid pulling dependencies twice,
# let's do one final pip pass to pull all sub-dependencies (like urllib3, certifi, etc.)
%{__python3} -m pip install fsspec-xrootd fsspec aiohttp requests %{_sourcedir}/%{dist_name}-%{version}-py3-none-any.whl --ignore-installed --root %{buildroot} --prefix %{_prefix}

%files
%defattr(-,root,root,-)
%{_bindir}/gfal*
# Grab the app and all bundled dependencies
%{python3_sitelib}/*

%changelog -f CHANGELOG
