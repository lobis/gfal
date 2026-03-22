Name:           python3-gfal-cli
Version:        %{pkg_version}
Release:        %{pkg_release}%{?dist}
Summary:        GFAL2-compatible CLI tools based on fsspec

License:        BSD-3-Clause
URL:            https://github.com/lobis/gfal-cli
Source0:        https://github.com/lobis/gfal-cli/archive/v%{version}.tar.gz

BuildArch:      noarch
BuildRequires:  python3-devel
BuildRequires:  python3-pip
BuildRequires:  python3-wheel
BuildRequires:  python3-hatchling
BuildRequires:  python3-hatch-vcs

Requires:       python3-fsspec
Requires:       python3-fsspec-xrootd
Requires:       python3-aiohttp
Requires:       python3-requests
Requires:       python3-rich
Requires:       python3-textual

%description
A pip-installable Python rewrite of the gfal2-util CLI tools, built on fsspec.
This package provides a modern, Pythonic alternative to gfal2-util with
support for HTTP/HTTPS and XRootD protocols.

%prep
%autosetup -n gfal-cli-%{version}

%build
%pyproject_wheel

%install
%pyproject_install

%files
%license LICENSE
%doc README.md
%{_bindir}/gfal
%{python3_sitelib}/gfal_cli/
%{python3_sitelib}/gfal_cli-%{version}.dist-info/

%changelog -f %{_sourcedir}/CHANGELOG
