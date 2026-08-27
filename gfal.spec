Name:           python3-gfal
Version:        %{pkg_version}
Release:        %{pkg_release}%{?dist}
Summary:        GFAL-compatible command-line tools backed by XRootD clients

License:        BSD-3-Clause
URL:            https://github.com/lobis/gfal
Source0:        gfal-%{source_version}.tar.gz

BuildArch:      noarch
BuildRequires:  python3-devel
BuildRequires:  pyproject-rpm-macros

Requires:       xrootd-client
Requires:       xrdcl-http

%description
gfal provides a dependency-free Python command-line interface compatible with
the commonly used gfal2-util commands. Remote operations are delegated to the
xrdfs and xrdcp programs supplied by XRootD. The package supports XRootD and
WebDAV endpoints without bundling Python protocol implementations.

%prep
%autosetup -n gfal-%{source_version}

%generate_buildrequires
%pyproject_buildrequires

%build
%pyproject_wheel

%install
%pyproject_install
%pyproject_save_files -l gfal

mkdir -p %{buildroot}%{_datadir}/bash-completion/completions
mkdir -p %{buildroot}%{_datadir}/zsh/site-functions
PYTHONPATH=%{buildroot}%{python3_sitelib} \
    %{python3} -m gfal.cli.main completion bash \
    > %{buildroot}%{_datadir}/bash-completion/completions/gfal
PYTHONPATH=%{buildroot}%{python3_sitelib} \
    %{python3} -m gfal.cli.main completion zsh \
    > %{buildroot}%{_datadir}/zsh/site-functions/_gfal

%check
PYTHONPATH=%{buildroot}%{python3_sitelib} %{python3} -c \
    'import gfal, gfal.cli.main, gfal.cli.xrdfs, gfal.xrdcp, gfal.xrdfs'
PYTHONPATH=%{buildroot}%{python3_sitelib} %{python3} -m gfal.cli.main --help

%files -f %{pyproject_files}
%{_bindir}/gfal
%{_datadir}/bash-completion/completions/gfal
%{_datadir}/zsh/site-functions/_gfal

%changelog
* Thu Aug 27 2026 Luis Antonio Obis Aparicio <luis.obis@cern.ch> - %{version}-%{release}
- Package the xrdfs-based command-line interface for EPEL
