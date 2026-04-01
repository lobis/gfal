%{!?__python3: %global __python3 /usr/bin/python3}

%define base_name gfal
%define dist_name gfal
%define install_dir /opt/%{base_name}

Name: python3-%{base_name}
Version: %{pkg_version}
Release: %{pkg_release}%{?dist}
Summary: Grid File Access Library — Python rewrite of gfal2, based on fsspec (HTTP/HTTPS and XRootD)
License: BSD-3-Clause
URL: https://github.com/lobis/gfal
Source0: %{dist_name}-%{version}-py3-none-any.whl

BuildRequires: python3-devel
BuildRequires: python3-pip

# All runtime deps are available in EPEL / base RHEL
Requires: python3-fsspec
Requires: python3-aiohttp
Requires: python3-requests
Requires: python3-rich
Requires: python3-click
Requires: python3-xrootd
# XRootD filesystem support (fsspec-xrootd) is not packaged in EPEL.
# Install it separately with: pip install fsspec-xrootd

# Stop RPM from auto-generating strict version requirements
AutoReq: no

%description
gfal (Grid File Access Library) is a pip-installable Python-only rewrite of the gfal2-util CLI tools, built on fsspec — no C library required.
Supports HTTP/HTTPS and XRootD. XRootD support requires python3-xrootd from EPEL.

%prep
# Nothing to prep for wheel

%build
# Nothing to build for wheel

%install
# 1. Create the base directories
mkdir -p %{buildroot}%{install_dir}
mkdir -p %{buildroot}%{_bindir}

# 2. Create a virtual environment with access to system site-packages (EPEL deps)
%{__python3} -m venv --system-site-packages %{buildroot}%{install_dir}

# 3. Install gfal itself — no bundled deps needed, all are in EPEL (declared as Requires above)
%{buildroot}%{install_dir}/bin/python -m pip install --no-cache-dir --no-deps \
    %{_sourcedir}/%{dist_name}-%{version}-py3-none-any.whl

# 5. Clean up hardcoded build paths from shebangs and pyvenv.cfg
find %{buildroot}%{install_dir}/bin -type f -exec sed -i "s|%{buildroot}||g" {} +
sed -i "s|%{buildroot}||g" %{buildroot}%{install_dir}/pyvenv.cfg

# 6. Symlink executables to /usr/bin
pushd %{buildroot}%{install_dir}/bin/
for cmd in gfal*; do
    if [ -x "$cmd" ]; then
        ln -sf %{install_dir}/bin/$cmd %{buildroot}%{_bindir}/$cmd
    fi
done
popd

%files
%defattr(-,root,root,-)
%{_bindir}/gfal*
%{install_dir}/

%changelog -f %{_sourcedir}/CHANGELOG
