%{!?__python3: %global __python3 /usr/bin/python3}

%define base_name gfal-cli
%define dist_name gfal_cli
%define install_dir /opt/%{base_name}

Name: python3-%{base_name}
Version: %{pkg_version}
Release: %{pkg_release}%{?dist}
Summary: GFAL2-compatible CLI tools based on fsspec (HTTP/HTTPS and XRootD)
License: BSD-3-Clause
URL: https://github.com/lobis/gfal-cli
Source0: %{dist_name}-%{version}-py3-none-any.whl

BuildRequires: python3-devel
BuildRequires: python3-pip
BuildRequires: python3-wheel

# Still require the OS to provide the heavy C++ XRootD bindings
Requires: python3-xrootd

# Stop RPM from auto-generating strict version requirements
AutoReq: no

%description
A pip-installable Python rewrite of the gfal2-util CLI tools, built on fsspec.
This package is bundled in an isolated environment in %{install_dir} to prevent system conflicts.

%prep
# Nothing to prep for wheel

%build
# Nothing to build for wheel

%install
# 1. Create the base directories
mkdir -p %{buildroot}%{install_dir}
mkdir -p %{buildroot}%{_bindir}

# 2. Create a virtual environment inside the RPM buildroot
# --system-site-packages is REQUIRED so it can still find python3-xrootd from the OS
%{__python3} -m venv --system-site-packages %{buildroot}%{install_dir}

# 3. Use the venv's isolated pip to install the app and all bundled dependencies
%{buildroot}%{install_dir}/bin/python -m pip install --no-cache-dir fsspec-xrootd fsspec aiohttp requests %{_sourcedir}/%{dist_name}-%{version}-py3-none-any.whl

# 4. Clean up hardcoded build paths
# Pip hardcodes the temporary GitHub Actions build path into the script shebangs.
# We use sed to strip %{buildroot} out, so the shebang correctly becomes: #!/opt/gfal-cli/bin/python
find %{buildroot}%{install_dir}/bin -type f -exec sed -i 's|%{buildroot}||g' {} +
sed -i 's|%{buildroot}||g' %{buildroot}%{install_dir}/pyvenv.cfg

# 5. Symlink the executables to /usr/bin
# This allows users to run `gfal-copy` from anywhere, but it routes traffic into the isolated /opt/ environment
for cmd in %{buildroot}%{install_dir}/bin/gfal*; do
    ln -s %{install_dir}/bin/$(basename $cmd) %{buildroot}%{_bindir}/$(basename $cmd)
done

%files
%defattr(-,root,root,-)
# Own the symlinks we created in /usr/bin/
%{_bindir}/gfal*
# Own the entire isolated directory in /opt/
%{install_dir}/

%changelog -f CHANGELOG
