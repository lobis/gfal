#!/usr/bin/env bash

set -euo pipefail

remote=${1:-lobis-eos-dev}
ref=${2:-HEAD}

repo_root=$(git rev-parse --show-toplevel)
commit=$(git -C "$repo_root" rev-parse --verify "${ref}^{commit}")
short_commit=$(git -C "$repo_root" rev-parse --short=10 "$commit")
latest_tag=$(git -C "$repo_root" describe --tags --abbrev=0 --match 'v[0-9]*' "$commit")
base_version=${latest_tag#v}
commit_count=$(git -C "$repo_root" rev-list --count "${latest_tag}..${commit}")

if [[ $commit_count -eq 0 ]]; then
    deploy_version=$base_version
else
    IFS=. read -r version_major version_minor version_patch <<<"$base_version"
    deploy_version="${version_major}.${version_minor}.$((version_patch + 1)).dev${commit_count}+g${short_commit}"
fi

remote_dir=$(ssh "$remote" 'mktemp -d /tmp/gfal-rpm-deploy.XXXXXX')
cleanup() {
    ssh "$remote" rm -rf -- "$remote_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Deploying gfal commit $short_commit to $remote as version $deploy_version"
git -C "$repo_root" archive "$commit" | ssh "$remote" tar -xf - -C "$remote_dir"

ssh "$remote" bash -s -- "$remote_dir" "$deploy_version" "$short_commit" <<'REMOTE_SCRIPT'
set -euo pipefail

source_dir=$1
deploy_version=$2
short_commit=$3

echo "Installing EL/EPEL build requirements"
dnf -qy install epel-release dnf-plugins-core
dnf config-manager --set-enabled crb
dnf -qy install \
    make pyproject-rpm-macros python3-build python3-devel \
    python3-hatch-vcs python3-hatchling rpm-build

echo "Building the gfal RPM"
build_log=$source_dir/rpm-build.log
if ! SETUPTOOLS_SCM_PRETEND_VERSION=$deploy_version \
    make -s -C "$source_dir" rpm >"$build_log" 2>&1; then
    cat "$build_log"
    exit 1
fi
rpm_path=$(find "$source_dir/rpmbuild/RPMS/noarch" -maxdepth 1 \
    -name 'python3-gfal-*.noarch.rpm' -print -quit)
test -n "$rpm_path"

unexpected_requires=$(rpm -qp --requires "$rpm_path" | grep -Ei \
    'python(3)?dist\((aiohttp|click|fsspec|rich|xrootd)\)' || true)
if [[ -n $unexpected_requires ]]; then
    echo "Unexpected Python runtime dependencies:" >&2
    echo "$unexpected_requires" >&2
    exit 1
fi

echo "Installing $rpm_path"
dnf -qy install "$rpm_path"

client_prefix=/opt/xrootd-xrd-cli
if [[ -x $client_prefix/usr/bin/xrdfs && -x $client_prefix/usr/bin/xrdcp ]]; then
    wrapper=/usr/local/bin/gfal
    if [[ -e $wrapper ]] && ! grep -Fq '# gfal xrd-cli development wrapper' "$wrapper"; then
        echo "$wrapper exists and is not managed by this deployment script" >&2
        exit 1
    fi
    install -m 0755 "$source_dir/scripts/gfal-xrd-cli-wrapper.sh" "$wrapper"
    hash -r
    xrdfs_command=$client_prefix/usr/bin/xrdfs
    xrdfs_library_path=$client_prefix/usr/lib64
else
    xrdfs_command=$(command -v xrdfs)
    xrdfs_library_path=
fi

echo
echo "Installed $(rpm -q python3-gfal) from commit $short_commit"
gfal --version
gfal --help >/dev/null
gfal completion bash >/dev/null
LD_LIBRARY_PATH="${xrdfs_library_path}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    "$xrdfs_command" --help 2>&1 | grep -F 'stat [--json]' >/dev/null
rpm -qf /usr/bin/gfal >/dev/null
REMOTE_SCRIPT
