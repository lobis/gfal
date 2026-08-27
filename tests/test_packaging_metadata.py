"""Regression tests for packaging metadata."""

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def test_pip_metadata_does_not_force_urllib3():
    pyproject = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert '"urllib3' not in pyproject
    assert '"truststore' not in pyproject


def test_conda_recipe_does_not_force_urllib3():
    recipe = (_ROOT / "recipe" / "meta.yaml").read_text(encoding="utf-8")

    assert "urllib3" not in recipe
    assert "truststore" not in recipe


def test_rpm_spec_does_not_force_urllib3():
    spec = (_ROOT / "gfal.spec").read_text(encoding="utf-8")

    assert "Requires: python3-urllib3" not in spec
    assert "Requires: python3-truststore" not in spec


def test_rpm_spec_requires_xrdfs_runtime():
    requirements = {
        " ".join(line.split())
        for line in (_ROOT / "gfal.spec").read_text(encoding="utf-8").splitlines()
        if line.startswith("Requires:")
    }

    assert "Requires: xrootd-client" in requirements
    assert "Requires: xrdcl-http" in requirements


def test_base_package_has_no_python_runtime_dependencies():
    pyproject = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    dependencies = pyproject.split("dependencies = [", 1)[1].split("]", 1)[0]

    assert not dependencies.strip()


def test_rpm_does_not_bundle_or_disable_dependency_generation():
    spec = (_ROOT / "gfal.spec").read_text(encoding="utf-8").lower()

    for forbidden in ("fsspec", "aiohttp", "rich-click", "autoreq: no", "pip install"):
        assert forbidden not in spec
    assert "%pyproject_buildrequires" in spec
    assert "%pyproject_wheel" in spec
    assert "%pyproject_install" in spec
