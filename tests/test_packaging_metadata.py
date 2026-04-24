"""Regression tests for packaging metadata."""

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def test_pip_metadata_does_not_force_urllib3():
    pyproject = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert '"urllib3' not in pyproject


def test_conda_recipe_does_not_force_urllib3():
    recipe = (_ROOT / "recipe" / "meta.yaml").read_text(encoding="utf-8")

    assert "urllib3" not in recipe


def test_rpm_spec_does_not_force_urllib3():
    spec = (_ROOT / "gfal.spec").read_text(encoding="utf-8")

    assert "Requires: python3-urllib3" not in spec
