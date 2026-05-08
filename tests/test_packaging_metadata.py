"""Regression tests for packaging metadata."""

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _project_scripts(pyproject_text):
    scripts = {}
    in_scripts = False
    for raw_line in pyproject_text.splitlines():
        line = raw_line.strip()
        if line.startswith("["):
            in_scripts = line == "[project.scripts]"
            continue
        if not in_scripts or not line or line.startswith("#"):
            continue
        name, value = line.split("=", 1)
        scripts[name.strip()] = value.strip().strip('"')
    return scripts


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


def test_gfal2_legacy_console_scripts_are_registered():
    scripts = _project_scripts((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    expected = {
        "gfal",
        "gfal-archivepoll",
        "gfal-bringonline",
        "gfal-cat",
        "gfal-chmod",
        "gfal-copy",
        "gfal-cp",
        "gfal-evict",
        "gfal-ls",
        "gfal-mkdir",
        "gfal-rename",
        "gfal-rm",
        "gfal-save",
        "gfal-stat",
        "gfal-sum",
        "gfal-token",
        "gfal-xattr",
    }

    assert expected <= set(scripts)
    assert {scripts[name] for name in expected} == {"gfal.cli.shell:main"}
