"""Unit tests for deployment-backed integration helpers."""

from __future__ import annotations

import deployment_env


def test_run_deployment_gfal_passes_proxy_as_cert_for_https(tmp_path, monkeypatch):
    """When only a proxy is available (no explicit cert/key), the proxy file
    must be forwarded as -E/--key so that gfal's WebDAV layer uses it for
    mutual-TLS auth (e.g. EOS XrdHttp).  X509_USER_PROXY alone is only
    consumed by fsspec-xrootd, not by the HTTP stack."""
    proxy = tmp_path / "proxy.pem"
    proxy.write_text("proxy")

    calls = {}

    def fake_run_gfal(cmd, *args, input=None, env=None):
        calls["cmd"] = cmd
        calls["args"] = args
        calls["input"] = input
        calls["env"] = env
        return 0, "", ""

    monkeypatch.setattr(deployment_env, "run_gfal", fake_run_gfal)

    config = deployment_env.DeploymentConfig(
        name="eos-kind",
        http_writable_base="https://example.test/writable",
        http_denied_base=None,
        http_denied_markers=("Permission denied", "403", "access denied"),
        root_writable_base="root://example.test//writable",
        root_denied_base=None,
        root_denied_markers=("Permission denied", "3010", "access denied"),
        verify_ssl=False,
        cert=None,
        key=None,
        proxy=str(proxy),
        supports_listing=True,
    )

    deployment_env.run_deployment_gfal(config, "cp", "file:///tmp/src", "root://dst")

    assert calls["cmd"] == "cp"
    assert calls["args"] == (
        "-E",
        str(proxy),
        "--key",
        str(proxy),
        "--no-verify",
        "file:///tmp/src",
        "root://dst",
    )
    assert calls["env"] == {"X509_USER_PROXY": str(proxy)}


def test_run_deployment_gfal_prefers_explicit_cert_and_key(tmp_path, monkeypatch):
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    cert.write_text("cert")
    key.write_text("key")

    calls = {}

    def fake_run_gfal(cmd, *args, input=None, env=None):
        calls["cmd"] = cmd
        calls["args"] = args
        calls["input"] = input
        calls["env"] = env
        return 0, "", ""

    monkeypatch.setattr(deployment_env, "run_gfal", fake_run_gfal)

    config = deployment_env.DeploymentConfig(
        name="storm-kind",
        http_writable_base="https://example.test/writable",
        http_denied_base=None,
        http_denied_markers=("Permission denied", "403", "access denied"),
        root_writable_base=None,
        root_denied_base=None,
        root_denied_markers=("Permission denied", "3010", "access denied"),
        verify_ssl=True,
        cert=str(cert),
        key=str(key),
        proxy=None,
        supports_listing=True,
    )

    deployment_env.run_deployment_gfal(
        config, "ls", "https://example.test/writable", stdin_data="ignored"
    )

    assert calls["cmd"] == "ls"
    assert calls["args"] == (
        "-E",
        str(cert),
        "--key",
        str(key),
        "https://example.test/writable",
    )
    assert calls["env"] is None


def test_load_deployment_config_accepts_backend_specific_denied_markers(monkeypatch):
    monkeypatch.setenv("GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE", "https://example.test/w")
    monkeypatch.setenv("GFAL_DEPLOYMENT_HTTP_DENIED_BASE", "https://example.test/d")
    monkeypatch.setenv("GFAL_DEPLOYMENT_HTTP_DENIED_MARKERS", "403, 500 ,HTTP error")

    config = deployment_env.load_deployment_config()

    assert config is not None
    assert config.http_denied_markers == ("403", "500", "HTTP error")
    assert config.root_denied_markers == (
        "Permission denied",
        "3010",
        "access denied",
    )
