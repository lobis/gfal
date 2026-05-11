"""Tests for the CERN CA bundle helper used by integration tests."""

import sys
from pathlib import Path

import conftest


def test_base_ca_bundle_prefers_existing_ssl_cert_file(monkeypatch, tmp_path):
    bundle = tmp_path / "conda-ca.pem"
    bundle.write_text("conda-ca")
    monkeypatch.setenv("SSL_CERT_FILE", str(bundle))

    assert conftest._base_ca_bundle() == bundle


def test_base_ca_bundle_ignores_missing_ssl_cert_file(monkeypatch):
    fallback = Path("/fallback/certifi.pem")

    class FakeCertifi:
        @staticmethod
        def where():
            return str(fallback)

    monkeypatch.setenv("SSL_CERT_FILE", "/does/not/exist.pem")
    monkeypatch.setitem(sys.modules, "certifi", FakeCertifi)

    assert conftest._base_ca_bundle() == fallback


def test_write_combined_ca_bundle_appends_cern_ca(tmp_path):
    base = tmp_path / "base.pem"
    cern = tmp_path / "cern.pem"
    combined = tmp_path / "combined.pem"
    base.write_text("base-ca")
    cern.write_text("cern-ca")

    conftest._write_combined_ca_bundle(base, cern, combined)

    assert combined.read_text() == "base-ca\ncern-ca"
