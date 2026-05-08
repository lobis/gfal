"""Tests for storage-element token request construction and retrieval."""

from __future__ import annotations

import json

import pytest

from gfal.core import token


class _Response:
    def __init__(self, url, status_code=200, payload=None):
        self.url = url
        self.status_code = status_code
        self.headers = {}
        self.content = payload if payload is not None else b"{}"


class _FakeSession:
    responses = []
    calls = []
    closed = False

    def __init__(self, options):
        self.options = options

    def request(self, method, url, *, headers=None, data=None, timeout=None):
        self.__class__.calls.append({
            "method": method,
            "url": url,
            "headers": headers or {},
            "data": data,
            "timeout": timeout,
        })
        response = self.__class__.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def close(self):
        self.__class__.closed = True


@pytest.fixture(autouse=True)
def fake_session(monkeypatch):
    _FakeSession.responses = []
    _FakeSession.calls = []
    _FakeSession.closed = False
    monkeypatch.setattr(token, "_SyncAiohttpSession", _FakeSession)
    return _FakeSession


def _json_response(url, **payload):
    return _Response(url, payload=json.dumps(payload).encode("utf-8"))


class TestTokenRequestContent:
    def test_default_read_activities(self):
        assert token.default_activities(False) == ["LIST", "DOWNLOAD"]

    def test_default_write_activities(self):
        assert token.default_activities(True) == [
            "LIST",
            "DOWNLOAD",
            "MANAGE",
            "UPLOAD",
            "DELETE",
        ]

    def test_macaroon_request_body_matches_gfal2_spacing(self):
        body = token.macaroon_request_content(60, ["LIST", "DOWNLOAD"])
        assert body == '{"caveats": ["activity:LIST,DOWNLOAD"], "validity": "PT60M"}'
        assert len(body) == 60

    def test_oauth_macaroon_body_uses_gfal2_field_names(self):
        body = token.oauth_macaroon_request_content(
            "/eos/pilot/file", 5, ["LIST", "DOWNLOAD"]
        )
        assert body == (
            "grant_type=client_credentials&expire_in=300&scopes="
            "LIST%3A%2Feos%2Fpilot%2Ffile%20DOWNLOAD%3A%2Feos%2Fpilot%2Ffile"
        )


class TestRetrieveToken:
    def test_posts_macaroon_request_to_target_url(self, fake_session):
        fake_session.responses = [
            _json_response("https://storage.example/eos/file", macaroon="mac-token")
        ]

        result = token.retrieve_token("https://storage.example/eos/file")

        assert result == "mac-token"
        assert fake_session.closed is True
        assert fake_session.calls == [
            {
                "method": "POST",
                "url": "https://storage.example/eos/file",
                "headers": {"Content-Type": "application/macaroon-request"},
                "data": (
                    b'{"caveats": ["activity:LIST,DOWNLOAD"], "validity": "PT60M"}'
                ),
                "timeout": None,
            }
        ]

    def test_write_access_uses_write_activity_set(self, fake_session):
        fake_session.responses = [
            _json_response("https://storage.example/eos/file", macaroon="write-token")
        ]

        result = token.retrieve_token(
            "https://storage.example/eos/file", write_access=True, validity=10
        )

        assert result == "write-token"
        assert fake_session.calls[0]["data"] == (
            b'{"caveats": ["activity:LIST,DOWNLOAD,MANAGE,UPLOAD,DELETE"], '
            b'"validity": "PT10M"}'
        )

    def test_custom_activities_override_write_defaults(self, fake_session):
        fake_session.responses = [
            _json_response("https://storage.example/eos/file", macaroon="custom-token")
        ]

        result = token.retrieve_token(
            "https://storage.example/eos/file",
            write_access=True,
            activities=["READ_METADATA", "LIST"],
        )

        assert result == "custom-token"
        assert fake_session.calls[0]["data"] == (
            b'{"caveats": ["activity:READ_METADATA,LIST"], "validity": "PT60M"}'
        )

    def test_davs_urls_are_normalized_to_https(self, fake_session):
        fake_session.responses = [
            _json_response("https://storage.example/eos/file", macaroon="dav-token")
        ]

        result = token.retrieve_token("davs://storage.example/eos/file")

        assert result == "dav-token"
        assert fake_session.calls[0]["url"] == "https://storage.example/eos/file"

    def test_rejects_non_https_urls(self, fake_session):
        with pytest.raises(ValueError, match="HTTPS"):
            token.retrieve_token("file:///tmp/file")

        assert fake_session.calls == []

    def test_authz_token_is_added_to_request_url(self, fake_session):
        fake_session.responses = [
            _json_response(
                "https://eospilot.cern.ch/eos/file?authz=zteos64%3Aabc",
                macaroon="authz-token",
            )
        ]

        result = token.retrieve_token(
            "https://eospilot.cern.ch/eos/file",
            storage_options={"authz_token": "zteos64:abc"},
        )

        assert result == "authz-token"
        assert fake_session.calls[0]["url"] == (
            "https://eospilot.cern.ch/eos/file?authz=zteos64%3Aabc"
        )

    def test_forwards_timeout_to_requests(self, fake_session):
        fake_session.responses = [
            _json_response("https://storage.example/eos/file", macaroon="tok")
        ]

        token.retrieve_token(
            "https://storage.example/eos/file", storage_options={"timeout": 7}
        )

        assert fake_session.calls[0]["timeout"] == 7

    def test_issuer_first_tries_scitokens_endpoint(self, fake_session):
        fake_session.responses = [
            _json_response(
                "https://issuer.example/.well-known/oauth-authorization-server",
                token_endpoint="https://issuer.example/token",
            ),
            _json_response("https://issuer.example/token", access_token="jwt-token"),
        ]

        result = token.retrieve_token(
            "https://storage.example/eos/file", issuer="https://issuer.example"
        )

        assert result == "jwt-token"
        assert fake_session.calls[0]["method"] == "GET"
        assert fake_session.calls[0]["url"] == (
            "https://issuer.example/.well-known/oauth-authorization-server"
        )
        assert fake_session.calls[1]["method"] == "POST"
        assert fake_session.calls[1]["url"] == "https://issuer.example/token"
        assert fake_session.calls[1]["headers"] == {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        assert fake_session.calls[1]["data"] == b"grant_type=client_credentials"

    def test_issuer_falls_back_to_oauth_macaroon(self, fake_session):
        fake_session.responses = [
            _Response(
                "https://issuer.example/.well-known/oauth-authorization-server",
                status_code=404,
            ),
            _json_response(
                "https://issuer.example/.well-known/oauth-authorization-server",
                token_endpoint="https://issuer.example/macaroon",
            ),
            _json_response("https://issuer.example/macaroon", access_token="oauth-mac"),
        ]

        result = token.retrieve_token(
            "https://storage.example/eos/file",
            issuer="https://issuer.example",
            validity=2,
        )

        assert result == "oauth-mac"
        assert fake_session.calls[2]["url"] == "https://issuer.example/macaroon"
        assert fake_session.calls[2]["data"] == (
            b"grant_type=client_credentials&expire_in=120&scopes="
            b"LIST%3A%2Feos%2Ffile%20DOWNLOAD%3A%2Feos%2Ffile"
        )

    def test_issuer_discovery_falls_back_to_target_macaroon(self, fake_session):
        fake_session.responses = [
            _Response(
                "https://issuer.example/.well-known/oauth-authorization-server",
                status_code=404,
            ),
            _Response(
                "https://issuer.example/.well-known/oauth-authorization-server",
                status_code=404,
            ),
            _Response(
                "https://issuer.example/.well-known/openid-configuration",
                status_code=404,
            ),
            _json_response("https://storage.example/eos/file", macaroon="fallback"),
        ]

        result = token.retrieve_token(
            "https://storage.example/eos/file", issuer="https://issuer.example"
        )

        assert result == "fallback"
        assert fake_session.calls[-1]["url"] == "https://storage.example/eos/file"
        assert fake_session.calls[-1]["headers"] == {
            "Content-Type": "application/macaroon-request"
        }

    def test_metadata_endpoint_keeps_issuer_path(self):
        assert token._metadata_endpoint("https://issuer.example/base") == (
            "https://issuer.example/.well-known/oauth-authorization-server/base"
        )

    def test_openid_configuration_endpoint_keeps_issuer_path(self):
        assert token._openid_configuration_endpoint("https://issuer.example/base") == (
            "https://issuer.example/base/.well-known/openid-configuration"
        )

    def test_missing_response_key_is_an_error(self, fake_session):
        fake_session.responses = [_json_response("https://storage.example/eos/file")]

        with pytest.raises(ValueError, match="macaroon"):
            token.retrieve_token("https://storage.example/eos/file")

    def test_large_response_is_an_error(self, fake_session):
        fake_session.responses = [
            _Response(
                "https://storage.example/eos/file",
                payload=b"x" * token.RESPONSE_MAX_SIZE,
            )
        ]

        with pytest.raises(ValueError, match="maximum size"):
            token.retrieve_token("https://storage.example/eos/file")

    def test_http_error_is_raised(self, fake_session):
        fake_session.responses = [
            _Response("https://storage.example/eos/file", status_code=403)
        ]

        with pytest.raises(token.HttpStatusError):
            token.retrieve_token("https://storage.example/eos/file")
