"""Storage-element issued token retrieval helpers."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from urllib.parse import quote, urlparse, urlunparse

from gfal.core.fs import eos_authz_url, normalize_url
from gfal.core.webdav import HttpStatusError, _SyncAiohttpSession

log = logging.getLogger(__name__)

RESPONSE_MAX_SIZE = 1024 * 1024
DEFAULT_TOKEN_VALIDITY = 60
READ_ACTIVITIES = ("LIST", "DOWNLOAD")
WRITE_ACTIVITIES = ("LIST", "DOWNLOAD", "MANAGE", "UPLOAD", "DELETE")


@dataclass(frozen=True)
class TokenRequest:
    """Prepared HTTP token request."""

    method: str
    url: str
    headers: dict[str, str]
    body: str
    token_key: str


def default_activities(write_access: bool) -> list[str]:
    """Return gfal2-compatible default macaroon activities."""
    return list(WRITE_ACTIVITIES if write_access else READ_ACTIVITIES)


def macaroon_request_content(validity: int, activities: list[str]) -> str:
    """Build the gfal2-compatible macaroon request body."""
    return (
        '{"caveats": ["activity:'
        + ",".join(activities)
        + f'"], "validity": "PT{validity}M"}}'
    )


def oauth_macaroon_request_content(
    path: str, validity: int, activities: list[str]
) -> str:
    """Build the OAuth-style macaroon request body used with token issuers."""
    scopes = " ".join(f"{activity}:{path}" for activity in activities)
    return (
        f"grant_type=client_credentials&expire_in={validity * 60}"
        f"&scopes={quote(scopes, safe='')}"
    )


def scitokens_request_content() -> str:
    """Build the SciTokens request body used by gfal2."""
    return "grant_type=client_credentials"


def _format_https_url(url: str) -> str:
    normalized = normalize_url(url)
    parsed = urlparse(normalized)
    if parsed.scheme == "davs":
        parsed = parsed._replace(scheme="https")
    if parsed.scheme != "https":
        raise ValueError("Token request must be done over HTTPS")
    return urlunparse(parsed)


def _request_target(url: str) -> str:
    parsed = urlparse(url)
    target = parsed.path or "/"
    if parsed.query:
        target += f"?{parsed.query}"
    return target


def _metadata_endpoint(issuer: str) -> str:
    parsed = urlparse(_format_https_url(issuer))
    netloc = parsed.netloc
    path = "/.well-known/oauth-authorization-server"
    if parsed.path and parsed.path != "/":
        path += parsed.path
    return urlunparse((parsed.scheme, netloc, path, "", "", ""))


def _openid_configuration_endpoint(issuer: str) -> str:
    parsed = urlparse(_format_https_url(issuer))
    path = parsed.path or "/"
    if not path.endswith("/"):
        path += "/"
    path += ".well-known/openid-configuration"
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def _parse_json_key(body: bytes, key: str) -> str:
    if len(body) >= RESPONSE_MAX_SIZE:
        raise ValueError(
            f"Token response exceeds maximum size: {len(body)} bytes "
            f"(max size = {RESPONSE_MAX_SIZE})"
        )
    if not body:
        raise ValueError("Response with no data")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Response was not valid JSON") from exc
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Response did not include '{key}' key")
    return value


def _raise_for_status(response, description: str) -> None:
    if response.status_code != 200:
        raise HttpStatusError(response.status_code, response.url, response.headers)


def _request_json_key(
    session: _SyncAiohttpSession,
    request: TokenRequest,
    *,
    timeout: float | None,
) -> str:
    log.info("Davix: > %s %s HTTP/1.1", request.method, _request_target(request.url))
    for name, value in request.headers.items():
        log.info("> %s: %s", name, value)
    log.info("> Content-Length: %d", len(request.body.encode("utf-8")))
    response = session.request(
        request.method,
        request.url,
        headers=request.headers,
        data=request.body.encode("utf-8"),
        timeout=timeout,
    )
    log.info("Davix: < HTTP/1.1 %s", response.status_code)
    _raise_for_status(response, request.method)
    return _parse_json_key(response.content, request.token_key)


def _discover_token_endpoint(
    session: _SyncAiohttpSession,
    issuer: str,
    *,
    fallback: bool,
    timeout: float | None,
) -> str:
    urls = [_metadata_endpoint(issuer)]
    if fallback:
        urls.append(_openid_configuration_endpoint(issuer))

    last_error: Exception | None = None
    for url in urls:
        try:
            response = session.request("GET", url, timeout=timeout)
            _raise_for_status(response, "Token endpoint discovery")
            return _parse_json_key(response.content, "token_endpoint")
        except Exception as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    raise ValueError("Invalid or empty token issuer endpoint")


def _activities(write_access: bool, activities: list[str] | None) -> list[str]:
    return list(activities) if activities else default_activities(write_access)


def _prepare_macaroon_request(
    url: str,
    *,
    endpoint: str,
    validity: int,
    activities: list[str],
    oauth: bool,
) -> TokenRequest:
    if oauth:
        path = urlparse(url).path
        return TokenRequest(
            method="POST",
            url=endpoint,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            body=oauth_macaroon_request_content(path, validity, activities),
            token_key="access_token",
        )

    return TokenRequest(
        method="POST",
        url=endpoint,
        headers={"Content-Type": "application/macaroon-request"},
        body=macaroon_request_content(validity, activities),
        token_key="macaroon",
    )


def _prepare_scitokens_request(endpoint: str) -> TokenRequest:
    return TokenRequest(
        method="POST",
        url=endpoint,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        body=scitokens_request_content(),
        token_key="access_token",
    )


def retrieve_token(
    url: str,
    *,
    issuer: str | None = None,
    validity: int = DEFAULT_TOKEN_VALIDITY,
    write_access: bool = False,
    activities: list[str] | None = None,
    storage_options: dict | None = None,
    operation: str | None = None,
) -> str:
    """Retrieve a storage-element issued token for *url*."""
    if validity < 0:
        raise ValueError("Validity must be a number >= 0")

    target_url = _format_https_url(url)
    request_activities = _activities(write_access, activities)
    options = {} if storage_options is None else dict(storage_options)
    authz_token = options.pop("authz_token", None)
    if authz_token:
        target_url = eos_authz_url(target_url, authz_token) or target_url
    timeout = options.get("timeout")

    session = _SyncAiohttpSession(options)
    try:
        if issuer:
            try:
                endpoint = _discover_token_endpoint(
                    session, issuer, fallback=False, timeout=timeout
                )
                request = _prepare_scitokens_request(endpoint)
                retrieved = _request_json_key(session, request, timeout=timeout)
                log.debug(
                    "(SEToken) Set bearer token in credential_map[%s] "
                    "(access=%s) (validity=%s)",
                    target_url,
                    operation or ("write" if write_access else "read"),
                    validity,
                )
                return retrieved
            except Exception:
                pass

            try:
                endpoint = _discover_token_endpoint(
                    session, issuer, fallback=True, timeout=timeout
                )
                request = _prepare_macaroon_request(
                    target_url,
                    endpoint=endpoint,
                    validity=validity,
                    activities=request_activities,
                    oauth=True,
                )
            except Exception:
                request = _prepare_macaroon_request(
                    target_url,
                    endpoint=target_url,
                    validity=validity,
                    activities=request_activities,
                    oauth=False,
                )
        else:
            request = _prepare_macaroon_request(
                target_url,
                endpoint=target_url,
                validity=validity,
                activities=request_activities,
                oauth=False,
            )

        retrieved = _request_json_key(session, request, timeout=timeout)
        log.debug(
            "(SEToken) Set bearer token in credential_map[%s] (access=%s) (validity=%s)",
            target_url,
            operation or ("write" if write_access else "read"),
            validity,
        )
        return retrieved
    finally:
        session.close()
