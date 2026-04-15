"""Helpers for deployment-backed integration tests."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from helpers import run_gfal


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class DeploymentConfig:
    name: str
    http_writable_base: Optional[str]
    http_denied_base: Optional[str]
    root_writable_base: Optional[str]
    root_denied_base: Optional[str]
    verify_ssl: bool
    cert: Optional[str]
    key: Optional[str]
    proxy: Optional[str]
    supports_listing: bool

    @property
    def has_http(self) -> bool:
        return bool(self.http_writable_base)

    @property
    def has_root(self) -> bool:
        return bool(self.root_writable_base)


def load_deployment_config() -> Optional[DeploymentConfig]:
    http_writable = os.environ.get("GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE")
    root_writable = os.environ.get("GFAL_DEPLOYMENT_ROOT_WRITABLE_BASE")
    if not http_writable and not root_writable:
        return None

    cert = os.environ.get("GFAL_DEPLOYMENT_CERT")
    key = os.environ.get("GFAL_DEPLOYMENT_KEY") or cert
    proxy = os.environ.get("GFAL_DEPLOYMENT_PROXY") or os.environ.get("X509_USER_PROXY")
    if proxy and not Path(proxy).is_file():
        proxy = None

    return DeploymentConfig(
        name=os.environ.get("GFAL_DEPLOYMENT_NAME", "deployment"),
        http_writable_base=http_writable,
        http_denied_base=os.environ.get("GFAL_DEPLOYMENT_HTTP_DENIED_BASE"),
        root_writable_base=root_writable,
        root_denied_base=os.environ.get("GFAL_DEPLOYMENT_ROOT_DENIED_BASE"),
        verify_ssl=_env_flag("GFAL_DEPLOYMENT_VERIFY_SSL", default=True),
        cert=cert,
        key=key,
        proxy=proxy,
        supports_listing=_env_flag("GFAL_DEPLOYMENT_SUPPORTS_LISTING", default=True),
    )


def deployment_skip_reason() -> str:
    return (
        "No deployment contract configured "
        "(set GFAL_DEPLOYMENT_HTTP_WRITABLE_BASE and/or "
        "GFAL_DEPLOYMENT_ROOT_WRITABLE_BASE)"
    )


def join_remote(base: str, name: str) -> str:
    return base.rstrip("/") + "/" + name


def run_deployment_gfal(
    config: DeploymentConfig,
    cmd: str,
    *args: str,
    stdin_data: Optional[str] = None,
):
    cmd_args = []
    if config.cert:
        cmd_args.extend(["-E", config.cert])
        if config.key and config.key != config.cert:
            cmd_args.extend(["--key", config.key])
    elif config.proxy:
        # Use the proxy as the client certificate for HTTPS mutual-TLS auth
        # (e.g. EOS XrdHttp).  X509_USER_PROXY alone is only picked up by
        # fsspec-xrootd; the WebDAV layer needs -E / --key explicitly.
        cmd_args.extend(["-E", config.proxy, "--key", config.proxy])
    if not config.verify_ssl:
        cmd_args.append("--no-verify")

    env = None
    if config.proxy:
        env = {"X509_USER_PROXY": config.proxy}

    return run_gfal(cmd, *cmd_args, *args, input=stdin_data, env=env)
