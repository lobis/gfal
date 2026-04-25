import contextlib
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import ANY, MagicMock, patch

import aiohttp
import pytest

from gfal.cli.base import CommandBase, build_client_kwargs
from gfal.core.fs import _verify_get_client, build_storage_options
from gfal.core.webdav import _SyncAiohttpSession


class _LoopbackHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _ReuseAddrHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


class _IPv6HTTPServer(_ReuseAddrHTTPServer):
    address_family = socket.AF_INET6

    def server_bind(self):
        with contextlib.suppress(AttributeError, OSError):
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        super().server_bind()


@contextlib.contextmanager
def _loopback_server(host, server_class=_ReuseAddrHTTPServer):
    server = server_class((host, 0), _LoopbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _ipv6_loopback_available():
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
        with contextlib.suppress(AttributeError, OSError):
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        try:
            sock.bind(("::1", 0))
        except OSError:
            return False
    return True


def test_ipv4_v6_parsing():
    """Verify that -4/--ipv4 and -6/--ipv6 are correctly parsed."""

    class DummyCommand(CommandBase):
        def execute_test(self):
            return 0

    cmd = DummyCommand()
    # Test IPv4
    cmd.parse(cmd.execute_test, ["gfal-test", "-4"])
    assert cmd.params.ipv4_only is True
    assert cmd.params.ipv6_only is False

    cmd.parse(cmd.execute_test, ["gfal-test", "--ipv4"])
    assert cmd.params.ipv4_only is True

    # Test IPv6
    cmd = DummyCommand()
    cmd.parse(cmd.execute_test, ["gfal-test", "-6"])
    assert cmd.params.ipv4_only is False
    assert cmd.params.ipv6_only is True

    cmd.parse(cmd.execute_test, ["gfal-test", "--ipv6"])
    assert cmd.params.ipv6_only is True


def test_ipv4_and_ipv6_mutually_exclusive_long_options(capsys):
    """Verify that the preferred long options are mutually exclusive."""

    class DummyCommand(CommandBase):
        def execute_test(self):
            return 0

    cmd = DummyCommand()
    with pytest.raises(SystemExit) as excinfo:
        cmd.parse(cmd.execute_test, ["gfal-test", "--ipv4", "--ipv6"])

    assert excinfo.value.code == 2
    assert "--ipv4 and --ipv6 are mutually exclusive" in capsys.readouterr().err


def test_ipv4_and_ipv6_mutually_exclusive_mixed_aliases(capsys):
    """Short aliases should also be rejected when combined with long options."""

    class DummyCommand(CommandBase):
        def execute_test(self):
            return 0

    cmd = DummyCommand()
    with pytest.raises(SystemExit) as excinfo:
        cmd.parse(cmd.execute_test, ["gfal-test", "-4", "--ipv6"])

    assert excinfo.value.code == 2
    assert "--ipv4 and --ipv6 are mutually exclusive" in capsys.readouterr().err


def test_build_storage_options_ipv():
    """Verify that build_storage_options captures IP flags."""
    params = MagicMock()
    params.ipv4_only = True
    params.ipv6_only = False
    opts = build_storage_options(params)
    assert opts["ipv4_only"] is True
    assert "ipv6_only" not in opts or opts["ipv6_only"] is False

    params.ipv4_only = False
    params.ipv6_only = True
    opts = build_storage_options(params)
    assert opts["ipv6_only"] is True
    assert "ipv4_only" not in opts or opts["ipv4_only"] is False


def test_build_client_kwargs_ipv():
    """Verify that build_client_kwargs forwards IP family flags."""
    params = MagicMock()
    params.cert = None
    params.key = None
    params.timeout = 30
    params.ssl_verify = True
    params.ipv4_only = True
    params.ipv6_only = False
    params.authz_token = "zteos64:abc"

    kwargs = build_client_kwargs(params)

    assert kwargs["timeout"] == 30
    assert kwargs["ipv4_only"] is True
    assert kwargs["ipv6_only"] is False
    assert kwargs["authz_token"] == "zteos64:abc"


def test_build_client_kwargs_sets_cli_app():
    """Verify that build_client_kwargs always sets app='python3-gfal-cli'."""
    params = MagicMock()
    params.cert = None
    params.key = None
    params.timeout = 1800
    params.ssl_verify = True
    params.ipv4_only = False
    params.ipv6_only = False
    params.authz_token = None

    kwargs = build_client_kwargs(params)

    assert kwargs["app"] == "python3-gfal-cli"


def test_authz_token_common_arg_parsing():
    """Verify that --authz-token is parsed as a common option."""

    class DummyCommand(CommandBase):
        def execute_test(self):
            return 0

    cmd = DummyCommand()
    cmd.parse(cmd.execute_test, ["gfal-test", "--authz-token", "zteos64:abc"])

    assert cmd.params.authz_token == "zteos64:abc"


@pytest.mark.asyncio
async def test_aiohttp_connector_family():
    """Verify that _verify_get_client passes the correct family to TCPConnector."""
    with (
        patch("aiohttp.TCPConnector") as mock_connector,
        patch("aiohttp.ClientSession"),
    ):
        # Test IPv4
        await _verify_get_client(ipv4_only=True)
        mock_connector.assert_called_with(ssl=ANY, family=socket.AF_INET)

        # Test IPv6
        await _verify_get_client(ipv6_only=True)
        mock_connector.assert_called_with(ssl=ANY, family=socket.AF_INET6)

        # Test Default (Any)
        await _verify_get_client()
        mock_connector.assert_called_with(ssl=ANY, family=0)


def test_execute_accepts_ipv_flags_without_global_patch():
    """The CLI accepts -4/-6 without requiring any urllib3 hook."""

    class DummyCommand(CommandBase):
        def execute_test(self, _command):
            return 0

    DummyCommand.execute_test.is_interactive = True

    cmd = DummyCommand()
    cmd.parse(cmd.execute_test, ["gfal-test", "-4"])
    assert cmd.execute(cmd.execute_test) == 0

    cmd = DummyCommand()
    cmd.parse(cmd.execute_test, ["gfal-test", "-6"])
    assert cmd.execute(cmd.execute_test) == 0


@pytest.mark.asyncio
async def test_aiohttp_ipv4_only_reaches_ipv4_loopback():
    """The fsspec HTTP client can connect to an IPv4-only endpoint with -4."""
    with _loopback_server("127.0.0.1") as port:
        session = await _verify_get_client(
            verify=False,
            ipv4_only=True,
            timeout=2,
        )
        try:
            async with session.get(f"http://127.0.0.1:{port}/") as response:
                assert response.status == 200
                assert await response.text() == "ok"
        finally:
            await session.close()


@pytest.mark.asyncio
async def test_aiohttp_ipv4_only_does_not_connect_to_ipv6_literal():
    """The fsspec HTTP client does not silently fall back to IPv6 under -4."""
    if not _ipv6_loopback_available():
        pytest.skip("IPv6 loopback is not available")

    with _loopback_server("::1", _IPv6HTTPServer) as port:
        session = await _verify_get_client(
            verify=False,
            ipv4_only=True,
            timeout=2,
        )
        try:
            # aiohttp 3.13 can raise AssertionError after an address-family
            # mismatch leaves it with no usable address candidates.
            with pytest.raises((aiohttp.ClientError, OSError, AssertionError)):
                await session.get(f"http://[::1]:{port}/")
        finally:
            await session.close()


@pytest.mark.asyncio
async def test_aiohttp_ipv6_only_reaches_ipv6_loopback():
    """The fsspec HTTP client can connect to an IPv6-only endpoint with -6."""
    if not _ipv6_loopback_available():
        pytest.skip("IPv6 loopback is not available")

    with _loopback_server("::1", _IPv6HTTPServer) as port:
        session = await _verify_get_client(
            verify=False,
            ipv6_only=True,
            timeout=2,
        )
        try:
            async with session.get(f"http://[::1]:{port}/") as response:
                assert response.status == 200
                assert await response.text() == "ok"
        finally:
            await session.close()


def test_webdav_ipv4_only_reaches_ipv4_loopback():
    """The WebDAV control session can connect to an IPv4-only endpoint with -4."""
    with _loopback_server("127.0.0.1") as port:
        session = _SyncAiohttpSession({
            "ssl_verify": False,
            "ipv4_only": True,
            "timeout": 2,
        })
        try:
            response = session.request("GET", f"http://127.0.0.1:{port}/")
            assert response.status_code == 200
            assert response.content == b"ok"
        finally:
            session.close()


def test_webdav_ipv6_only_reaches_ipv6_loopback():
    """The WebDAV control session can connect to an IPv6-only endpoint with -6."""
    if not _ipv6_loopback_available():
        pytest.skip("IPv6 loopback is not available")

    with _loopback_server("::1", _IPv6HTTPServer) as port:
        session = _SyncAiohttpSession({
            "ssl_verify": False,
            "ipv6_only": True,
            "timeout": 2,
        })
        try:
            response = session.request("GET", f"http://[::1]:{port}/")
            assert response.status_code == 200
            assert response.content == b"ok"
        finally:
            session.close()
