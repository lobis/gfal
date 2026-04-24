import socket
from unittest.mock import ANY, MagicMock, patch

import pytest

from gfal.cli.base import CommandBase, build_client_kwargs
from gfal.core.fs import _verify_get_client, build_storage_options


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

    kwargs = build_client_kwargs(params)

    assert kwargs["timeout"] == 30
    assert kwargs["ipv4_only"] is True
    assert kwargs["ipv6_only"] is False


def test_build_client_kwargs_sets_cli_app():
    """Verify that build_client_kwargs always sets app='python3-gfal-cli'."""
    params = MagicMock()
    params.cert = None
    params.key = None
    params.timeout = 1800
    params.ssl_verify = True
    params.ipv4_only = False
    params.ipv6_only = False

    kwargs = build_client_kwargs(params)

    assert kwargs["app"] == "python3-gfal-cli"


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
