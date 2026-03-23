import socket
from unittest.mock import ANY, MagicMock, patch

import pytest

from gfal.cli.base import CommandBase
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


def test_urllib3_patching():
    """Verify that CommandBase.execute patches urllib3 globally."""

    class DummyCommand(CommandBase):
        def execute_test(self):
            return 0

    cmd = DummyCommand()
    cmd.parse(cmd.execute_test, ["gfal-test", "-4"])

    import urllib3.util.connection as nsock

    # Record the original function
    original_gai = nsock.allowed_gai_family

    try:
        # We need to mock the Thread behavior because execute() starts a thread.
        # For testing, we just want to see if the patch is applied.
        with patch("gfal.cli.base.Thread"):
            cmd.execute(cmd.execute_test)
            # The lambda should have been assigned
            assert nsock.allowed_gai_family() == socket.AF_INET
    finally:
        # Restore
        nsock.allowed_gai_family = original_gai

    # Test IPv6
    cmd.parse(cmd.execute_test, ["gfal-test", "-6"])
    try:
        with patch("gfal.cli.base.Thread"):
            cmd.execute(cmd.execute_test)
            assert nsock.allowed_gai_family() == socket.AF_INET6
    finally:
        nsock.allowed_gai_family = original_gai
