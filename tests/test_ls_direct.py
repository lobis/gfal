"""Direct unit tests for CommandLs execute_ls (src/gfal/cli/ls.py).

These tests instantiate CommandLs and set self.params directly (as a
SimpleNamespace) to avoid subprocess overhead and ensure coverage is collected
in the pytest process.
"""

import stat
from types import SimpleNamespace
from unittest.mock import patch

from gfal.cli.ls import (
    CommandLs,
    _apply_sort,
    _fmt_full_iso,
    _fmt_iso,
    _fmt_locale,
    _fmt_long_iso,
    _human_size,
)


def _make_cmd():
    cmd = CommandLs()
    cmd.prog = "gfal-ls"
    return cmd


def _default_params(**kwargs):
    defaults = {
        "cert": None,
        "key": None,
        "timeout": 1800,
        "ssl_verify": True,
        "verbose": 0,
        "log_file": None,
        "long": False,
        "human_readable": False,
        "directory": False,
        "all": False,
        "recursive": False,
        "sort": "name",
        "reverse": False,
        "time_style": "locale",
        "full_time": False,
        "color": "never",
        "xattr": None,
        "ipv4_only": False,
        "ipv6_only": False,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# Time formatters
# ---------------------------------------------------------------------------


class TestTimeFormatters:
    def test_fmt_full_iso(self):
        result = _fmt_full_iso(0)
        assert "+0000" in result

    def test_fmt_long_iso(self):
        result = _fmt_long_iso(0)
        assert "-" in result  # YYYY-MM-DD format

    def test_fmt_iso_recent(self):
        import time

        recent = time.time() - 3600  # 1 hour ago
        result = _fmt_iso(recent)
        assert ":" in result  # HH:MM format for recent files

    def test_fmt_iso_old(self):
        result = _fmt_iso(0)  # epoch - definitely > 180 days ago
        assert "-" in result  # YYYY-MM-DD format for old files

    def test_fmt_locale_recent(self):
        import time

        recent = time.time() - 3600
        result = _fmt_locale(recent)
        assert ":" in result  # HH:MM for recent files

    def test_fmt_locale_old(self):
        result = _fmt_locale(0)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# _human_size
# ---------------------------------------------------------------------------


class TestHumanSize:
    def test_bytes(self):
        result_0 = _human_size(0)
        assert result_0 in ("0", "0.0")
        result_500 = _human_size(500)
        assert result_500 in ("500", "500.0")

    def test_kilobytes(self):
        result = _human_size(1024)
        assert "K" in result or "1" in result

    def test_megabytes(self):
        result = _human_size(1024 * 1024)
        assert "M" in result

    def test_gigabytes(self):
        result = _human_size(1024**3)
        assert "G" in result

    def test_large_bytes_no_decimal(self):
        result = _human_size(1025)
        assert "K" in result


# ---------------------------------------------------------------------------
# _apply_sort
# ---------------------------------------------------------------------------


class TestApplySort:
    def _make_entry(self, name, size=100, mtime=0):
        return {"name": name, "size": size, "mtime": mtime, "type": "file"}

    def test_sort_name(self):
        entries = [self._make_entry("c"), self._make_entry("a"), self._make_entry("b")]
        result = _apply_sort(entries, "name", False)
        names = [e["name"] for e in result]
        assert names == sorted(names, key=lambda n: n.lower())

    def test_sort_name_reverse(self):
        entries = [self._make_entry("a"), self._make_entry("c"), self._make_entry("b")]
        result = _apply_sort(entries, "name", True)
        names = [e["name"] for e in result]
        assert names[0] > names[-1]

    def test_sort_size(self):
        entries = [
            self._make_entry("a", size=10),
            self._make_entry("b", size=100),
            self._make_entry("c", size=50),
        ]
        result = _apply_sort(entries, "size", False)
        sizes = [e["size"] for e in result]
        assert sizes == [100, 50, 10]  # largest first by default

    def test_sort_size_reverse(self):
        entries = [
            self._make_entry("a", size=100),
            self._make_entry("b", size=10),
        ]
        result = _apply_sort(entries, "size", True)
        assert result[0]["size"] == 10  # smallest first when reversed

    def test_sort_time(self):
        entries = [
            self._make_entry("a", mtime=100),
            self._make_entry("b", mtime=300),
            self._make_entry("c", mtime=200),
        ]
        result = _apply_sort(entries, "time", False)
        mtimes = [e["mtime"] for e in result]
        assert mtimes == [300, 200, 100]  # newest first

    def test_sort_none(self):
        entries = [self._make_entry("a"), self._make_entry("b"), self._make_entry("c")]
        result = _apply_sort(entries, "none", False)
        # Same order as input
        assert [e["name"] for e in result] == ["a", "b", "c"]

    def test_sort_none_reverse(self):
        entries = [self._make_entry("a"), self._make_entry("b"), self._make_entry("c")]
        result = _apply_sort(entries, "none", True)
        assert [e["name"] for e in result] == ["c", "b", "a"]

    def test_sort_version(self):
        entries = [
            self._make_entry("file10.txt"),
            self._make_entry("file2.txt"),
            self._make_entry("file1.txt"),
        ]
        result = _apply_sort(entries, "version", False)
        names = [e["name"] for e in result]
        assert names.index("file1.txt") < names.index("file10.txt")

    def test_sort_extension(self):
        entries = [
            self._make_entry("file.txt"),
            self._make_entry("file.py"),
            self._make_entry("file.md"),
        ]
        result = _apply_sort(entries, "extension", False)
        assert len(result) == 3  # just verify it doesn't crash


# ---------------------------------------------------------------------------
# execute_ls
# ---------------------------------------------------------------------------


class TestExecuteLs:
    def test_ls_directory(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.txt").write_text("b")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[tmp_path.as_uri()])
        rc = cmd.execute_ls()
        captured = capsys.readouterr()
        assert rc == 0
        assert "a.txt" in captured.out
        assert "b.txt" in captured.out

    def test_ls_long(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "file.txt"
        f.write_bytes(b"x" * 100)
        cmd = _make_cmd()
        cmd.params = _default_params(file=[tmp_path.as_uri()], long=True)
        rc = cmd.execute_ls()
        captured = capsys.readouterr()
        assert rc == 0
        assert "100" in captured.out

    def test_ls_human_readable(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "big.bin"
        f.write_bytes(b"x" * 1025)
        cmd = _make_cmd()
        cmd.params = _default_params(
            file=[tmp_path.as_uri()],
            long=True,
            human_readable=True,
        )
        rc = cmd.execute_ls()
        captured = capsys.readouterr()
        assert rc == 0
        assert "K" in captured.out or "1" in captured.out

    def test_ls_directory_flag(self, tmp_path, capsys, monkeypatch):
        """With --directory (-d), list the directory itself, not its contents."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[tmp_path.as_uri()], directory=True)
        rc = cmd.execute_ls()
        assert rc == 0

    def test_ls_nonexistent_returns_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[(tmp_path / "no_such").as_uri()])
        rc = cmd.execute_ls()
        assert rc != 0

    def test_ls_multiple_dirs(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        d1 = tmp_path / "dir1"
        d2 = tmp_path / "dir2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file1.txt").write_text("a")
        (d2 / "file2.txt").write_text("b")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[d1.as_uri(), d2.as_uri()])
        rc = cmd.execute_ls()
        captured = capsys.readouterr()
        assert rc == 0
        assert "file1.txt" in captured.out
        assert "file2.txt" in captured.out

    def test_ls_file_directly(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "solo.txt"
        f.write_bytes(b"hello")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[f.as_uri()])
        rc = cmd.execute_ls()
        captured = capsys.readouterr()
        assert rc == 0
        assert captured.out.splitlines() == ["solo.txt"]

    def test_ls_file_directly_when_backend_ls_raises_enoent(self, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        cmd = _make_cmd()
        remote_path = "sftp://host/remote/solo.txt"
        cmd.params = _default_params(file=[remote_path])
        file_info = {
            "name": "/remote/solo.txt",
            "type": "file",
            "size": 5,
            "mode": stat.S_IFREG | 0o644,
            "uid": 0,
            "gid": 0,
            "nlink": 1,
            "mtime": 0,
        }
        mock_stat = SimpleNamespace(
            st_mode=file_info["mode"],
            st_size=file_info["size"],
            st_uid=file_info["uid"],
            st_gid=file_info["gid"],
            st_atime=0,
            st_mtime=file_info["mtime"],
            st_ctime=0,
            info=file_info,
        )

        with (
            patch("gfal.cli.ls.GfalClient") as mock_client_cls,
            patch(
                "gfal.cli.ls.fs.url_to_fs", return_value=(object(), "/remote/solo.txt")
            ),
        ):
            mock_client = mock_client_cls.return_value
            mock_client.stat.return_value = mock_stat
            mock_client.ls.return_value = [mock_stat]
            rc = cmd.execute_ls()

        captured = capsys.readouterr()
        assert rc == 0
        assert "solo.txt" in captured.out

    def test_ls_full_time(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "f.txt"
        f.write_bytes(b"x")
        cmd = _make_cmd()
        cmd.params = _default_params(
            file=[tmp_path.as_uri()], full_time=True, long=True
        )
        # full_time sets time_style to "long-iso" in execute_ls
        rc = cmd.execute_ls()
        assert rc == 0

    def test_ls_all_flag_shows_hidden(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        hidden = tmp_path / ".hidden"
        hidden.write_text("x")
        normal = tmp_path / "normal.txt"
        normal.write_text("y")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[tmp_path.as_uri()], all=True)
        rc = cmd.execute_ls()
        captured = capsys.readouterr()
        assert rc == 0
        assert ".hidden" in captured.out

    def test_ls_sort_by_size(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        (tmp_path / "big.txt").write_bytes(b"x" * 1000)
        (tmp_path / "small.txt").write_bytes(b"x")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[tmp_path.as_uri()], sort="size")
        rc = cmd.execute_ls()
        assert rc == 0

    def test_ls_reverse(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "z.txt").write_text("z")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[tmp_path.as_uri()], reverse=True)
        rc = cmd.execute_ls()
        captured = capsys.readouterr()
        assert rc == 0
        # reversed: z.txt should appear before a.txt
        assert captured.out.index("z.txt") < captured.out.index("a.txt")

    def test_ls_empty_directory(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        d = tmp_path / "empty"
        d.mkdir()
        cmd = _make_cmd()
        cmd.params = _default_params(file=[d.as_uri()])
        rc = cmd.execute_ls()
        assert rc == 0

    def test_ls_passes_ipv6_only_to_client(self, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        cmd = _make_cmd()
        cmd.params = _default_params(file=["file:///tmp"], ipv6_only=True)
        fake_stat = SimpleNamespace(
            st_mode=stat.S_IFDIR | 0o755,
            st_size=0,
            st_uid=0,
            st_gid=0,
            st_atime=0,
            st_mtime=0,
            st_ctime=0,
            info={"name": "/tmp", "type": "directory"},
        )

        with patch("gfal.cli.ls.GfalClient") as mock_client_cls:
            mock_client_cls.return_value.stat.return_value = fake_stat
            mock_client_cls.return_value.ls.return_value = []
            rc = cmd.execute_ls()

        assert rc == 0
        assert mock_client_cls.call_args.kwargs["ipv6_only"] is True
        assert mock_client_cls.call_args.kwargs["ipv4_only"] is False


# ---------------------------------------------------------------------------
# _print_entry
# ---------------------------------------------------------------------------


class TestPrintEntry:
    def test_long_format(self, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        from gfal.core.fs import StatInfo

        cmd = _make_cmd()
        cmd.params = _default_params(
            long=True,
            human_readable=False,
            time_style="locale",
            color="never",
            xattr=None,
        )
        info = {
            "name": "/tmp/test.txt",
            "size": 100,
            "type": "file",
            "mtime": 0,
            "mode": stat.S_IFREG | 0o644,
            "uid": 1000,
            "gid": 1000,
            "nlink": 1,
        }
        st = StatInfo(info)
        cmd._print_entry("test.txt", st, {})
        captured = capsys.readouterr()
        assert "100" in captured.out
        assert "test.txt" in captured.out

    def test_short_format(self, capsys):
        from gfal.core.fs import StatInfo

        cmd = _make_cmd()
        cmd.params = _default_params(
            long=False,
            color="never",
        )
        info = {
            "name": "/tmp/test.txt",
            "size": 100,
            "type": "file",
            "mtime": 0,
            "mode": stat.S_IFREG | 0o644,
        }
        st = StatInfo(info)
        cmd._print_entry("test.txt", st)
        captured = capsys.readouterr()
        assert "test.txt" in captured.out


# ---------------------------------------------------------------------------
# Additional tests for ls.py: colorize, multi-dir headers, empty dir
# ---------------------------------------------------------------------------


class TestColorize:
    def test_color_never_returns_name_unchanged(self):

        cmd = _make_cmd()
        cmd.params = _default_params(color="never")
        result = cmd._colorize("test.txt", stat.S_IFREG | 0o644)
        assert result == "test.txt"

    def test_color_auto_not_tty_returns_name_unchanged(self):
        """With color=auto in a non-tty environment, name is returned unchanged."""

        cmd = _make_cmd()
        cmd.params = _default_params(color="auto")
        # sys.stdout in test is not a tty
        result = cmd._colorize("test.txt", stat.S_IFREG | 0o644)
        assert result == "test.txt"

    def test_color_always_with_no_ls_colors(self, monkeypatch):
        """With color=always but empty LS_COLORS, name is returned unchanged."""
        monkeypatch.setenv("LS_COLORS", "")
        from gfal.cli.ls import CommandLs

        cmd = CommandLs()
        cmd.prog = "gfal-ls"
        cmd.params = _default_params(color="always")
        result = cmd._colorize("test.txt", stat.S_IFREG | 0o644)
        assert result == "test.txt"


class TestExecuteLsMultipleHeaders:
    def test_two_dirs_print_headers(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        d1 = tmp_path / "dir1"
        d2 = tmp_path / "dir2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "f1.txt").write_text("a")
        (d2 / "f2.txt").write_text("b")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[d1.as_uri(), d2.as_uri()])
        rc = cmd.execute_ls()
        captured = capsys.readouterr()
        assert rc == 0
        # Both directory headers should appear
        assert "dir1" in captured.out
        assert "dir2" in captured.out

    def test_empty_dir_with_multiple_args(self, tmp_path, capsys, monkeypatch):
        """An empty directory listed alongside other dirs should show empty output."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        empty = tmp_path / "empty"
        nonempty = tmp_path / "nonempty"
        empty.mkdir()
        nonempty.mkdir()
        (nonempty / "file.txt").write_text("x")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[empty.as_uri(), nonempty.as_uri()])
        rc = cmd.execute_ls()
        assert rc == 0

    def test_directory_flag_with_multiple_args(self, tmp_path, capsys, monkeypatch):
        """--directory (-d) with multiple URIs should list each URI itself."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        cmd = _make_cmd()
        cmd.params = _default_params(file=[d1.as_uri(), d2.as_uri()], directory=True)
        rc = cmd.execute_ls()
        assert rc == 0

    def test_long_format_with_full_iso_time(self, tmp_path, capsys, monkeypatch):
        """Long format with full-iso time style."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "f.txt"
        f.write_bytes(b"x")
        cmd = _make_cmd()
        cmd.params = _default_params(
            file=[tmp_path.as_uri()], long=True, time_style="full-iso"
        )
        rc = cmd.execute_ls()
        assert rc == 0

    def test_long_format_with_long_iso_time(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "f.txt"
        f.write_bytes(b"x")
        cmd = _make_cmd()
        cmd.params = _default_params(
            file=[tmp_path.as_uri()], long=True, time_style="long-iso"
        )
        rc = cmd.execute_ls()
        assert rc == 0

    def test_long_format_with_iso_time(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "f.txt"
        f.write_bytes(b"x")
        cmd = _make_cmd()
        cmd.params = _default_params(
            file=[tmp_path.as_uri()], long=True, time_style="iso"
        )
        rc = cmd.execute_ls()
        assert rc == 0


# ---------------------------------------------------------------------------
# Colorize with color dict populated
# ---------------------------------------------------------------------------


class TestColorizeWithColors:
    """Test _colorize when _color_dict has entries."""

    def test_colorize_directory(self, monkeypatch):
        """With color=always and di= in _color_dict, directories get ANSI codes."""
        import gfal.cli.ls as ls_mod

        monkeypatch.setattr(ls_mod, "_color_dict", {"di": "01;34", "fi": "00;32"})
        cmd = _make_cmd()
        cmd.params = _default_params(color="always")
        result = cmd._colorize("mydir", stat.S_IFDIR | 0o755)
        assert "\033[" in result
        assert "mydir" in result

    def test_colorize_regular_file(self, monkeypatch):
        """With color=always and fi= in _color_dict, regular files get ANSI codes."""
        import gfal.cli.ls as ls_mod

        monkeypatch.setattr(ls_mod, "_color_dict", {"fi": "00;32"})
        cmd = _make_cmd()
        cmd.params = _default_params(color="always")
        result = cmd._colorize("myfile.txt", stat.S_IFREG | 0o644)
        assert "\033[" in result

    def test_colorize_executable(self, monkeypatch):
        """With color=always and ex= in _color_dict, executables get ANSI codes."""
        import gfal.cli.ls as ls_mod

        monkeypatch.setattr(ls_mod, "_color_dict", {"ex": "01;32"})
        cmd = _make_cmd()
        cmd.params = _default_params(color="always")
        result = cmd._colorize("script.sh", stat.S_IFREG | 0o755)
        assert "\033[" in result

    def test_colorize_symlink(self, monkeypatch):
        """With color=always and ln= in _color_dict, symlinks get ANSI codes."""
        import gfal.cli.ls as ls_mod

        monkeypatch.setattr(ls_mod, "_color_dict", {"ln": "01;36"})
        cmd = _make_cmd()
        cmd.params = _default_params(color="always")
        result = cmd._colorize("link.txt", stat.S_IFLNK | 0o777)
        assert "\033[" in result

    def test_colorize_none_mode(self, monkeypatch):
        """With color=always and no= in _color_dict, None mode gets ANSI codes."""
        import gfal.cli.ls as ls_mod

        monkeypatch.setattr(ls_mod, "_color_dict", {"no": "00"})
        cmd = _make_cmd()
        cmd.params = _default_params(color="always")
        result = cmd._colorize("file.txt", None)
        assert "\033[" in result

    def test_colorize_extension_match(self, monkeypatch):
        """Extension-based color matching with *.ext in _color_dict."""
        import gfal.cli.ls as ls_mod

        monkeypatch.setattr(ls_mod, "_color_dict", {"*.py": "01;33"})
        cmd = _make_cmd()
        cmd.params = _default_params(color="always")
        result = cmd._colorize("script.py", stat.S_IFREG | 0o644)
        assert "\033[" in result


# ---------------------------------------------------------------------------
# Long format with xattr column
# ---------------------------------------------------------------------------


class TestLsLongXattr:
    def test_long_no_xattr_requested(self, tmp_path, capsys, monkeypatch):
        """Without --xattr, _fetch_xattrs returns empty dict."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "f.txt"
        f.write_bytes(b"x")
        cmd = _make_cmd()
        cmd.params = _default_params(
            file=[tmp_path.as_uri()],
            long=True,
            xattr=None,  # no xattr requested
        )
        rc = cmd.execute_ls()
        assert rc == 0


# ---------------------------------------------------------------------------
# Additional _list_one edge case tests
# ---------------------------------------------------------------------------


class TestListOneEdgeCases:
    def test_two_files_is_self_only_not_first(self, tmp_path, capsys, monkeypatch):
        """When listing two files (not dirs), second entry has not-first header."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f1 = tmp_path / "file1.txt"
        f2 = tmp_path / "file2.txt"
        f1.write_bytes(b"a")
        f2.write_bytes(b"b")
        cmd = _make_cmd()
        # Multiple files → multi=True → print_header=True
        cmd.params = _default_params(file=[f1.as_uri(), f2.as_uri()])
        rc = cmd.execute_ls()
        assert rc == 0
        captured = capsys.readouterr()
        # Both file names should appear in output
        assert "file1.txt" in captured.out
        assert "file2.txt" in captured.out

    def test_hidden_files_not_shown_when_all_false(self, tmp_path, capsys, monkeypatch):
        """Hidden files should not appear when all=False."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        hidden = tmp_path / ".hidden_file"
        visible = tmp_path / "visible.txt"
        hidden.write_text("h")
        visible.write_text("v")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[tmp_path.as_uri()], all=False)
        rc = cmd.execute_ls()
        captured = capsys.readouterr()
        assert rc == 0
        assert ".hidden_file" not in captured.out
        assert "visible.txt" in captured.out

    def test_long_format_with_xattr_list(self, tmp_path, capsys, monkeypatch):
        """With long=True and xattr=['user.test'], _fetch_xattrs is called."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "f.txt"
        f.write_bytes(b"x")
        cmd = _make_cmd()
        # Even though local fs doesn't support xattr, the suppress catches it
        cmd.params = _default_params(
            file=[tmp_path.as_uri()],
            long=True,
            xattr=["user.test"],
        )
        rc = cmd.execute_ls()
        assert rc == 0

    def test_long_format_xattr_directory_flag(self, tmp_path, capsys, monkeypatch):
        """With directory=True, long=True, and xattr, _fetch_xattrs is also called."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        cmd = _make_cmd()
        cmd.params = _default_params(
            file=[tmp_path.as_uri()],
            long=True,
            directory=True,
            xattr=["user.test"],
        )
        rc = cmd.execute_ls()
        assert rc == 0

    def test_ls_error_propagates_to_rc(self, tmp_path, monkeypatch):
        """When _list_one raises a non-EPIPE exception, rc should be set to 1."""
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        cmd = _make_cmd()
        # Mix: valid dir + nonexistent
        (tmp_path / "a.txt").write_text("a")
        cmd.params = _default_params(
            file=[
                (tmp_path / "a.txt").as_uri(),
                (tmp_path / "no_such.txt").as_uri(),
            ]
        )
        rc = cmd.execute_ls()
        assert rc != 0
