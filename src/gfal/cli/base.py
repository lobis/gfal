"""
Base class and shared infrastructure for all gfal-cli commands.
"""

import contextlib
import errno
import logging
import os
import signal
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from urllib.parse import urlparse

try:
    import rich_click as click
except ImportError:
    import click  # type: ignore[no-redef]

from rich.console import Console

from gfal.core.errors import (
    GfalFileExistsError,
    GfalFileNotFoundError,
    GfalIsADirectoryError,
    GfalNotADirectoryError,
    GfalPermissionError,
    GfalTimeoutError,
    is_xrootd_not_found_message,
    is_xrootd_permission_message,
)


def exception_exit_code(e: Exception) -> int:
    """Return the most appropriate process exit code for an exception.

    Priority:
    1a. Gfal custom exception types → canonical POSIX errno.
    1b. aiohttp SSL / connection errors (checked early because
        ``ClientConnectorCertificateError.errno`` can be 1 on Linux).
    1c. ``e.errno`` — explicit POSIX errno already set (OSError).
    2.  Python built-in exception type → corresponding errno.
    3.  HTTP status code (aiohttp ``ClientResponseError``) → POSIX errno.
    4.  XRootD permission-denied messages → EACCES.
    5.  Fallback: 1.
    """
    # 1a. GfalError subclasses carry well-known semantic meaning; map them to
    #     their canonical POSIX errno regardless of platform (avoids Windows
    #     WSA codes like WSAETIMEDOUT=10060 which exceed the 0-255 range).
    _gfal_type_map = (
        (GfalFileNotFoundError, errno.ENOENT),
        (GfalPermissionError, errno.EACCES),
        (GfalFileExistsError, errno.EEXIST),
        (GfalIsADirectoryError, errno.EISDIR),
        (GfalNotADirectoryError, errno.ENOTDIR),
        (GfalTimeoutError, errno.ETIMEDOUT),
    )
    for exc_type, code in _gfal_type_map:
        if isinstance(e, exc_type):
            return code

    # 1b. aiohttp SSL / connection errors — checked before the generic errno
    #    test because ClientConnectorCertificateError inherits from OSError
    #    and may carry a misleading errno value (e.g. 1 from the SSL error
    #    number on Linux) that would otherwise short-circuit to exit code 1.
    #    Map to EHOSTDOWN to match gfal2/neon/davix behaviour ("Host is down"
    #    for all connection failures including cert mismatches).
    try:
        import aiohttp as _aiohttp

        if isinstance(e, _aiohttp.ClientSSLError):
            # Catches ClientConnectorCertificateError and ClientConnectorSSLError.
            return errno.EHOSTDOWN
        if isinstance(e, _aiohttp.ClientConnectionError):
            # Any other aiohttp connection-level error without an OS errno.
            return errno.ECONNREFUSED
    except ImportError:
        pass

    # 1c. Explicit errno attribute (standard OSError and
    #    aiohttp.ClientConnectorError which inherits from OSError and
    #    stores the underlying OS errno directly on self.errno).
    ecode = getattr(e, "errno", None)
    if isinstance(ecode, int) and 0 < ecode <= 255:
        return ecode

    # 2. Python built-in exception type mapping (fsspec often raises these
    #    with just a message, leaving errno=None)
    _type_map = (
        (FileNotFoundError, errno.ENOENT),
        (PermissionError, errno.EACCES),
        (FileExistsError, errno.EEXIST),
        (IsADirectoryError, errno.EISDIR),
        (NotADirectoryError, errno.ENOTDIR),
        (TimeoutError, errno.ETIMEDOUT),
        (InterruptedError, errno.EINTR),
        (ConnectionRefusedError, errno.ECONNREFUSED),
        (ConnectionResetError, errno.ECONNRESET),
    )
    for exc_type, code in _type_map:
        if isinstance(e, exc_type):
            return code

    # 3. HTTP status code → POSIX errno (aiohttp ClientResponseError, etc.)
    status = getattr(e, "status", None)
    if isinstance(status, int):
        _http_map: dict[int, int] = {
            400: errno.EINVAL,  # Bad Request
            401: errno.EACCES,  # Unauthorized
            403: errno.EACCES,  # Forbidden
            404: errno.ENOENT,  # Not Found
            408: errno.ETIMEDOUT,  # Request Timeout
            409: errno.EEXIST,  # Conflict
            410: errno.ENOENT,  # Gone
            413: errno.EFBIG,  # Payload Too Large
            423: errno.EACCES,  # Locked
            500: errno.EIO,  # Internal Server Error
            502: errno.EIO,  # Bad Gateway
            503: errno.EAGAIN,  # Service Unavailable
            504: errno.ETIMEDOUT,  # Gateway Timeout
        }
        mapped = _http_map.get(status)
        if mapped is not None:
            return mapped

    # 4. XRootD error messages
    if is_xrootd_not_found_message(str(e)):
        return errno.ENOENT

    # 5. XRootD permission-denied messages
    if is_xrootd_permission_message(str(e)):
        return errno.EACCES

    return 1


try:
    VERSION = _pkg_version("gfal")
except PackageNotFoundError:
    VERSION = "0.0.0+unknown"


# ---------------------------------------------------------------------------
# rich-click configuration (only when rich-click is installed)
# ---------------------------------------------------------------------------

if hasattr(click, "rich_click"):
    click.rich_click.TEXT_MARKUP = "rich"
    click.rich_click.SHOW_ARGUMENTS = True
    click.rich_click.GROUP_ARGUMENTS_OPTIONS = False
    click.rich_click.STYLE_ERRORS_SUGGESTION = "italic"
    click.rich_click.ERRORS_SUGGESTION = ""
    click.rich_click.MAX_WIDTH = 100
    click.rich_click.STYLE_OPTIONS_PANEL_BORDER = "dim"
    click.rich_click.STYLE_HELPTEXT = ""


# ---------------------------------------------------------------------------
# @arg and @interactive decorators (mirrors gfal2-util API)
# ---------------------------------------------------------------------------


def arg(*args, **kwargs):
    """Decorator that attaches argparse-style argument specs to an execute_* method."""

    def _decorator(func):
        if not hasattr(func, "arguments"):
            func.arguments = []
        if (args, kwargs) not in func.arguments:
            func.arguments.insert(0, (args, kwargs))
        return func

    return _decorator


def interactive(func):
    """Decorator that marks an execute_* method as interactive (must run in main thread)."""
    func.is_interactive = True
    return func


# ---------------------------------------------------------------------------
# URL normalisation helper
# ---------------------------------------------------------------------------


def surl(value):
    """
    Argparse/Click type converter: turns bare paths into file:// URLs.
    Passes '-' (stdin/stdout sentinel) through unchanged.
    """
    if value == "-":
        return value
    parsed = urlparse(value)
    # A single-char scheme is a Windows drive letter (e.g. "C:"), not a real URL scheme
    if not parsed.scheme or len(parsed.scheme) == 1:
        p = Path(value)
        if not p.is_absolute():
            p = Path.cwd() / p
        return p.as_uri()
    return value


class SurlParamType(click.ParamType):
    """Click ParamType that converts bare paths to file:// URIs."""

    name = "surl"

    def convert(self, value, param, ctx):
        if value is None:
            return None
        return surl(value)


SURL = SurlParamType()


def is_gfal2_compat():
    """
    Check if we should be in strict gfal2 compatibility mode.
    This disables modern output/colors and reverts to legacy formatting.
    """
    return os.getenv("GFAL_CLI_GFAL2") in ("1", "true", "TRUE", "yes")


def get_console(stderr=False):
    """
    Returns a rich Console, but with formatting/color disabled
    if the environment requests gfal2 compatibility.
    """
    if is_gfal2_compat():
        # Disable markup, emoji, and colors
        return Console(
            force_terminal=False,
            color_system=None,
            highlight=False,
            markup=False,
            emoji=False,
            stderr=stderr,
        )
    return Console(stderr=stderr)


def build_client_kwargs(params):
    """Build common ``GfalClient`` kwargs from parsed CLI params."""
    return {
        "cert": getattr(params, "cert", None),
        "key": getattr(params, "key", None),
        "timeout": getattr(params, "timeout", 1800),
        "ssl_verify": getattr(params, "ssl_verify", True),
        "ipv4_only": getattr(params, "ipv4_only", False),
        "ipv6_only": getattr(params, "ipv6_only", False),
        "app": "python3-gfal-cli",
    }


# ---------------------------------------------------------------------------
# Argparse → Click translator
# ---------------------------------------------------------------------------


def _argparse_to_click_params(arguments):
    """
    Translate a list of (args, kwargs) argparse specs (from @arg decorators)
    into intermediate spec dicts for building Click Param objects.

    Returns a list of spec dicts with keys:
      kind: "option" | "argument" | "const_option"
      param_decls: list of flag strings (for options) or [NAME] for arguments
      click_kw: kwargs for click.Option / click.Argument
      dest: target attribute name in self.params (may differ from click param name)
      orig_name: original positional name (for arguments)
      const: value to set when flag is present (for const_option only)
    """
    specs = []
    has_preceding_optional_positional = False

    for args, kwargs in arguments:
        is_option = args and args[0].startswith("-")
        if is_option:
            specs.append(_argspec_to_click_option(args, kwargs))
        else:
            spec = _argspec_to_click_argument(
                args,
                kwargs,
                has_preceding_optional=has_preceding_optional_positional,
            )
            # Track whether this positional is optional (nargs="?")
            if kwargs.get("nargs") == "?":
                has_preceding_optional_positional = True
            specs.append(spec)

    return specs


def _argspec_to_click_option(args, kwargs):
    """Convert an argparse option spec to an intermediate Click spec dict."""
    # Make a mutable copy so we don't alter the stored decorator data
    kwargs = dict(kwargs)
    action = kwargs.pop("action", None)
    nargs = kwargs.pop("nargs", None)
    arg_type = kwargs.pop("type", None)
    dest = kwargs.pop("dest", None)
    choices = kwargs.pop("choices", None)
    default = kwargs.pop("default", None)
    metavar = kwargs.pop("metavar", None)
    help_text = kwargs.pop("help", None)
    const = kwargs.pop("const", None)

    option_names = list(args)
    click_kw = {}

    if metavar:
        click_kw["metavar"] = metavar
    if help_text:
        click_kw["help"] = help_text

    if action == "store_true":
        click_kw["is_flag"] = True
        click_kw["default"] = default if default is not None else False
        # Explicitly set flag_value=True so Click never auto-derives it
        # from the default.  Click 8.0 auto-sets flag_value=not(default),
        # which turns ``--preserve-times`` (default True) into a toggle
        # that *disables* the feature when the flag is present.
        click_kw["flag_value"] = True
        return {
            "kind": "option",
            "param_decls": option_names,
            "click_kw": click_kw,
            "dest": dest,
        }

    if action == "store_false":
        # Treat like store_const with const=False.  Click's ``flag_value``
        # parameter does not reliably invert a boolean default, so we reuse
        # the ``const_option`` machinery which already handles "set *dest*
        # to *const* when the flag is present".
        click_kw["is_flag"] = True
        click_kw["default"] = False
        click_kw["hidden"] = True
        return {
            "kind": "const_option",
            "param_decls": option_names,
            "click_kw": click_kw,
            "dest": dest,
            "const": False,
        }

    if action == "store_const":
        # store_const: flag sets dest to const value. We handle this specially:
        # create a flag with a unique param name (derived from the flag letters),
        # then in parse() post-processing we apply the const to dest.
        # We use a flag that is True when present, then map True → const in parse().
        click_kw["is_flag"] = True
        click_kw["default"] = False
        click_kw["hidden"] = True  # hide from help since it would duplicate
        return {
            "kind": "const_option",
            "param_decls": option_names,
            "click_kw": click_kw,
            "dest": dest,
            "const": const,
        }

    if action == "count":
        click_kw["count"] = True
        click_kw["default"] = default if default is not None else 0
        return {
            "kind": "option",
            "param_decls": option_names,
            "click_kw": click_kw,
            "dest": dest,
        }

    if action == "append":
        click_kw["multiple"] = True
        # argparse returns None when no values given; click returns ()
        # we'll convert in parse()
        return {
            "kind": "option",
            "param_decls": option_names,
            "click_kw": click_kw,
            "dest": dest,
        }

    # Regular value option
    if nargs and nargs != 1:
        click_kw["nargs"] = nargs
    if arg_type is not None:
        if arg_type is surl:
            click_kw["type"] = SURL
        elif choices:
            click_kw["type"] = click.Choice(choices)
            choices = None
        else:
            click_kw["type"] = arg_type
    elif choices:
        click_kw["type"] = click.Choice(choices)
        choices = None
    if default is not None:
        click_kw["default"] = default
    else:
        click_kw["default"] = None

    if choices and "type" not in click_kw:
        click_kw["type"] = click.Choice(choices)

    return {
        "kind": "option",
        "param_decls": option_names,
        "click_kw": click_kw,
        "dest": dest,
    }


def _argspec_to_click_argument(args, kwargs, has_preceding_optional=False):
    """Convert an argparse positional spec to an intermediate Click spec dict."""
    kwargs = dict(kwargs)
    arg_name = args[0]
    nargs = kwargs.pop("nargs", None)
    arg_type = kwargs.pop("type", None)
    default = kwargs.pop("default", None)
    kwargs.pop("help", None)
    kwargs.pop("metavar", None)
    kwargs.pop("dest", None)

    click_kw = {}

    if arg_type is not None:
        if arg_type is surl:
            click_kw["type"] = SURL
        else:
            click_kw["type"] = arg_type

    if nargs == "+":
        click_kw["nargs"] = -1
        # When there is an optional positional before this one, Click will
        # "steal" the last arg for the optional param, leaving this empty.
        # In that case we make this non-required and let the command validate.
        click_kw["required"] = not has_preceding_optional
    elif nargs == "*":
        click_kw["nargs"] = -1
        click_kw["required"] = False
    elif nargs == "?":
        click_kw["required"] = False
        click_kw["default"] = default
    elif nargs is not None:
        click_kw["nargs"] = nargs
    elif has_preceding_optional:
        # A required single-value argument after an optional positional: Click
        # will assign the lone provided value to the earlier optional slot,
        # leaving this argument empty. Make it non-required so parsing succeeds
        # and the command body can validate or handle the situation.
        click_kw["required"] = False
        click_kw["default"] = default

    return {
        "kind": "argument",
        "param_decls": [arg_name.upper()],
        "click_kw": click_kw,
        "orig_name": arg_name,
        "dest": None,
    }


_COMMON_GENERAL_OPTS = [
    "--help",
    "--version",
    "--verbose",
    "--quiet",
    "--timeout",
    "--log-file",
]
_COMMON_AUTH_OPTS = ["--cert", "--key", "--no-verify"]
_COMMON_COMPAT_OPTS = ["--definition", "--client-info", "--ipv4", "--ipv6"]

# Per-command option groups: maps a command suffix (e.g. "cp") to a list of
# group-dicts as expected by rich_click.OPTION_GROUPS.  Options not listed in
# any group end up in a catch-all group automatically.
_COMMAND_OPTION_GROUPS: dict[str, list[dict]] = {
    "cp": [
        {
            "name": "Copy Options",
            "options": [
                "--force",
                "--compare",
                "--parent",
                "--recursive",
                "--from-file",
                "--dry-run",
                "--abort-on-failure",
                "--transfer-timeout",
                "--just-copy",
                "--checksum",
                "--checksum-mode",
            ],
        },
        {
            "name": "Third-Party Copy (TPC)",
            "options": [
                "--tpc",
                "--tpc-only",
                "--tpc-mode",
                "--copy-mode",
                "--scitag",
                "--no-delegation",
                "--evict",
                "--disable-cleanup",
            ],
        },
        {
            "name": "GridFTP / SRM Compatibility",
            "options": [
                "--nbstreams",
                "--tcp-buffersize",
                "--src-spacetoken",
                "--dst-spacetoken",
            ],
        },
    ],
    "ls": [
        {
            "name": "Listing Options",
            "options": [
                "--all",
                "--long",
                "--directory",
                "--human-readable",
                "--time-style",
                "--full-time",
                "--color",
                "--reverse",
                "--sort",
                "--xattr",
            ],
        },
    ],
    "rm": [
        {
            "name": "Removal Options",
            "options": [
                "--recursive",
                "--dry-run",
                "--just-delete",
                "--from-file",
                "--bulk",
            ],
        },
    ],
    "mkdir": [
        {
            "name": "Directory Options",
            "options": ["--mode", "--parents"],
        },
    ],
}


def _configure_option_groups(prog_name: str, cmd_suffix: str) -> None:
    """Populate rich_click.OPTION_GROUPS for *prog_name* (e.g. 'gfal ls')."""
    groups: list[dict] = []

    # Command-specific groups (if defined)
    for grp in _COMMAND_OPTION_GROUPS.get(cmd_suffix, []):
        groups.append(grp)

    groups.append({"name": "General", "options": _COMMON_GENERAL_OPTS})
    groups.append({"name": "Authentication", "options": _COMMON_AUTH_OPTS})
    groups.append({"name": "Compatibility", "options": _COMMON_COMPAT_OPTS})

    if hasattr(click, "rich_click"):
        click.rich_click.OPTION_GROUPS[prog_name] = groups


def _build_click_command(method, prog_name, help_text):
    """
    Build a Click Command from an execute_* method's @arg decorators.

    Returns: (click_command, param_name_map, const_option_map)
    - param_name_map: maps click param var name → desired self.params attr name
    - const_option_map: maps click param var name → (dest, const) for store_const
    """
    arguments_specs = getattr(method, "arguments", [])
    spec_list = _argparse_to_click_params(list(arguments_specs))

    # Configure option panels for this command.
    # The supported package interface is "gfal <command>", but some internal
    # tests also construct command objects with simple program names.
    if " " in prog_name:
        cmd_suffix = prog_name.split()[-1]
    elif "-" in prog_name:
        cmd_suffix = prog_name.rsplit("-", 1)[-1]
    else:
        cmd_suffix = prog_name
    _configure_option_groups(prog_name, cmd_suffix)

    params = []
    param_name_map = {}  # click_var → dest attr name
    const_option_map = {}  # click_var → (dest, const)

    # Common options (version, verbose, timeout, cert, etc.)
    params.extend(_build_common_params())

    # Track which dest names already have a primary param in this command
    # (to handle store_const needing a unique hidden param name)
    used_param_names = set()

    for spec in spec_list:
        kind = spec["kind"]
        param_decls = spec["param_decls"]
        click_kw = dict(spec["click_kw"])
        dest = spec.get("dest")
        orig_name = spec.get("orig_name")

        if kind == "option":
            # Derive Click's variable name from the longest --xxx-yyy flag
            click_param = click.Option(param_decls, **click_kw)
            click_var = click_param.name
            if dest and click_var != dest:
                param_name_map[click_var] = dest
            params.append(click_param)
            used_param_names.add(click_var)

        elif kind == "const_option":
            # Generate a unique param name for this hidden flag
            # Use the flag letters (e.g. -S → _s_flag, -U → _u_flag)
            flag_letters = "".join(
                c for f in param_decls for c in f if c.isalnum()
            ).lower()
            unique_name = f"_const_{flag_letters}_flag"
            # Add a hidden option with the unique name
            # We add the unique_name as the last param decl so Click uses it
            hidden_decls = param_decls + [f"--{unique_name.replace('_', '-')}"]
            click_kw["hidden"] = True
            click_param = click.Option(hidden_decls, **click_kw)
            click_var = click_param.name  # will be unique_name
            const_option_map[click_var] = (dest, spec["const"])
            params.append(click_param)

        else:  # argument
            click_param = click.Argument(param_decls, **click_kw)
            click_var = click_param.name
            if orig_name and click_var.lower() != orig_name:
                param_name_map[click_var.lower()] = orig_name
            params.append(click_param)

    CommandClass = getattr(click, "RichCommand", click.Command)
    cmd = CommandClass(
        name=prog_name,
        params=params,
        help=help_text,
        callback=None,
    )
    return cmd, param_name_map, const_option_map


def _build_common_params():
    """Build the common Click params for all commands.

    Returns a list of click.Option objects.

    Note on naming: Click derives the Python variable name from the longest
    --xxx-yyy flag (converting hyphens to underscores). For flags where the
    desired attribute name differs from what Click would derive, we handle
    renaming in parse() via the common_rename_map.
    """
    params = [
        click.Option(
            ["-V", "--version"],
            is_flag=True,
            is_eager=True,
            expose_value=False,
            callback=_version_callback,
            help=f"Show the version and exit (gfal {VERSION}).",
        ),
        click.Option(
            ["-v", "--verbose"],
            count=True,
            default=0,
            help="Enable verbose mode (-v warnings, -vv info, -vvv debug).",
        ),
        click.Option(
            ["-q", "--quiet"],
            is_flag=True,
            default=False,
            help="Suppress warnings and informational messages; only errors are shown (takes precedence over --verbose).",
        ),
        click.Option(
            ["-t", "--timeout"],
            type=int,
            default=1800,
            help="Maximum seconds for the operation (default: 1800).",
        ),
        click.Option(
            ["-E", "--cert"],
            type=str,
            default=None,
            help="User certificate (X.509 PEM or proxy).",
        ),
        click.Option(
            ["--key"],
            type=str,
            default=None,
            help="User private key (defaults to --cert if omitted).",
        ),
        click.Option(
            ["--log-file"],
            type=str,
            default=None,
            help="Write log output to this file instead of stderr.",
        ),
        # --verify/--no-verify: Click name will be 'verify'; we rename to 'ssl_verify' in parse()
        click.Option(
            ["--verify/--no-verify"],
            default=True,
            help="Enable SSL certificate verification (default). Use --no-verify to skip verification (insecure; for self-signed certs).",
        ),
        click.Option(
            ["-D", "--definition"],
            type=str,
            multiple=True,
            metavar="DEFINITION",
            help="Override a gfal2 parameter (accepted for compatibility; ignored).",
        ),
        # -C/--client-info: Click name is 'client_info' (correct already)
        click.Option(
            ["-C", "--client-info"],
            type=str,
            default=None,
            metavar="CLIENT_INFO",
            help="Provide custom client-side information (accepted for compatibility; ignored).",
        ),
        # -4/--ipv4: Click name is 'ipv4'; we rename to 'ipv4_only' in parse()
        click.Option(
            ["-4", "--ipv4"],
            is_flag=True,
            default=False,
            help="Force IPv4 addresses only.",
        ),
        # -6/--ipv6: Click name is 'ipv6'; we rename to 'ipv6_only' in parse()
        click.Option(
            ["-6", "--ipv6"],
            is_flag=True,
            default=False,
            help="Force IPv6 addresses only.",
        ),
    ]
    return params


# Renames applied to common options after parsing
# (Click's derived name → desired self.params attribute name)
_COMMON_RENAME_MAP = {
    "verify": "ssl_verify",
    "ipv4": "ipv4_only",
    "ipv6": "ipv6_only",
    "client_info": "client_info",  # already correct, kept for explicitness
    "log_file": "log_file",  # already correct
}


def _version_callback(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(f"gfal {VERSION}")
    ctx.exit()


# Mapping from POSIX errno values to human-readable descriptions, used by
# CommandBase._format_error() when an exception carries an errno but no strerror.
_ERRNO_DESCRIPTIONS = {
    errno.ENOENT: "No such file or directory",
    errno.EACCES: "Permission denied",
    errno.EEXIST: "File exists",
    errno.EISDIR: "Is a directory",
    errno.ENOTDIR: "Not a directory",
    errno.ETIMEDOUT: "Operation timed out",
}


# ---------------------------------------------------------------------------
# CommandBase
# ---------------------------------------------------------------------------


class CommandBase:
    def __init__(self):
        self.return_code = -1
        self.progress_bar = None
        self.console = get_console()
        self.err_console = get_console(stderr=True)
        self.params = None
        self.prog = None
        self.argv = None
        self._cancel_event = Event()

    @contextlib.contextmanager
    def spinner(self, message):
        """Displays a rich status spinner for blocking operations.
        Quiet in GFAL2 compat mode.
        """
        if is_gfal2_compat() or self._is_quiet():
            yield
        else:
            with self.err_console.status(message) as status:
                yield status

    @staticmethod
    def get_subclasses():
        return CommandBase.__subclasses__()

    # ------------------------------------------------------------------
    # Logging setup
    # ------------------------------------------------------------------

    @staticmethod
    def _setup_logger(level, log_file, quiet=False):
        level = max(0, min(3, level))
        log_level = (
            logging.ERROR
            if quiet
            else logging.ERROR - level * 10  # 0→ERROR, 1→WARN, 2→INFO, 3→DEBUG
        )

        root = logging.getLogger()
        root.setLevel(log_level)
        handler = (
            logging.FileHandler(log_file, mode="w")
            if log_file
            else logging.StreamHandler(sys.stderr)
        )
        handler.setLevel(log_level)
        fmt = logging.Formatter("%(levelname)s %(name)s: %(message)s")
        handler.setFormatter(fmt)
        root.addHandler(handler)

    def _is_quiet(self):
        return bool(getattr(self.params, "quiet", False))

    # ------------------------------------------------------------------
    # Argument parsing (Click-based)
    # ------------------------------------------------------------------

    def parse(self, func, argv):
        """Parse argv using Click, populating self.params as a SimpleNamespace."""
        prog = Path(argv[0]).name
        self.prog = prog
        self.argv = list(argv)

        doc = (func.__doc__ or "").strip().split("\n")[0]
        help_text = f"{doc}"

        # Build the Click command from @arg decorators
        cmd, param_name_map, const_option_map = _build_click_command(
            func, prog, help_text
        )

        # Parse argv[1:] with Click; let SystemExit (from --help/--version) propagate.
        # Convert Click user errors to argparse-style messages and exit.
        try:
            ctx = cmd.make_context(prog, list(argv[1:]))
        except click.exceptions.Exit as e:
            sys.exit(int(e.exit_code))
        except click.exceptions.UsageError as e:
            sys.stderr.write(f"{prog}: {e.format_message()}\n")
            sys.exit(2)

        # Build self.params as SimpleNamespace from the Click params
        params_dict = dict(ctx.params)

        # Apply store_const overrides (e.g. -S → sort="size", -U → sort="none")
        # These const flags set the dest to const when the flag is present.
        # Last-set wins: process in order they appear in param list.
        for click_var, (dest, const_val) in const_option_map.items():
            flag_val = params_dict.pop(click_var, False)
            if flag_val and dest:
                params_dict[dest] = const_val

        # Apply common option renames (e.g. no_verify → ssl_verify, ipv4 → ipv4_only)
        for click_name, dest_name in _COMMON_RENAME_MAP.items():
            if click_name in params_dict and click_name != dest_name:
                params_dict[dest_name] = params_dict.pop(click_name)

        # Apply command-specific param_name_map remapping
        for click_name, dest_name in param_name_map.items():
            if click_name in params_dict:
                val = params_dict.pop(click_name)
                params_dict[dest_name] = val

        # Convert nargs=-1 tuples to lists for compatibility
        # (argparse returns lists; click returns tuples)
        for k in list(params_dict.keys()):
            v = params_dict[k]
            if isinstance(v, tuple):
                params_dict[k] = list(v)

        # Fix the src/dst interaction for "gfal cp" when --from-file is used.
        # Click assigns the single positional arg to the optional 'src',
        # leaving 'dst' empty. When 'from_file' is set, we need to move
        # 'src' → 'dst[0]' and set 'src' to None.
        if (
            "from_file" in params_dict
            and params_dict.get("from_file")
            and "src" in params_dict
            and params_dict.get("src") is not None
            and "dst" in params_dict
            and not params_dict.get("dst")
        ):
            params_dict["dst"] = [params_dict["src"]]
            params_dict["src"] = None

        if params_dict.get("ipv4_only") and params_dict.get("ipv6_only"):
            sys.stderr.write(f"{prog}: --ipv4 and --ipv6 are mutually exclusive\n")
            sys.exit(2)

        self.params = SimpleNamespace(**params_dict)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    @staticmethod
    def _format_error(e):
        """Return a user-friendly error string for an exception.

        fsspec raises FileNotFoundError (and friends) with only the URL as
        the message and no errno/strerror.  In that case Python's default
        str(e) looks identical to a normal program output line, so we append
        the POSIX description ourselves.
        """
        msg = str(e)
        if not msg:
            msg = e.__class__.__name__

        # Identify the relevant path/URL if possible.
        path = getattr(e, "filename", None)
        if not path and e.args:
            # Handle standard OSError(errno, strerror, filename)
            if len(e.args) >= 3 and e.args[2] and isinstance(e.args[2], (str, bytes)):
                path = e.args[2]
            # Handle fsspec FileNotFoundError("root://url")
            elif isinstance(e.args[0], (str, bytes)) and (
                "://" in str(e.args[0]) or "/" in str(e.args[0])
            ):
                path = e.args[0]

        if isinstance(path, bytes):
            path = path.decode("utf-8", "replace")

        # 1. Handle Windows-specific error codes
        winerror = getattr(e, "winerror", None)
        _win_map = {
            2: "No such file or directory",  # ERROR_FILE_NOT_FOUND
            3: "No such file or directory",  # ERROR_PATH_NOT_FOUND
            5: "Permission denied",  # ERROR_ACCESS_DENIED
            17: "File exists",  # ERROR_ALREADY_EXISTS
            183: "File exists",  # ERROR_ALREADY_EXISTS (alt)
        }
        desc = None
        if winerror is not None:
            desc = _win_map.get(winerror)
        elif "[WinError 2]" in msg:
            desc = _win_map[2]
        elif "[WinError 3]" in msg:
            desc = _win_map[3]
        elif "[WinError 5]" in msg:
            desc = _win_map[5]
        elif "[WinError 17]" in msg or "[WinError 183]" in msg:
            desc = _win_map[17]

        if desc:
            return f"{path}: {desc}" if path else desc

        # 2. Map common exception types to POSIX strings for consistency.
        _descriptions = {
            FileNotFoundError: "No such file or directory",
            PermissionError: "Permission denied",
            IsADirectoryError: "Is a directory",
            NotADirectoryError: "Not a directory",
            FileExistsError: "File exists",
            TimeoutError: "Operation timed out",
        }
        for exc_type, description in _descriptions.items():
            if isinstance(e, exc_type):
                return f"{path}: {description}" if path else description

        strerror = getattr(e, "strerror", None)
        if strerror:
            # Avoid doubling if path is already in the string.
            if strerror not in msg:
                return f"{msg}: {strerror}"
            return msg
        # GfalError subclasses set errno but not strerror (constructed via
        # super().__init__(message) with a single string argument).  Derive the
        # POSIX description from errno so the user sees e.g.
        # "url: No such file or directory" instead of a bare URL.
        err_code = getattr(e, "errno", None)
        if isinstance(err_code, int) and isinstance(e, OSError):
            desc = _ERRNO_DESCRIPTIONS.get(err_code) or os.strerror(err_code)
            if desc and desc not in msg:
                return f"{path}: {desc}" if path else f"{msg}: {desc}"
        # SSL / connection errors from aiohttp (used by fsspec)
        try:
            import aiohttp as _aiohttp

            if isinstance(e, _aiohttp.ClientSSLError):
                cause = str(e)
                if "WRONG_VERSION_NUMBER" in cause or "UNKNOWN_PROTOCOL" in cause:
                    return f"{msg}: server does not speak HTTPS on this port (try http:// instead)"
                return f"{msg}: SSL certificate error (use --no-verify to skip, or install the server CA. See: https://lobis.github.io/gfal-cli/installation/#cern-ca-certificates)"
            if isinstance(e, _aiohttp.ClientConnectorSSLError):
                return f"{msg}: SSL certificate error (use --no-verify to skip, or install the server CA. See: https://lobis.github.io/gfal-cli/installation/#cern-ca-certificates)"
            if isinstance(e, _aiohttp.ClientConnectorError):
                cause = str(e.__cause__ or e).lower()
                if "ssl" in cause or "certificate" in cause:
                    return f"{msg}: SSL certificate error (use --no-verify to skip, or install the server CA. See: https://lobis.github.io/gfal-cli/installation/#cern-ca-certificates)"
            if isinstance(e, _aiohttp.ClientConnectionError):
                cause = str(e.__cause__ or e).lower()
                if "ssl" in cause or "certificate" in cause:
                    return f"{msg}: SSL certificate error (use --no-verify to skip, or install the server CA. See: https://lobis.github.io/gfal-cli/installation/#cern-ca-certificates)"
                return msg
        except ImportError:
            pass
        # HTTP errors from aiohttp: status attribute carries the HTTP code
        status = getattr(e, "status", None)
        if status is not None:
            _http_descriptions = {
                400: "Bad Request",
                401: "Unauthorized",
                403: "Permission denied",
                404: "No such file or directory",
                405: "Method not allowed",
                408: "Request timeout",
                409: "Conflict",
                410: "Gone",
                500: "Internal server error",
                503: "Service unavailable",
            }
            url = getattr(e, "request_info", None)
            url_str = str(url.url) if url is not None else ""
            description = _http_descriptions.get(status, f"HTTP {status}")
            if url_str:
                return f"{url_str}: {description}"
            return f"{description}"
        # CERN hint
        if "cern.ch" in msg.lower() and any(
            term in msg.lower()
            for term in ("timeout", "connection", "unreachable", "refused", "gai")
        ):
            return f"{msg}: Could not connect. Are you on the CERN VPN?"

        # Last resort: str(e) was empty (e.g. NotImplementedError()).  Show the
        # exception type so the user sees something actionable instead of a
        # blank "ERROR:" line.
        if not msg:
            return f"({type(e).__name__})"
        return msg

    def _print_error(self, e):
        """Prints a formatted error message to stderr, respecting compatibility mode."""
        msg = self._format_error(e)
        if is_gfal2_compat():
            sys.stderr.write(f"{self.prog}: {msg}\n")
        else:
            self.err_console.print(f"[bold red]{self.prog}[/]: {msg}")

    def _executor(self, func):
        """Runs func(self) inside the worker thread, captures exceptions."""
        try:
            self.return_code = func(self)
            if self.return_code is None:
                self.return_code = 0
        except Exception as e:
            # Broken pipe (e.g. piped to `head`) is not an error.
            if isinstance(e, OSError) and e.errno == errno.EPIPE:
                self.return_code = 0
                return
            self._print_error(e)
            self.return_code = exception_exit_code(e)

    def execute(self, func):
        # Forced IP family (IPv4/v6)
        # This affects urllib3-based transports globally for the duration
        # of the command.
        ipv4_only = getattr(self.params, "ipv4_only", False)
        ipv6_only = getattr(self.params, "ipv6_only", False)
        if ipv4_only or ipv6_only:
            import socket

            import urllib3.util.connection as nsock

            family = socket.AF_INET if ipv4_only else socket.AF_INET6
            nsock.allowed_gai_family = lambda: family

        # Apply cert/key to environment (XRootD reads X509_* env vars)
        if self.params.cert:
            key = self.params.key or self.params.cert
            os.environ["X509_USER_CERT"] = self.params.cert
            os.environ["X509_USER_KEY"] = key
            os.environ.pop("X509_USER_PROXY", None)
        elif not os.environ.get("X509_USER_PROXY") and hasattr(os, "getuid"):
            # Auto-detect proxy at the standard location used by voms-proxy-init
            # (Unix only — os.getuid() is not available on Windows)
            default_proxy = Path(f"/tmp/x509up_u{os.getuid()}")
            if default_proxy.exists():
                os.environ["X509_USER_PROXY"] = str(default_proxy)

        self._setup_logger(
            self.params.verbose,
            self.params.log_file,
            quiet=getattr(self.params, "quiet", False),
        )

        # Interactive commands must run in the main thread (e.g. TUI signal handling).
        # This bypasses the worker thread and timeout logic.
        if getattr(func, "is_interactive", False):
            self._executor(func)
            return self.return_code

        t = Thread(target=self._executor, args=[func], daemon=True)
        t.start()

        try:
            timeout = self.params.timeout if self.params.timeout > 0 else None
            # join in a loop so KeyboardInterrupt is catchable
            deadline = timeout  # seconds remaining
            while t.is_alive():
                t.join(min(3600, deadline) if deadline is not None else 3600)
                if deadline is not None:
                    deadline -= 3600
                    if deadline <= 0:
                        break

            if t.is_alive():
                if self.progress_bar is not None:
                    self.progress_bar.stop(False)
                sys.stderr.write(
                    f"Command timed out after {self.params.timeout} seconds\n"
                )
                return errno.ETIMEDOUT

            return self.return_code

        except KeyboardInterrupt:
            self._cancel_event.set()
            if self.progress_bar is not None:
                self.progress_bar.stop(False)
            t.join(2)
            sys.stderr.write("\nInterrupted\n")
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            return errno.EINTR
