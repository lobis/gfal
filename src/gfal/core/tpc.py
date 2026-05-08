"""
Third-party copy (TPC) implementations.

Supported backends
------------------
HTTP/HTTPS
    WebDAV COPY method.  Two flavours:

    pull (default)
        The client sends ``COPY <dst>`` with a ``Source: <src>`` header.
        The *destination* server contacts the source and pulls the data.

    push
        The client sends ``COPY <src>`` with a ``Destination: <dst>`` header.
        The *source* server contacts the destination and pushes the data.

    Per the WLCG HTTP-TPC specification the server may respond with
    ``202 Accepted`` and stream performance markers in the body, finishing
    with a ``success: ...`` or ``failure: ...`` line.

XRootD
    Native TPC via pyxrootd ``CopyProcess(thirdparty=True)``.  Works for
    ``root://`` -> ``root://`` transfers.
"""

import contextlib
import logging
import sys
import threading
from urllib.parse import urlparse

from gfal.core import fs
from gfal.core.token import retrieve_token

log = logging.getLogger(__name__)

_HTTP_TPC_SUBMIT_AHEAD_DELAY = 0.35
_HTTP_TPC_READ_TOKEN_VALIDITY = 180
_HTTP_TPC_WRITE_TOKEN_VALIDITY = 130

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def do_tpc(
    src_url,
    dst_url,
    opts,
    *,
    mode="pull",
    timeout=None,
    verbose=False,
    scitag=None,
    no_delegation=False,
    progress_callback=None,
    start_callback=None,
    submission_ready_callback=None,
):
    """Perform a third-party copy between two remote URLs.

    Parameters
    ----------
    progress_callback:
        Optional callable ``(bytes_transferred: int) -> None`` that is called
        each time a WLCG performance marker is received during HTTP TPC.
        Values are cumulative (total bytes transferred so far, not a delta).
        Not called for XRootD TPC (use *start_callback* for that).
    start_callback:
        Optional callable ``() -> None`` invoked just before the blocking
        XRootD CopyProcess starts.  Allows the caller to show a progress
        indicator even though XRootD TPC provides no byte-level markers.

    Returns ``True`` on success.

    Raises
    ------
    NotImplementedError
        TPC is not applicable or the server does not support it.
        The caller should fall back to client-side streaming when this is
        raised and ``--tpc-only`` was not requested.
    OSError
        A definitive transfer failure (server reported an error).
    """
    src_scheme = urlparse(src_url).scheme.lower()
    dst_scheme = urlparse(dst_url).scheme.lower()

    # XRootD <-> XRootD: use native CopyProcess
    if src_scheme in ("root", "xroot") and dst_scheme in ("root", "xroot"):
        return _xrootd_tpc(
            src_url,
            dst_url,
            timeout=timeout,
            verbose=verbose,
            start_callback=start_callback,
        )

    # HTTP(S) <-> HTTP(S): use WebDAV COPY
    if src_scheme in ("http", "https") and dst_scheme in ("http", "https"):
        return _http_tpc(
            src_url,
            dst_url,
            opts,
            mode=mode,
            timeout=timeout,
            verbose=verbose,
            scitag=scitag,
            no_delegation=no_delegation,
            progress_callback=progress_callback,
            start_callback=start_callback,
            submission_ready_callback=submission_ready_callback,
        )

    raise NotImplementedError(
        f"TPC not supported for {src_scheme}:// -> {dst_scheme}://"
    )


# ---------------------------------------------------------------------------
# HTTP / WebDAV TPC
# ---------------------------------------------------------------------------


def _build_session(opts):
    """Return a synchronous aiohttp-backed session configured from storage options."""
    from gfal.core.webdav import _make_session

    return _make_session(opts)


def _is_eos_https_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme.lower() == "https" and fs._is_eos_host(parsed.hostname)


def _bearer_header(token: str) -> str:
    return token if token.lower().startswith("bearer ") else f"Bearer {token}"


def _redacted_header(value: str) -> str:
    if not value:
        return ""
    return "<redacted bearer token>"


def _request_target(url: str) -> str:
    parsed = urlparse(url)
    target = parsed.path or "/"
    if parsed.query:
        target += f"?{parsed.query}"
    return target


def _with_http_tpc_token_auth(src_url: str, dst_url: str, opts: dict) -> dict:
    """Return TPC options augmented with gfal2-style SE-issued tokens."""
    if opts.get("transfer_bearer_token") or opts.get("bearer_token"):
        return dict(opts)
    if not (_is_eos_https_url(src_url) and _is_eos_https_url(dst_url)):
        return dict(opts)

    log.info("Using client X509 for HTTPS session authorization")
    source_token = retrieve_token(
        src_url,
        validity=_HTTP_TPC_READ_TOKEN_VALIDITY,
        write_access=False,
        storage_options=opts,
        operation="read",
    )
    destination_token = retrieve_token(
        dst_url,
        validity=_HTTP_TPC_WRITE_TOKEN_VALIDITY,
        write_access=True,
        storage_options=opts,
        operation="write",
    )
    log.info("Using bearer token for HTTPS request authorization")

    token_opts = dict(opts)
    token_opts["bearer_token"] = destination_token
    token_opts["transfer_bearer_token"] = source_token
    return token_opts


def _parse_tpc_body(
    resp,
    progress_callback=None,
    submission_ready_callback=None,
):
    """Parse a WebDAV TPC response, including WLCG streaming perf-markers.

    The WLCG HTTP-TPC spec allows the server to send ``202 Accepted`` and
    then stream text lines of the form::

        Perf Marker
          Timestamp: 1700000000
          Stripe Bytes Transferred: 1048576
          ...
        End

    The final line is either ``success: <message>`` or ``failure: <message>``.

    EOS (and some other servers) respond with ``201 Created`` instead of
    ``202 Accepted`` while still streaming performance markers.  We therefore
    read the body for *any* 2xx response — an empty body is treated as
    immediate success (covers plain 200/204 from simple servers).

    If *progress_callback* is provided it is called with the cumulative number
    of bytes transferred each time a complete perf-marker block is received.
    """
    if resp.status_code == 405:
        raise NotImplementedError(
            "Server returned HTTP 405 Method Not Allowed — "
            "WebDAV COPY not supported (no TPC)"
        )

    if resp.status_code == 501:
        raise NotImplementedError(
            "Server returned HTTP 501 Not Implemented — no TPC support"
        )

    if not (200 <= resp.status_code < 300):
        resp.raise_for_status()
        return

    # 2xx: read the (possibly streaming) body for success/failure markers.
    # Servers that complete immediately return an empty body; that is fine —
    # the loop simply doesn't execute, and we fall through to success.
    last_non_empty = ""
    in_marker = False
    marker_bytes = 0
    ready_flag = threading.Event()
    ready_timer = None

    def _mark_submission_ready():
        if submission_ready_callback is None or ready_flag.is_set():
            return
        ready_flag.set()
        submission_ready_callback()

    if submission_ready_callback is not None:
        ready_timer = threading.Timer(
            _HTTP_TPC_SUBMIT_AHEAD_DELAY,
            _mark_submission_ready,
        )
        ready_timer.daemon = True
        ready_timer.start()
    try:
        for raw in resp.iter_lines(decode_unicode=True):
            line = (raw or "").strip()
            if line == "Perf Marker":
                in_marker = True
                marker_bytes = 0
                _mark_submission_ready()
            elif line == "End" and in_marker:
                in_marker = False
                if progress_callback is not None and marker_bytes > 0:
                    progress_callback(marker_bytes)
            elif in_marker and line.startswith("Stripe Bytes Transferred:"):
                with contextlib.suppress(ValueError):
                    marker_bytes = int(line.split(":", 1)[1].strip())
            elif line.startswith("success:"):
                _mark_submission_ready()
                return
            elif line.startswith("failure:"):
                raise OSError(f"HTTP TPC server reported failure: {line[8:].strip()}")
            if line:
                last_non_empty = line
    except ConnectionError:
        if last_non_empty.startswith("success:"):
            _mark_submission_ready()
            return
        raise
    finally:
        if ready_timer is not None:
            ready_timer.cancel()
        with contextlib.suppress(Exception):
            resp.close()

    # Body ended without an explicit success/failure line
    if last_non_empty.startswith("failure:"):
        raise OSError(f"HTTP TPC server reported failure: {last_non_empty[8:].strip()}")
    # Treat silent end-of-body as success (some implementations omit the line)
    _mark_submission_ready()


def _http_tpc(
    src_url,
    dst_url,
    opts,
    *,
    mode,
    timeout,
    verbose,
    scitag,
    no_delegation=False,
    progress_callback=None,
    start_callback=None,
    submission_ready_callback=None,
):
    """Send a WebDAV COPY request to initiate an HTTP TPC transfer."""
    if mode == "pull":
        opts = _with_http_tpc_token_auth(src_url, dst_url, opts)

    headers = {
        "Overwrite": "T",
        "Content-Length": "0",
    }
    if scitag is not None:
        headers["SciTag"] = str(scitag)

    if mode == "push":
        # Source server pushes to destination
        url = src_url
        headers["Destination"] = dst_url
        if verbose:
            sys.stderr.write(f"[TPC push] {src_url} -> {dst_url}\n")
    else:
        # Destination server pulls from source (default / "pull")
        url = dst_url
        headers["Source"] = src_url
        transfer_token = opts.get("transfer_bearer_token")
        if transfer_token:
            headers["TransferHeaderAuthorization"] = _bearer_header(transfer_token)
        # Match gfal2's token-based passive TPC mode: the destination receives
        # a write bearer token as Authorization, and the source read token is
        # forwarded via TransferHeaderAuthorization.
        headers["Credential"] = "none"
        if verbose:
            sys.stderr.write(f"[TPC pull] {dst_url} <- {src_url}\n")

    request_timeout = timeout if timeout else None
    session = _build_session(opts)
    try:
        if start_callback is not None:
            start_callback()
        log.info("Davix: > COPY %s HTTP/1.1", _request_target(url))
        for name, value in headers.items():
            if name.lower() == "transferheaderauthorization":
                value = _redacted_header(value)
            log.info("> %s: %s", name, value)
        authorization = getattr(session, "headers", {}).get("Authorization")
        if authorization:
            log.info("> Authorization: %s", _redacted_header(authorization))
        resp = session.request(
            "COPY",
            url,
            headers=headers,
            timeout=request_timeout,
            stream=True,
        )
        log.info("Davix: < HTTP/1.1 %s", resp.status_code)
        _parse_tpc_body(
            resp,
            progress_callback=progress_callback,
            submission_ready_callback=submission_ready_callback,
        )
    finally:
        with contextlib.suppress(Exception):
            session.close()
    return True


# ---------------------------------------------------------------------------
# XRootD TPC
# ---------------------------------------------------------------------------


def _xrootd_tpc(src_url, dst_url, *, timeout, verbose, start_callback=None):
    """XRootD native third-party copy via pyxrootd CopyProcess."""
    try:
        from XRootD import client as xrd_client  # noqa: PLC0415
    except ImportError as exc:
        raise NotImplementedError(
            "XRootD support is not installed; "
            "install XRootD bindings in your environment, for example with: conda install -c conda-forge xrootd"
        ) from exc

    if verbose:
        sys.stderr.write(f"[TPC xrootd] {src_url} -> {dst_url}\n")

    # XRootD CopyProcess.run() is blocking with no byte-level callbacks, so
    # start the progress display now (before the call) if the caller provided one.
    if start_callback is not None:
        start_callback()

    process = xrd_client.CopyProcess()

    props = {
        "source": src_url,
        "target": dst_url,
        "thirdparty": "only",
        "force": True,
    }
    if timeout:
        props["tpctimeout"] = int(timeout)

    process.add_job(**props)

    prepared = process.prepare()
    status = prepared[0] if isinstance(prepared, tuple) else prepared
    if not status.ok:
        if "tpc not supported" in status.message.lower():
            raise NotImplementedError(status.message)
        raise OSError(f"XRootD TPC prepare failed: {status.message}")

    completed = process.run()
    if isinstance(completed, tuple):
        status, results = completed
    else:
        status, results = completed, None
    if not status.ok:
        if "tpc not supported" in status.message.lower():
            raise NotImplementedError(status.message)
        raise OSError(f"XRootD TPC failed: {status.message}")

    # Check individual job results
    if results:
        for r in results:
            job_status = getattr(r, "status", None)
            if job_status is not None and not job_status.ok:
                if "tpc not supported" in job_status.message.lower():
                    raise NotImplementedError(job_status.message)
                raise OSError(f"XRootD TPC job failed: {job_status.message}")

    return True
