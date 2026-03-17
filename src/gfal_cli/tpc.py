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

import sys
from urllib.parse import urlparse

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
):
    """Perform a third-party copy between two remote URLs.

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
        return _xrootd_tpc(src_url, dst_url, timeout=timeout, verbose=verbose)

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
        )

    raise NotImplementedError(
        f"TPC not supported for {src_scheme}:// -> {dst_scheme}://"
    )


# ---------------------------------------------------------------------------
# HTTP / WebDAV TPC
# ---------------------------------------------------------------------------


def _build_session(opts):
    """Return a requests.Session configured from fsspec storage_options."""
    import requests

    session = requests.Session()
    if opts.get("client_cert"):
        key = opts.get("client_key", opts["client_cert"])
        session.cert = (opts["client_cert"], key)
    if not opts.get("ssl_verify", True):
        # Suppress the InsecureRequestWarning that urllib3 emits
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        session.verify = False
    return session


def _parse_tpc_body(resp):
    """Parse a WebDAV TPC response, including WLCG streaming perf-markers.

    The WLCG HTTP-TPC spec allows the server to send ``202 Accepted`` and
    then stream text lines of the form::

        Perf Marker
          Timestamp: 1700000000
          Stripe Bytes Transferred: 1048576
          ...
        End

    The final line is either ``success: <message>`` or ``failure: <message>``.
    """
    if resp.status_code in (200, 201, 204):
        return  # immediate success

    if resp.status_code == 405:
        raise NotImplementedError(
            "Server returned HTTP 405 Method Not Allowed — "
            "WebDAV COPY not supported (no TPC)"
        )

    if resp.status_code == 501:
        raise NotImplementedError(
            "Server returned HTTP 501 Not Implemented — no TPC support"
        )

    if resp.status_code != 202:
        resp.raise_for_status()
        return

    # 202 Accepted: read streaming performance markers
    last_non_empty = ""
    for raw in resp.iter_lines(decode_unicode=True):
        line = (raw or "").strip()
        if line.startswith("success:"):
            return
        if line.startswith("failure:"):
            raise OSError(f"HTTP TPC server reported failure: {line[8:].strip()}")
        if line:
            last_non_empty = line

    # Body ended without an explicit success/failure line
    if last_non_empty.startswith("failure:"):
        raise OSError(f"HTTP TPC server reported failure: {last_non_empty[8:].strip()}")
    # Treat silent end-of-body as success (some implementations omit the line)


def _http_tpc(src_url, dst_url, opts, *, mode, timeout, verbose, scitag):
    """Send a WebDAV COPY request to initiate an HTTP TPC transfer."""
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
        if verbose:
            sys.stderr.write(f"[TPC pull] {dst_url} <- {src_url}\n")

    request_timeout = timeout if timeout else None
    session = _build_session(opts)
    resp = session.request(
        "COPY",
        url,
        headers=headers,
        timeout=request_timeout,
        stream=True,
    )
    _parse_tpc_body(resp)
    return True


# ---------------------------------------------------------------------------
# XRootD TPC
# ---------------------------------------------------------------------------


def _xrootd_tpc(src_url, dst_url, *, timeout, verbose):
    """XRootD native third-party copy via pyxrootd CopyProcess."""
    try:
        from XRootD import client as xrd_client  # noqa: PLC0415
    except ImportError as exc:
        raise NotImplementedError(
            "XRootD Python bindings are not installed; "
            "install them with: pip install xrootd"
        ) from exc

    if verbose:
        sys.stderr.write(f"[TPC xrootd] {src_url} -> {dst_url}\n")

    process = xrd_client.CopyProcess()

    props = {
        "source": src_url,
        "target": dst_url,
        "thirdparty": True,
        "force": True,
    }
    if timeout:
        props["tpctimeout"] = int(timeout)

    process.add_job(**props)

    status, _ = process.prepare()
    if not status.ok:
        raise OSError(f"XRootD TPC prepare failed: {status.message}")

    status, results = process.run()
    if not status.ok:
        raise OSError(f"XRootD TPC failed: {status.message}")

    # Check individual job results
    if results:
        for r in results:
            job_status = getattr(r, "status", None)
            if job_status is not None and not job_status.ok:
                raise OSError(f"XRootD TPC job failed: {job_status.message}")

    return True
