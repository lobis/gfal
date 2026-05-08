# HTTP TPC token trace: gfal2 vs gfal

This note captures a `-vvv` comparison for an EOS Pilot HTTP third-party copy
where native `gfal2-util` succeeds and this Python implementation currently
fails.  It is intended as a reference for matching gfal2 behavior in the copy
path.

Observed on 2026-05-08 against:

```text
src = https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/traffic-shaping-files/source_1
dst = https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/tmp/destination_<timestamp>?eos.app=traffic-shaping-benchmark-fts-manual
```

Tokens and capability URLs are redacted or shortened below. Do not paste raw
macaroons, bearer tokens, or `cap.*` URLs into committed docs.

## Commands

Local Python implementation:

```bash
ts=$(date +%s)
dst="https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/tmp/destination_${ts}_gfal3_push?eos.app=traffic-shaping-benchmark-fts-manual"
.venv/bin/gfal cp -vvv --no-verify \
  "https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/traffic-shaping-files/source_1" \
  "$dst" \
  > /tmp/gfal3_cp.stdout 2> /tmp/gfal3_cp.stderr
```

Native gfal2-util on lxplus:

```bash
ts=$(date +%s)
dst="https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/tmp/destination_${ts}_gfal2_docs?eos.app=traffic-shaping-benchmark-fts-manual"
gfal-copy -vvv \
  "https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/traffic-shaping-files/source_1" \
  "$dst" \
  > /tmp/gfal2_docs_${ts}.stdout 2> /tmp/gfal2_docs_${ts}.stderr
```

## Current gfal result before tokenized TPC

Before automatic TPC token retrieval was added, the Python implementation
selected HTTP TPC pull mode, then failed with `403`.

```text
rc=13
Copying source_1 (TPC pull) 1073741824 bytes  https://.../source_1  =>  https://.../destination_1778239885_gfal3_push?eos.app=traffic-shaping-benchmark-fts-manual

gfal cp: 403, message='HTTP error',
url='https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778239885_gfal3_push?eos.app=traffic-shaping-benchmark-fts-manual':
Permission denied
```

The local trace shows destination existence probes, but no pre-copy storage
token retrieval in the copy path:

```text
DEBUG fsspec.http: Retrieve file size for https://.../destination_1778239885_gfal3_push?eos.app=traffic-shaping-benchmark-fts-manual
aiohttp.client_exceptions.ClientResponseError: 404, message='NOT_FOUND'
...
gfal cp: 403, message='HTTP error'
```

Current TPC request construction in `src/gfal/core/tpc.py` sends pull-mode
headers like:

```text
COPY <destination>
Source: <source>
Credential: none
RequireChecksumVerification: false
```

It did not first obtain SE-issued bearer tokens for the source and destination,
and it did not send the source token as `TransferHeaderAuthorization`.

## Current gfal tokenized trace

After adding automatic tokenized EOS HTTPS TPC, `gfal cp -vvv` emits a
gfal2-comparable token and COPY sequence:

```text
INFO gfal.core.token: Davix: > POST //eos/pilot/test/lobisapa/layout-erasure/traffic-shaping-files/source_1 HTTP/1.1
INFO gfal.core.token: > Content-Type: application/macaroon-request
INFO gfal.core.token: > Content-Length: 61
INFO gfal.core.token: Davix: < HTTP/1.1 200
DEBUG gfal.core.token: (SEToken) Set bearer token in credential_map[https://.../source_1] (access=read) (validity=180)

INFO gfal.core.token: Davix: > POST //eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778240871_gfal3_final?eos.app=traffic-shaping-benchmark-fts-manual HTTP/1.1
INFO gfal.core.token: > Content-Type: application/macaroon-request
INFO gfal.core.token: > Content-Length: 82
INFO gfal.core.token: Davix: < HTTP/1.1 200
DEBUG gfal.core.token: (SEToken) Set bearer token in credential_map[https://.../destination_1778240871_gfal3_final?eos.app=traffic-shaping-benchmark-fts-manual] (access=write) (validity=130)

INFO gfal.core.tpc: Using bearer token for HTTPS request authorization
INFO gfal.core.tpc: Davix: > COPY //eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778240871_gfal3_final?eos.app=traffic-shaping-benchmark-fts-manual HTTP/1.1
INFO gfal.core.tpc: > Content-Length: 0
INFO gfal.core.tpc: > Source: https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/traffic-shaping-files/source_1
INFO gfal.core.tpc: > TransferHeaderAuthorization: <redacted bearer token>
INFO gfal.core.tpc: > Credential: none
INFO gfal.core.tpc: > Authorization: <redacted bearer token>
```

On the local macOS machine used for this trace, the final COPY still returned
`403 Permission denied` because no X.509 proxy or `~/.globus` certificate was
available. The token/COPY sequence now matches the critical gfal2 header flow;
validate final success in an environment with a usable CERN X.509 identity.

## gfal2 successful result

Native gfal2-util completed the same transfer:

```text
RC=0
DST=https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778240020_gfal2_docs?eos.app=traffic-shaping-benchmark-fts-manual
INFO Copy succeeded using mode 3rd pull
success: Created
```

The important difference is that gfal2 automatically retrieves and stores
SE-issued macaroons before protected HTTP operations. It uses X.509 only to get
those tokens, then uses bearer authorization for stat and copy.

### Destination read token

Before probing the destination, gfal2 posts a macaroon request to the
destination URL:

```text
INFO Davix: > POST //eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778240020_gfal2_docs?eos.app=traffic-shaping-benchmark-fts-manual HTTP/1.1
> Content-Type: application/macaroon-request
> Content-Length: 61

INFO Davix: < HTTP/1.1 200 OK
INFO Davix: < Content-Length: 552
DEBUG (SEToken) Set bearer token in credential_map[https://.../destination_1778240020_gfal2_docs?eos.app=traffic-shaping-benchmark-fts-manual] (access=read) (validity=180)
INFO Using bearer token for HTTPS request authorization
```

`Content-Length: 61` matches a read macaroon request with 180 minutes of
validity:

```json
{"caveats": ["activity:LIST,DOWNLOAD"], "validity": "PT180M"}
```

The destination `PROPFIND` and `HEAD` then include a bearer authorization
header:

```text
INFO Davix: > PROPFIND //eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778240020_gfal2_docs?eos.app=traffic-shaping-benchmark-fts-manual HTTP/1.1
> Depth: 0
> Authorization: <redacted bearer token>

INFO Davix: < HTTP/1.1 404 NOT_FOUND

INFO Davix: > HEAD //eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778240020_gfal2_docs?eos.app=traffic-shaping-benchmark-fts-manual HTTP/1.1
> Authorization: <redacted bearer token>

INFO Davix: < HTTP/1.1 404 NOT_FOUND
```

### Source read token

gfal2 repeats token retrieval for the source:

```text
INFO Davix: > POST //eos/pilot/test/lobisapa/layout-erasure/traffic-shaping-files/source_1 HTTP/1.1
> Content-Type: application/macaroon-request
> Content-Length: 61

INFO Davix: < HTTP/1.1 200 OK
INFO Davix: < Content-Length: 543
DEBUG (SEToken) Set bearer token in credential_map[https://.../source_1] (access=read) (validity=180)
INFO Using bearer token for HTTPS request authorization
```

The source stat then uses the source bearer token:

```text
INFO Davix: > PROPFIND //eos/pilot/test/lobisapa/layout-erasure/traffic-shaping-files/source_1 HTTP/1.1
> Authorization: <redacted source bearer token>

INFO Davix: < HTTP/1.1 207 MULTI_STATUS
INFO Davix: < Content-Type: application/xml; charset=utf-8
```

### Destination write token for TPC

Just before the actual third-party copy, gfal2 retrieves a destination write
token:

```text
INFO Using bearer token for HTTPS request authorization (passive TPC)

INFO Davix: > POST //eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778240020_gfal2_docs?eos.app=traffic-shaping-benchmark-fts-manual HTTP/1.1
> Content-Type: application/macaroon-request
> Content-Length: 82

INFO Davix: < HTTP/1.1 200 OK
INFO Davix: < Content-Length: 579
DEBUG (SEToken) Set bearer token in credential_map[https://.../destination_1778240020_gfal2_docs?eos.app=traffic-shaping-benchmark-fts-manual] (access=write) (validity=130)
INFO Using bearer token for HTTPS request authorization
```

`Content-Length: 82` matches the write activity set with 130 minutes of
validity:

```json
{"caveats": ["activity:LIST,DOWNLOAD,MANAGE,UPLOAD,DELETE"], "validity": "PT130M"}
```

### TPC COPY request

The successful gfal2 COPY is a pull-mode request sent to the destination. It
uses:

- `Source` for the source URL
- `Authorization` for the destination write token
- `TransferHeaderAuthorization` for the source read token, forwarded to the
  destination server so it can pull from the source
- `Credential: none`

```text
INFO Davix: > COPY //eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778240020_gfal2_docs?eos.app=traffic-shaping-benchmark-fts-manual HTTP/1.1
> Source: https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/traffic-shaping-files/source_1
> TransferHeaderAuthorization: <redacted source bearer token>
> Credential: none
> Authorization: <redacted destination bearer token>
```

EOS redirects the COPY to a capability URL on a storage node. The second COPY
keeps the same auth pattern:

```text
INFO Davix: > COPY //eos/pilot/test/lobisapa/layout-erasure/tmp/destination_1778240020_gfal2_docs?...&cap.*=<redacted>&... HTTP/1.1
> Source: https://eospilot.cern.ch//eos/pilot/test/lobisapa/layout-erasure/traffic-shaping-files/source_1
> TransferHeaderAuthorization: <redacted source bearer token>
> Credential: none
> Authorization: <redacted destination bearer token>
```

The server streams WLCG performance markers and finishes with success:

```text
Stripe Bytes Transferred: 0
Stripe Bytes Transferred: 128991232
Stripe Bytes Transferred: 267403264
Stripe Bytes Transferred: 402669568
Stripe Bytes Transferred: 548421632
Stripe Bytes Transferred: 684736512
Stripe Bytes Transferred: 817905664
Stripe Bytes Transferred: 959463424
success: Created
INFO Copy succeeded using mode 3rd pull
```

## Implementation status

The Python HTTP TPC pull path now implements the critical tokenized COPY
sequence for EOS HTTPS URLs:

- it retrieves a source read token with 180 minutes of validity
- it retrieves a destination write token with 130 minutes of validity
- it sends the destination write token as `Authorization: Bearer ...` on the
  `COPY` request
- it sends the source read token as `TransferHeaderAuthorization: Bearer ...`
  on the `COPY` request
- it keeps `Credential: none`
- it preserves `eos.app=traffic-shaping-benchmark-fts-manual` on destination
  token and COPY URLs

Remaining gfal2 parity work:

1. Retrieve a source read token before source stat/TPC.
2. Retrieve a destination read token for destination probes.
3. Preserve bearer authorization on redirected COPY requests if a server
   redirects to a different host and the HTTP client strips `Authorization`.

The implemented `gfal-token` command provides the direct macaroon POST
mechanism used by the copy/TPC path.
