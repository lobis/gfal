# gfal2 command compatibility audit

This audit compares the Python `gfal` CLI against the captured gfal2-util
reference in `docs/gfal2-util-help-reference.md`. The reference was captured
from `lxplus.cern.ch` on 2026-03-18 and remains the canonical flag-level source
for command compatibility.

The audit scope is the gfal2-util command family implemented by this package:

- `gfal-ls`
- `gfal-copy` / `gfal-cp`
- `gfal-rm`
- `gfal-cat`
- `gfal-stat`
- `gfal-rename`
- `gfal-mkdir`
- `gfal-chmod`
- `gfal-sum`
- `gfal-xattr`
- `gfal-save`
- `gfal-bringonline`
- `gfal-archivepoll`
- `gfal-evict`
- `gfal-token`

The top-level `gfal <command>` form and `gfal-mount` are extensions, not native
gfal2-util compatibility requirements.

## Cross-command findings

| Area | Status | Notes |
| --- | --- | --- |
| Legacy executable names | Fixed | `pyproject.toml` now registers the gfal2-style `gfal-*` console scripts, including both `gfal-copy` and `gfal-cp`. The dispatcher accepts those program names directly. |
| `gfal <command>` form | Extension | Kept as the primary modern entry point. |
| Common flags | Compatible plus extensions | gfal2 common flags are accepted: `-h/--help`, `-V/--version`, `-v`, `-D/--definition`, `-t/--timeout`, `-E/--cert`, `--key`, `-4`, `-6`, `-C/--client-info`, `--log-file`. This implementation also exposes `--quiet`, `--authz-token`, and `--verify/--no-verify`. |
| Ignored gfal2-specific flags | Compatible | `-D`, `-C`, `-4`, `-6` are accepted. IPv4/IPv6 influence HTTP session creation where possible; gfal2/GridFTP-specific parameter injection remains intentionally unsupported. |
| Output compatibility mode | Partial | `GFAL_CLI_GFAL2=1` keeps plain output and legacy formatting for key commands. Rich help layout differs from argparse gfal2 help, but flags are accepted. |
| Protocol scope | Intentional subset | This project supports local files, HTTP/WebDAV, and XRootD. SRM/GridFTP/tape-specific runtime behavior is stubbed or ignored by design. |

## Command matrix

| Command | CLI/flags | Runtime behavior | Existing comparison coverage | Audit result |
| --- | --- | --- | --- | --- |
| `gfal-ls` | Compatible core flags. Extensions: multiple paths, `-r/--reverse`, `-S`, `-U`, `--sort`. | Lists local, WebDAV/HTTP, and XRootD. HTTP directory listing depends on WebDAV support. | Unit tests mirror gfal2 functional tests; Docker gfal2 comparison covers eospublic XRootD listing and ENOENT exit code. | Compatible for supported protocols, with documented extensions. |
| `gfal-copy` / `gfal-cp` | Compatible core gfal2 flags plus extensions `--tpc`, `--tpc-only`, `--parallel`, `--limit`, `--compare`, preserve-time toggles. `--copy-mode` additionally accepts `auto` as an extension. | Local, HTTP/WebDAV, and XRootD copies. HTTP TPC now follows gfal2's EOS macaroon sequence for pull mode. GridFTP/SRM knobs are accepted with warnings/limited no-op behavior. | Unit coverage is extensive. Docker comparison covers local-file existing-destination and mtime behavior. Live `-vvv` comparison against eospilot is documented in `docs/http-tpc-token-trace.md`. | Compatible for supported protocols; HTTP TPC critical trace now aligned with gfal2. |
| `gfal-rm` | Compatible: `-r/-R/--recursive`, `--dry-run`, `--just-delete`, `--from-file`, `--bulk`. | Removes supported local/HTTP/XRootD targets. `--bulk` is accepted but sequential. | Unit tests mirror gfal2 behavior; Docker/eospilot comparison covers ENOENT and successful remove. | Compatible for supported protocols; bulk is compatibility no-op. |
| `gfal-cat` | Compatible: `-b/--bytes`, multiple files accepted as in gfal2. | Streams file bytes to stdout; handles broken pipe. | Unit tests plus Docker comparison against eospublic HTTP content and ENOENT. | Compatible for supported protocols. |
| `gfal-stat` | Compatible core single-file behavior. Extension: multiple paths. | Formats stat output in gfal2-compatible mode. | Unit tests plus Docker comparisons for eospublic/eospilot size and ENOENT exit codes. | Compatible for supported protocols, with multi-path extension. |
| `gfal-rename` | Compatible positional interface: `source destination`. | Uses backend-native move where available, including XRootD client rename. | Unit tests and eospilot comparison via stat-after-rename. | Compatible for supported protocols. |
| `gfal-mkdir` | Compatible: `-m/--mode`, `-p/--parents`, multiple directories. | Creates local/HTTP WebDAV/XRootD directories where backend supports it. | Unit tests mirror gfal2; eospilot comparison covers `-p` and EEXIST. | Compatible for supported protocols. |
| `gfal-chmod` | Compatible positional interface. Extension: multiple files. | Applies local/XRootD chmod when supported; HTTP chmod is a no-op because HTTP has no POSIX mode. | Unit tests cover local behavior and unsupported surfaces. | Compatible for supported protocols where chmod exists; HTTP no-op is an intentional protocol limitation. |
| `gfal-sum` | Compatible positional interface: `file checksum_type`. Extra algorithms are supported. | Computes checksums via remote metadata where available, otherwise client-side fallback. | Unit tests plus Docker comparison for eospublic ADLER32. | Compatible for supported algorithms/protocols; extra algorithms are extensions. |
| `gfal-xattr` | Compatible positional interface: `file [attribute]`, with `key=value` for set. | Uses local xattrs or backend xattr helpers where supported. | Unit tests cover get/set/list/error behavior. | Compatible where backend xattrs exist; unsupported filesystems report a clear error. |
| `gfal-save` | Compatible positional interface: `file`; overwrites existing file from stdin. | Writes stdin to supported destinations. | Unit tests and eospilot comparison via readback. | Compatible for supported protocols. |
| `gfal-bringonline` | Compatible flags/positionals. | Stub: returns non-zero with clear unsupported message. | Unit tests cover flags and unsupported behavior. | CLI-compatible stub by design; runtime is intentionally omitted. |
| `gfal-archivepoll` | Compatible flags/positionals. | Stub: returns non-zero with clear unsupported message. | Unit tests cover flags and unsupported behavior. | CLI-compatible stub by design; runtime is intentionally omitted. |
| `gfal-evict` | Fixed positional order to `file [token]`. | Stub: returns non-zero with clear unsupported message. | Unit tests cover help/order and unsupported behavior. | CLI-compatible stub by design; runtime is intentionally omitted. |
| `gfal-token` | Fixed positional order to `path [activities...]`; supports `--issuer`, `--validity`, `-w/--write`. | Implements HTTPS SE-issued macaroon retrieval, including gfal2-compatible direct macaroon body construction and issuer fallbacks. | Unit tests cover request construction; live token trace comparison was used for `docs/http-tpc-token-trace.md`. | Compatible for HTTPS token retrieval; SRM/tape token meanings remain out of scope. |

## Intentional extensions

These are accepted beyond gfal2-util and should not be treated as regressions:

- parent command form: `gfal <command>`
- `gfal mount`
- `--quiet`
- `--authz-token`
- `--verify/--no-verify`
- copy: `--tpc`, `--tpc-only`, `--copy-mode auto`, `--compare`, `--parallel`,
  `--limit`, `--preserve-times`, `--no-preserve-times`
- ls: multiple paths, sorting flags, reverse ordering
- stat/chmod: multiple paths
- sum: extra checksum algorithms beyond the gfal2 examples

## Remaining live-check gaps

The following are best verified with live gfal2 on `lxplus.cern.ch` because the
captured help reference does not fully specify output details:

1. `gfal-ls -l --xattr` formatting on an EOS path with readable xattrs.
2. `gfal-xattr` list/get/set exact output on EOS Pilot.
3. `gfal-chmod` exact success/error behavior on EOS Pilot over XRootD and HTTP.
4. Additional `-vvv` trace comparison for non-TPC HTTP `stat`, `cat`, and `sum`
   if strict trace shape becomes a goal beyond the tokenized TPC path.

Per the repository SSH policy, run live `lxplus.cern.ch` checks only after
explicit confirmation and only on read-only eospublic targets or bounded eospilot
paths where the requested operation necessarily writes.
