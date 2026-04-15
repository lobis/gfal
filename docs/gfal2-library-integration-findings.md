# gfal2 Library Integration Findings for DIRAC and Rucio

This note looks at how two major downstream users, DIRAC and Rucio, use the
old Python `gfal2` bindings at the library level, and what that implies for a
Python-native `gfal` replacement.

The goal here is not to copy the `gfal2` API verbatim. The useful question is:
which capabilities do these systems actually need, and what would be a better,
more Pythonic interface for providing the same role?

## Sources

- DIRAC `GFAL2_StorageBase.py` at commit `d000937352c04e844d14f313500731c1f1027bc6`:
  [GitHub link](https://github.com/DIRACGrid/DIRAC/blob/d000937352c04e844d14f313500731c1f1027bc6/src/DIRAC/Resources/Storage/GFAL2_StorageBase.py)
- Rucio `gfal.py` at commit `6a4e74aa0da1fe3ee42ef26ba40653ebb18c9804`:
  [GitHub link](https://github.com/rucio/rucio/blob/6a4e74aa0da1fe3ee42ef26ba40653ebb18c9804/lib/rucio/rse/protocols/gfal.py)

## Executive Summary

DIRAC and Rucio do not depend on every part of `gfal2`. Most of their usage
clusters around a relatively small storage API:

- create a configured client/context
- `stat` as the universal metadata and existence probe
- `filecopy` with per-transfer options
- `unlink` / `rmdir`
- `rename`
- recursive directory creation
- directory listing
- checksums
- selected xattrs
- timeout/auth/plugin configuration

DIRAC uses more of the long tail than Rucio, especially:

- tape staging (`bring_online`, `bring_online_poll`, `release`)
- recursive directory inspection and removal
- bulk metadata collection
- xattr enumeration

Rucio uses a narrower but operationally important subset:

- upload/download via `filecopy`
- delete
- rename
- exists
- stat + checksum
- space-usage via a specific xattr
- cancellation of stuck transfers

For `gfal`, the main conclusion is that a replacement library should center on
an explicit high-level client plus a transfer-options object, not on exposing a
mutable plugin context like `gfal2`.

## What DIRAC Uses

DIRAC's `GFAL2_StorageBase` is effectively a storage backend abstraction built
directly on top of a single `gfal2` context. The important calls are:

- `gfal2.creat_context()`
- `ctx.set_opt_boolean`, `set_opt_integer`, `set_opt_string`, `set_opt_string_list`
- `ctx.stat(path)`
- `ctx.transfer_parameters()`
- `ctx.filecopy(params, src, dst)`
- `ctx.unlink(path)`
- `ctx.rmdir(path)`
- `ctx.mkdir_rec(path, mode)`
- `ctx.listdir(path)`
- `ctx.rename(path, new_path)`
- `ctx.checksum(path, algorithm)`
- `ctx.listxattr(path)` and `ctx.getxattr(path, attr)`
- `ctx.bring_online(path, lifetime, timeout, async)`
- `ctx.bring_online_poll(path, token)`
- `ctx.release(path, token)`

### DIRAC usage patterns

#### 1. `stat` is the central primitive

DIRAC uses `ctx.stat()` for:

- existence checks
- file vs directory classification
- file size lookup
- building higher-level metadata dicts

This is consistent with `gfal2` usage elsewhere too: `stat` is the one call
every integration assumes will work reliably.

Implication for `gfal`:

- `stat()` should remain a first-class API
- it should return a rich object with typed accessors, not a string to parse
- an `exists()` helper should be provided, but should be implemented in terms of
  the same underlying metadata path

#### 2. Transfer behavior matters as much as copy itself

DIRAC does not just "copy files". It configures transfers with:

- timeout
- overwrite behavior
- create-parent behavior
- number of streams
- checksum verification mode
- source/destination space tokens

It then calls `ctx.filecopy(params, src, dst)`.

Implication for `gfal`:

- a future library API needs an explicit `copy()` method with structured
  options, not a pile of boolean kwargs
- checksumming policy should be part of transfer options
- timeout should support both a default client timeout and a per-transfer
  override
- tokens and TPC-related settings belong in the transfer options layer, not in
  the core client constructor

#### 3. DIRAC relies on recursive directory workflows

DIRAC uses:

- `mkdir_rec`
- `listdir`
- repeated `stat` per entry
- `rmdir`
- `unlink`

It builds recursive delete, directory size, and directory metadata operations
itself above these primitives.

Implication for `gfal`:

- we do not need a huge directory API to satisfy this role
- the important part is to expose reliable primitives:
  `mkdir(parents=True)`, `iterdir()`, `stat()`, `unlink()`, `rmdir()`
- once those are solid, higher-level helpers can be built in Python

#### 4. Extended attributes are part of the integration surface

DIRAC uses `listxattr` and `getxattr` to enrich metadata. It does not appear to
require a generic metadata schema from `gfal2`; it is comfortable composing one
itself.

Implication for `gfal`:

- xattrs are worth keeping in the Python API
- they should raise a clear "unsupported" error on protocols that do not expose
  them
- bulk helpers such as `xattrs()` returning a mapping would be more ergonomic
  than forcing repeated `listxattr` + `getxattr`

#### 5. Tape staging is a separate concern

DIRAC uses `bring_online`, `bring_online_poll`, and `release`. This is clearly
outside the current scope of this repository, which intentionally stubs tape/SRM
commands.

Implication for `gfal`:

- this should not be forced into the base storage client API today
- if ever implemented, it should live in a separate capability layer or
  protocol-specific extension

## What Rucio Uses

Rucio's use is narrower and more operational:

- configure a context
- upload/download using `filecopy`
- delete with `unlink`
- rename
- existence checks via `stat`
- stat + checksum for replica validation
- query space usage via xattr
- cancel operations on timeout

### Rucio usage patterns

#### 1. Rucio treats `gfal2` as an execution backend

Rucio is much less interested in rich filesystem browsing than DIRAC. It mainly
needs a reliable engine for:

- `get`
- `put`
- `delete`
- `rename`
- `exists`
- `stat`

This is encouraging for `gfal`, because it means the core replacement target is
small and concrete.

#### 2. Per-transfer control is still central

Rucio uses `transfer_parameters()` and `filecopy()` with:

- `create_parent`
- `strict_copy`
- source/destination space tokens
- timeout overrides

It also arms a watchdog timer and calls `ctx.cancel()` if a transfer exceeds the
deadline.

Implication for `gfal`:

- the future copy API should support cancellation or at least time-bounded
  execution with deterministic cleanup semantics
- `strict_copy` is an example of a policy flag that probably belongs in a
  transfer-options dataclass
- a transfer handle object could be cleaner than emulating mutable `gfal2`
  context state

#### 3. Rucio uses `stat()` in a brittle way today

Its `stat()` implementation calls `ctx.stat(path)`, converts the result to
`str`, then parses fields out of the string. That is a limitation of how the old
 binding is consumed there, not a desirable interface.

Implication for `gfal`:

- we should not imitate this
- we should return a typed result with `size`, `mode`, timestamps, and
  classification helpers

#### 4. Space usage is xattr-shaped

Rucio gets storage-space data with:

- `ctx.getxattr(path, "spacetoken.description?<TOKEN>")`

and then parses the JSON payload.

Implication for `gfal`:

- even if generic xattrs remain low-level, a convenience helper like
  `space_usage(url, token=...)` would make sense
- this is exactly the sort of protocol-specific operation that can sit on top of
  the xattr foundation without contaminating the core API

#### 5. Rucio depends on auth and plugin knobs

Rucio configures:

- bearer-token auth
- X.509 cert/key auth
- plugin-specific timeouts
- XRootD path normalization behavior
- SRM transfer URL protocols

Implication for `gfal`:

- the replacement does need a credible auth/config story
- but it does not need to expose raw plugin mutation to callers
- most consumers want outcomes, not plugin handles

## Shared Minimum Contract

Across DIRAC and Rucio, the common `gfal2` dependency surface is roughly:

| Capability | DIRAC | Rucio | Importance |
| --- | --- | --- | --- |
| Configured client/context | yes | yes | essential |
| `stat` | yes | yes | essential |
| `exists` | via `stat` | via `stat` | essential |
| file copy with options | yes | yes | essential |
| delete file | yes | yes | essential |
| rename/move | yes | yes | essential |
| mkdir with parents | yes | yes | essential |
| list directory | yes | limited | high |
| checksum | yes | yes | high |
| xattrs | yes | yes | medium/high |
| cancel transfer | no | yes | medium/high |
| recursive delete helpers | yes | no | medium |
| tape staging | yes | no | out of current scope |
| SRM/GridFTP-specific tuning | yes | yes | out of current scope for this repo |

## What This Means for `gfal`

`gfal` already has a useful start through `GfalClient`:

- `stat`
- `ls`
- `mkdir`
- `rm`
- `rmdir`
- `rename`
- `chmod`
- `open`
- `checksum`
- `getxattr` / `setxattr` / `listxattr`

For the HTTP/XRootD-only scope of this project, that is already close to the
shared minimum contract above.

The biggest remaining gap is not raw filesystem access. It is the lack of a
library-level transfer API comparable in role to `gfal2`'s `filecopy +
transfer_parameters`.

Today, most copy sophistication lives in the CLI implementation, not in
`src/gfal/core/api.py`.

## Recommended Pythonic Replacement Shape

Instead of copying the old `gfal2` binding shape, a better API would be
something like:

```python
from dataclasses import dataclass
from typing import Literal


@dataclass
class ChecksumPolicy:
    algorithm: str
    mode: Literal["source", "target", "both"] = "both"


@dataclass
class CopyOptions:
    overwrite: bool = False
    create_parents: bool = False
    timeout: int | None = None
    checksum: ChecksumPolicy | None = None
    source_space_token: str | None = None
    destination_space_token: str | None = None
    strict: bool = False
    streams: int | None = None
    tpc: Literal["never", "auto", "only"] = "never"


client = GfalClient(...)
client.copy(src, dst, options=CopyOptions(create_parents=True, overwrite=True))
```

Key points:

- immutable options objects are clearer than mutating a live context
- typed results are better than parsing strings
- protocol-specific extras can be layered on top rather than pushed into every
  call signature

## Concrete Recommendations

### 1. Add a library-level `copy()` API

This is the single biggest missing piece if `gfal` is to replace `gfal2` for
consumers like DIRAC and Rucio.

It should support:

- local <-> remote and remote <-> remote copies
- overwrite
- parent creation
- timeout override
- checksum verification policy
- TPC mode
- optional source/destination token fields

### 2. Add `exists()` explicitly

Both downstreams effectively need it. Even if implemented using `stat()`, it is
worth exposing directly.

### 3. Add a bulk xattr helper

Something like:

```python
attrs = client.xattrs(url)
```

would map more naturally to how DIRAC and Rucio consume metadata.

### 4. Consider a cancellable transfer abstraction

Rucio's watchdog + `ctx.cancel()` behavior suggests that long-running transfers
need an interruption story. A Pythonic design could be:

- `client.copy(...)` for simple blocking use
- `client.start_copy(...) -> TransferHandle` for advanced callers

where the handle can expose:

- `wait()`
- `cancel()`
- `done()`

### 5. Keep tape/SRM out of the base client for now

DIRAC's staging methods are real usage, but they do not fit the current
HTTP/XRootD-only scope. The clean approach is:

- keep core storage operations protocol-agnostic
- add protocol/capability extensions later if this project expands

## Practical Compatibility Goal

A realistic goal is not "be a drop-in replacement for all `gfal2` code". That
would pull this project toward SRM, GridFTP, tape APIs, and plugin-by-plugin
compatibility.

A better goal is:

- be a replacement for the common storage-manipulation subset
- on local files, HTTP/WebDAV, and XRootD
- with a more typed and Pythonic API than `gfal2`

For DIRAC- and Rucio-like consumers, that subset is already well defined:

- metadata
- existence
- copy
- delete
- rename
- mkdir
- listing
- checksums
- xattrs
- auth/timeouts

If `gfal` provides those well at the library level, it can fulfill the same
practical role for a large share of real-world usage without cloning the entire
legacy `gfal2` surface.
