# Performance Investigation Findings

This note documents the benchmarking and debugging work done to compare
`gfal2-util` and this Python-native `gfal` implementation on CERN EOS, with a
focus on HTTP/WebDAV and XRootD copy performance.

The intended long-term goal is clear:

- `gfal` should be at least as fast as `gfal2-util` for the supported
  protocols.
- In practice, the most important comparison matrix is:
  - `gfal2` over `https://`
  - `gfal2` over `root://`
  - `gfal` over `https://`
  - `gfal` over `root://`

## Test Environment

Benchmarking was run remotely on `lobis-eos-dev` through the `lxplus` tunnel,
using the same proxy and EOS paths used by CI.

Representative endpoints:

- public source over HTTPS:
  `https://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37020379._000600.pool.root.1`
- public source over XRootD:
  `root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37020379._000600.pool.root.1`
- pilot destination over HTTPS:
  `https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp/...`
- pilot destination over XRootD:
  `root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp/...`

Representative workloads:

- local file (`256 MiB`) -> EOS pilot
- EOS public file (`1.59 GiB`) -> EOS pilot

## Main Bug That Was Fixed

An actual performance bug was found in the HTTP/WebDAV destination write path.

Before the fix:

- `gfal cp` to HTTP/WebDAV opened the destination with a buffered writer
- the implementation accumulated the full payload in memory
- the final HTTP `PUT` only happened on `close()`

That meant a streamed copy effectively became:

1. download the full source to the client
2. only then start uploading to the destination

This was much worse than expected behavior and could make transfers look
artificially slow.

The fix was to use a dedicated streaming HTTP upload path for copy operations,
while keeping normal buffered `PUT` behavior for generic file writes such as
`gfal save` and `open(..., "wb")`.

## Regression That Was Avoided

After introducing streamed HTTP writes, a correctness regression showed up in
compare tests:

- `gfal save` / generic write paths could leave zero-byte files on EOS
- `stat` would report size `0`
- `cat` would return empty content

The fix was to split the behaviors:

- buffered `PUT` for generic writes
- streaming `PUT` only for `gfal cp`

This kept the copy-path improvement without breaking the general file-write API.

## Stable Benchmark Results

The most useful benchmark comparisons observed during the investigation were:

### HTTPS

`local -> eospilot`, `256 MiB`, streamed:

- `gfal2-util`: about `1.3s` to `1.5s`
- `gfal`: about `1.45s` to `1.67s`

This is a small gap. `gfal` is somewhat slower, but in the same ballpark.

`eospublic -> eospilot`, `1.59 GiB`, streamed:

- `gfal2-util`: about `6.5s`
- `gfal`: initially about `13.3s`, and in later reruns sometimes hung long
  enough to hit a `240s` timeout

This is the main unresolved performance problem.

### XRootD

`local -> eospilot`, `256 MiB`:

- `gfal2-util`: about `1.55s`
- `gfal`: about `1.85s`

Again, this is slower but still reasonably close.

`eospublic -> eospilot`, `1.59 GiB`:

- `gfal`: about `9.6s`
- `gfal2-util`: one observed run failed with a redirect-limit style error on
  destination, so the comparison was not always symmetric

The important conclusion is that the large performance gap is primarily in the
HTTPS destination path, not in XRootD.

## What The Wire-Level Investigation Found

The HTTP/WebDAV slowdown was investigated further by sampling sockets and
trying equivalent manual uploads.

The most important observation was:

- EOS pilot namespace `PUT` requests can return `307 Temporary Redirect`
- the redirect target is a signed data-node URL
- `gfal2-util` handles this flow correctly
- the current aiohttp-based streamed upload path in `gfal` does not yet match
  that behavior robustly

Using `curl -v` against the namespace endpoint showed the pattern clearly:

1. client sends `PUT` to `eospilot.cern.ch`
2. EOS replies with `307 TEMPORARY_REDIRECT`
3. reply includes a signed `Location:` URL pointing to a data node on port `8443`

This matters because a naive streamed upload path can end up talking to the
namespace endpoint in a way that looks superficially successful but does not
complete the real write.

## Important Debugging Findings

Several non-obvious observations were useful:

- For the problematic public HTTPS copy, our process often pushed essentially
  the full payload over the socket and then stalled waiting for completion.
- During those stalls, `gfal stat` on the destination often still reported a
  zero-byte file.
- A plain `curl -T file https://eospilot...` could also appear to succeed
  quickly while still leaving a zero-byte file behind if the redirect flow was
  not followed appropriately.
- An explicit two-step shell experiment worked:
  - first issue an empty `PUT` to the namespace endpoint
  - capture the signed `Location` from the `307`
  - then upload the real body directly to that signed data-node URL
  - in that experiment the destination file was written correctly

This strongly suggests that the remaining gap is not just "Python is slower",
but specifically that the current streamed HTTP upload strategy does not yet
model the EOS redirect/upload flow well enough.

## Experiments That Were Tried And Rejected

Several candidate fixes were tested and were not merged because they were not
safe or did not actually solve the issue:

- forcing `Content-Length` on the aiohttp streamed upload
  - this caused hangs even for simpler uploads in some runs
- forcing `Connection: close`
  - did not fix the stalled completion path
- returning before reading the full response body
  - did not remove the EOS completion problem
- directly pre-resolving the redirect in the Python implementation
  - explained part of the behavior, but the attempted implementation was not
    yet robust enough to keep
- trying `Expect: 100-continue`
  - did not produce a safe, convincing fix

These experiments were useful for narrowing the problem, but they should be
treated as dead ends unless revisited carefully.

## Safe Improvements That Were Kept

Not every improvement from the investigation was discarded.

The small safe optimization that was kept was:

- avoid an unnecessary `bytes()` copy in the streaming writer when the chunk is
  already a `bytes` object

This is a minor optimization, but it is correct and low-risk.

## Recommended Benchmark Matrix Going Forward

Future work should keep using the same four-scenario comparison for each transfer
case:

1. `gfal2` over `https://`
2. `gfal2` over `root://`
3. `gfal` over `https://`
4. `gfal` over `root://`

For each scenario, use the same source and destination pattern. The most useful
pair so far has been:

- `local -> eospilot`
- `eospublic -> eospilot`

Keep these variables identical across runs:

- source file
- destination area
- host used for the benchmark
- proxy/auth material
- copy mode

## Current Conclusion

The investigation reached three practical conclusions:

1. A real HTTP destination buffering bug was found and fixed.
2. `gfal` is reasonably competitive with `gfal2-util` for local uploads and for
   XRootD transfers.
3. The remaining large gap is in HTTPS uploads to EOS, especially for
   `eospublic -> eospilot`, and it appears to be tied to EOS's redirect-based
   upload flow rather than to a simple Python throughput problem.

So the work is incomplete from a performance point of view:

- we improved correctness and removed one major bottleneck
- but we have not yet achieved parity with `gfal2-util` for the hardest HTTPS
  case

That remaining gap should be treated as an active follow-up item, not as solved.
