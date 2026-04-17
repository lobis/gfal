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

- `gfal2-util`: about `1.4s` to `1.5s`
- `gfal`: about `1.57s` to `1.59s`

This is a small gap. `gfal` is somewhat slower, but in the same ballpark.

`eospublic -> eospilot`, `1.59 GiB`, streamed:

- `gfal2-util`: about `6.6s` to `7.0s`
- `gfal`: about `6.9s`

After the final read-path fix, this is effectively at parity on the CI pilot
destination.

### XRootD

`local -> eospilot`, `256 MiB`:

- `gfal2-util`: about `1.55s`
- `gfal`: about `1.85s`

This is effectively identical.

`eospublic -> eospilot`, `1.59 GiB`:

- `gfal`: about `9.6s`
- `gfal2-util`: one observed run failed with a redirect-limit style error on
  destination, so the comparison was not always symmetric

The important conclusion is that the hardest remaining work was in the HTTPS
copy path; XRootD was already reasonably close throughout the investigation.

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

This showed that the EOS namespace redirect flow had to be understood
explicitly, but it was not the whole story.

The other important breakthrough was on the source side:

- fsspec's default HTTP reader was much slower than necessary for large
  sequential reads from `eospublic`
- using a true streaming GET reader for copy operations dropped
  `eospublic https -> local` from about `5.6s` to about `2.2s`
- once that faster source path was paired with the existing streamed upload on
  the CI pilot destination, end-to-end `https -> https` copy reached parity

## Experiments That Were Tried And Rejected

Several candidate fixes were tested and were not merged because they were not
safe or did not actually solve the issue:

- forcing `Content-Length` on the aiohttp streamed upload
  - this caused hangs even for simpler uploads in some runs
- forcing `Connection: close`
  - did not fix the stalled completion path
- returning before reading the full response body
  - did not remove the EOS completion problem
- staging through a local temporary file and then re-uploading
  - stable, but slower than the fully streamed path because it serialized the
    download and upload legs
- trying `Expect: 100-continue`
  - did not produce a safe, convincing fix

These experiments were useful for narrowing the problem, but they should be
treated as dead ends unless revisited carefully.

## Safe Improvements That Were Kept

The improvements that were kept are:

- a dedicated streaming HTTP upload path for `gfal cp`, while keeping buffered
  writes for generic `save` / `open(..., "wb")`
- redirect-aware EOS namespace handling for streamed HTTP writes
- a true streaming HTTP GET reader for copy operations, instead of the slower
  default fsspec HTTP reader
- avoid an unnecessary `bytes()` copy in the streaming writer when the chunk is
  already a `bytes` object

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

The investigation reached four practical conclusions:

1. A real HTTP destination buffering bug was found and fixed.
2. The EOS HTTPS upload flow really does depend on understanding the namespace
   redirect to the signed data-node URL.
3. The final performance breakthrough came from fixing the HTTP source read
   path, not only from tuning the destination write path.
4. On the actual CI pilot destination, `gfal` now reaches parity with
   `gfal2-util` for the hardest `https -> https` public-file benchmark, while
   staying at parity or near-parity for the other benchmarked scenarios.

One practical warning remains:

- benchmark results depend on using the same pilot destination area as CI
- some ad hoc EOS pilot paths can behave differently from the CI path, both for
  redirects and for write permissions

So future benchmarking should always use the CI destination path when making
performance claims.
