# EOS Public Examples

These examples use real public CERN Open Data paths on `eospublic.cern.ch`.
They are intended to be copy-pasted exactly as written, without credentials.

The examples below use the same small PHENIX file and directory that the
integration suite exercises in `tests/test_integration_eospublic.py`.

For a medium-sized public source family, the first 37 files matching
`DAOD_PHYSLITE.37019892.*` in
`https://eospublic.cern.ch/eos/opendata/atlas/rucio/data16_13TeV/`
add up to about `5.0 GiB` as measured on April 22, 2026.

## Pick a Working Public Path

If you want one file that is small, stable, and easy to inspect, start with:

```text
https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C
```

The matching XRootD path is:

```text
root://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C
```

The parent directory is:

```text
root://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/
```

## Ready-to-Run Shell Variables

```bash
FILE_HTTPS="https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C"
FILE_ROOT="root://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C"
DIR_ROOT="root://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/"
```

If you installed only `pip install gfal`, use the `https://` examples first.
`root://` requires XRootD support in the environment, for example via
`pip install "gfal[xrootd]"` or `conda install -c conda-forge xrootd`.

## Quick Smoke Tests

Show metadata for the public file:

```bash
gfal stat "$FILE_HTTPS"
```

Print the first few lines of the file:

```bash
gfal cat "$FILE_HTTPS" | head -n 5
```

Download the file to `/tmp`:

```bash
gfal cp "$FILE_HTTPS" file:///tmp/single_cluster_r5.C
```

## Checksums You Can Compare Against

This file is small enough that it works well as a checksum sanity check.

```bash
gfal sum "$FILE_HTTPS" MD5
gfal sum "$FILE_ROOT" ADLER32
```

Expected values:

- `MD5`: `93f402e24c6f870470e1c5fcc5400e25`
- `ADLER32`: `335e754f`
- Size: `2184` bytes

## Directory Listing Examples

List the public PHENIX directory over XRootD:

```bash
gfal ls "$DIR_ROOT"
```

Long listing:

```bash
gfal ls -lH "$DIR_ROOT"
```

You should see entries including:

- `single_cluster_r5.C`
- `single_cluster_r6.C`
- `gamma_gamma_r5.C`
- `pi0ntup_v2.pdf`

## HTTPS vs XRootD

Both schemes point at the same public EOS content, but they are useful for
slightly different first steps:

- Use `https://` for the easiest no-credentials demos: `stat`, `cat`, `sum`, and `cp` to a local file.
- Use `root://` when you want XRootD-native access or directory listing examples.
- Remember that absolute XRootD paths need a double slash after the host:
  `root://eospublic.cern.ch//eos/...`

## A Short End-to-End Demo

```bash
FILE_HTTPS="https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C"
DIR_ROOT="root://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/"

gfal stat "$FILE_HTTPS"
gfal cat "$FILE_HTTPS" | head -n 5
gfal sum "$FILE_HTTPS" MD5
gfal ls "$DIR_ROOT"
gfal cp "$FILE_HTTPS" file:///tmp/single_cluster_r5.C
```

## What Will Not Work on EOS Public

`eospublic.cern.ch` is read-only public storage. Commands that create, rename,
chmod, or delete files need a writable destination instead, such as:

- your own EOS area
- a local `file:///tmp/...` path
- a dedicated writable test endpoint such as `eospilot`
