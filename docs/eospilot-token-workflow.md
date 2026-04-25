# EOS Pilot token workflow

EOS can issue scoped `zteos64:` tokens for a path or directory tree. `gfal`
does not generate these tokens itself. Generate them with the EOS tools, then
pass them to `gfal`; it appends the token to EOS URLs as the `authz` query
parameter.

Generate a read/write token for a directory tree with 12 hours of validity:

```bash
EXPIRES=$(($(date +%s) + 12 * 60 * 60))
TOKEN=$(ssh eospilot \
  eos root://eospilot.cern.ch token \
  --path /eos/pilot/test/lobisapa/iaxo/ \
  --permission rwx \
  --tree \
  --expires "$EXPIRES")
```

Use the token explicitly for copies:

```bash
gfal cp --authz-token "$TOKEN" \
  ./file.dat root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/file.dat
```

Use the HTTPS/WebDAV endpoint for token-authenticated deletes. If the CERN CA
is not installed locally, add `--no-verify` only for an explicit insecure test:

```bash
gfal rm --authz-token "$TOKEN" --no-verify \
  https://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/file.dat
```

Use the same token for a read-only mount:

```bash
gfal mount --authz-token "$TOKEN" \
  root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/ /tmp/iaxo
```

If you do not want to put the token on each command line, export it with the
EOS-native environment variable instead:

```bash
export EOSAUTHZ="$TOKEN"

gfal cp ./file.dat root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/file.dat
gfal mount root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/ /tmp/iaxo
```

`gfal` also accepts `GFAL_AUTHZ_TOKEN` as a gfal-specific fallback. An explicit
`--authz-token` option takes priority over both environment variables.

The mount command is currently read-only. A read/write token allows upload and
delete operations through commands such as `gfal cp` and HTTPS `gfal rm`, but it
does not make the FUSE mount writable. In live EOS Pilot testing, XRootD reads
and uploads accepted `authz` query tokens, while token-authenticated deletion
worked reliably through the HTTPS/WebDAV endpoint.
