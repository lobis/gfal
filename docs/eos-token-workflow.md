# EOS authz token workflow

EOS can issue scoped `zteos64:` tokens that grant access to a single path or
a directory tree for a limited time. `gfal` does not generate these tokens
itself — generate them with the EOS tools on the EOS side, then pass them to
`gfal`, which appends the token to EOS URLs as the `authz` query parameter.

This works against any EOS instance (`eospublic`, `eospilot`, `eosatlas`,
`eoscms`, …); the examples below use `eospilot.cern.ch` and the path
`/eos/pilot/test/lobisapa/iaxo/`, but you should substitute the host and path
for your own EOS instance.

## Generate the token (EOS side)

The `eos token` command must run on a node that has permission to mint tokens
for the path — typically the EOS instance itself or an `lxplus`-style login
host configured for it. Connect to that host and run `eos token` **without**
an explicit `root://…` node argument; passing one routes the request through
the remote node and EOS rejects it with `cannot issue tokens: Operation not
permitted`.

Generate a read/write token for a directory tree with 12 hours of validity:

```bash
EXPIRES=$(($(date +%s) + 12 * 60 * 60))
TOKEN=$(ssh eospilot \
  eos token \
  --path /eos/pilot/test/lobisapa/iaxo/ \
  --permission rwx \
  --tree \
  --expires "$EXPIRES")
```

For read-only access use `--permission rx`. For a single file rather than a
tree, drop `--tree` and point `--path` at the file.

## Use the token (gfal side)

Use the HTTPS/WebDAV endpoint for token-authenticated transfers. If the CERN
CA is not installed locally, add `--no-verify` only for an explicit insecure
test:

```bash
gfal cp --authz-token "$TOKEN" --no-verify \
  ./file.dat https://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/file.dat
```

Read a file:

```bash
gfal cat --authz-token "$TOKEN" --no-verify \
  https://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/file.dat
```

The same endpoint handles token-authenticated deletes:

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

gfal cp --no-verify ./file.dat https://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/file.dat
gfal mount root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/ /tmp/iaxo
```

`gfal` also accepts `GFAL_AUTHZ_TOKEN` as a gfal-specific fallback. An
explicit `--authz-token` option takes priority over both environment
variables.

> **Note on Kerberos / X509:** if you already have a valid CERN Kerberos
> ticket (`klist` shows a TGT) or an X509 proxy at `/tmp/x509up_u<uid>`, the
> XRootD client may authenticate you that way and silently ignore the
> `authz` token in the URL. The HTTPS endpoint does not have this ambiguity
> — it uses the token directly.

The mount command is currently read-only. A read/write token allows upload
and delete operations through commands such as `gfal cp` and `gfal rm` over
HTTPS, but it does not make the FUSE mount writable. In live testing against
EOS Pilot, HTTPS/WebDAV accepted `authz` query tokens reliably for reads,
uploads, and deletes; XRootD also accepts them but can be shadowed by other
auth methods.
