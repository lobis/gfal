# EOS Pilot token workflow

EOS can issue scoped `zteos64:` tokens for a path or directory tree. `gfal`
can generate one on EOS Pilot over SSH and pass it back to EOS URLs as the
`authz` query parameter.

Generate a read/write token for a directory tree:

```bash
TOKEN=$(gfal token \
  --ssh-host eospilot \
  --eos-instance root://eospilot.cern.ch \
  --write \
  --tree \
  --validity 720 \
  root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/)
```

Use the token explicitly for copies:

```bash
gfal cp --authz-token "$TOKEN" \
  ./file.dat root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/file.dat
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
delete operations through commands such as `gfal cp` and `gfal rm`, but it does
not make the FUSE mount writable.
