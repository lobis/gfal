# EOS Pilot token workflow

EOS can issue scoped `zteos64:` tokens for a path or directory tree. `gfal`
can generate one on EOS Pilot over SSH, save it locally, and pass it back to
EOS URLs as the `authz` query parameter.

Generate a read/write token for a directory tree:

```bash
gfal token \
  --ssh-host eospilot \
  --eos-instance root://eospilot.cern.ch \
  --write \
  --tree \
  --validity 720 \
  --output-file ~/.cache/gfal/eospilot-iaxo.token \
  root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/
```

Use the token for copies:

```bash
gfal cp --authz-token-file ~/.cache/gfal/eospilot-iaxo.token \
  ./file.dat root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/file.dat
```

Use the same token for a read-only mount:

```bash
gfal mount --authz-token-file ~/.cache/gfal/eospilot-iaxo.token \
  root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/ /tmp/iaxo
```

`--authz-token-file` is preferred over passing token material directly on the
command line because long tokens are sensitive credentials and can otherwise
leak through shell history or process listings. The token file is read when the
`gfal` client is constructed. `gfal token --output-file` creates the file with
`0600` permissions.

The mount command is currently read-only. A read/write token allows upload and
delete operations through commands such as `gfal cp` and `gfal rm`, but it does
not make the FUSE mount writable.
