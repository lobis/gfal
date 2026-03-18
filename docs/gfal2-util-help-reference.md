# gfal2-util CLI Reference

Help output captured from `lxplus.cern.ch` on 2026-03-18. Used as the authoritative
reference for CLI backwards-compatibility in this reimplementation.

---

## gfal-ls

```
usage: gfal-ls [-h] [-V] [-v] [-D DEFINITION] [-t TIMEOUT] [-E CERT]
               [--key KEY] [-4] [-6] [-C CLIENT_INFO] [--log-file LOG_FILE]
               [-a] [-l] [-d] [-H] [--xattr XATTR]
               [--time-style {full-iso,long-iso,iso,locale}] [--full-time]
               [--color {always,never,auto}]
               file

Gfal util LS command. List directory's contents.

positional arguments:
  file                  file's uri

optional arguments:
  -h, --help            show this help message and exit
  -V, --version         output version information and exit
  -v, --verbose         enable the verbose mode
  -D DEFINITION, --definition DEFINITION
                        override a gfal parameter
  -t TIMEOUT, --timeout TIMEOUT
                        maximum time for the operation to terminate - default is 1800 seconds
  -E CERT, --cert CERT  user certificate
  --key KEY             user private key
  -4                    forces gfal2-util to use IPv4 addresses only
  -6                    forces gfal2-util to use IPv6 addresses only
  -C CLIENT_INFO, --client-info CLIENT_INFO
                        provide custom client-side information
  --log-file LOG_FILE   write Gfal2 library logs to the given file location
  -a, --all             display hidden files
  -l, --long            long listing format
  -d, --directory       list directory entries instead of contents
  -H, --human-readable  with -l, prints size in human readable format (e.g., 1K 234M 2G)
  --xattr XATTR         query additional attributes. Can be specified multiple times.
                        Only works for --long output
  --time-style {full-iso,long-iso,iso,locale}
                        time style
  --full-time           same as --time-style=full-iso
  --color {always,never,auto}
                        print colored entries with -l
```

**Note on `--full-time`:** The help text says "same as --time-style=full-iso" but the
actual output from gfal2-util uses long-iso format (`YYYY-MM-DD HH:MM`).

**Notes:**
- The original only accepts a single `file` URI (not `nargs="+"`); our implementation accepts multiple (extension).
- The original has no `-r/--reverse`, `-S`, `-U`, `--sort` flags; our implementation adds these (extension).

---

## gfal-copy (also invoked as gfal-cp on some systems)

```
usage: gfal-copy [-h] [-V] [-v] [-D DEFINITION] [-t TIMEOUT] [-E CERT]
                 [--key KEY] [-4] [-6] [-C CLIENT_INFO] [--log-file LOG_FILE]
                 [-f] [-p] [-n NBSTREAMS] [--tcp-buffersize TCP_BUFFERSIZE]
                 [-s SRC_SPACETOKEN] [-S DST_SPACETOKEN] [-T TRANSFER_TIMEOUT]
                 [-K CHECKSUM] [--checksum-mode {source,target,both}]
                 [--from-file FROM_FILE] [--copy-mode {pull,push,streamed}]
                 [--just-copy] [--disable-cleanup] [--no-delegation] [--evict]
                 [--scitag SCITAG] [-r] [--abort-on-failure] [--dry-run]
                 [src] dst [dst ...]

Gfal util COPY command. Copy a file or set of files.

positional arguments:
  src                   source file
  dst                   destination file(s). If more than one is given, they
                        will be chained copy: src -> dst1, dst1->dst2, ...

optional arguments:
  -h, --help            show this help message and exit
  -V, --version         output version information and exit
  -v, --verbose         enable the verbose mode
  -D DEFINITION, --definition DEFINITION
                        override a gfal parameter
  -t TIMEOUT, --timeout TIMEOUT
                        maximum time for the operation to terminate - default is 1800 seconds
  -E CERT, --cert CERT  user certificate
  --key KEY             user private key
  -4                    forces gfal2-util to use IPv4 addresses only
  -6                    forces gfal2-util to use IPv6 addresses only
  -C CLIENT_INFO, --client-info CLIENT_INFO
                        provide custom client-side information
  --log-file LOG_FILE   write Gfal2 library logs to the given file location
  -f, --force           if destination file(s) cannot be overwritten, delete it and try again
  -p, --parent          if the destination directory does not exist, create it
  -n NBSTREAMS, --nbstreams NBSTREAMS
                        specify the maximum number of parallel streams to use for the copy
  --tcp-buffersize TCP_BUFFERSIZE
                        specify the TCP buffersize
  -s SRC_SPACETOKEN, --src-spacetoken SRC_SPACETOKEN
                        source spacetoken to use for the transfer
  -S DST_SPACETOKEN, --dst-spacetoken DST_SPACETOKEN
                        destination spacetoken to use for the transfer
  -T TRANSFER_TIMEOUT, --transfer-timeout TRANSFER_TIMEOUT
                        global timeout for the transfer operation
  -K CHECKSUM, --checksum CHECKSUM
                        checksum algorithm to use, or algorithm:value
  --checksum-mode {source,target,both}
                        checksum validation mode
  --from-file FROM_FILE
                        read sources from a file
  --copy-mode {pull,push,streamed}
                        copy mode. N.B. supported only for HTTP/DAV to HTTP/DAV transfers,
                        if not specified the pull mode will be executed first with fallbacks
                        to other modes in case of errors
  --just-copy           just do the copy and skip any preparation
                        (i.e. checksum, overwrite, etc.)
  --disable-cleanup     disable the copy clean-up happening when a transfer fails
  --no-delegation       disable TPC with proxy delegation
  --evict               evict source file from disk buffer when the transfer is finished
  --scitag SCITAG       SciTag transfer flow identifier (number in [65-65535] range)
                        (available only for HTTP-TPC)
  -r, --recursive       copy directories recursively
  --abort-on-failure    abort the whole copy as soon as one failure is encountered
  --dry-run             do not perform any action, just print what would be done
```

**Notes:**
- `--copy-mode {pull,push,streamed}` maps to our `--tpc-mode` + `--tpc` flags:
  `pull`/`push` → TPC with that direction; `streamed` → force client-side streaming.
- `-n/--nbstreams`, `--tcp-buffersize`, `-s/--src-spacetoken`, `-S/--dst-spacetoken`
  are GridFTP/SRM-specific and accepted but ignored with a warning.
- Our implementation adds `--tpc`, `--tpc-only` as extensions.

---

## gfal-rm

```
usage: gfal-rm [-h] [-V] [-v] [-D DEFINITION] [-t TIMEOUT] [-E CERT]
               [--key KEY] [-4] [-6] [-C CLIENT_INFO] [--log-file LOG_FILE]
               [-r] [--dry-run] [--just-delete] [--from-file FROM_FILE]
               [--bulk]
               [file ...]

Gfal util RM command. Removes files or directories.

positional arguments:
  file                  uri(s) of the file(s) to be deleted

optional arguments:
  -h, --help            show this help message and exit
  -V, --version         output version information and exit
  -v, --verbose         enable the verbose mode
  ...common flags...
  -r, -R, --recursive   remove directories and their contents recursively
  --dry-run             do not perform any actual change, just print what would happen
  --just-delete         do not perform any check on the file (needed for HTTP signed URLs)
  --from-file FROM_FILE
                        read surls from a file
  --bulk                use bulk deletion
```

---

## gfal-cat

```
usage: gfal-cat [-h] [-V] [-v] ... [-b] file [file ...]

Gfal util CAT command. Sends to stdout the contents of files.

positional arguments:
  file                  uri of the file to be displayed

optional arguments:
  ...common flags...
  -b, --bytes           handle file contents as bytes (only in Python3)
```

---

## gfal-stat

```
usage: gfal-stat [-h] [-V] [-v] ... file

Gfal util STAT command. Stats a file.

positional arguments:
  file                  uri of the file to be stat
```

**Notes:**
- The original accepts only a single `file` URI; our implementation accepts multiple (extension).

---

## gfal-rename

```
usage: gfal-rename [-h] [-V] [-v] ... source destination

Gfal util RENAME command. Renames files or directories.

positional arguments:
  source                original file name
  destination           new file name
```

---

## gfal-mkdir

```
usage: gfal-mkdir [-h] [-V] [-v] ... [-m MODE] [-p] directory [directory ...]

Gfal util MKDIR command. Makes directories. By default, it sets file mode 0755.

positional arguments:
  directory             Directory's uri

optional arguments:
  ...common flags...
  -m MODE, --mode MODE  file permissions (octal)
  -p, --parents         no error if existing, make parent directories as needed
```

---

## gfal-chmod

```
usage: gfal-chmod [-h] [-V] [-v] ... mode file

Gfal util CHMOD command. Change the permissions of a file.

positional arguments:
  mode                  new mode, in octal
  file                  uri of the file to change permissions
```

---

## gfal-sum

```
usage: gfal-sum [-h] [-V] [-v] ... file checksum_type

Gfal util SUM command. Calculates the checksum of a file.

positional arguments:
  file                  file uri to use for checksum calculation
  checksum_type         checksum algorithm to use. For example: ADLER32, CRC32, MD5
```

---

## gfal-xattr

```
usage: gfal-xattr [-h] [-V] [-v] ... file [attribute]

Gfal util XATTR command. Gets or set the extended attributes of files and directories.

positional arguments:
  file                  file uri
  attribute             attribute to retrieve or set. To set, use key=value
```

---

## gfal-save

```
usage: gfal-save [-h] [-V] [-v] ... file

Gfal util SAVE command. Reads from stdin and writes to a file.
If the file exists, it will be overwritten.

positional arguments:
  file                  uri of the file to be written
```

---

## gfal-bringonline

```
usage: gfal-bringonline [-h] [-V] [-v] ... [--pin-lifetime PIN_LIFETIME]
                        [--desired-request-time DESIRED_REQUEST_TIME]
                        [--staging-metadata STAGING_METADATA]
                        [--polling-timeout POLLING_TIMEOUT]
                        [--from-file FROM_FILE]
                        [surl]

Gfal util BRINGONLINE command. Execute bring online.

positional arguments:
  surl                  Site URL

optional arguments:
  ...common flags...
  --pin-lifetime PIN_LIFETIME
                        Desired pin lifetime
  --desired-request-time DESIRED_REQUEST_TIME
                        Desired total request time
  --staging-metadata STAGING_METADATA
                        Metadata for the bringonline operation
  --polling-timeout POLLING_TIMEOUT
                        Timeout for the polling operation
  --from-file FROM_FILE
                        read surls from a file
```

---

## gfal-archivepoll

```
usage: gfal-archivepoll [-h] [-V] [-v] ... [--polling-timeout POLLING_TIMEOUT]
                        [--from-file FROM_FILE]
                        [surl]

Gfal util ARCHIVEPOLL command. Execute bring online.

positional arguments:
  surl                  Site URL

optional arguments:
  ...common flags...
  --polling-timeout POLLING_TIMEOUT
                        Timeout for the polling operation
  --from-file FROM_FILE
                        read surls from a file
```

---

## gfal-evict

```
usage: gfal-evict [-h] [-V] [-v] ... file [token]

Gfal util EVICT command. Evict file from a disk buffer.

positional arguments:
  file                  URI to the file to be evicted
  token                 The token from the bring online request
```

---

## gfal-token

```
usage: gfal-token [-h] [-V] [-v] ... [--issuer ISSUER] [--validity VALIDITY]
                  [-w] path [activities ...]

Gfal util TOKEN command. Retrieve a SE-issued token.

positional arguments:
  path                  URI to request token for
  activities            activities for macaroon request

optional arguments:
  ...common flags...
  --issuer ISSUER       token issuer URL
  --validity VALIDITY   token validity in minutes
  -w, --write           flag to request write access token
```

---

## Common flags (all commands)

These appear on every command in the original gfal2-util:

| Flag | Description | Status in this implementation |
|------|-------------|-------------------------------|
| `-h, --help` | show help | ✅ supported |
| `-V, --version` | show version | ✅ supported |
| `-v, --verbose` | verbose mode (-v/-vv/-vvv) | ✅ supported |
| `-t, --timeout` | operation timeout (default 1800s) | ✅ supported |
| `-E, --cert` | user certificate | ✅ supported |
| `--key` | user private key | ✅ supported |
| `--log-file` | write logs to file | ✅ supported |
| `-D, --definition` | override gfal2 parameter | ⚠️ accepted, ignored (gfal2-specific) |
| `-C, --client-info` | custom client-side info | ⚠️ accepted, ignored (gfal2-specific) |
| `-4` | force IPv4 (GridFTP only) | ⚠️ accepted, ignored (GridFTP-specific) |
| `-6` | force IPv6 (GridFTP only) | ⚠️ accepted, ignored (GridFTP-specific) |
