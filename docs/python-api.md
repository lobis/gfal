# Python API

`gfal` provides both an **async-first** client (`AsyncGfalClient`) and a **synchronous** wrapper (`GfalClient`). Both offer the same methods — choose whichever fits your application.

## Quick start

### Synchronous

```python
import gfal

client = gfal.GfalClient()

# stat a file
info = client.stat("file:///tmp/data.txt")
print(f"size={info.size}, is_file={info.is_file()}")

# list a directory
for entry in client.ls("/tmp/mydir"):
    print(f"{entry.size:>10}  {entry.info['name']}")

# copy a file
client.copy("file:///tmp/src.txt", "file:///tmp/dst.txt")

# compute a checksum
print(client.checksum("file:///tmp/data.txt", "MD5"))
```

### Asynchronous

```python
import asyncio
import gfal


async def main():
    client = gfal.AsyncGfalClient()

    info = await client.stat("file:///tmp/data.txt")
    print(f"size={info.size}")

    entries = await client.ls("/tmp/mydir")
    for entry in entries:
        print(entry.info["name"])

    await client.copy("file:///tmp/src.txt", "file:///tmp/dst.txt")


asyncio.run(main())
```

!!! tip "Local paths"
    Both `"file:///tmp/data.txt"` and `"/tmp/data.txt"` are accepted.
    Bare paths are automatically converted to `file://` URIs internally.

---

## Client configuration

Both clients accept the same parameters:

```python
client = gfal.GfalClient(
    cert="/path/to/cert.pem",     # X.509 client certificate
    key="/path/to/key.pem",       # client key (defaults to cert path)
    timeout=1800,                 # global timeout in seconds
    ssl_verify=True,              # TLS certificate verification
)
```

Or use `ClientConfig` for reusable configuration:

```python
config = gfal.ClientConfig(
    cert="/path/to/cert.pem",
    key="/path/to/key.pem",
    timeout=3600,
    ssl_verify=True,
)

sync_client = gfal.GfalClient(config=config)
async_client = gfal.AsyncGfalClient(config=config)
```

### `ClientConfig`

| Parameter    | Type           | Default | Description                     |
|-------------|----------------|---------|----------------------------------|
| `cert`      | `str \| None`  | `None`  | Path to client certificate (PEM) |
| `key`       | `str \| None`  | `None`  | Path to client key (PEM)         |
| `timeout`   | `int`          | `1800`  | Global timeout in seconds        |
| `ssl_verify`| `bool`         | `True`  | Enable TLS certificate verification |
| `ipv4_only` | `bool`         | `False` | Force IPv4 connections           |
| `ipv6_only` | `bool`         | `False` | Force IPv6 connections           |
| `app`       | `str \| None`  | auto    | User-agent / application name    |

---

## File operations

### `stat(url) → StatResult`

Get POSIX-style metadata for a file or directory.

```python
info = client.stat("root://server//eos/data/file.root")
print(f"Size: {info.size} bytes")
print(f"Mode: {oct(info.mode)}")
print(f"Is directory: {info.is_dir()}")
print(f"Modification time: {info.mtime}")
```

### `exists(url) → bool`

Check whether a file or directory exists.

```python
if client.exists("root://server//eos/data/file.root"):
    print("File exists")
```

### `ls(url, detail=True) → list`

List directory contents.

- `detail=True` (default): returns a list of `StatResult` objects.
- `detail=False`: returns a list of file name strings.

```python
# Full details
entries = client.ls("root://server//eos/data/")
for entry in entries:
    kind = "d" if entry.is_dir() else "f"
    print(f"[{kind}] {entry.info['name']}  ({entry.size} bytes)")

# Names only
names = client.ls("root://server//eos/data/", detail=False)
print(names)  # ['file1.root', 'file2.root', 'subdir']
```

### `open(url, mode="rb") → file-like`

Open a remote file for reading or writing. Returns a file-like object.

```python
# Read
with client.open("https://example.com/data.csv") as f:
    content = f.read()

# Write
with client.open("root://server//eos/output.txt", "wb") as f:
    f.write(b"hello world\n")
```

### `checksum(url, algorithm) → str`

Compute a checksum. Supported algorithms: `ADLER32`, `CRC32`, `CRC32C`, `MD5`, `SHA1`, `SHA256`, `SHA512`.

```python
md5 = client.checksum("file:///tmp/data.root", "MD5")
adler = client.checksum("root://server//eos/data/file.root", "ADLER32")
```

---

## Directory operations

### `mkdir(url, mode=0o755, parents=False)`

Create a directory.

```python
client.mkdir("root://server//eos/user/j/jdoe/newdir")

# With parents (like mkdir -p)
client.mkdir("root://server//eos/user/j/jdoe/a/b/c", parents=True)
```

### `rmdir(url)`

Remove an empty directory.

```python
client.rmdir("root://server//eos/user/j/jdoe/emptydir")
```

### `rm(url, recursive=False)`

Remove a file or directory.

```python
# Remove a file
client.rm("file:///tmp/old.txt")

# Remove a directory tree
client.rm("root://server//eos/user/j/jdoe/olddir", recursive=True)
```

### `rename(src_url, dst_url)`

Rename or move a file within the same filesystem.

```python
client.rename(
    "root://server//eos/data/old_name.root",
    "root://server//eos/data/new_name.root",
)
```

### `chmod(url, mode)`

Change file permissions.

```python
client.chmod("root://server//eos/data/file.root", 0o644)
```

---

## Copy operations

### `copy(src_url, dst_url, options=None)`

Copy a file (or directory tree with `recursive=True`) between any supported endpoints.

```python
# Simple copy
client.copy("file:///tmp/input.txt", "file:///tmp/output.txt")

# Copy with overwrite and checksum verification
client.copy(
    "root://server//eos/data/file.root",
    "file:///tmp/file.root",
    options=gfal.CopyOptions(
        overwrite=True,
        checksum=gfal.ChecksumPolicy("ADLER32"),
    ),
)

# Recursive directory copy
client.copy(
    "root://server//eos/data/mydir/",
    "file:///tmp/mydir/",
    options=gfal.CopyOptions(recursive=True, create_parents=True),
)
```

### `CopyOptions`

| Parameter              | Type                    | Default  | Description                                      |
|------------------------|-------------------------|----------|--------------------------------------------------|
| `overwrite`            | `bool`                  | `False`  | Overwrite existing destination                   |
| `create_parents`       | `bool`                  | `False`  | Create parent directories at destination         |
| `recursive`            | `bool`                  | `False`  | Copy directories recursively                     |
| `timeout`              | `int \| None`           | `None`   | Per-transfer timeout in seconds                  |
| `checksum`             | `ChecksumPolicy \| None`| `None`  | Checksum verification policy                     |
| `tpc`                  | `str`                   | `"auto"` | Third-party copy: `"auto"`, `"always"`, `"never"`, `"only"` |
| `tpc_direction`        | `str`                   | `"pull"` | TPC direction: `"pull"` or `"push"`              |
| `abort_on_failure`     | `bool`                  | `False`  | Stop on first error during recursive copy        |
| `preserve_times`       | `bool`                  | `False`  | Preserve modification timestamps                 |
| `compare`              | `str \| None`           | `None`   | Skip if destination matches: `"size"`, `"size_mtime"`, `"checksum"`, `"none"` |
| `dry_run`              | `bool`                  | `False`  | Preview without executing                        |
| `just_copy`            | `bool`                  | `False`  | Skip all preparation/validation steps            |
| `scitag`               | `int \| None`           | `None`   | WLCG SciTag identifier (65–65535)                |

### `ChecksumPolicy`

| Parameter        | Type           | Default  | Description                                    |
|-----------------|----------------|----------|------------------------------------------------|
| `algorithm`     | `str`          | required | Algorithm: `ADLER32`, `MD5`, `SHA256`, etc.    |
| `mode`          | `str`          | `"both"` | Verify: `"both"`, `"source"`, `"target"`       |
| `expected_value`| `str \| None`  | `None`   | Expected hash value for validation             |

### Dry-run example

```python
client.copy(
    "root://server//eos/data/",
    "file:///tmp/backup/",
    options=gfal.CopyOptions(recursive=True, dry_run=True),
)
# Nothing is actually copied — preview only
```

### Third-party copy (TPC)

When both source and destination are remote (e.g. two XRootD servers, or HTTP endpoints supporting WebDAV COPY), gfal can instruct the servers to transfer data directly without routing through the client:

```python
client.copy(
    "root://server-a//eos/data/file.root",
    "root://server-b//eos/data/file.root",
    options=gfal.CopyOptions(tpc="only"),
)
```

---

## Background transfers

### `start_copy(src_url, dst_url, options=None) → TransferHandle`

Start a copy in a background thread. Returns a `TransferHandle` for monitoring.

```python
handle = client.start_copy(
    "root://server//eos/data/large_file.root",
    "file:///tmp/large_file.root",
    options=gfal.CopyOptions(overwrite=True),
)

# Do other work while the transfer runs...
print(f"Transfer done? {handle.done()}")

# Wait for completion
handle.wait(timeout=300)
```

### `TransferHandle`

| Method              | Description                                       |
|---------------------|---------------------------------------------------|
| `done() → bool`    | Check if the transfer has completed                |
| `wait(timeout=None)` | Block until transfer completes or timeout expires |
| `wait_async(timeout=None)` | Async version of `wait()`                  |
| `cancel()`          | Cancel the transfer                               |

---

## Extended attributes

### `getxattr(url, name) → str`

```python
value = client.getxattr("root://server//eos/data/file.root", "xroot.checksum")
```

### `setxattr(url, name, value)`

```python
client.setxattr("root://server//eos/data/file.root", "user.tag", "important")
```

### `listxattr(url) → list[str]`

```python
attrs = client.listxattr("root://server//eos/data/file.root")
```

### `xattrs(url) → dict[str, str]`

Get all extended attributes as a dictionary:

```python
all_attrs = client.xattrs("root://server//eos/data/file.root")
for name, value in all_attrs.items():
    print(f"{name} = {value}")
```

---

## Progress callbacks

Both `copy()` and `start_copy()` accept optional callbacks for monitoring transfers:

```python
def on_progress(bytes_transferred: int) -> None:
    print(f"Transferred: {bytes_transferred} bytes")

def on_start() -> None:
    print("Transfer started")

def on_mode(mode: str) -> None:
    print(f"Transfer mode: {mode}")  # "streamed", "tpc-pull", "tpc-xrootd"

client.copy(
    "root://server//eos/data/file.root",
    "file:///tmp/file.root",
    progress_callback=on_progress,
    start_callback=on_start,
    transfer_mode_callback=on_mode,
)
```

---

## Error handling

All operations raise typed exceptions that inherit from `GfalError` (which inherits from `OSError`):

| Exception                  | errno         | Meaning                              |
|---------------------------|---------------|--------------------------------------|
| `GfalFileNotFoundError`   | `ENOENT`      | File or directory not found          |
| `GfalPermissionError`     | `EACCES`      | Access denied                        |
| `GfalFileExistsError`     | `EEXIST`      | File already exists                  |
| `GfalNotADirectoryError`  | `ENOTDIR`     | Not a directory                      |
| `GfalIsADirectoryError`   | `EISDIR`      | Is a directory                       |
| `GfalTimeoutError`        | `ETIMEDOUT`   | Operation timed out                  |
| `GfalError`               | varies        | Base class for all gfal errors       |

```python
import gfal

client = gfal.GfalClient()

try:
    client.stat("file:///nonexistent")
except gfal.GfalFileNotFoundError:
    print("File not found")
except gfal.GfalPermissionError:
    print("Permission denied")
except gfal.GfalError as e:
    print(f"Error (errno={e.errno}): {e}")
```

---

## `StatResult`

Returned by `stat()` and `ls()` (when `detail=True`).

| Property     | Type    | Description                        |
|-------------|---------|-------------------------------------|
| `size`      | `int`   | File size in bytes                  |
| `mode`      | `int`   | POSIX file mode                     |
| `uid`       | `int`   | Owner user ID                       |
| `gid`       | `int`   | Owner group ID                      |
| `nlink`     | `int`   | Number of hard links                |
| `mtime`     | `float` | Modification time (UNIX timestamp)  |
| `atime`     | `float` | Access time (UNIX timestamp)        |
| `ctime`     | `float` | Change time (UNIX timestamp)        |
| `info`      | `dict`  | Raw fsspec info dictionary          |

| Method       | Return  | Description                        |
|-------------|---------|-------------------------------------|
| `is_dir()`  | `bool`  | True if the entry is a directory    |
| `is_file()` | `bool`  | True if the entry is a regular file |

---

## Supported URL schemes

| Scheme              | Description                  | Requirements                     |
|---------------------|------------------------------|----------------------------------|
| `file://`           | Local filesystem             | Built-in                         |
| `/path` (bare path) | Local filesystem (auto-converted to `file://`) | Built-in |
| `http://`, `https://` | HTTP/WebDAV               | Built-in                         |
| `dav://`, `davs://`  | WebDAV (converted to `http://`/`https://`) | Built-in        |
| `root://`           | XRootD                       | `gfal[xrootd]` or `xrootd` + `fsspec-xrootd` |

---

## Complete async example

```python
import asyncio
import gfal


async def backup_directory():
    client = gfal.AsyncGfalClient(
        cert="/tmp/x509up_u1000",
        timeout=3600,
    )

    src = "root://server//eos/data/analysis/"
    dst = "file:///tmp/backup/"

    # Check source exists
    if not await client.exists(src):
        print("Source directory not found")
        return

    # Copy with checksum verification
    await client.copy(
        src,
        dst,
        options=gfal.CopyOptions(
            recursive=True,
            create_parents=True,
            overwrite=True,
            checksum=gfal.ChecksumPolicy("ADLER32"),
        ),
    )

    # Verify
    entries = await client.ls(dst)
    print(f"Backed up {len(entries)} items")


asyncio.run(backup_directory())
```
