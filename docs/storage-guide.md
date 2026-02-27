# Storage Guide

This guide covers the most common storage workflows.

## Initialize client

```python
from gfslib.storage.client import StorageServices

svc = StorageServices("https://.../services/storage")
svc.set_api_key("your_api_key")
```

## List files

```python
short = svc.ls()
long = svc.ls_long()
```

## Upload a file

```python
svc.upload("folder/sample.txt", "path/to/local/sample.txt")
```

## Download a file

```python
# bytes in memory
content = svc.download("folder/sample.txt")

# write directly to disk
svc.download("folder/sample.txt", "downloaded/sample.txt")
```

## Metadata

```python
meta = svc.metadata(["folder/sample.txt"], ignore_sha=False)
print(meta)
```

## Delete a file

```python
svc.delete("folder/sample.txt")
```

## Sync local folder to remote

```python
result = svc.sync_local_to_remote(
    local_dir="./data",
    remote_prefix="uploads",
    ignore_sha=False,
    dry_run=False,
)
print(result)
```
