# Getting Started

## Installation

From PyPI with pip:

```bash
pip install gfslib
```

## First request

```python
import os
from gfslib.storage.client import StorageServices

GFSLIB_STORAGE_URL="https://.../services/storage"
GFSLIB_API_KEY="your_api_key"

svc = StorageServices(GFSLIB_STORAGE_URL)
svc.set_api_key(GFSLIB_API_KEY)

response = svc.ls()
print(response.status_code)
print(response.text)
```
