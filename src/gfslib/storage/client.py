"""High-level StorageServices for GeMMA Fusion Server.

Provides methods for listing, uploading, downloading, deleting,
fetching metadata and syncing local files with the remote storage service.

Usage:
    client = StorageServices("https://fusion.gemma.feri.um.si/gf-test/api/ws/<workspace-id>/services/storage")
    client.set_api_key("...")
    client.ls()
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Dict, Iterable, List, Optional, Tuple, Any
from urllib.parse import quote

import requests


class StorageServices:
    """Client for the server storage API.

    Initialize with the base storage service URL. Examples of base URL:
    - https://.../api/ws/<workspace-id>/services/storage
    """

    def __init__(self, base_url: str, timeout: Optional[float] = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._api_key: Optional[str] = None
        self.timeout = timeout

    def set_api_key(self, key: str) -> None:
        """Set the X-API-Key to use for requests."""
        self._api_key = key

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {
            "Accept": "*/*",
        }
        if self._api_key:
            headers["X-API-Key"] = self._api_key
        if extra:
            headers.update(extra)
        return headers

    def _file_url(self, remote_path: str) -> str:
        # Ensure we don't double the slashes; keep path parts encoded except '/'
        rp = remote_path.lstrip("/")
        return f"{self.base_url}/files/{quote(rp, safe='/') }"

    def ls(
        self,
        path: Optional[str] = None,
        recursive: bool = False,
        include_directories: bool = False,
    ) -> requests.Response:
        """List remote files (short). Returns the requests.Response object."""
        url = f"{self.base_url}/ls"
        params: Dict[str, str] = {}
        if path is not None:
            params["path"] = path
        params["recursive"] = str(recursive).lower()
        params["includeDirectories"] = str(include_directories).lower()
        return requests.get(
            url, headers=self._headers(), params=params, timeout=self.timeout
        )

    def ls_long(
        self,
        path: Optional[str] = None,
        recursive: bool = False,
        include_directories: bool = False,
    ) -> requests.Response:
        """List remote files (long). Returns the requests.Response object."""
        url = f"{self.base_url}/ls/long"
        params: Dict[str, str] = {}
        if path is not None:
            params["path"] = path
        params["recursive"] = str(recursive).lower()
        params["includeDirectories"] = str(include_directories).lower()
        return requests.get(
            url, headers=self._headers(), params=params, timeout=self.timeout
        )

    def upload(
        self, remote_path: str, data: bytes | str | os.PathLike[str]
    ) -> requests.Response:
        """Upload data to `remote_path` using PUT.

        `data` can be raw bytes, a string (will be encoded as utf-8), or a path
        to a local file to stream.
        """
        url = self._file_url(remote_path)
        headers = self._headers({"Content-Type": "application/octet-stream"})

        if isinstance(data, (bytes, bytearray)):
            body = data
            resp = requests.put(url, headers=headers, data=body, timeout=self.timeout)
            return resp

        if isinstance(data, str) and os.path.exists(data):
            # treat as file path
            with open(data, "rb") as fh:
                return requests.put(url, headers=headers, data=fh, timeout=self.timeout)

        # otherwise treat as string content
        if isinstance(data, str):
            body = data.encode("utf-8")
            return requests.put(url, headers=headers, data=body, timeout=self.timeout)

        raise TypeError("data must be bytes, string, or path to file")

    def download(
        self,
        remote_path: str | List[str],
        dest: Optional[os.PathLike[str] | str] = None,
        byte_range: Optional[Tuple[int, Optional[int]]] = None,
        ignore_sha: bool = False,
        write_chunk_size: int = 10 * 1024 * 1024,
        validate_paths: bool = True,
    ) -> bytes | List[Dict[str, Any]]:
        """Download a file or multiple files.

        If the remote path is a single string, a single file will be downloaded
        and returned as bytes (or written to `dest` if provided).

        For multiple files, pass a list of strings in `remote_path` and provide
        `dest` as a destination folder. The server returns a stream in the form
        <METADATA_JSON><FILE><METADATA_JSON><FILE> and the files are written
        into `dest`.
        """
        if isinstance(remote_path, list):
            if dest is None:
                raise ValueError(
                    "dest must be provided when downloading multiple files"
                )
            return self._download_many(
                remote_path,
                dest_dir=dest,
                ignore_sha=ignore_sha,
                write_chunk_size=write_chunk_size,
                validate_paths=validate_paths,
            )

        url = self._file_url(remote_path)
        headers = self._headers()
        if byte_range is not None:
            start, end = byte_range
            if end is None:
                headers["Range"] = f"bytes={start}-"
            else:
                headers["Range"] = f"bytes={start}-{end}"

        resp = requests.get(url, headers=headers, stream=True, timeout=self.timeout)
        resp.raise_for_status()

        if dest is not None:
            dest_path = str(dest)
            os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
            with open(dest_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
            return b""

        # collect into bytes
        return resp.content

    def _download_many(
        self,
        remote_paths: List[str],
        dest_dir: os.PathLike[str] | str,
        ignore_sha: bool,
        write_chunk_size: int,
        validate_paths: bool,
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/download"
        if ignore_sha:
            url = f"{url}?ignoreSha=True"

        body = json.dumps(list(remote_paths))
        headers = self._headers(
            {
                "Content-Type": "application/json",
                "Content-Length": str(len(body)),
                "Accept-Encoding": "gzip, deflate, br",
            }
        )
        resp = requests.post(
            url,
            headers=headers,
            data=body.encode("utf-8"),
            stream=True,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        resp.raw.decode_content = True
        return self._deserialize_stream_to_files(
            resp.raw,
            dest_dir=dest_dir,
            write_chunk_size=write_chunk_size,
            validate_paths=validate_paths,
        )

    def _deserialize_stream_to_files(
        self,
        stream: Any,
        dest_dir: os.PathLike[str] | str,
        write_chunk_size: int,
        validate_paths: bool,
    ) -> List[Dict[str, Any]]:
        out_dir = os.fspath(dest_dir)
        if not os.path.isdir(out_dir):
            raise ValueError("Output folder does not exist")

        meta_size = 1024 * 1024
        buffer_size = meta_size * 10
        results: List[Dict[str, Any]] = []

        def read_more(size: int) -> bytes:
            return stream.read(size) or b""

        workbuffer = bytearray(read_more(buffer_size))
        meta_start = 0

        def parse_json_object() -> Tuple[Dict[str, Any], int]:
            nonlocal workbuffer, meta_start
            start_count = 0
            end_count = 0
            meta_end = meta_start
            i = meta_start

            while True:
                if meta_end - meta_start > meta_size:
                    raise RuntimeError("Metadata too large: closing brace not found")
                if i >= len(workbuffer):
                    more = read_more(buffer_size)
                    if not more:
                        raise RuntimeError("Metadata start not found")
                    workbuffer.extend(more)
                sym = workbuffer[i]
                meta_end += 1
                if sym == ord("{"):
                    start_count += 1
                if sym == ord("}"):
                    end_count += 1
                if start_count == 0:
                    raise RuntimeError("Metadata start not found")
                if start_count == end_count:
                    break
                i += 1

            meta_bytes = bytes(workbuffer[meta_start:meta_end])
            meta = json.loads(meta_bytes)
            return meta, meta_end

        def validate_path(path: str) -> None:
            if not validate_paths:
                return
            if "../" in path or "..\\" in path or ":" in path:
                raise RuntimeError("File path contains illegal characters")

        while True:
            if meta_start >= len(workbuffer):
                workbuffer = bytearray(read_more(buffer_size))
                meta_start = 0
                if not workbuffer:
                    break

            meta, meta_end = parse_json_object()
            path = str(meta.get("Path", ""))
            validate_path(path)
            size = int(meta.get("Size", 0))

            rel_path = os.path.basename(path.lstrip("/").replace("\\", "/"))
            out_path = os.path.join(out_dir, rel_path)
            os.makedirs(os.path.dirname(out_path) or out_dir, exist_ok=True)

            file_written = 0
            data_start = meta_end
            available = max(0, len(workbuffer) - data_start)
            first_chunk = min(available, size)
            if first_chunk:
                with open(out_path, "wb") as fh:
                    fh.write(workbuffer[data_start : data_start + first_chunk])
                    file_written += first_chunk
                    while file_written < size:
                        chunk = read_more(min(write_chunk_size, size - file_written))
                        if not chunk:
                            raise RuntimeError(
                                "Unexpected EOF while reading file payload"
                            )
                        fh.write(chunk)
                        file_written += len(chunk)
            else:
                with open(out_path, "wb") as fh:
                    while file_written < size:
                        chunk = read_more(min(write_chunk_size, size - file_written))
                        if not chunk:
                            raise RuntimeError(
                                "Unexpected EOF while reading file payload"
                            )
                        fh.write(chunk)
                        file_written += len(chunk)

            results.append({"metadata": meta, "path": out_path})

            meta_start = data_start + file_written
            if meta_start > 0 and meta_start < len(workbuffer):
                workbuffer = workbuffer[meta_start:]
                meta_start = 0

        return results

    def delete(self, remote_path: str) -> requests.Response:
        """Delete a remote file."""
        url = self._file_url(remote_path)
        return requests.delete(url, headers=self._headers(), timeout=self.timeout)

    def metadata(self, filepaths: Iterable[str], ignore_sha: bool = False) -> Any:
        """Get metadata for the provided filepaths."""
        url = f"{self.base_url}/metadata"
        if ignore_sha:
            url = f"{url}?ignoreSha=true"

        body = json.dumps(list(filepaths))
        headers = self._headers(
            {"Content-Type": "application/json", "Content-Length": str(len(body))}
        )
        resp = requests.post(
            url, headers=headers, data=body.encode("utf-8"), timeout=self.timeout
        )
        # return parsed JSON (list/dict) for easier consumption by callers
        try:
            return resp.json()
        except ValueError:
            return {}

    @staticmethod
    def compute_sha256(path: os.PathLike[str] | str) -> str:
        """Compute SHA-256 hex digest of a file."""
        h = hashlib.sha256()
        with open(str(path), "rb") as fh:
            for chunk in iter(lambda: fh.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def sync_local_to_remote(
        self,
        local_dir: os.PathLike[str],
        remote_prefix: str = "",
        ignore_sha: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, str]:
        """Sync files from `local_dir` to remote storage under `remote_prefix`.

        Behaviour:
        - Walk `local_dir`, collect relative file paths.
        - Query `metadata` for those paths (prefixed by `remote_prefix`).
        - Upload files that are missing or whose SHA differs (unless `ignore_sha=True`).

        Returns a dict mapping relative path -> action performed ("uploaded", "skipped").
        """
        rem_prefix = remote_prefix.strip("/")

        rel_paths: List[str] = []
        for root, _dirs, files in os.walk(local_dir):
            for f in files:
                full = os.path.join(root, f)
                rel = os.path.relpath(full, local_dir).replace("\\", "/")
                if rem_prefix:
                    rel_paths.append(f"{rem_prefix}/{rel}")
                else:
                    rel_paths.append(rel)

        result: Dict[str, str] = {}
        if not rel_paths:
            return result

        # request metadata in one batch (server should accept arrays)
        remote_meta = self.metadata(rel_paths, ignore_sha=ignore_sha)
        if remote_meta is None:
            remote_meta = {}

        # Build a mapping path -> metadata (prefer Path/path), fallback to name
        remote_map = {}

        def _norm(s: str) -> str:
            return s.replace("\\", "/")

        if isinstance(remote_meta, list):
            for item in remote_meta:
                p = item.get("Path")
                if isinstance(p, str):
                    remote_map[_norm(p)] = item

        # For each local file decide upload or skip
        for rel in rel_paths:
            # derive local file path
            if rem_prefix:
                # strip prefix when mapping back to local relative path
                local_rel = rel[len(rem_prefix) + 1 :]
            else:
                local_rel = rel
            local_path = os.path.join(local_dir, local_rel.replace("/", os.sep))

            action = "uploaded"
            # if remote metadata contains sha, use it
            remote_item = remote_map.get(_norm(rel))
            if remote_item and not ignore_sha:
                # prefer Sha2 (server uses SHA-256)
                remote_sha = remote_item.get("Sha2") or remote_item.get("sha2")
                if remote_sha:
                    local_sha = self.compute_sha256(local_path)
                    if local_sha == remote_sha:
                        action = "skipped"

            if action == "uploaded":
                if not dry_run:
                    self.upload(rel, local_path)
                result[local_rel] = "uploaded (dry-run)" if dry_run else "uploaded"
            else:
                result[local_rel] = "skipped"

        return result
