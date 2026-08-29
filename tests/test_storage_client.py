import io
import json
from pathlib import Path
from typing import Any

import pytest
import requests

from gfslib.storage.client import StorageServices


class DummyRaw(io.BytesIO):
    decode_content: bool


class DummyResponse:
    def __init__(
        self,
        content: bytes = b"",
        json_data: Any = None,
        json_error: Exception | None = None,
    ) -> None:
        self.content = content
        self._json_data = json_data
        self._json_error = json_error
        self.raw = DummyRaw(content)
        self.raw.decode_content = False

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return [self.content[:1], b"", self.content[1:]]

    def json(self) -> Any:
        if self._json_error:
            raise self._json_error
        return self._json_data


class ChunkedStream:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def read(self, size: int = -1) -> bytes:
        if not self._chunks:
            return b""
        return self._chunks.pop(0)


def _stream_item(path: str, data: bytes) -> bytes:
    metadata = json.dumps({"Path": path, "Size": len(data)}).encode("utf-8")
    return metadata + data


def test_upload_supports_bytes_string_and_rejects_invalid_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_put(url: str, **kwargs: Any) -> DummyResponse:
        calls.append({"url": url, **kwargs})
        return DummyResponse()

    monkeypatch.setattr(requests, "put", fake_put)

    svc = StorageServices("https://example.test/storage")
    svc.upload("folder/raw.bin", b"raw")
    svc.upload("folder/text.txt", "hello")

    assert calls[0]["data"] == b"raw"
    assert calls[1]["data"] == b"hello"

    with pytest.raises(TypeError):
        svc.upload("folder/bad.txt", object())  # type: ignore[arg-type]


def test_download_requires_destination_for_many_files() -> None:
    svc = StorageServices("https://example.test/storage")

    with pytest.raises(ValueError):
        svc.download(["one.txt", "two.txt"])


def test_download_single_file_supports_range_and_bytes_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_get(url: str, **kwargs: Any) -> DummyResponse:
        calls.append({"url": url, **kwargs})
        return DummyResponse(content=b"abc")

    monkeypatch.setattr(requests, "get", fake_get)

    svc = StorageServices("https://example.test/storage")
    assert svc.download("folder/file.txt", byte_range=(2, None)) == b"abc"
    assert calls[-1]["headers"]["Range"] == "bytes=2-"

    assert svc.download("folder/file.txt", byte_range=(2, 4)) == b"abc"
    assert calls[-1]["headers"]["Range"] == "bytes=2-4"


def test_download_single_file_writes_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: DummyResponse(content=b"downloaded"),
    )

    dest = tmp_path / "nested" / "file.txt"
    svc = StorageServices("https://example.test/storage")

    assert svc.download("folder/file.txt", dest) == b""
    assert dest.read_bytes() == b"downloaded"


@pytest.mark.parametrize(
    ("ignore_sha", "ignore_missing", "expected_query"),
    [
        (False, False, ""),
        (True, False, "?ignoreSha=True"),
        (False, True, "?ignoreMissingFiles=True"),
        (True, True, "?ignoreSha=True&ignoreMissingFiles=True"),
    ],
)
def test_download_many_uses_query_options_and_deserializes(
    ignore_sha: bool,
    ignore_missing: bool,
    expected_query: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, **kwargs: Any) -> DummyResponse:
        calls.append({"url": url, **kwargs})
        return DummyResponse(content=_stream_item("remote/folder/file.txt", b"data"))

    monkeypatch.setattr(requests, "post", fake_post)

    svc = StorageServices("https://example.test/storage")
    results = svc.download(
        ["remote/folder/file.txt"],
        dest=tmp_path,
        ignore_sha=ignore_sha,
        ignore_missing=ignore_missing,
    )

    assert calls[0]["url"] == f"https://example.test/storage/download{expected_query}"
    assert (tmp_path / "file.txt").read_bytes() == b"data"
    assert isinstance(results, list)
    assert results[0]["metadata"]["Path"] == "remote/folder/file.txt"


def test_deserialize_stream_rejects_missing_output_folder(tmp_path: Path) -> None:
    svc = StorageServices("https://example.test/storage")

    with pytest.raises(ValueError):
        svc._deserialize_stream_to_files(
            io.BytesIO(b""),
            dest_dir=tmp_path / "missing",
            write_chunk_size=1,
            validate_paths=True,
        )


def test_deserialize_stream_can_skip_path_validation(tmp_path: Path) -> None:
    svc = StorageServices("https://example.test/storage")

    results = svc._deserialize_stream_to_files(
        io.BytesIO(_stream_item("../safe_after_basename.txt", b"ok")),
        dest_dir=tmp_path,
        write_chunk_size=1,
        validate_paths=False,
    )

    assert (tmp_path / "safe_after_basename.txt").read_bytes() == b"ok"
    assert len(results) == 1


def test_deserialize_stream_rejects_illegal_paths(tmp_path: Path) -> None:
    svc = StorageServices("https://example.test/storage")

    with pytest.raises(RuntimeError, match="illegal characters"):
        svc._deserialize_stream_to_files(
            io.BytesIO(_stream_item("../bad.txt", b"bad")),
            dest_dir=tmp_path,
            write_chunk_size=1,
            validate_paths=True,
        )


def test_deserialize_stream_reports_malformed_metadata(tmp_path: Path) -> None:
    svc = StorageServices("https://example.test/storage")

    with pytest.raises(RuntimeError, match="Metadata start not found"):
        svc._deserialize_stream_to_files(
            io.BytesIO(b"x"),
            dest_dir=tmp_path,
            write_chunk_size=1,
            validate_paths=True,
        )

    with pytest.raises(RuntimeError, match="Metadata start not found"):
        svc._deserialize_stream_to_files(
            ChunkedStream([b'{"Path"']),
            dest_dir=tmp_path,
            write_chunk_size=1,
            validate_paths=True,
        )


def test_deserialize_stream_reports_oversized_metadata(tmp_path: Path) -> None:
    svc = StorageServices("https://example.test/storage")

    with pytest.raises(RuntimeError, match="Metadata too large"):
        svc._deserialize_stream_to_files(
            io.BytesIO(b"{" * (1024 * 1024 + 2)),
            dest_dir=tmp_path,
            write_chunk_size=1,
            validate_paths=True,
        )


def test_deserialize_stream_reports_unexpected_payload_eof(tmp_path: Path) -> None:
    svc = StorageServices("https://example.test/storage")

    metadata = json.dumps({"Path": "file.txt", "Size": 5}).encode("utf-8")
    with pytest.raises(RuntimeError, match="Unexpected EOF"):
        svc._deserialize_stream_to_files(
            io.BytesIO(metadata + b"a"),
            dest_dir=tmp_path,
            write_chunk_size=1,
            validate_paths=True,
        )

    with pytest.raises(RuntimeError, match="Unexpected EOF"):
        svc._deserialize_stream_to_files(
            io.BytesIO(metadata),
            dest_dir=tmp_path,
            write_chunk_size=1,
            validate_paths=True,
        )


def test_deserialize_stream_handles_metadata_and_payload_across_reads(
    tmp_path: Path,
) -> None:
    svc = StorageServices("https://example.test/storage")

    split_metadata = ChunkedStream([b'{"Path"', b': "split.txt", "Size": 0}'])
    svc._deserialize_stream_to_files(
        split_metadata,
        dest_dir=tmp_path,
        write_chunk_size=1,
        validate_paths=True,
    )
    assert (tmp_path / "split.txt").read_bytes() == b""

    metadata = json.dumps({"Path": "first-chunk.txt", "Size": 3}).encode("utf-8")
    svc._deserialize_stream_to_files(
        ChunkedStream([metadata + b"a", b"bc"]),
        dest_dir=tmp_path,
        write_chunk_size=2,
        validate_paths=True,
    )
    assert (tmp_path / "first-chunk.txt").read_bytes() == b"abc"

    metadata = json.dumps({"Path": "later-chunk.txt", "Size": 3}).encode("utf-8")
    svc._deserialize_stream_to_files(
        ChunkedStream([metadata, b"abc"]),
        dest_dir=tmp_path,
        write_chunk_size=2,
        validate_paths=True,
    )
    assert (tmp_path / "later-chunk.txt").read_bytes() == b"abc"


def test_metadata_returns_empty_dict_for_non_json_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        requests,
        "post",
        lambda *args, **kwargs: DummyResponse(json_error=ValueError("not json")),
    )

    svc = StorageServices("https://example.test/storage")
    assert svc.metadata(["missing.txt"]) == {}


def test_sync_local_to_remote_handles_empty_dir_and_dry_run_without_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    svc = StorageServices("https://example.test/storage")
    assert svc.sync_local_to_remote(tmp_path) == {}

    local = tmp_path / "plain.txt"
    local.write_text("plain")
    monkeypatch.setattr(svc, "metadata", lambda *args, **kwargs: [])

    assert svc.sync_local_to_remote(tmp_path, dry_run=True) == {
        "plain.txt": "uploaded (dry-run)"
    }


def test_sync_local_to_remote_handles_missing_metadata_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = tmp_path / "plain.txt"
    local.write_text("plain")

    svc = StorageServices("https://example.test/storage")
    monkeypatch.setattr(svc, "metadata", lambda *args, **kwargs: None)
    uploads: list[tuple[str, str]] = []
    monkeypatch.setattr(
        svc, "upload", lambda remote, path: uploads.append((remote, path))
    )

    assert svc.sync_local_to_remote(tmp_path) == {"plain.txt": "uploaded"}
    assert uploads == [("plain.txt", str(local))]
