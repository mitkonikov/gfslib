import os
import tempfile
import pytest
import hashlib

from gfslib.storage.client import StorageServices
from pathlib import Path
from typing import Optional, Tuple, Any, Dict


def _load_dotenv_if_available() -> None:
    """Try to load a .env file from the project root if `python-dotenv` is installed.
    Non-fatal: if the package is missing, this does nothing.
    """
    try:
        from dotenv import load_dotenv

        project_root = Path(__file__).resolve().parents[1]
        dotenv_path = project_root / ".env"
        if dotenv_path.exists():
            load_dotenv(dotenv_path)
    except Exception:
        return


def _get_env() -> Tuple[Optional[str], Optional[str]]:
    _load_dotenv_if_available()
    url = os.environ.get("GFSLIB_STORAGE_URL")
    key = os.environ.get("GFSLIB_API_KEY")
    return url, key


def _skip_unless_env() -> Tuple[str, str]:
    url, key = _get_env()
    if not url or not key:
        pytest.skip(
            "GFSLIB_STORAGE_URL or GFSLIB_API_KEY not set; skipping integration tests"
        )
    return url, key


@pytest.mark.integration
def test_integration_ls_and_metadata() -> None:
    url, key = _skip_unless_env()

    svc = StorageServices(url)
    svc.set_api_key(key)

    r = svc.ls()
    assert r.status_code == 200

    # metadata should accept a small list
    meta = svc.metadata(["nonexistent.file"], ignore_sha=True)

    assert isinstance(meta, dict)
    assert meta["status"] == 500 or meta == {}


@pytest.mark.integration
def test_integration_upload_download_delete_roundtrip() -> None:
    url, key = _skip_unless_env()

    svc = StorageServices(url)
    svc.set_api_key(key)

    # create a temp file to upload
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "gfslib_test.txt"
        p.write_text("integration-test-content")

        # upload
        up = svc.upload("gfslib/gfslib_test.txt", str(p))
        assert up.status_code in (200, 201, 204)

        # download to a new file
        dest = Path(td) / "downloaded.txt"
        data = svc.download("gfslib/gfslib_test.txt", dest)
        # when dest is provided, data is empty bytes
        assert data == b""
        assert dest.exists()
        assert dest.read_text() == "integration-test-content"

        # delete
        d = svc.delete("gfslib/gfslib_test.txt")
        assert d.status_code in (200, 204)


@pytest.mark.integration
def test_integration_upload_get_metadata_and_delete() -> None:
    url, key = _skip_unless_env()

    svc = StorageServices(url)
    svc.set_api_key(key)

    content = "metadata-check-content"
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "meta_local.txt"
        p.write_text(content)

        remote = "gfslib/meta_test_roundtrip.txt"

        # upload
        up = svc.upload(remote, str(p))
        assert up.status_code in (200, 201, 204)

        # fetch metadata for the uploaded file
        meta = svc.metadata([remote], ignore_sha=False)

        # server returns a list of metadata objects; accept list or dict
        assert meta is not None
        if isinstance(meta, list):
            assert len(meta) >= 1
            item = meta[0]
        else:
            # dict mapping or single object
            if isinstance(meta, dict) and len(meta) == 0:
                pytest.skip("metadata returned empty dict; skipping assertions")
            # pick first value
            first_key = next(iter(meta))
            item = meta[first_key] if isinstance(meta[first_key], dict) else meta

        # basic checks
        assert "Path" in item
        assert item["Path"].endswith("meta_test_roundtrip.txt")
        assert "Size" in item
        assert item["Size"] == len(content.encode("utf-8"))

        # validate sha2 if provided (server uses sha2)
        sha2 = item.get("Sha2")
        if sha2:

            computed = hashlib.sha256(content.encode("utf-8")).hexdigest()
            assert sha2.lower() == computed.lower()

        # cleanup
        d = svc.delete(remote)
        assert d.status_code in (200, 204)


@pytest.mark.integration
def test_integration_sync_files() -> None:
    url, key = _skip_unless_env()

    svc = StorageServices(url)
    svc.set_api_key(key)

    with tempfile.TemporaryDirectory() as td:
        base = Path(td) / "syncdir"
        (base / "sub").mkdir(parents=True)

        # create files
        (base / "one.txt").write_text("one")
        (base / "sub" / "two.txt").write_text("two")

        # perform sync to remote prefix
        res = svc.sync_local_to_remote(
            base, remote_prefix="sync-tests", ignore_sha=True, dry_run=False
        )

        # expect keys for uploaded files (relative paths)
        assert "one.txt" in res
        assert "sub/two.txt" in res

        # verify via ls that remote files exist
        ls_resp = svc.ls()
        assert ls_resp.status_code == 200
        try:
            listing = ls_resp.json()
        except Exception:
            listing = None

        # Build a set of remote paths returned by the server in a few possible formats
        found = set()
        if isinstance(listing, list):
            for it in listing:
                found.add(it)

        assert any(
            p.endswith("sync-tests/one.txt") for p in found
        ), "one.txt not found in remote listing"
        assert any(
            p.endswith("sync-tests/sub/two.txt") for p in found
        ), "two.txt not found in remote listing"

        # cleanup remote files
        d1 = svc.delete("sync-tests/one.txt")
        d2 = svc.delete("sync-tests/sub/two.txt")
        assert d1.status_code in (200, 204)
        assert d2.status_code in (200, 204)


@pytest.mark.integration
def test_integration_metadata_multiple_files() -> None:
    url, key = _skip_unless_env()

    svc = StorageServices(url)
    svc.set_api_key(key)

    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        f1 = base / "multi_a.txt"
        f2 = base / "multi_b.txt"
        c1 = "alpha"
        c2 = "beta"
        f1.write_text(c1)
        f2.write_text(c2)

        r1 = "gfslib/multi/multi_a.txt"
        r2 = "gfslib/multi/multi_b.txt"

        up1 = svc.upload(r1, str(f1))
        up2 = svc.upload(r2, str(f2))
        assert up1.status_code in (200, 201, 204)
        assert up2.status_code in (200, 201, 204)

        meta = svc.metadata([r1, r2], ignore_sha=False)

        # Expect a list of metadata dicts similar to single-file test
        assert isinstance(meta, list)

        def find_item(suffix: str) -> Dict[Any, Any]:
            for it in meta:
                if isinstance(it, dict):
                    p = it.get("Path")
                    if isinstance(p, str):
                        pn = p.replace("\\", "/")
                        if pn.endswith(suffix):
                            return it
            pytest.fail(f"metadata for {suffix} not found")

        i1 = find_item("multi/multi_a.txt")
        i2 = find_item("multi/multi_b.txt")

        size1 = int(i1.get("Size") or -1)
        size2 = int(i2.get("Size") or -1)
        assert size1 == len(c1.encode("utf-8"))
        assert size2 == len(c2.encode("utf-8"))

        sha1 = i1.get("Sha2")
        sha2v = i2.get("Sha2")
        if isinstance(sha1, str):
            assert (
                sha1.lower() == hashlib.sha256(c1.encode("utf-8")).hexdigest().lower()
            )
        if isinstance(sha2v, str):
            assert (
                sha2v.lower() == hashlib.sha256(c2.encode("utf-8")).hexdigest().lower()
            )

        # cleanup
        d1 = svc.delete(r1)
        d2 = svc.delete(r2)
        assert d1.status_code in (200, 204)
        assert d2.status_code in (200, 204)


@pytest.mark.integration
def test_integration_sync_skips_on_sha2() -> None:
    url, key = _skip_unless_env()

    svc = StorageServices(url)
    svc.set_api_key(key)

    with tempfile.TemporaryDirectory() as td:
        base = Path(td) / "skipsha2"
        base.mkdir()

        # Create a file
        local = base / "same.txt"
        content = "same-content-sha2"
        local.write_text(content)

        # Upload it
        remote_prefix = "skip-sha2-tests"
        remote_path = f"{remote_prefix}/same.txt"
        up = svc.upload(remote_path, str(local))
        assert up.status_code in (200, 201, 204)

        # Now run sync with ignore_sha=False; it should detect same Sha2 and skip
        res = svc.sync_local_to_remote(
            base, remote_prefix=remote_prefix, ignore_sha=False, dry_run=False
        )
        assert res.get("same.txt") == "skipped"

        # Cleanup
        d = svc.delete(remote_path)
        assert d.status_code in (200, 204)
