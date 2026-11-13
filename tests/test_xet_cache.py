import base64
import json
import sys
import types

import pytest
import typer

# accli/__init__ imports the CLI, which imports hf_xet; these tests don't need it.
sys.modules.setdefault("hf_xet", types.ModuleType("hf_xet"))

from accli import xet_cache  # noqa: E402


def make_token(payload: dict) -> str:
    def b64(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'ES256'})}.{b64(payload)}.signature"


SERVER = "https://accelerator.example.org"


@pytest.fixture(autouse=True)
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("SUDO_USER", raising=False)
    monkeypatch.delenv("PKEXEC_UID", raising=False)
    monkeypatch.setattr(xet_cache, "_mount_running", lambda: False)
    return tmp_path


def test_decode_gc_id():
    assert xet_cache.decode_gc_id(make_token({"type": "TERM_CLI", "xet_gc_id": "a3f9c1"})) == "a3f9c1"
    assert xet_cache.decode_gc_id(make_token({"type": "TERM_CLI"})) is None
    assert xet_cache.decode_gc_id("not-a-jwt") is None


def test_no_claim_skips_check():
    xet_cache.check_gc_id(SERVER, make_token({"type": "TERM_CLI"}))


def test_missing_saved_id_is_a_mismatch():
    with pytest.raises(typer.Exit):
        xet_cache.check_gc_id(SERVER, make_token({"xet_gc_id": "a3f9c1"}))


def test_matching_id_passes_and_other_id_stops():
    xet_cache.save_gc_id(SERVER, "a3f9c1")
    xet_cache.check_gc_id(SERVER, make_token({"xet_gc_id": "a3f9c1"}))
    with pytest.raises(typer.Exit):
        xet_cache.check_gc_id(SERVER, make_token({"xet_gc_id": "b7e2d0"}))


def test_ids_are_saved_per_server():
    xet_cache.save_gc_id(SERVER, "a3f9c1")
    xet_cache.save_gc_id("https://other.example.org", "zzz")
    assert xet_cache.get_saved_gc_id(SERVER) == "a3f9c1"
    assert xet_cache.get_saved_gc_id("https://other.example.org") == "zzz"


def test_clear_deletes_cache_then_saves_server_id():
    folder = xet_cache.xet_cache_dir(SERVER)
    (folder / "shard-cache").mkdir(parents=True)
    (folder / "shard-cache" / "x.mdb").write_bytes(b"data")

    xet_cache.clear_cache(SERVER, lambda: make_token({"xet_gc_id": "a3f9c1"}), all_servers=False, yes=True, force=False)

    assert not folder.exists()
    assert xet_cache.get_saved_gc_id(SERVER) == "a3f9c1"
    xet_cache.check_gc_id(SERVER, make_token({"xet_gc_id": "a3f9c1"}))


def test_failed_delete_keeps_old_id(monkeypatch):
    xet_cache.save_gc_id(SERVER, "old")
    xet_cache.xet_cache_dir(SERVER).mkdir(parents=True)

    def boom(_path):
        raise OSError("busy")
    monkeypatch.setattr(xet_cache.shutil, "rmtree", boom)

    with pytest.raises(typer.Exit):
        xet_cache.clear_cache(SERVER, lambda: make_token({"xet_gc_id": "new"}), all_servers=False, yes=True, force=False)
    assert xet_cache.get_saved_gc_id(SERVER) == "old"


def test_clear_refuses_while_mount_runs(monkeypatch):
    monkeypatch.setattr(xet_cache, "_mount_running", lambda: True)
    with pytest.raises(typer.Exit):
        xet_cache.clear_cache(SERVER, lambda: make_token({}), all_servers=False, yes=True, force=False)
    # --force overrides
    xet_cache.clear_cache(SERVER, lambda: make_token({}), all_servers=False, yes=True, force=True)


def test_clear_refuses_while_another_upload_runs(monkeypatch):
    marker_dir = xet_cache._active_dir()
    marker_dir.mkdir(parents=True)
    (marker_dir / "424242").write_text("copy")
    monkeypatch.setattr(xet_cache, "_pid_alive", lambda pid: pid == 424242)
    with pytest.raises(typer.Exit):
        xet_cache.clear_cache(SERVER, lambda: make_token({}), all_servers=False, yes=True, force=False)


def test_shard_rejection_detection():
    err = Exception("Request error: HTTP status client error (400 Bad Request) for url (https://x/api/xet-cas/v1/shards)")
    assert xet_cache.is_shard_rejection(err)
    assert not xet_cache.is_shard_rejection(Exception("HTTP status server error (503) for url (https://x/v1/shards)"))
