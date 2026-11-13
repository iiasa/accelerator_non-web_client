"""
Local Xet cache handling and GC ID sync.

The server puts a GC ID in every access token (claim ``xet_gc_id``) and changes it after each
storage cleanup. accli saves the ID it last synced with, per server, in ``~/.accli/state.json``.
When the two differ, the local Xet cache may point at data the server removed, so uploads stop
and ask the user to run ``accli cache clear``.

Each server gets its own cache folder, used as HF_XET_CACHE by ``copy`` and ``mount start``,
so clearing it never touches other Hugging Face tools or other servers.
"""
import base64
import json
import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import typer
from rich import print

from accli.token import get_db_path

GC_ID_CLAIM = "xet_gc_id"

MISMATCH_MESSAGE = (
    "[bold red]Server storage was cleaned up since this machine last cleared its Xet cache.[/bold red]\n"
    "Run:  [bold]accli cache clear[/bold]\n"
    "then run this command again."
)

SHARD_REJECTED_HINT = (
    "[yellow]The server rejected the upload's metadata (HTTP 400 on /shards). This usually means the "
    "local Xet cache references data that was cleaned up on the server.[/yellow]\n"
    "Run:  [bold]accli cache clear[/bold]\n"
    "then run the upload again."
)


# ---- Paths ----

def cache_base_dir() -> Path:
    """Base folder of accli's caches (same as `mount start` uses)."""
    if platform.system() == "Windows":
        return Path("C:/ProgramData/accli/cache")
    return Path.home() / ".accli" / "cache"


def xet_root_dir() -> Path:
    return cache_base_dir() / "xet"


def _server_key(server_url: str) -> str:
    parsed = urlparse(server_url or "")
    host = parsed.netloc or parsed.path or "default"
    return "".join(c if c.isalnum() or c in "-._" else "_" for c in host.lower()) or "default"


def xet_cache_dir(server_url: str) -> Path:
    """HF_XET_CACHE folder for one server."""
    return xet_root_dir() / _server_key(server_url)


def _active_dir() -> Path:
    return cache_base_dir() / "xet-active"


def _state_path() -> Path:
    return Path(get_db_path()).parent / "state.json"


# ---- State (saved GC IDs) ----

def _load_state() -> dict:
    try:
        return json.loads(_state_path().read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_saved_gc_id(server_url: str) -> Optional[str]:
    return _load_state().get("servers", {}).get(server_url, {}).get(GC_ID_CLAIM)


def save_gc_id(server_url: str, gc_id: Optional[str]) -> None:
    state = _load_state()
    servers = state.setdefault("servers", {})
    entry = servers.setdefault(server_url, {})
    if gc_id is None:
        entry.pop(GC_ID_CLAIM, None)
    else:
        entry[GC_ID_CLAIM] = gc_id
    path = _state_path()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(tmp, path)


# ---- Token ----

def decode_gc_id(access_token: str) -> Optional[str]:
    """Reads the GC ID claim from the access token's payload. No signature check is needed:
    the value is only compared for equality. Returns None if the token has no claim."""
    try:
        payload_b64 = access_token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        value = payload.get(GC_ID_CLAIM)
        return str(value) if value else None
    except Exception:
        return None


def check_gc_id(server_url: str, access_token: str) -> None:
    """Stops with the 'clear your cache' message if the server's GC ID differs from the saved one.

    No claim in the token (ID not set on the server, or an older server): no check.
    No saved ID counts as a mismatch.
    """
    server_id = decode_gc_id(access_token)
    if server_id is None:
        return
    if get_saved_gc_id(server_url) != server_id:
        print(MISMATCH_MESSAGE)
        raise typer.Exit(1)


def is_shard_rejection(error: Exception) -> bool:
    text = str(error)
    return "400" in text and "/shards" in text


# ---- Activity (refuse to clear while the cache is in use) ----

def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if platform.system() == "Windows":
        try:
            res = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True, errors="replace")
            return str(pid) in (res.stdout or "")
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


class CacheInUse:
    """Marks this process as using the Xet cache (for `copy`), so `cache clear` refuses meanwhile."""

    def __enter__(self):
        try:
            _active_dir().mkdir(parents=True, exist_ok=True)
            self._marker = _active_dir() / str(os.getpid())
            self._marker.write_text("copy", encoding="utf-8")
        except Exception:
            self._marker = None
        return self

    def __exit__(self, *exc):
        try:
            if self._marker:
                self._marker.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def _active_uploads() -> list[int]:
    pids = []
    try:
        for marker in _active_dir().iterdir():
            try:
                pid = int(marker.name)
            except ValueError:
                continue
            if pid != os.getpid() and _pid_alive(pid):
                pids.append(pid)
            elif pid != os.getpid():
                marker.unlink(missing_ok=True)
    except FileNotFoundError:
        pass
    return pids


def _mount_running() -> bool:
    if platform.system() == "Windows":
        try:
            res = subprocess.run(["tasklist", "/FI", "IMAGENAME eq hf-mount-nfs.exe"], capture_output=True, text=True, errors="replace")
            return "hf-mount-nfs.exe" in (res.stdout or "")
        except Exception:
            return False
    try:
        res = subprocess.run(["pgrep", "-f", "hf-mount"], capture_output=True, text=True)
        return res.returncode == 0
    except Exception:
        return False


# ---- Commands ----

def _folder_size(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += (Path(root) / f).stat().st_size
            except OSError:
                pass
    return total


def _human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n} B"


def cache_info(server_url: str, access_token: Optional[str]) -> None:
    folder = xet_cache_dir(server_url)
    size = _folder_size(folder) if folder.exists() else 0
    saved = get_saved_gc_id(server_url)
    server_id = decode_gc_id(access_token) if access_token else None
    print(f"[cyan]Server:[/cyan]          {server_url}")
    print(f"[cyan]Xet cache folder:[/cyan] {folder}")
    print(f"[cyan]Size:[/cyan]             {_human(size)}")
    print(f"[cyan]Local GC ID:[/cyan]      {saved or '(none)'}")
    print(f"[cyan]Server GC ID:[/cyan]     {server_id or '(not set)'}")
    if server_id is not None and saved != server_id:
        print("[yellow]The IDs differ: run 'accli cache clear' before your next upload.[/yellow]")


def clear_cache(server_url: str, fetch_access_token, all_servers: bool, yes: bool, force: bool) -> None:
    """Deletes the Xet cache, then (only if that worked) saves the server's current GC ID."""
    target = xet_root_dir() if all_servers else xet_cache_dir(server_url)

    if not force:
        uploads = _active_uploads()
        if uploads:
            print(f"[bold red]ERROR: Another accli upload is running (pid {', '.join(map(str, uploads))}). "
                  "Wait for it to finish, or use --force.[/bold red]")
            raise typer.Exit(1)
        if _mount_running():
            print("[bold red]ERROR: A mount is running and uses this cache. Run 'accli mount stop' first, "
                  "or use --force.[/bold red]")
            raise typer.Exit(1)

    if not yes:
        if not typer.confirm(f"Delete the Xet cache at {target}?"):
            raise typer.Exit(0)

    if target.exists():
        try:
            shutil.rmtree(target)
        except Exception as e:
            # The old ID stays saved, so the next upload asks again.
            print(f"[bold red]ERROR: Could not delete {target}: {e}[/bold red]")
            raise typer.Exit(1)
    print(f"[green]Deleted the Xet cache at {target}.[/green]")

    # An empty cache is valid for any GC ID, so fetching it after the delete is safe.
    server_id = decode_gc_id(fetch_access_token())
    save_gc_id(server_url, server_id)
    if server_id:
        print(f"[green]Synced with the server's GC ID ({server_id}). You can upload again.[/green]")
