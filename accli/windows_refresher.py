import os
import sys
import time
import ctypes
import subprocess
from pathlib import Path
from datetime import datetime

# Windows Shell Notification constants
SHCNE_UPDATEDIR = 0x00001000
SHCNF_PATHW = 0x0005


def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [Watcher] {msg}", flush=True)


def notify_explorer(path_str: str):
    """Notifies Windows Shell / Explorer to refresh views for path_str."""
    log(f"Broadcasting SHChangeNotify on '{path_str}'...")
    try:
        ctypes.windll.shell32.SHChangeNotify(
            SHCNE_UPDATEDIR,
            SHCNF_PATHW,
            path_str,
            None
        )
    except Exception as e:
        log(f"SHChangeNotify error: {e}")

    try:
        script_path = Path(__file__).parent / "refresh_explorer.ps1"
        if script_path.is_file():
            log(f"Executing COM window refresh for '{path_str}'...")
            res = subprocess.run(
                [
                    "powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                    "-File", str(script_path), path_str
                ],
                capture_output=True,
                text=True,
                creationflags=0x08000000  # CREATE_NO_WINDOW
            )
            if res.stdout and res.stdout.strip():
                log(f"COM output: {res.stdout.strip()}")
            if res.stderr and res.stderr.strip():
                log(f"COM error: {res.stderr.strip()}")
            log("Explorer windows refreshed.")
    except Exception as e:
        log(f"COM refresh error: {e}")


def scan_directory_state(root_path: Path):
    """
    Fast snapshot of directory state: mapping of relative_path -> (size, mtime_ns).
    Returns None if root_path is not accessible or unmounted.
    """
    state = {}
    try:
        if not root_path.exists():
            return None
        
        for root, dirs, files in os.walk(root_path):
            for name in files:
                full_path = os.path.join(root, name)
                try:
                    st = os.stat(full_path)
                    rel = os.path.relpath(full_path, root_path)
                    state[rel] = (st.st_size, st.st_mtime_ns)
                except (OSError, PermissionError):
                    continue
            for name in dirs:
                full_path = os.path.join(root, name)
                try:
                    st = os.stat(full_path)
                    rel = os.path.relpath(full_path, root_path)
                    state[rel] = (-1, st.st_mtime_ns)
                except (OSError, PermissionError):
                    continue
        return state
    except Exception:
        return None


def describe_changes(old_state, new_state):
    old_keys = set(old_state.keys())
    new_keys = set(new_state.keys())
    added = new_keys - old_keys
    removed = old_keys - new_keys
    modified = {k for k in (old_keys & new_keys) if old_state[k] != new_state[k]}
    
    parts = []
    if added:
        parts.append(f"Added: {list(added)[:5]}")
    if removed:
        parts.append(f"Removed: {list(removed)[:5]}")
    if modified:
        parts.append(f"Modified: {list(modified)[:5]}")
    return "; ".join(parts)


def run_watcher(mount_point: str, interval: float):
    """
    Periodically inspects mount_point for remote additions/deletions/modifications.
    Triggers Windows Shell & open Explorer window refreshes whenever changes are detected.
    """
    drive_root = mount_point.rstrip("\\/") + "\\"
    root_path = Path(drive_root)
    
    log(f"Starting watcher on '{drive_root}' (poll interval: {interval}s)...")
    
    # Baseline snapshot
    try:
        last_state = scan_directory_state(root_path)
    except Exception as e:
        last_state = None
        log(f"Initial scan error: {e}")

    if last_state is not None:
        log(f"Initial baseline snapshot indexed {len(last_state)} items on '{drive_root}'.")
    else:
        log(f"Warning: Could not read '{drive_root}'. Waiting for drive to become available...")
    
    while True:
        try:
            time.sleep(interval)
        except (KeyboardInterrupt, SystemExit):
            log("Watcher stopped.")
            break
        except Exception as e:
            log(f"Sleep interrupted: {e}")
            continue
            
        try:
            current_state = scan_directory_state(root_path)
            if current_state is None:
                continue
                
            if last_state is not None:
                if current_state != last_state:
                    diff_summary = describe_changes(last_state, current_state)
                    log(f"Change detected on '{drive_root}' -> {diff_summary}")
                    notify_explorer(drive_root)
                    last_state = current_state
            else:
                last_state = current_state
                log(f"Baseline established: {len(last_state)} items found.")
        except Exception as e:
            log(f"Loop error: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python windows_refresher.py <mount_point> <interval_seconds>")
        sys.exit(1)
    mp = sys.argv[1]
    poll_int = float(sys.argv[2])
    run_watcher(mp, poll_int)
