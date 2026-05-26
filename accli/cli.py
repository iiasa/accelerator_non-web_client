import glob
import importlib.util
import os
import re
import warnings
import hashlib
import hf_xet
from contextlib import contextmanager
from pathlib import Path

import requests
import typer
from typing import List

from accli.AcceleratorTerminalCliProjectService import AcceleratorTerminalCliProjectService
from accli.CsvRegionalTimeseriesValidator import CsvRegionalTimeseriesValidator
from accli.token import save_token_details, get_token, get_server_url, set_project_slug, get_project_slug, get_db_path
from rich import print
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from typing_extensions import Annotated

from ._version import VERSION

warnings.filterwarnings('ignore')

ACCLI_DEBUG = os.environ.get('ACCLI_DEBUG', False)

app = typer.Typer(
    add_completion=False,
    pretty_exceptions_show_locals=False,
    no_args_is_help=True
)


def get_size(path):
    size = 0

    for file in glob.iglob(f"{path}/**/*.*", recursive=True):
        size += os.path.getsize(file)

    return size


@contextmanager
def pushd(new_dir):
    """Temporarily change the working directory."""
    prev_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(prev_dir)


@app.command()
def about():
    print(
        "[bold cyan]This is a terminal client for Accelerator hosted on https://accelerator.iiasa.ac.at . [/bold cyan]\n")
    print(
        "[bold cyan]Please file feature requests and suggestions at https://github.com/iiasa/accli/issues .[/bold cyan]\n")
    print("[bold cyan]License: The MIT License (MIT)[/bold cyan]\n")
    print(f"[bold cyan]Version: {VERSION}[/bold cyan]\n")


@app.command()
def login(
        server: Annotated[
            str, typer.Option(..., '-s', help="Accelerator server url.")] = "https://accelerator.iiasa.ac.at",
        webcli: Annotated[str, typer.Option(..., '-c',
                                            help="Accelerator web client for authorization.")] = "https://accelerator.iiasa.ac.at"
):
    print(
        f"[bold cyan]Welcome to Accelerator Terminal Client.[/bold cyan]\n"
        f"[bold cyan]Powered by IIASA[/bold cyan]\n"
    )

    print(f"[italic]Get authorization code on following web url: [cyan]{webcli}/acli-auth-code[/cyan][/italic] \n")

    device_authorization_code = typer.prompt("Enter the authorization code?")

    token_response = requests.post(
        f"{server}/api/v1/oauth/device/token/",
        json={"device_authorization_code": device_authorization_code},
        verify=(not ACCLI_DEBUG)
    )

    print("")

    if token_response.status_code == 400:
        print(f"[bold red]ERROR: {token_response.json().get('detail')}[/bold red]")
        raise typer.Exit(1)

    save_token_details(token_response.json(), server, webcli)

    print("[bold green]Successfully logged in.[/bold green]:rocket: :rocket:")


# Upload functionality is now integrated into the unified 'copy' command.


@app.command()
def validate(
        project_slug: Annotated[str, typer.Argument(help="Unique Accelerator project slug.")],
        template_slug: Annotated[str, typer.Argument(help="Unique project template slug")],
        filepath: Annotated[str, typer.Argument(help="Path of the file to validate")],
        server: Annotated[
            str, typer.Option(..., '-s', help="Accelerator server url.")] = "https://accelerator.iiasa.ac.at",
):
    term_cli_project_service = AcceleratorTerminalCliProjectService(
        user_token="",
        server_url=server,
        verify_cert=(not ACCLI_DEBUG)
    )

    validate = CsvRegionalTimeseriesValidator(
        project_slug=project_slug,
        dataset_template_slug=template_slug,
        input_filepath=filepath,
        project_service=term_cli_project_service,
    )

    validate()


@app.command()
def dispatch(
        project_slug: Annotated[str, typer.Argument(help="Unique Accelerator project slug.")],
        root_task_variable: Annotated[str, typer.Argument(help="Root task variable in workflow_file.")],
        workflow_filename: Annotated[str, typer.Option(..., '-f', help="Python workflow filepath.")] = "wkube.py"
):
    set_project_slug(project_slug)
    server_url = get_server_url()
    # Exchange the refresh token for a short-lived access token
    _, access_token, _ = exchange_refresh_token(project_slug)

    term_cli_project_service = AcceleratorTerminalCliProjectService(
        user_token=access_token,
        server_url=server_url,
        verify_cert=(not ACCLI_DEBUG)
    )

    # ✅ Resolve the workflow file path properly
    if os.path.isabs(workflow_filename):
        workflow_filepath = workflow_filename
    else:
        workflow_filepath = os.path.abspath(os.path.join(os.getcwd(), workflow_filename))

    if not os.path.isfile(workflow_filepath):
        raise FileNotFoundError(f"Workflow file not found: {workflow_filepath}")

    workflow_dir = os.path.dirname(workflow_filepath)

    # ✅ Temporarily switch to the workflow file's directory
    with pushd(workflow_dir):
        # ✅ Import the module dynamically
        spec = importlib.util.spec_from_file_location("workflow", workflow_filepath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        job_to_dispatch = getattr(module, root_task_variable, None)

        if not job_to_dispatch:
            raise ValueError(f"No root task variable found with name '{root_task_variable}' in {workflow_filepath}")

        print(job_to_dispatch.description)

        root_job_id = term_cli_project_service.dispatch(
            project_slug,
            job_to_dispatch.description
        )

    print(f"Dispatched root job #ID: {root_job_id}")


def compute_sha256(filepath: str) -> str:
    """Helper to compute the SHA-256 hash of a file locally."""
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()

import threading

_token_lock = threading.Lock()
_cached_cas_token = None
_cached_expires_at = 0
_cached_access_token = None


def exchange_refresh_token(project_slug: str) -> tuple[str, str, int]:
    """
    Exchanges the stored refresh token for a short-lived access token
    and a new rotated refresh token. Updates the local TinyDB token cache.
    Returns (cas_token, access_token, expires_at).
    """
    import time
    from tinydb import TinyDB
    from accli.token import get_db_path, save_token_details
    
    global _cached_cas_token, _cached_expires_at, _cached_access_token
    
    # 1. Fast-path check: Reuse valid in-memory cache if still fresh (> 5 mins remaining)
    # and matches the requested project slug prefix.
    now = int(time.time())
    if _cached_cas_token and _cached_access_token and (_cached_expires_at - now > 300):
        expected_prefix = f"xet_session_prj_{project_slug}_"
        if _cached_cas_token.startswith(expected_prefix):
            return _cached_cas_token, _cached_access_token, _cached_expires_at
            
    # 2. Block/lock to serialize requests and avoid rotation race conditions (RTR invalidation)
    with _token_lock:
        # Re-check cache inside the lock (double-checked locking pattern)
        now = int(time.time())
        if _cached_cas_token and _cached_access_token and (_cached_expires_at - now > 300):
            expected_prefix = f"xet_session_prj_{project_slug}_"
            if _cached_cas_token.startswith(expected_prefix):
                return _cached_cas_token, _cached_access_token, _cached_expires_at
                
        db_path = get_db_path()
        db = TinyDB(db_path)
        item = next(iter(db), {})
        refresh_token = item.get('token')
        server_url = item.get('server_url', "https://accelerator.iiasa.ac.at")
        webcli_url = item.get('webcli_url', "https://accelerator.iiasa.ac.at")
        
        if not refresh_token:
            print("[bold red]ERROR: No token found. Please run 'accli login' first.[/bold red]")
            raise typer.Exit(1)
            
        refresh_endpoint = f"{server_url.rstrip('/')}/api/v1/oauth/device/access-token/"
        
        try:
            response = requests.post(
                refresh_endpoint,
                json={"refresh_token": refresh_token},
                verify=(not ACCLI_DEBUG)
            )
            response.raise_for_status()
            data = response.json()
            
            access_token = data["access_token"]
            new_refresh_token = data["refresh_token"]
            
            # Save rotated refresh token back to local TinyDB
            save_token_details(new_refresh_token, server_url, webcli_url)
            
            cas_token = f"xet_session_prj_{project_slug}_{access_token}"
            # Expire slightly before the 1-hour limit (e.g. 50 minutes)
            expires_at = int(time.time()) + 3000
            
            # Populate in-memory cache
            _cached_cas_token = cas_token
            _cached_access_token = access_token
            _cached_expires_at = expires_at
            
            return cas_token, access_token, expires_at
        except Exception as e:
            print(f"[bold red]ERROR: Failed to authenticate/exchange refresh token: {e}[/bold red]")
            raise typer.Exit(1)


@app.command()
def copy(
        source: Annotated[str, typer.Argument(help="Source path (local path or remote acc://...)")],
        destination: Annotated[str, typer.Argument(help="Destination path (local path or remote acc://...)")],
        token_pass: Annotated[str, typer.Option(..., "-t", help="Optional authorization token pass.")] = "",
):
    """
    Copy files between local filesystems and Accelerator project spaces using hf-xet.
    At least one of the paths must be a remote path starting with 'acc://'.
    """
    is_src_remote = source.startswith("acc://")
    is_dest_remote = destination.startswith("acc://")

    if not is_src_remote and not is_dest_remote:
        print("[bold red]ERROR: Either source or destination must start with acc://[/bold red]")
        raise typer.Exit(1)
    if is_src_remote and is_dest_remote:
        print("[bold red]ERROR: Remote-to-remote copy is not supported. One path must be local.[/bold red]")
        raise typer.Exit(1)

    # Resolve project_slug from the remote path
    remote_path_str = source if is_src_remote else destination
    parsed = remote_path_str[len("acc://"):]
    if "/" in parsed:
        project_slug, remote_subpath = parsed.split("/", 1)
    else:
        project_slug = parsed
        remote_subpath = ""

    server_url = get_server_url()
    cas_token, access_token, expires_at = exchange_refresh_token(project_slug)

    term_cli_project_service = AcceleratorTerminalCliProjectService(
        user_token=access_token,
        server_url=server_url,
        verify_cert=(not ACCLI_DEBUG),
    )

    def make_token_refresher(slug: str):
        def refresher() -> tuple[str, int]:
            token_str, _, exp_time = exchange_refresh_token(slug)
            return token_str, exp_time
        return refresher

    if is_src_remote:
        # Download flow: acc://[project_slug]/[remote_prefix] -> local destination
        source_parsed = source[len("acc://"):]
        if "/" in source_parsed:
            _, remote_prefix = source_parsed.split("/", 1)
        else:
            remote_prefix = ""

        cas_endpoint = f"{server_url.rstrip('/')}/api/xet-cas"

        print(f"[cyan]Enumerating remote files for prefix [white]{source_parsed}[/white]...[/cyan]")
        filenames: List[str] = term_cli_project_service.enumerate_files_by_prefix(source_parsed, token_pass=token_pass)

        if not filenames:
            print("[bold yellow]No files found matching the remote prefix.[/bold yellow]")
            raise typer.Exit(0)

        dest_path = Path(destination).expanduser().resolve()
        is_dest_dir = (
            dest_path.is_dir()
            or destination.endswith("/")
            or destination.endswith(os.sep)
            or len(filenames) > 1
        )

        if is_dest_dir:
            dest_path.mkdir(parents=True, exist_ok=True)
        else:
            dest_path.parent.mkdir(parents=True, exist_ok=True)

        download_infos = []
        for filename in filenames:
            # Filename returned usually starts with project_slug/
            if filename.startswith(f"{project_slug}/"):
                rel_filename = filename[len(project_slug):]
            elif filename.startswith(project_slug):
                rel_filename = filename[len(project_slug):]
            else:
                rel_filename = filename

            if not rel_filename.startswith("/"):
                rel_filename = "/" + rel_filename

            print(f"[dim]Resolving metadata for {filename}...[/dim]")
            stat = term_cli_project_service.get_file_stat(project_slug, rel_filename)
            if not stat:
                print(f"[yellow]Warning: Could not get metadata for {filename}, skipping.[/yellow]")
                continue

            merkle_hash = stat.get("merkle_hash")
            file_size = stat.get("size")
            if not merkle_hash or file_size is None:
                print(f"[yellow]Warning: Missing merkle hash or size for {filename}, skipping.[/yellow]")
                continue

            if is_dest_dir:
                # Determine local subpath under destination
                if remote_prefix:
                    prefix_to_strip = f"{project_slug}/{remote_prefix}"
                    if filename.startswith(prefix_to_strip):
                        rel_subpath = filename[len(prefix_to_strip):].lstrip("/")
                    else:
                        rel_subpath = filename[len(project_slug):].lstrip("/")
                else:
                    rel_subpath = filename[len(project_slug):].lstrip("/")

                local_dest = dest_path / rel_subpath
            else:
                local_dest = dest_path

            local_dest.parent.mkdir(parents=True, exist_ok=True)

            download_infos.append(
                hf_xet.PyXetDownloadInfo(
                    destination_path=str(local_dest.resolve()),
                    hash=merkle_hash,
                    file_size=file_size
                )
            )

        if not download_infos:
            print("[bold red]ERROR: No valid files identified for download.[/bold red]")
            raise typer.Exit(1)

        print(f"[bold cyan]Downloading {len(download_infos)} files using hf-xet...[/bold cyan]")
        try:
            hf_xet.download_files(
                files=download_infos,
                endpoint=cas_endpoint,
                token_info=(cas_token, expires_at),
                token_refresher=make_token_refresher(project_slug),
                progress_updater=None,
                request_headers=None
            )
            print("[bold green]✔ Download completed successfully![/bold green]")
        except Exception as e:
            print(f"[bold red]ERROR: Download failed: {e}[/bold red]")
            raise typer.Exit(1)

    else:
        # Upload flow: local source -> acc://[project_slug]/[remote_path]
        dest_parsed = destination[len("acc://"):]
        if "/" in dest_parsed:
            project_slug, remote_path = dest_parsed.split("/", 1)
        else:
            project_slug = dest_parsed
            remote_path = ""

        cas_endpoint = f"{server_url.rstrip('/')}/api/xet-cas"
        register_url = f"{server_url.rstrip('/')}/api/xet-cas/v1/cas/bulk-register"

        local_src_path = Path(source).expanduser().resolve()
        if not local_src_path.exists():
            print(f"[bold red]ERROR: Local source path does not exist: {source}[/bold red]")
            raise typer.Exit(1)

        # Collect local files and map to remote filenames
        local_paths = []
        remote_filenames = []

        if local_src_path.is_dir():
            # Recursively find all files
            for local_file in local_src_path.rglob("*"):
                if local_file.is_file():
                    rel_subpath = local_file.relative_to(local_src_path)
                    # Remote filename should be relative under project_slug
                    if remote_path:
                        remote_name = f"{remote_path.rstrip('/')}/{rel_subpath}"
                    else:
                        remote_name = str(rel_subpath)
                    
                    if os.name == 'nt':
                        remote_name = remote_name.replace('\\', '/')
                        
                    local_paths.append(local_file)
                    remote_filenames.append(remote_name)
        else:
            # Single file
            if remote_path.endswith("/") or not remote_path:
                remote_name = f"{remote_path.rstrip('/')}/{local_src_path.name}".lstrip("/")
            else:
                remote_name = remote_path
                
            local_paths.append(local_src_path)
            remote_filenames.append(remote_name)

        if not local_paths:
            print("[bold yellow]No files to upload.[/bold yellow]")
            raise typer.Exit(0)

        print(f"[bold cyan]Uploading {len(local_paths)} files using hf-xet...[/bold cyan]")
        try:
            upload_results = hf_xet.upload_files(
                file_paths=[str(p) for p in local_paths],
                endpoint=cas_endpoint,
                token_info=(cas_token, expires_at),
                token_refresher=make_token_refresher(project_slug),
                progress_updater=None,
                _repo_type=None,
                request_headers=None,
                sha256s=None,
                skip_sha256=False
            )
        except Exception as e:
            print(f"[bold red]ERROR: Upload failed: {e}[/bold red]")
            raise typer.Exit(1)

        # Compute SHA-256 locally and register metadata in database
        print("[cyan]Computing SHA-256 hashes and registering metadata...[/cyan]")
        registration_items = []
        for local_path, remote_filename, upload_info in zip(local_paths, remote_filenames, upload_results):
            sha256_hash = compute_sha256(str(local_path))
            registration_items.append({
                "filename": f"{project_slug}/{remote_filename}",
                "merkle_hash": upload_info.hash,
                "sha256": sha256_hash,
                "file_size": upload_info.file_size,
                "content_type": "application/octet-stream"
            })

        headers = {
            "Content-Type": "application/json",
            "X-Project-Slug": project_slug,
            "Authorization": f"Bearer {cas_token}"
        }

        try:
            response = requests.post(
                register_url,
                json={"items": registration_items},
                headers=headers,
                verify=(not ACCLI_DEBUG)
            )
            response.raise_for_status()
            print("[bold green]✔ Upload and bulk metadata registration completed successfully![/bold green]")
        except requests.exceptions.HTTPError as e:
            detail = None
            try:
                detail = e.response.json().get("detail")
            except Exception:
                pass
            if detail:
                print(f"[bold red]ERROR: Bulk metadata registration failed: {detail}[/bold red]")
            else:
                print(f"[bold red]ERROR: Bulk metadata registration failed: {e}[/bold red]")
            raise typer.Exit(1)
        except Exception as e:
            print(f"[bold red]ERROR: Bulk metadata registration failed: {e}[/bold red]")
            raise typer.Exit(1)


# --- Mount commands group ---
mount_app = typer.Typer(
    help="Manage mounting Accelerator project spaces locally using hf-mount.",
    no_args_is_help=True
)
app.add_typer(mount_app, name="mount")


def find_available_windows_drive(preferred: str = "W") -> str:
    """Finds an available Windows drive letter, starting with preferred, then checking others."""
    import os
    preferred = preferred.upper()
    if not os.path.exists(f"{preferred}:\\"):
        return f"{preferred}:"
        
    # Search order: subsequent letters first, then previous descending to D
    search_order = ["X", "Y", "Z"] + [chr(x) for x in range(ord("V"), ord("D") - 1, -1)]
    for letter in search_order:
        if not os.path.exists(f"{letter}:\\"):
            return f"{letter}:"
            
    raise RuntimeError("No available Windows drive letters found.")


def enable_windows_nfs_features():
    """Automatically check and enable Windows Client for NFS optional features if needed."""
    import subprocess
    import platform
    
    print("[cyan]Checking if Windows Client for NFS features are enabled...[/cyan]")
    check_cmd = [
        "powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
        "Get-WindowsOptionalFeature -Online -FeatureName ServicesForNFS-ClientOnly"
    ]
    try:
        res = subprocess.run(check_cmd, capture_output=True, text=True, check=True)
        if "State : Enabled" in res.stdout or "State: Enabled" in res.stdout:
            print("[bold green]✔ ServicesForNFS-ClientOnly is already enabled.[/bold green]")
            return
    except Exception:
        pass

    print("[yellow]Windows Client for NFS features are not enabled. Attempting automatic enablement...[/yellow]")
    print("[italic]Note: This requires Administrator/elevated privileges.[/italic]")
    
    enable_client_cmd = [
        "powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
        "Enable-WindowsOptionalFeature -Online -FeatureName ServicesForNFS-ClientOnly,ClientForNFS-Infrastructure -All -NoRestart"
    ]
    
    enable_server_cmd = [
        "powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
        "Install-WindowsFeature -Name NFS-Client"
    ]
    
    try:
        result = subprocess.run(enable_client_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("[bold green]✔ Successfully enabled Windows NFS Client features.[/bold green]")
            return
        else:
            result_server = subprocess.run(enable_server_cmd, capture_output=True, text=True)
            if result_server.returncode == 0:
                print("[bold green]✔ Successfully installed Windows Server NFS-Client feature.[/bold green]")
                return
            else:
                print("[bold red]✖ Failed to enable Windows NFS Client features automatically.[/bold red]")
                if result.stderr:
                    print(f"[red]{result.stderr.strip()}[/red]")
                if result_server.stderr:
                    print(f"[red]{result_server.stderr.strip()}[/red]")
                print("[yellow]Please run the following command manually in an elevated/Administrator PowerShell:[/yellow]")
                print("  Enable-WindowsOptionalFeature -Online -FeatureName ServicesForNFS-ClientOnly,ClientForNFS-Infrastructure -All")
    except Exception as e:
        print(f"[bold red]ERROR: Could not execute enablement commands: {e}[/bold red]")
        print("[yellow]Please make sure you are running as Administrator and try running the command manually.[/yellow]")


@mount_app.command("start")
def mount_start(
    project_slug: Annotated[str, typer.Argument(help="Unique Accelerator project slug, bucket, or repository ID.")],
    mount_point: Annotated[Path, typer.Argument(help="Local directory path where the filesystem will be mounted. (Defaults: W: or first available drive on Windows, ~/accelerator/mnt/<project_slug> on Unix/Linux).")] = None,
    mode: Annotated[str, typer.Option("--mode", "-m", help="Mounting mode: 'bucket' (read-write, default) or 'repo' (read-only).")] = "bucket",
    fuse: Annotated[bool, typer.Option("--fuse", "-f", help="Use FUSE backend instead of the default NFS backend.")] = False,
    overlay: Annotated[bool, typer.Option("--overlay", "-o", help="Enable overlay mode (local writes only, remote read-only).")] = False,
    read_only: Annotated[bool, typer.Option("--read-only", "-r", help="Force read-only mount.")] = False,
    binary_version: Annotated[str, typer.Option("--binary-version", "-b", help="Specific hf-mount release version to download.")] = "v0.6.1-acc-p1-pr140",
):
    """
    Start a mount as a background daemon.
    """
    from . import mount_downloader
    import platform
    import re
    
    sys_name = platform.system()
    
    # Get credentials
    try:
        token = get_token()
        server_url = get_server_url()
    except Exception as e:
        print(f"[bold red]ERROR: Could not retrieve login details. Please run 'accli login' first. Details: {e}[/bold red]")
        raise typer.Exit(1)

    # Validate mode
    if mode == "repo":
        print("[bold red]ERROR: 'repo' mode is not implemented yet. Please use 'bucket' mode.[/bold red]")
        raise typer.Exit(1)
    elif mode != "bucket":
        print("[bold red]ERROR: Mode must be either 'bucket' or 'repo'.[/bold red]")
        raise typer.Exit(1)

    # Ensure binaries are downloaded and cached
    try:
        mount_downloader.ensure_binaries(version=binary_version, use_fuse=fuse)
    except Exception as e:
        print(f"[bold red]ERROR: Binary download/preparation failed: {e}[/bold red]")
        raise typer.Exit(1)

    # Standardize mount_point and handle defaults / auto-selection
    if sys_name == "Windows":
        if mount_point is None:
            # Auto-detect available drive letter starting with W
            try:
                drive = find_available_windows_drive("W")
                mount_point_abs = Path(drive)
                print(f"[cyan]No mount point specified. Selected available drive letter: [bold white]{drive}[/bold white][/cyan]")
            except Exception as e:
                print(f"[bold red]ERROR: {e}[/bold red]")
                raise typer.Exit(1)
        else:
            mount_point_str = str(mount_point)
            if re.match(r"^[a-zA-Z]:?\\?$", mount_point_str) or re.match(r"^[a-zA-Z]:/?$", mount_point_str):
                drive_letter = mount_point_str[0].upper()
                if os.path.exists(f"{drive_letter}:\\"):
                    print(f"[yellow]Warning: Drive '{drive_letter}:' is already in use.[/yellow]")
                    try:
                        fallback_drive = find_available_windows_drive("W")
                        print(f"[cyan]Falling back to first available drive: [bold white]{fallback_drive}[/bold white][/cyan]")
                        mount_point_abs = Path(fallback_drive)
                    except Exception as e:
                        print(f"[bold red]ERROR: Drive is in use and no alternative could be found: {e}[/bold red]")
                        raise typer.Exit(1)
                else:
                    mount_point_abs = Path(f"{drive_letter}:")
            else:
                mount_point_abs = mount_point.resolve()
    else:
        if mount_point is None:
            # Default mount point on Unix: ~/accelerator/mnt/<project_slug>
            mnt_dir = Path.home() / "accelerator" / "mnt" / project_slug
            mnt_dir.mkdir(parents=True, exist_ok=True)
            mount_point_abs = mnt_dir.resolve()
            print(f"[cyan]No mount point specified. Selected default directory: [bold white]{mount_point_abs}[/bold white][/cyan]")
        else:
            mount_point_abs = mount_point.resolve()

    # Construct environment
    env = os.environ.copy()
    env["ACCELERATOR_MOUNT"] = "1"
    env["ACC_ENDPOINT"] = server_url
    env["ACC_CAS_ENDPOINT"] = f"{server_url.rstrip('/')}/api/xet-cas"
    env["ACC_TOKEN"] = token

    if sys_name == "Windows":
        # Enable Windows NFS Client features if needed
        enable_windows_nfs_features()
        
        hf_mount_bin = mount_downloader.get_binary_path("hf-mount-nfs")
        if not hf_mount_bin.is_file():
            import shutil
            found_bin = shutil.which("hf-mount-nfs")
            if found_bin:
                hf_mount_bin = Path(found_bin)
            else:
                print("[bold red]ERROR: hf-mount-nfs binary not found in cache or system PATH.[/bold red]")
                raise typer.Exit(1)

        # Windows NFS Mount Setup
        db_path = os.path.normpath(get_db_path())
        args = [
            str(hf_mount_bin),
            "--token-file", db_path,
            "--hub-endpoint", server_url,
            mode,
            project_slug,
            str(mount_point_abs)
        ]
        if overlay:
            args.append("--overlay")
        if read_only:
            args.append("--read-only")

        print(f"[bold cyan]Starting Windows NFS mount for project '[white]{project_slug}[/white]' at '[white]{mount_point_abs}[/white]' in background...[/bold cyan]")
        
        # Prepare background logger
        log_file_path = Path.home() / ".accli" / "mount.log"
        # Ensure directory exists
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            log_file = open(log_file_path, "a", encoding="utf-8")
        except Exception:
            log_file = subprocess.DEVNULL

        import subprocess
        try:
            # Spawn decoupled background process on Windows using DETACHED_PROCESS creation flag (0x00000008)
            creationflags = 0x00000008
            process = subprocess.Popen(
                args,
                env=env,
                creationflags=creationflags,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                close_fds=True
            )
            
            # Startup verification (1-second boot poll)
            import time
            time.sleep(1.0)
            if process.poll() is not None:
                # Process has already terminated!
                print(f"[bold red]✖ Failed to start Windows NFS mount process (exit code {process.returncode}).[/bold red]")
                try:
                    if log_file_path.is_file():
                        with open(log_file_path, "r", encoding="utf-8") as lf:
                            lines = lf.readlines()
                            last_lines = "".join(lines[-10:])
                            print("[red]Recent log output:[/red]")
                            print(f"[dim]{last_lines.strip()}[/dim]")
                except Exception:
                    pass
                print(f"[yellow]Please check the log file at {log_file_path} for more details.[/yellow]")
                raise typer.Exit(1)
                
            print(f"[bold green]✔ NFS mount process spawned successfully (PID: {process.pid}).[/bold green]")
            print(f"[cyan]Use 'umount' or 'accli mount stop' to unmount the drive.[/cyan]")
        except Exception as e:
            if isinstance(e, typer.Exit):
                raise e
            print(f"[bold red]ERROR: Failed to spawn Windows mount process: {e}[/bold red]")
            raise typer.Exit(1)

    else:
        # Unix FUSE / NFS Daemonizer Setup
        hf_mount_bin = mount_downloader.get_binary_path("hf-mount")
        if not hf_mount_bin.is_file():
            import shutil
            found_bin = shutil.which("hf-mount")
            if found_bin:
                hf_mount_bin = Path(found_bin)
            else:
                print("[bold red]ERROR: hf-mount binary not found in cache or system PATH.[/bold red]")
                raise typer.Exit(1)

        args = [str(hf_mount_bin), "start"]
        if fuse:
            args.append("--fuse")
        
        db_path = get_db_path()
        args.extend(["--token-file", db_path])
        args.extend(["--hub-endpoint", server_url])
        args.extend([mode, project_slug, str(mount_point_abs)])
        
        if overlay:
            args.append("--overlay")
        if read_only:
            args.append("--read-only")

        print(f"[bold cyan]Starting mount for project '[white]{project_slug}[/white]' at '[white]{mount_point_abs}[/white]'...[/bold cyan]")
        import subprocess
        try:
            result = subprocess.run(args, env=env, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"[bold green]✔ Mount started successfully![/bold green]")
                if result.stdout:
                    print(result.stdout)
            else:
                print(f"[bold red]✖ Failed to start mount (exit code {result.returncode}):[/bold red]")
                if result.stderr:
                    print(f"[red]{result.stderr}[/red]")
                if result.stdout:
                    print(result.stdout)
                raise typer.Exit(result.returncode)
        except Exception as e:
            print(f"[bold red]ERROR: Failed to execute mount process: {e}[/bold red]")
            raise typer.Exit(1)


@mount_app.command("stop")
def mount_stop(
    mount_point: Annotated[Path, typer.Argument(help="Mount point of the daemon to stop.")] = None,
    binary_version: Annotated[str, typer.Option("--binary-version", "-b", help="Specific hf-mount release version to download.")] = "v0.6.1-acc-p1-pr140",
):
    """
    Stop a running daemon.
    """
    from . import mount_downloader
    import platform
    import re
    
    sys_name = platform.system()
    
    # Resolve mount point if None
    if mount_point is None:
        if sys_name == "Windows":
            mount_point_abs = Path("W:")
        else:
            try:
                project_slug = get_project_slug()
            except Exception:
                project_slug = None
            if not project_slug:
                print("[bold red]ERROR: Mount point not specified and active project slug could not be determined.[/bold red]")
                raise typer.Exit(1)
            mount_point_abs = Path.home() / "accelerator" / "mnt" / project_slug
            print(f"[cyan]No mount point specified. Selected default directory: [bold white]{mount_point_abs}[/bold white][/cyan]")
    else:
        mount_point_str = str(mount_point)
        if sys_name == "Windows" and re.match(r"^[a-zA-Z]:?\\?$", mount_point_str):
            mount_point_abs = Path(mount_point_str[0].upper() + ":")
        else:
            mount_point_abs = mount_point.resolve()

    if sys_name == "Windows":
        print(f"[bold cyan]Stopping mount at '[white]{mount_point_abs}[/white]' on Windows...[/bold cyan]")
        import subprocess
        
        # 1. Run Windows standard umount command
        res = subprocess.run(["umount", "-f", str(mount_point_abs)], capture_output=True, text=True)
        
        # 2. Selective Kill: Target and terminate only the specific process using the target mount point using a PowerShell command-line filter
        try:
            escaped_mount_point = str(mount_point_abs).replace("'", "''")
            kill_cmd = [
                "powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
                f"Get-CimInstance Win32_Process -Filter \"name='hf-mount-nfs.exe'\" | "
                f"Where-Object {{ $_.CommandLine -like '*{escaped_mount_point}*' }} | "
                f"ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}"
            ]
            subprocess.run(kill_cmd, capture_output=True)
        except Exception:
            # Fallback to killing all instances if selective filter fails
            subprocess.run(["taskkill", "/f", "/im", "hf-mount-nfs.exe"], capture_output=True)
        
        if res.returncode == 0 or "successfully" in res.stdout or "successfully" in res.stderr:
            print("[bold green]✔ Mount stopped successfully.[/bold green]")
        else:
            print("[yellow]Attempted to unmount. Please verify drive status manually using 'umount'.[/yellow]")
            if res.stderr:
                print(f"[red]{res.stderr.strip()}[/red]")
        return

    # Ensure binaries are downloaded and cached (Unix)
    try:
        mount_downloader.ensure_binaries(version=binary_version)
    except Exception as e:
        print(f"[bold red]ERROR: Binary download/preparation failed: {e}[/bold red]")
        raise typer.Exit(1)

    hf_mount_bin = mount_downloader.get_binary_path("hf-mount")
    if not hf_mount_bin.is_file():
        import shutil
        found_bin = shutil.which("hf-mount")
        if found_bin:
            hf_mount_bin = Path(found_bin)
        else:
            print("[bold red]ERROR: hf-mount binary not found in cache or system PATH.[/bold red]")
            raise typer.Exit(1)

    args = [str(hf_mount_bin), "stop", str(mount_point_abs)]

    print(f"[bold cyan]Stopping mount at '[white]{mount_point_abs}[/white]'...[/bold cyan]")
    
    import subprocess
    try:
        result = subprocess.run(args, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"[bold green]✔ Mount stopped successfully.[/bold green]")
        else:
            print(f"[bold red]✖ Failed to stop mount (exit code {result.returncode}):[/bold red]")
            if result.stderr:
                print(f"[red]{result.stderr}[/red]")
            raise typer.Exit(result.returncode)
    except Exception as e:
        print(f"[bold red]ERROR: Failed to execute stop process: {e}[/bold red]")
        raise typer.Exit(1)


@mount_app.command("status")
def mount_status(
    binary_version: Annotated[str, typer.Option("--binary-version", "-b", help="Specific hf-mount release version to download.")] = "v0.6.1-acc-p1-pr140",
):
    """
    List all running mount daemons.
    """
    from . import mount_downloader
    import platform
    
    sys_name = platform.system()

    if sys_name == "Windows":
        import subprocess
        print("[bold cyan]Active NFS mounts on Windows:[/bold cyan]")
        try:
            # Windows 'mount' command lists active NFS mounts
            result = subprocess.run(["mount"], capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                print(result.stdout)
            else:
                # Check tasklist for active daemon process
                task_res = subprocess.run(["tasklist", "/FI", "IMAGENAME eq hf-mount-nfs.exe"], capture_output=True, text=True)
                if "hf-mount-nfs.exe" in task_res.stdout:
                    print("[cyan]hf-mount-nfs.exe process is running in the background.[/cyan]")
                else:
                    print("[cyan]No running mounts found.[/cyan]")
        except Exception as e:
            print(f"[red]Could not retrieve mount status: {e}[/red]")
        return

    # Ensure binaries are downloaded and cached (Unix)
    try:
        mount_downloader.ensure_binaries(version=binary_version)
    except Exception as e:
        print(f"[bold red]ERROR: Binary download/preparation failed: {e}[/bold red]")
        raise typer.Exit(1)

    hf_mount_bin = mount_downloader.get_binary_path("hf-mount")
    if not hf_mount_bin.is_file():
        import shutil
        found_bin = shutil.which("hf-mount")
        if found_bin:
            hf_mount_bin = Path(found_bin)
        else:
            print("[bold red]ERROR: hf-mount binary not found in cache or system PATH.[/bold red]")
            raise typer.Exit(1)

    args = [str(hf_mount_bin), "status"]

    import subprocess
    try:
        result = subprocess.run(args, capture_output=True, text=True)
        if result.returncode == 0:
            stdout = result.stdout.strip()
            if not stdout:
                print("[cyan]No running mounts found.[/cyan]")
            else:
                print("[bold cyan]Active Mounts:[/bold cyan]")
                print(stdout)
        else:
            print(f"[bold red]✖ Failed to retrieve mount status (exit code {result.returncode}):[/bold red]")
            if result.stderr:
                print(f"[red]{result.stderr}[/red]")
            raise typer.Exit(result.returncode)
    except Exception as e:
        print(f"[bold red]ERROR: Failed to execute status process: {e}[/bold red]")
        raise typer.Exit(1)

