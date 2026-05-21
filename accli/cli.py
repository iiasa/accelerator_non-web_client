import glob
import importlib.util
import os
import re
import warnings
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

    print(f"[italic]Get authorization code on following web url: [cyan]{webcli}/acli-auth-code[cyan][/italic] \n")

    device_authorization_code = typer.prompt("Enter the authorization code?")

    token_response = requests.post(
        f"{server}/api/v1/oauth/device/token/",
        json={"device_authorization_code": device_authorization_code},
        verify=(not ACCLI_DEBUG)
    )

    print("")

    if token_response.status_code == 400:
        print(f"[bold red]ERROR: {token_response.json().get('detail')}[/red]")
        raise typer.Exit(1)

    save_token_details(token_response.json(), server, webcli)

    print("[bold green]Successfully logged in.[/bold green]:rocket: :rocket:")


def upload_file(project_slug, accelerator_filename, local_filepath, progress, task, folder_name,
                max_workers=os.cpu_count()):
    access_token = get_token()

    server_url = get_server_url()

    term_cli_project_service = AcceleratorTerminalCliProjectService(
        user_token=access_token,
        server_url=server_url,
        verify_cert=(not ACCLI_DEBUG)
    )

    stat = term_cli_project_service.get_file_stat(project_slug, f"{folder_name}/{accelerator_filename}")

    if stat:
        progress.update(task, advance=stat.get('size'))
    else:

        with open(local_filepath, 'rb') as file_stream:
            term_cli_project_service.upload_filestream_to_accelerator(project_slug,
                                                                      f"{folder_name}/{accelerator_filename}",
                                                                      file_stream, progress, task,
                                                                      max_workers=max_workers)


@app.command()
def upload(
        project_slug: Annotated[str, typer.Argument(help="Unique Accelerator project slug.")],
        path: Annotated[str, typer.Argument(help="Folder path to upload to Accelerator project space.")],
        folder_name: Annotated[str, typer.Argument(help="Name of the folder to be made in Accelerator project space.")],
        max_workers: Annotated[
            int, typer.Option(..., '-w', help="Maximum worker pool for multipart upload.")] = os.cpu_count()
):
    # TODO make user free to put anywere except for reserved folder

    if not re.fullmatch(r'[a-zA-Z0-9\-_]+', folder_name):
        raise ValueError("Folder name is invalid.")

    with Progress(
            TextColumn("[progress.description]"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TextColumn("{task.description}"),
            transient=True
    ) as progress:

        if os.path.isdir(path):

            if not path.endswith("/"):
                path = path + "/"

            folder_size = get_size(path)
            print('Folder size', folder_size)
            upload_task = progress.add_task("[cyan]Uploading.", total=folder_size)

            for local_file_path in glob.iglob(f"{path}/**/*.*", recursive=True):
                accelerator_filename = os.path.relpath(local_file_path, path)

                if os.name == 'nt':
                    accelerator_filename = accelerator_filename.replace('\\', '/')

                progress.update(
                    upload_task,
                    description=f"[cyan]Uploading {local_file_path} \t"
                )

                if not os.path.isfile(local_file_path):
                    continue
                upload_file(project_slug, accelerator_filename, local_file_path, progress, upload_task, folder_name,
                            max_workers=max_workers)
        elif os.path.isfile(path):
            raise NotImplementedError('Only folder can be uploaded. File upload is not implemented.')
        else:
            print("ERROR: No such file or directory.")
            typer.Exit(1)


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
    access_token = get_token()
    server_url = get_server_url()

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


@app.command()
def copy(
        acc_src: Annotated[str, typer.Argument(help="Source path in Accelerator project space")],
        destination: Annotated[str, typer.Option(..., "-d", help="Destination directory")] = "./",
        token_pass: Annotated[str, typer.Option(..., "-t", help="Destination directory")] = "",
):
    access_token = get_token()
    server_url = get_server_url()

    term_cli_project_service = AcceleratorTerminalCliProjectService(
        user_token=access_token,
        server_url=server_url,
        verify_cert=(not ACCLI_DEBUG),
    )

    dest_path = Path(destination).expanduser().resolve()
    dest_path.mkdir(parents=True, exist_ok=True)

    filenames: List[str] = term_cli_project_service.enumerate_files_by_prefix(acc_src, token_pass=token_pass)

    with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            transient=True,
    ) as progress:

        for filename in filenames:
            local_file = dest_path / filename
            local_file.parent.mkdir(parents=True, exist_ok=True)

            final_file = local_file
            partial_file = local_file.with_suffix(local_file.suffix + ".part")

            if final_file.exists():
                typer.echo(f"Skipping {final_file} (already exists)")
                continue

            typer.echo(f"Downloading {filename} -> {final_file}")
            try:
                file_url = term_cli_project_service.get_file_url_from_repo(filename, token_pass=token_pass)
                with requests.get(file_url, stream=True, verify=(not ACCLI_DEBUG)) as r:
                    r.raise_for_status()
                    total = int(r.headers.get("Content-Length", 0))

                    task = progress.add_task(
                        f"[cyan]Downloading {filename}", total=total or None
                    )

                    with open(partial_file, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                progress.update(task, advance=len(chunk))

                partial_file.rename(final_file)
                typer.echo(f"✔ Downloaded {final_file}")

            except Exception as e:
                if partial_file.exists():
                    partial_file.unlink()
                typer.echo(f"✖ Failed to download {filename}: {e}", err=True)


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
    mount_point: Annotated[Path, typer.Argument(help="Local directory path where the filesystem will be mounted. (On Windows, drive letter like W:)")] = None,
    project_slug: Annotated[str, typer.Option(help="Unique Accelerator project slug (defaults to active project).")] = None,
    mode: Annotated[str, typer.Option(help="Mounting mode: 'bucket' (read-write, default) or 'repo' (read-only).")] = "bucket",
    fuse: Annotated[bool, typer.Option("--fuse", help="Use FUSE backend instead of the default NFS backend.")] = False,
    overlay: Annotated[bool, typer.Option("--overlay", help="Enable overlay mode (local writes only, remote read-only).")] = False,
    read_only: Annotated[bool, typer.Option("--read-only", help="Force read-only mount.")] = False,
    binary_version: Annotated[str, typer.Option("--binary-version", help="Specific hf-mount release version to download.")] = "v0.6.1-acc-pr140",
):
    """
    Start a mount as a background daemon.
    """
    from . import mount_downloader
    import platform
    import re
    
    sys_name = platform.system()
    
    # Resolve project_slug
    if not project_slug:
        try:
            project_slug = get_project_slug()
        except Exception:
            pass
            
    if not project_slug:
        print("[bold red]ERROR: Project slug is not set. Please provide --project-slug or set an active project.[/bold red]")
        raise typer.Exit(1)
        
    # Get credentials
    try:
        token = get_token()
        server_url = get_server_url()
    except Exception as e:
        print(f"[bold red]ERROR: Could not retrieve login details. Please run 'accli login' first. Details: {e}[/bold red]")
        raise typer.Exit(1)

    # Validate mode
    if mode not in ["bucket", "repo"]:
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
    binary_version: Annotated[str, typer.Option("--binary-version", help="Specific hf-mount release version to download.")] = "v0.6.1-acc-pr140",
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
    binary_version: Annotated[str, typer.Option("--binary-version", help="Specific hf-mount release version to download.")] = "v0.6.1-acc-pr140",
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

