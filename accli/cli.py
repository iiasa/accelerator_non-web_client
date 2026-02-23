import glob
import importlib.util
import os
import re
import warnings
from contextlib import contextmanager
from pathlib import Path

import requests
import typer
from accli.AcceleratorTerminalCliProjectService import AcceleratorTerminalCliProjectService
from accli.CsvRegionalTimeseriesValidator import CsvRegionalTimeseriesValidator
from accli.token import save_token_details, get_token, get_server_url, set_project_slug
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

    if not re.fullmatch(r'[a-zA-Z0-9\-\_]+', folder_name):
        raise ValueError("Folder name is invalid.")

    with Progress(
            TextColumn("[progress.description]"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TextColumn("{task.description}"),
            transient=True
    ) as progress:

        if (os.path.isdir(path)):

            if not path.endswith("/"):
                path = path + ("/")

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
        elif (os.path.isfile(path)):
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

    filenames: list[str] = term_cli_project_service.enumerate_files_by_prefix(acc_src, token_pass=token_pass)

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
