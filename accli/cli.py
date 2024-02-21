import re
import click
import glob
import os
import typer
import requests
import warnings
from rich import print
from typing_extensions import Annotated
from tinydb import TinyDB, Query
from ._version import VERSION


from accli.AcceleratorTerminalCliProjectService import AcceleratorTerminalCliProjectService

from rich.progress import Progress, SpinnerColumn, TextColumn, ProgressColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn

warnings.filterwarnings('ignore')


app = typer.Typer(
    add_completion=False, 
    pretty_exceptions_show_locals=False, 
    no_args_is_help=True
)

def get_db_path():

    home = os.path.expanduser("~")
    token_directory = f"{home}/.accli"

    if not os.path.exists(token_directory):
        os.makedirs(token_directory)
    
    return f"{token_directory}/data.json"


def save_token_details(token, server_url, webcli_url):

    db_path = get_db_path()

    try:
        os.remove(db_path)
    except OSError:
        pass

    db = TinyDB(db_path)
    db.insert({
        'token': token,
        'server_url': server_url,
        'webcli_url': webcli_url
    })

def get_token():
    db_path = get_db_path()

    db = TinyDB(db_path)

    for item in db:
        token = item.get('token')
        if token:
            break

    if not token:
        print("Token does not exists. Please login.")
    return token


def get_server_url():
    db_path = get_db_path()

    db = TinyDB(db_path)

    for item in db:
        server_url = item.get('server_url')
        if server_url:
            break

    if not server_url:
        print("Server url does not exists. Please login.")
    return server_url


def get_size(path):
    size = 0

    for file in glob.iglob(f"{path}/**/*.*", recursive=True):
        
        size += os.path.getsize(file)

    return size


@app.command()
def about():
    print("[bold cyan]This is a terminal client for Accelerator hosted on https://accelerator.iiasa.ac.at . [/bold cyan]\n")
    print("[bold cyan]Please file feature requests and suggestions at https://github.com/iiasa/accli/issues .[/bold cyan]\n")
    print("[bold cyan]License: The MIT License (MIT)[/bold cyan]\n")
    print(f"[bold cyan]Version: {VERSION}[/bold cyan]\n")


@app.command()
def login(
    server: Annotated[str, typer.Option(help="Accelerator server url.")] = "https://accelerator-api.iiasa.ac.at", 
    webcli: Annotated[str, typer.Option(help="Accelerator web client for authorization.")] = "https://accelerator.iiasa.ac.at"
):
    print(
        f"[bold cyan]Welcome to Accelerator Terminal Client.[/bold cyan]\n"
        f"[bold cyan]Powered by IIASA[/bold cyan]\n"
    )

    print(f"[italic]Get authorization code on following web url: [cyan]{webcli}/acli-auth-code[cyan][/italic] \n")

    device_authorization_code = typer.prompt("Enter the authorization code?")

    token_response = requests.post(
        f"{server}/v1/oauth/device/token/", 
        json={"device_authorization_code": device_authorization_code}
    )

    print("")

    if token_response.status_code == 400:
        print(f"[bold red]ERROR: {token_response.json().get('detail')}[/red]")
        raise typer.Exit(1)

    save_token_details(token_response.json(), server, webcli)

    print("[bold green]Successfully logged in.[/bold green]:rocket: :rocket:")

def upload_file(project_slug, accelerator_filename, local_filepath, progress, task, folder_name, max_workers=os.cpu_count()):

    access_token = get_token()

    server_url = get_server_url()

    term_cli_project_service = AcceleratorTerminalCliProjectService(
        user_token=access_token,
        cli_base_url=f"{server_url}/v1/aterm-cli",
        verify_cert=False
    )

    stat = term_cli_project_service.get_file_stat(project_slug, f"{folder_name}/{accelerator_filename}")

    if stat:
        progress.update(task, advance=stat.get('size'))
    else:

        with open(local_filepath, 'rb') as file_stream:
            term_cli_project_service.upload_filestream_to_accelerator(project_slug, f"{folder_name}/{accelerator_filename}", file_stream, progress, task, max_workers=max_workers)

@app.command()
def upload(
    project_slug: Annotated[str, typer.Argument(help="Unique Accelerator project slug.")],
    path: Annotated[str, typer.Argument(help="Folder path to upload to Accelerator project space.")],
    folder_name: Annotated[str, typer.Argument(help="Name of the folder to be made in Accelerator project space.")],
    max_workers: Annotated[int, typer.Option(help="Maximum worker pool for multipart upload.")] = os.cpu_count()
):
    
    if not re.fullmatch(r'\w+', folder_name):
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
                upload_file(project_slug, accelerator_filename, local_file_path,  progress, upload_task, folder_name, max_workers=max_workers)
        elif (os.path.isfile(path)):
            raise NotImplementedError('Only folder can be uploaded. File upload is not implemented.')
        else:
            print("ERROR: No such file or directory.")
            typer.Exit(1)

