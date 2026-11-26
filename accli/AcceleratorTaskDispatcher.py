import os
import zipfile
import hashlib
import shutil
import tempfile
import concurrent.futures
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

import requests
from functools import lru_cache
from pydantic import BaseModel, model_validator, Field
from accli.AcceleratorTerminalCliProjectService import AcceleratorTerminalCliProjectService
from accli.file_filter import confirm_version_control, iter_selected_files
from accli.token import (
    get_token, get_server_url,
    get_project_slug, exchange_refresh_token
)

ACCLI_DEBUG = os.environ.get('ACCLI_DEBUG', False)

FOLDER_JOB_REPO_URL = 'https://github.com/IIASA-Accelerator/wkube-job.git'


def compress_folder(folder_path, output_path):
    # Fixed timestamp for normalization
    fixed_time = (1980, 1, 1, 0, 0, 0)

    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            dirs.sort()
            files.sort()
            for file in files:
                file_path = os.path.join(root, file)
                arc_name = os.path.relpath(file_path, start=folder_path)
                # Add a file to zip with a fixed timestamp
                info = zipfile.ZipInfo(arc_name)
                info.date_time = fixed_time
                info.external_attr = 0o600 << 16  # Set file permissions
                with open(file_path, 'rb') as f:
                    zipf.writestr(info, f.read(), zipfile.ZIP_DEFLATED)


def get_file_sha1(file_path):
    sha1_hash = hashlib.sha1()
    with open(file_path, "rb") as f:
        chunk = f.read(8192)
        while chunk:
            sha1_hash.update(chunk)
            chunk = f.read(8192)
    return sha1_hash.hexdigest()


def copy_tree(src, dst, include=None, exclude=None):
    """Copy the files of src that include/exclude keep into dst.

    include/exclude work as described in accli/file_filter.py (the same rules as
    `accli copy --include/--exclude`).
    """
    for src_path, rel_path in iter_selected_files(src, include, exclude):
        dst_path = os.path.join(dst, *rel_path.split("/"))
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        shutil.copy2(src_path, dst_path)


def _upload_cas_file_worker(args):
    rel_path, file_path, term_cli_project_service, project_slug = args
    file_hash = get_file_sha1(file_path)
    blob_name = f"cas_{file_hash}.blob"
    
    print(f"Checking {rel_path} (hash: {file_hash[:8]})...")
    presigned_push_url = term_cli_project_service.get_jobstore_push_url(
        project_slug, blob_name
    )
    if presigned_push_url:
        print(f"Uploading {rel_path}...")
        with open(file_path, 'rb') as f:
            data = f.read()
            res = requests.put(
                presigned_push_url,
                data=data,
                headers={"Content-Length": str(len(data))},
                verify=False,
            )
            res.raise_for_status()
        print(f"Uploaded {rel_path}.")
    else:
        print(f"Skipped {rel_path} (already in jobstore).")
    
    mode = os.stat(file_path).st_mode
    return {"path": rel_path, "hash": file_hash, "mode": mode}


MAX_JOB_FOLDER_BYTES = 1024 * 1024 * 1024  # 1 GB

# Set by `accli dispatch -y`: upload version-control folders in the job folder without asking.
UPLOAD_VERSION_CONTROL = False

def _format_bytes(num_bytes):
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024 or unit == "GB":
            return f"{num_bytes:.1f} {unit}" if unit != "B" else f"{num_bytes} B"
        num_bytes /= 1024


def job_code_path():
    """A new project-space path for job code: jobstore/<date as YYYYMMDD>/<uuid>."""
    return f"jobstore/{datetime.now().strftime('%Y%m%d')}/{uuid.uuid4()}"


def check_job_folder_size(repo_dir, directory, project_slug):
    """Stop the dispatch if the filtered job folder is larger than MAX_JOB_FOLDER_BYTES."""
    total = 0
    for root, _, files in os.walk(repo_dir):
        for file in files:
            total += os.path.getsize(os.path.join(root, file))

    if total <= MAX_JOB_FOLDER_BYTES:
        return

    # A ready-to-use path, so the commands below can be copied as they are.
    code_path = job_code_path()

    raise ValueError(
        f"Job folder too large: '{directory}' is {_format_bytes(total)} after the include/exclude "
        f"in wkube.py; the limit is {_format_bytes(MAX_JOB_FOLDER_BYTES)}.\n"
        f"\n"
        f"'accli dispatch' uploads the job folder from wkube.py (job_folder, include, exclude) to build "
        f"the job's container image. Keep that upload to the files the build needs, upload the job's "
        f"code and data to the project space with 'accli copy', and run the job from there.\n"
        f"\n"
        f"Steps:\n"
        f"  1. Upload the job folder to the project space:\n"
        f"       accli copy \"{directory}\" acc://{project_slug}/{code_path} --exclude \"<patterns>\"\n"
        f"     Use --exclude (and --include) to leave out what the job does not need while it runs, such "
        f"as version-control folders (.git, .svn), environments and installed packages, caches, old "
        f"outputs, and temporary or test data. Look at the job's command in wkube.py and the code it "
        f"runs to work out what it needs.\n"
        f"     To see the largest files and folders, run:\n"
        f"       accli size \"{directory}\"\n"
        f"     Run it again with the --exclude/--include you plan to use to check what the upload will contain.\n"
        f"  2. In wkube.py, set include/exclude so the job folder upload contains only the files the "
        f"image build needs, for example:\n"
        f"       exclude=\"*\",\n"
        f"       include=\"<build files>\",\n"
        f"     To check it, run accli size with the same patterns: "
        f"accli size \"{directory}\" --exclude \"*\" --include \"<build files>\"\n"
        f"  3. In wkube.py, run the job from the uploaded copy:\n"
        f"       command=\"cd /mnt/wdrv/{code_path} && <your current command>\"\n"
        f"     acc://{project_slug}/{code_path} is available in the job at /mnt/wdrv/{code_path}.\n"
        f"  4. Run 'accli dispatch' again.\n"
        f"\n"
        f"include/exclude work the same in wkube.py and accli copy: comma-separated wildcard patterns, "
        f"matched against file and folder names and their paths inside the folder. A matching folder "
        f"covers everything in it. include wins over exclude. With only include, nothing else is "
        f"uploaded."
    )


@lru_cache(maxsize=None)
def push_folder_job(directory, include=None, exclude=None):
    server_url = get_server_url()
    project_slug = get_project_slug()
    _, access_token, _ = exchange_refresh_token(project_slug)

    term_cli_project_service = AcceleratorTerminalCliProjectService(
        user_token=access_token,
        server_url=server_url,
        verify_cert=(not ACCLI_DEBUG)
    )

    repo_dir = tempfile.mkdtemp()
    copy_tree(directory, repo_dir, include, exclude)

    if os.path.isfile(f'{repo_dir}/wkube.py'):
        os.remove(f'{repo_dir}/wkube.py')

    try:
        uploaded = [(rel, os.path.getsize(path)) for path, rel in iter_selected_files(repo_dir)]
        confirm_version_control(
            uploaded, UPLOAD_VERSION_CONTROL,
            'add them to exclude in wkube.py, for example exclude=".git"', _format_bytes,
        )
        check_job_folder_size(repo_dir, directory, project_slug)
    except ValueError:
        shutil.rmtree(repo_dir, ignore_errors=True)
        raise

    manifest = {"type": "cas_manifest", "files": []}
    
    upload_args = []
    for root, dirs, files in os.walk(repo_dir):
        for file in files:
            file_path = os.path.join(root, file)
            rel_path = Path(file_path).relative_to(repo_dir).as_posix()
            upload_args.append((rel_path, file_path, term_cli_project_service, project_slug))
            
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        for result in executor.map(_upload_cas_file_worker, upload_args):
            manifest["files"].append(result)
            
    manifest_path = os.path.join(repo_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)
        
    manifest_hash = get_file_sha1(manifest_path)
    manifest_name = f"manifest_{manifest_hash}.json"
    
    presigned_push_url = term_cli_project_service.get_jobstore_push_url(
        project_slug, manifest_name
    )
    
    if presigned_push_url:
        with open(manifest_path, 'rb') as f:
            res = requests.put(
                presigned_push_url,
                data=f,
                verify=False,
            )
            res.raise_for_status()

    shutil.rmtree(repo_dir)
    return f"s3accjobstore://{manifest_name}", manifest_hash


JOB_SECRETS_DESCRIPTION = (
    "Env var name to secret key, e.g. {'GIT_PAT': 'GIT_PAT'}. Keys name project or personal secrets "
    "stored under Project > Secrets in the web app; never put a secret value here, the gateway rejects it."
)
JOB_SECRET_FILES_DESCRIPTION = (
    "File name to secret key. The value is mounted at /var/run/secrets/wkube/<file name> instead of "
    "an env var, which keeps it out of env dumps and child processes."
)


class JobDispatchModel(BaseModel):
    is_holder_job: bool = True
    build_only_task: bool = False

    name: str

    execute_cluster: str
    job_location: str
    job_args: list
    job_kwargs: dict

    required_cores: Optional[float] = None
    required_ram: Optional[float] = None
    required_storage_local: Optional[float] = None

    # is ignored if it is a callback and child of non-free node jobs
    required_storage_workflow: Optional[float] = None

    job_secrets: Optional[Dict[str, str]] = Field({}, description=JOB_SECRETS_DESCRIPTION)
    job_secret_files: Optional[Dict[str, str]] = Field({}, description=JOB_SECRET_FILES_DESCRIPTION)

    timeout: Optional[int] = None
    pvc_id: Optional[str] = None
    node_id: Optional[str] = None

    ignore_duplicate_job: bool = False
    free_node: bool = True

    children: List['JobDispatchModel'] = []
    callback: Optional['JobDispatchModel'] = None


class WKubeTaskMeta(BaseModel):
    required_cores: float
    required_ram: float
    required_storage_local: Optional[float] = None

    # is ignored if it is a callback and child of non-free node jobs
    required_storage_workflow: Optional[float] = None

    job_secrets: Optional[Dict[str, str]] = Field({}, description=JOB_SECRETS_DESCRIPTION)
    job_secret_files: Optional[Dict[str, str]] = Field({}, description=JOB_SECRET_FILES_DESCRIPTION)

    timeout: int


class WKubeTaskKwargs(BaseModel):
    docker_image: Optional[str] = None

    job_folder: str = Field('./', description="Path to the directory containing job files. Must be a valid existing directory.")
    include: Optional[str] = Field(None, description="Comma-separated wildcard patterns to include (e.g. '*.py, data/*')")
    exclude: Optional[str] = Field(None, description="Comma-separated wildcard patterns to exclude (e.g. '*.txt, .venv/*')")

    repo_url: Optional[str] = None  # required when docker image is not present
    repo_branch: Optional[str] = None  # required when docker image is not present

    docker_filename: Optional[str] = None  # when not docker image;
    base_stack: Optional[str] = None  # when not github docker file #TODO add enum class of available base stack

    force_build: bool = False

    command: str  # may not be present with docker_image # TODO wrap a command in custom script to implement timeout or possibly log ingestion if required.

    conf: Dict[str, str] = {}

    build_timeout: Optional[int] = None

    def model_dump(self, *args, **kwargs):
        result = super().model_dump(*args, **kwargs)
        if 'job_folder' in result:
            del result['job_folder']
        if 'include' in result:
            del result['include']
        if 'exclude' in result:
            del result['exclude']
        return result

    @model_validator(mode="before")
    @classmethod
    def validate_root(cls, values):
        if not values.get('docker_image'):
            job_folder = values.get('job_folder', './')
            include = values.get('include')
            exclude = values.get('exclude')

            if not os.path.isdir(job_folder):
                raise ValueError(
                    f"Invalid job_folder: '{job_folder}' does not exist or is not a directory. "
                    f"Help: job_folder must be a valid path. You can also use 'include' and 'exclude' "
                    f"with comma-separated wildcards (e.g., include='*.py, data/*', exclude='*.txt, .venv/*')."
                )

            if not (values.get('repo_url') and values.get('repo_branch')):
                remote_url, branch_name = push_folder_job(
                    os.path.abspath(job_folder), include, exclude
                )
                values['repo_url'] = remote_url
                values['repo_branch'] = branch_name
            else:
                if not values.get('repo_url'):
                    # TODO if has no repo url just set it
                    raise ValueError("repo_url is required")

                if not values.get('repo_branch'):
                    raise ValueError('repo_branch is required')

            if not values.get('docker_filename'):
                if not values.get("base_stack"):
                    raise ValueError("base_stack is required when dockerfile is not defined")
        return values


class WKubeTaskPydantic(WKubeTaskMeta, WKubeTaskKwargs):
    pass


class GenericTask:
    def __init__(self, *args, **kwargs):
        self.dispatch_model_task: JobDispatchModel

    def add_child(self, task):
        if self.__class__ != task.__class__:
            raise ValueError(f"task should of {self.__class__} class")
        self.dispatch_model_task.children.append(task.dispatch_model_task)

    def add_callback(self, task):
        if self.__class__ != task.__class__:
            raise ValueError(f"task should of {self.__class__} class")
        self.dispatch_model_task.callback = task.dispatch_model_task

    @property
    def description(self):
        return self.dispatch_model_task.model_dump()


class WKubeTask(GenericTask):
    def __init__(self, *t_args, **t_kwargs):
        super().__init__(*t_args, **t_kwargs)
        wkube_task_kwargs = None
        wkube_task_meta = {}

        name = t_kwargs.pop('name') if 'name' in t_kwargs else None
        if t_args or t_kwargs:
            WKubeTaskPydantic(*t_args, **t_kwargs)
            wkube_task_kwargs = WKubeTaskKwargs(*t_args, **t_kwargs)
            wkube_task_meta.update(WKubeTaskMeta(*t_args, **t_kwargs).model_dump(exclude_unset=True))

        self.dispatch_model_task = JobDispatchModel(
            name=name,
            is_holder_job=not (t_args or t_kwargs),
            execute_cluster='WKUBE',
            job_location='acc_native_jobs.dispatch_wkube_task',
            job_args=[],
            job_kwargs=wkube_task_kwargs.model_dump(exclude_unset=True) if wkube_task_kwargs else {},
            **wkube_task_meta
        )
