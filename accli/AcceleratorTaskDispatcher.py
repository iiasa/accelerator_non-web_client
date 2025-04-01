import git
import os
import zipfile
import hashlib
import shutil
import tempfile
import requests
from typing import Optional, List, Dict
from pydantic.v1 import BaseModel, root_validator
from accli.AcceleratorTerminalCliProjectService import AcceleratorTerminalCliProjectService
from accli.token import (
    get_github_app_token, 
    get_token, get_server_url, 
    get_project_slug
)

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
                arcname = os.path.relpath(file_path, start=folder_path)
                # Add file to zip with fixed timestamp
                info = zipfile.ZipInfo(arcname)
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


def copy_tree(src, dst):
    """Recursively copy from src to dst, excluding .git folders."""
    for item in os.listdir(src):
        src_path = os.path.join(src, item)
        dst_path = os.path.join(dst, item)
        
        if os.path.isdir(src_path):
            if item == '.git':
                continue
            os.makedirs(dst_path, exist_ok=True)
            copy_tree(src_path, dst_path)
        else:
            shutil.copy2(src_path, dst_path)

def push_folder_job(dir):

    access_token = get_token()

    server_url = get_server_url()

    project_slug = get_project_slug()

    term_cli_project_service = AcceleratorTerminalCliProjectService(
        user_token=access_token,
        server_url=server_url,
        verify_cert=False
    )

    repo_dir = tempfile.mkdtemp()

    copy_tree(dir, repo_dir)

    if os.path.isfile(f'{repo_dir}/wkube.py'):
        os.remove(f'{repo_dir}/wkube.py')

    temp_zip_path = f"{repo_dir}/temp.zip"

    compress_folder(repo_dir, temp_zip_path)
    
    sha256_hash = get_file_sha1(temp_zip_path)

    final_zip_path = f"{repo_dir}/{sha256_hash}.zip"

    
    os.rename(temp_zip_path, final_zip_path)

    presigned_push_url = term_cli_project_service.get_jobstore_push_url(
        project_slug, f"{sha256_hash}.zip"
    )


    if presigned_push_url:
        with  open(final_zip_path, 'rb') as f:
            res = requests.put(
                presigned_push_url,
                data=f,
                verify=False,
            )
            res.raise_for_status()
    
    
    shutil.rmtree(repo_dir)

    return f"s3accjobstore://{sha256_hash}.zip", sha256_hash


class JobDispatchModel(BaseModel):
    is_holder_job: bool = True

    name: str
    
    execute_cluster: str
    job_location: str
    job_args: List
    job_kwargs: Dict

    required_cores: Optional[float]
    required_ram: Optional[float]
    required_storage_local: Optional[float]  
    
    # is ignored if it is a callback and child of non free node jobs
    required_storage_workflow: Optional[float]

    job_secrets: Optional[dict] = {}     
    
    timeout: Optional[int]
    pvc_id: Optional[str]
    node_id: Optional[str]

    ignore_duplicate_job: bool = False
    free_node: bool = False             # Only applies to immediate children jobs

    
    
    children: List['JobDispatchModel'] = []
    callback: Optional['JobDispatchModel']


class WKubeTaskMeta(BaseModel):
    required_cores: float
    required_ram: float
    required_storage_local: float  
    
    # is ignored if it is a callback and child of non free node jobs
    required_storage_workflow: float     

    job_secrets: Optional[dict] = {}  
    
    timeout:int

class WKubeTaskKwargs(BaseModel):
    docker_image: Optional[str]

    job_folder: str = './'
    
    repo_url: Optional[str]                # required when docker image is not present
    repo_branch: Optional[str]             # required when docker image is not present
    
    docker_filename: Optional[str]         # when not docker image;
    base_stack: Optional[str]              # when not github docker file #TODO add enum class of available base stack
    
    force_build: bool = False

    command: str                           # may not be present with docker_image # TODO wrap a command in custom script to implement timeout or possibly log ingestion if required.  

    conf: Dict[str,str] = {}

    build_timeout: Optional[int]

    def dict(self, *args, **kwargs):
        result = super().dict(*args, **kwargs)
        if 'job_folder' in result:
            del result['job_folder']
        return result

    @root_validator(pre=True)
    def validate_root(cls, values):
        if not values.get('docker_image'):

            job_folder = values.get('job_folder', './')

            if not (values.get('repo_url') and values.get('repo_branch')):
                    remote_url, branch_name = push_folder_job(job_folder)
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
        return self.dispatch_model_task.dict()

class WKubeTask(GenericTask):
    def __init__(self, *t_args, **t_kwargs):

        wkube_task_kwargs = None
        wkube_task_meta = dict()

        if (t_args or t_kwargs):
            name = t_kwargs.pop('name')        
            WKubeTaskPydantic(*t_args, **t_kwargs)
            wkube_task_kwargs = WKubeTaskKwargs(*t_args, **t_kwargs)
            wkube_task_meta.update(WKubeTaskMeta(*t_args, **t_kwargs).dict(exclude_unset=True))
            

        self.dispatch_model_task = JobDispatchModel(
            name=name,
            is_holder_job=not (t_args or t_kwargs),
            execute_cluster='WKUBE',
            job_location='acc_native_jobs.dispatch_wkube_task',
            job_args=[],
            job_kwargs= wkube_task_kwargs.dict(exclude_unset=True) if wkube_task_kwargs else dict(),
            **wkube_task_meta
        )


    