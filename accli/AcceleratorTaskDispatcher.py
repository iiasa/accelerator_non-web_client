import git
import os
import hashlib
import shutil
import tempfile
from typing import Optional, List, Dict
from pydantic.v1 import BaseModel, root_validator

from accli.token import get_github_app_token

FOLDER_JOB_REPO_URL = 'https://github.com/IIASA-Accelerator/wkube-job.git'

def hash_folder(folder_path):
    md5_hash = hashlib.md5()
    sha256_hash = hashlib.sha256()

    for root, _, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            try:
                with open(file_path, 'rb') as f:
                    # Read and update MD5 and SHA256 hash object with file content
                    for chunk in iter(lambda: f.read(4096), b""):
                        md5_hash.update(chunk)
                        sha256_hash.update(chunk)
            except IOError:
                # Handle read error (if any)
                print(f"Error reading file: {file_path}")

    # Get hexadecimal digest of hashes
    md5_digest = md5_hash.hexdigest()
    # sha256_digest = sha256_hash.hexdigest()

    return md5_digest

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

    repo_dir = tempfile.mkdtemp()

    copy_tree(dir, repo_dir)
    
    branch_name = hash_folder(repo_dir)
    
    repo = git.Repo.init(repo_dir)

    git_token = get_github_app_token()

    remote_url = f'https://x-access-token:{git_token}@{FOLDER_JOB_REPO_URL.split("https://")[1]}'
    remote = repo.create_remote("accelerator_job_repo", remote_url)

    remote.pull('master')

    try:
        repo.git.checkout(branch_name)
    except git.exc.GitCommandError:
        repo.git.checkout('-b', branch_name)

    repo.git.add('.')

    repo.index.commit(branch_name)

    # TODO see what happens with the same checksum
    remote.push(branch_name)
    shutil.rmtree(repo_dir)

    return FOLDER_JOB_REPO_URL, branch_name


class JobDispatchModel(BaseModel):
    is_holder_job: bool = True  
    
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

    job_folder: Optional[str]
    
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

            job_folder = values.get('job_folder')

            if not job_folder: 
            
                if not values.get('repo_url'): 
                    # TODO if has no repo url just set it
                    raise ValueError("repo_url is required")
                
                if not values.get('repo_branch'):
                    raise ValueError('repo_branch is required')
            else: 
                remote_url, branch_name = push_folder_job(job_folder)
                values['repo_url'] = remote_url
                values['repo_branch'] = branch_name

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
            
            WKubeTaskPydantic(*t_args, **t_kwargs)
            wkube_task_kwargs = WKubeTaskKwargs(*t_args, **t_kwargs)
            wkube_task_meta.update(WKubeTaskMeta(*t_args, **t_kwargs).dict())
            

        self.dispatch_model_task = JobDispatchModel(
            is_holder_job=not (t_args or t_kwargs),
            execute_cluster='WKUBE',
            job_location='acc_native_jobs.dispatch_wkube_task',
            job_args=[],
            job_kwargs= wkube_task_kwargs.dict() if wkube_task_kwargs else dict(),
            **wkube_task_meta
        )


    