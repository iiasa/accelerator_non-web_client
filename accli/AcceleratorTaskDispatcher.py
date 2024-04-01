from typing import Optional, List, Dict
from pydantic import BaseModel, root_validator


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
    
    timeout:int

class WKubeTaskKwargs(BaseModel):
    docker_image: Optional[str]
    
    repo_url: Optional[str]                # required when docker image is not present
    repo_branch: Optional[str]             # required when docker image is not present
    
    docker_filename: Optional[str]         # when not docker image;
    base_stack: Optional[str]              # when not github docker file #TODO add enum class of available base stack
    
    force_build: bool = False

    command: str                           # may not be present with docker_image # TODO wrap a command in custom script to implement timeout or possibly log ingestion if required.  
    
class WKubeTaskPydantic(WKubeTaskMeta, WKubeTaskKwargs):
    @root_validator(pre=True)
    def validate_root(cls, values):
        if not values.get('docker_image'):
            
            if not values.get('repo_url'): 
                raise ValueError("repo_url is required")
            
            if not values.get('repo_branch'):
                raise ValueError('repo_branch is required')
            
            if not values.get('docker_filename'):
                if not values.get("base_stack"):
                    raise ValueError("base_stack is required when dockerfile is not defined")
        return values


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


    