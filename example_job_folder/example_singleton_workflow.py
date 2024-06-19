from accli import WKubeTask

task = WKubeTask(
    job_folder='./example_job_folder',
    docker_filename='Dockerfile',
    command="python -c \"import task; task.task(1,2,3, location='singleton')\"",
    required_cores=1,
    required_ram=1024*1024*512,
    required_storage_local=1024*1024,
    required_storage_workflow=1024*1024,
    timeout=60*60
)


# python -m accli dispatch forest-navigator example_singleton_workflow.py task --server=http://web_be:8000