from accli import WKubeTask

task = WKubeTask(
    repo_url='https://github.com/iiasa/wkube-task-example.git',
    repo_branch='master',
    docker_filename='Dockerfile',
    command="python -c \"import task; task.task(1,2,3, location='singleton')\"",
    required_cores=1,
    required_ram=1024*1024,
    required_storage_local=1024*1024,
    required_storage_workflow=1024*1024,
    timeout=60*60
)