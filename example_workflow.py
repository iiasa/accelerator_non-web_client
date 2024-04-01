from accli import WKubeTask

root_job = WKubeTask()


callback_task = WKubeTask(
        repo_url='https://github.com/iiasa/wkube-task-example.git',
        repo_branch='master',
        docker_filename='Dockerfile',
        command="python -c \"import task; task.task(1,2,3, location='first callback')\"",
        required_cores=1,
        required_ram=1024*1024,
        required_storage_local=1024*1024,
        required_storage_workflow=1024*1024,
        timeout=60*60
    )

another_callback = WKubeTask(
        repo_url='https://github.com/iiasa/wkube-task-example.git',
        repo_branch='master',
        docker_filename='Dockerfile',
        command="python -c \"import task; task.task(1,2,3, location='another callback')\"",
        required_cores=1,
        required_ram=1024*1024,
        required_storage_local=1024*1024,
        required_storage_workflow=1024*1024,
        timeout=60*60
    )

callback_task.add_callback(
    another_callback
)

root_job.add_callback(
    callback_task
)

for item in ["parallel 1", "parallel 2", "parallel 3"]:

    child = WKubeTask(
            repo_url='https://github.com/iiasa/wkube-task-example.git',
            repo_branch='master',
            docker_filename='Dockerfile',
            command=f"python -c \"import task; task.task(1,2,3, location='{item}')\"",
            required_cores=1,
            required_ram=1024*1024,
            required_storage_local=1024*1024,
            required_storage_workflow=1024*1024,
            timeout=60*60
        )
    
    callb =  WKubeTask()

    child.add_callback(callb)
    
    for ii in ['cp1', 'cp2', 'cp3']:
        callb.add_child(
            WKubeTask(
                repo_url='https://github.com/iiasa/wkube-task-example.git',
                repo_branch='master',
                docker_filename='Dockerfile',
                command=f"python -c \"import task; task.task(1,2,3, location='{item}{ii}')\"",
                required_cores=1,
                required_ram=1024*1024,
                required_storage_local=1024*1024,
                required_storage_workflow=1024*1024,
                timeout=60*60
            )
        )

    root_job.add_child(
        child
    )

