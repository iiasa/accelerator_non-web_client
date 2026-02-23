from accli import WKubeTask

root = WKubeTask(name='root task')

root_task = WKubeTask(name="Rupesh Test")

job_arguments = [
    '1',
    '2',
    '3'
]

for args in job_arguments:
    task = WKubeTask(
        name=f"Test {args}",
        job_folder='./',
        base_stack='PYTHON3_7',
        command=f"python main.py",

        required_cores=0.5,
        required_ram=1024 * 1024 * 1024,
        required_storage_local=1024 * 1024 * 1024,  # MB
        required_storage_workflow=1024 * 1024,  # MB
        timeout=60 * 60 * 24 * 7,
        conf={}
    )

    root_task.add_child(task)

callback = WKubeTask(
    name=f"Test Callback",
    job_folder='./',
    base_stack='PYTHON3_7',
    command=f"python main.py",

    required_cores=0.5,
    required_ram=1024 * 1024 * 1024,
    required_storage_local=1024 * 1024 * 1024,  # MB
    required_storage_workflow=1024 * 1024,  # MB
    timeout=60 * 60 * 24 * 7,
    conf={}
)

root_task.add_callback(callback)
