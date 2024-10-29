from accli import WKubeTask

task = WKubeTask(
    job_folder='./examples/ca_job_example',
    docker_filename='Dockerfile',
    command="python /app/job.py",
    required_cores=1,
    required_ram=1024*1024*512,
    required_storage_local=1024*1024,
    required_storage_workflow=1024*1024,
    timeout=60*60,
    conf={
        "INPUT_FILE": "brightspace/SpatialX/10gb.nc",
    }
)


# python -m accli dispatch brightspace example_job_folder/example_singleton_workflow.py task --server=http://web_be:8000