from accli import WKubeTask

task = WKubeTask(
    job_folder='./example_job_folder',
    docker_filename='Dockerfile',
    command="python remote_converter.py",
    required_cores=1,
    required_ram=1024*1024*512,
    required_storage_local=1024*1024,
    required_storage_workflow=1024*1024,
    timeout=60*60,
    conf={
        "INPUT_FILE": "brightspace/SpatialX/10gb.nc",
        "VALIDATION_TEMPLATE": "nc-nu"
    }
)


# python -m accli dispatch brightspace example_job_folder/example_singleton_workflow.py task --server=http://web_be:8000