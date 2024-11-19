from accli import WKubeTask

task = WKubeTask(
    job_folder='./',
    docker_filename='Dockerfile',
    command="python /app/converter.py",
    required_cores=1,
    required_ram=1024*1024*1024*8,
    required_storage_local=1024*1024*1024*15,
    required_storage_workflow=1024*1024*1024*30,
    timeout=60*60,
    conf={
        "INPUT_FILE": "forest-navigator/Forest4model/Forest4model_v1_Forest_cover.nc",
    }
)