from accli import WKubeTask

task = WKubeTask(
    job_folder='./',
    docker_filename='Dockerfile',
    command="python /app/job.py",
    required_cores=1,
    required_ram=1024*1024*512,
    required_storage_local=1024*1024*2,
    required_storage_workflow=1024*1024,
    timeout=60*60,
    conf={
        "INPUT_FILE": "forest-navigator/TempDemo/SpatialData/ScenarioA/VariableX/2010.tif",
    }
)

# python -m accli login --webcli=https://localhost:8080 --server=http://web_be:8000
# python -m accli dispatch bightspace examples/ca_job_example/workflow.py task --server=http://web_be:8000