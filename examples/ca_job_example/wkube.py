from accli import WKubeTask

task = WKubeTask(
    name="Experiment 1",
    job_folder='./',
    docker_filename='Dockerfile',
    command="python /app/job.py",
    required_cores=1,
    required_ram=1024*1024*512,
    required_storage_local=1024*1024*2,
    required_storage_workflow=1024*1024,
    timeout=60*60,
    conf={
        "input_mapping": "acc://forest-navigator/xyz:/app/xyz",
        "output_mapping": "/app/output/output_w_bi.csv:acc://outputs"
    }
)

# python -m accli login -c https://localhost:8080 -s http://web_be:8000
# python -m accli dispatch bightspace task -s=http://web_be:8000