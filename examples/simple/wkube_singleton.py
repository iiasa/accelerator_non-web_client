from accli import WKubeTask

routine = WKubeTask(
    name="test routine",
    job_folder='./',
    base_stack='python3.7',
    command="python main.py",
    required_cores=1,
    required_ram=1024 * 1024 * 512,
    required_storage_local=1024 * 1024 * 2,
    required_storage_workflow=1024 * 1024,
    timeout=60 * 60,
    conf={
        "input_mappings": "",
        "output_mappings": ""
    }
)
