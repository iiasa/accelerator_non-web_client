from accli import WKubeTask

routine = WKubeTask(
    name="test routine",
    job_folder='./',
    base_stack='PYTHON3_7',
    command="python main.py",
    required_cores=1,
    required_ram=512 * 1024 ** 2,
    required_storage_local=2 * 1024 ** 3,
    required_storage_workflow=0,
    timeout=60 * 60,
    conf={
        "input_mappings": "",
        "output_mappings": ""
    }
)
