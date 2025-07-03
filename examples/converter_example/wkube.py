from accli import WKubeTask


my_params = [1,2,3]

root = WKubeTask(name="Exp 1")
for i in my_params:
    root.add_child(
        WKubeTask(
            name=f"Bro{i}",
            job_folder='./',
            docker_filename='Dockerfile',
            command=f"python /app/converter.py___{i}",
            required_cores=1,
            required_ram=1024*1024*1024*8,
            required_storage_local=1024*1024*1024*15,
            required_storage_workflow=1024*1024*1024*30,
            timeout=60*60,
            conf={
                "INPUT_FILE": "forest-navigator/Forest4model/Forest4model_v1_Forest_cover.nc",
            }
       )
    )