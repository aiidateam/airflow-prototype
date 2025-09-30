from airflow_provider_aiida.dags import AiidaDAG


####################
### WORKFLOW DEV ###
####################

def AddDAG(**kwargs):
    if 'params' not in kwargs:
        kwargs['params'] = {}
    kwargs['params'].update({
        "x": Param("5", type="string"), # TODO to int
        "y": Param("2", type="string"),
        "sleep": Param("20", type="string"),
        })
    return AiidaDAG(**kwargs)

with AddDAG(
    dag_id=Path(__file__).stem) as dag:

    @task
    def prepare(x: int, y: int, sleep: int) -> dict:
        # TODO add to database
        to_upload_files = {}
        submission_script = f"""
sleep {sleep}
echo "$(({x}+{y}))" > file.out
        """
        to_receive_files = {"file.out": "result.txt"}
        return {"to_upload_files": to_upload_files,
                "submission_script": submission_script,
                "to_receive_files": to_receive_files}

    @task
    def parse(local_workdir: str, received_files: dict[str, str]):
        for received_file in received_files.values():
            print(f"Final result: {(Path(local_workdir) / Path(received_file)).read_text()}")

    ##########################################################################
    ### THE CODE BELOW SHOULD BE AUTOMATICALLY CONNECTED TO THE CODE ABOVE ###

    # NOTE: no argument means all parms are passed
    prepare_op = prepare(x="{{ params.x }}", y="{{ params.y }}", sleep="{{ params.sleep }}")
    to_upload_files, submission_script, to_receive_files = prepare_op["to_upload_files"], prepare_op["submission_script"], prepare_op["to_receive_files"]

    calcjob_op = CalcJobTaskOperator(task_id="calcjob_task",
                   machine="{{ params.machine }}",
                   local_workdir="{{ params.local_workdir }}",
                   remote_workdir="{{ params.remote_workdir }}",
                   to_upload_files=to_upload_files,
                   to_receive_files=to_receive_files,
                   submission_script=submission_script,
                )


    parse_op = parse(local_workdir="{{ params.local_workdir }}", received_files=to_receive_files)

    prepare_op >> calcjob_op >> parse_op
