from airflow.sensors.time_sensor import TimeSensor
from datetime import time
from datetime import timedelta
from airflow.models import BaseOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context
from airflow.operators.python import PythonOperator
from airflow.sdk import DAG, task, Param, get_current_context
from pathlib import Path
from async_calcjob_trigger import AsyncCalcJobTrigger


import os
AIRFLOW_HOME_ = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
if AIRFLOW_HOME_ is None:
    raise ImportError("Could not find AIRFLOW_HOME.")
AIRFLOW_HOME = Path(AIRFLOW_HOME_)

LOCAL_WORKDIR = AIRFLOW_HOME / "storage" / "local_workdir"
LOCAL_WORKDIR.mkdir(exist_ok=True, parents=True)

REMOTE_WORKDIR = AIRFLOW_HOME / "storage" / "remote_workdir"
REMOTE_WORKDIR.mkdir(exist_ok=True, parents=True)

######################
### CORE OPERATORS ###
######################


class CalcJobTaskOperator(BaseOperator):
    template_fields = ["machine", "local_workdir", "remote_workdir"]

    def __init__(self, machine: str, local_workdir: str, remote_workdir: str, submission_script: str,
                 to_upload_files=None, to_receive_files=None, **kwargs):
        super().__init__(**kwargs)
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir
        self.submission_script = submission_script
        self.to_upload_files = to_upload_files or {}
        self.to_receive_files = to_receive_files or {}

    def execute(self, context: Context):
        # Pull XCom values from the prepare task
        task_instance = context['task_instance']

        to_upload_files = task_instance.xcom_pull(task_ids='prepare', key='to_upload_files')
        to_receive_files = task_instance.xcom_pull(task_ids='prepare', key='to_receive_files')
        submission_script = task_instance.xcom_pull(task_ids='prepare', key='submission_script')

        self.defer(
            trigger=AsyncCalcJobTrigger(
                machine=self.machine,
                local_workdir=self.local_workdir,
                remote_workdir=self.remote_workdir,
                to_upload_files=to_upload_files,
                to_receive_files=to_receive_files,
                submission_script=submission_script
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Complete the deferred operation."""
        self.log.info(f"CalcJob completed with event: {event}")
        return event




#################
### CORE DAG ###
#################

def AiidDAG(**kwargs):
    kwargs['params'].update({
        # TODO move to nested transport params
        "machine": Param("localhost", type="string", section="Submission config"),
        "remote_workdir": Param(str(REMOTE_WORKDIR), type="string", section="Submission config"),
        "local_workdir": Param(str(LOCAL_WORKDIR), type="string", section="Submission config"),
        })
    return DAG(**kwargs)


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
    if 'tags' not in kwargs:
        kwargs['tags'] = []
    kwargs['tags'].append('aiida')
    return AiidDAG(**kwargs)

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

    ##########################################################################


### END USER ###

# Information in conf is
# workdir

# https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#loading-dags

#example_asset = Asset(uri="data.xyz", name="my_dataset")

if __name__ == "__main__":
    dag.test(
        run_conf={"machine": "localhost",
                  "local_workdir": str(LOCAL_WORKDIR),
                  "remote_workdir": str(REMOTE_WORKDIR),
                  }
    )
