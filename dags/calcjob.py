from airflow.sensors.time_sensor import TimeSensor
from datetime import time
from datetime import timedelta
from airflow.models import BaseOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context
from airflow.operators.python import PythonOperator
from airflow.sdk import DAG, task, Param, get_current_context
from airflow.utils.task_group import TaskGroup
from pathlib import Path
from typing import Tuple

import sys
sys.path.append("/Users/alexgo/code/airflow/dags")
from transport.ssh import AsyncSshTransport

######################
### CORE OPERATORS ###
######################

class UploadOperator(BaseOperator):
    template_fields = ["machine", "local_workdir", "remote_workdir", "to_upload_files"]

    # TODO remote kwargs or use for something useful
    def __init__(self, machine: str, local_workdir: str, remote_workdir: str, to_upload_files: dict[str, str], **kwargs):
        super().__init__(**kwargs)
        self.machine = machine
        self.remote_workdir = remote_workdir
        self.local_workdir = local_workdir
        self.to_upload_files = to_upload_files

    def execute(self, context: Context):
        # Pull to_upload_files from XCom if it's empty
        to_upload_files = self.to_upload_files
        if not to_upload_files:
            to_upload_files = context['task_instance'].xcom_pull(task_ids='prepare', key='to_upload_files')

        remote_workdir = Path(self.remote_workdir)
        transport = AsyncSshTransport(machine=self.machine)
        with transport.open() as connection:
            for localpath, remotepath in to_upload_files.items():
                connection.putfile(Path(localpath).absolute(), remote_workdir / Path(remotepath))


class SubmitOperator(BaseOperator):
    template_fields = ["machine", "local_workdir", "remote_workdir"]

    def __init__(self, machine: str, local_workdir: str, remote_workdir: str, submission_script: str, **kwargs):
        super().__init__(**kwargs)
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir
        self.submission_script = submission_script

    def execute(self, context: Context):
        # Pull submission_script from XCom if it's empty
        submission_script = self.submission_script
        if not submission_script:
            submission_script = context['task_instance'].xcom_pull(task_ids='prepare', key='submission_script')

        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)
        transport = AsyncSshTransport(machine=self.machine)
        submission_script_path = local_workdir / Path("submit.sh")
        submission_script_path.write_text(submission_script)
        with transport.open() as connection:
            connection.putfile(submission_script_path, remote_workdir / "submit.sh" )
            exit_code, stdout, stderr = connection.exec_command_wait(f"(bash {submission_script_path} > /dev/null 2>&1 & echo $!) &", workdir=remote_workdir)
        if exit_code != 0:
            raise ValueError(f"Submission did not work, {stderr}")
        job_id = int(stdout.strip())
        self.log.info(f"Output of submission of process: {job_id}") #parse out correction
        return job_id

class UpdateOperator(BaseOperator):
    template_fields = ["machine", "sleep"]

    def __init__(self, machine: str, job_id: int, sleep: int = 2, **kwargs):
        super().__init__(**kwargs)
        self.machine = machine
        self.job_id = job_id
        self.sleep = sleep

    def execute(self, context: Context):
        # Do one iteration of work/check
        if self.check_submission_alive(context):
            self.log.info("Condition met; finishing task.")
            return
        # Not done yet: go to sleep without blocking a worker
        self.log.info("Not done; deferring for %s...", self.sleep)
        raise self.defer(
            trigger=TimeDeltaTrigger(timedelta(seconds=self.sleep)),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event=None):
        # Wakes up here after the trigger fires -> run another iteration
        return self.execute(context)

    def check_submission_alive(self, context) -> bool:
        transport = AsyncSshTransport(machine=self.machine)
        job_id = self.job_id.resolve(context)
        with transport.open() as connection:
            # -0 does not kill the process, only verifies it
            retval, stdout_bytes, stderr_bytes = connection.exec_command_wait(f"kill -0 {job_id}")
        self.log.info(f"retval={retval}")
        return bool(retval)
        # TODO check why it is not alive


class ReceiveOperator(BaseOperator):
    template_fields = ["machine", "local_workdir", "remote_workdir", "to_receive_files"]

    def __init__(self, machine: str, local_workdir: str, remote_workdir: str, to_receive_files: dict[str, str], **kwargs):
        super().__init__(**kwargs)
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir
        self.to_receive_files = to_receive_files

    def execute(self, context: Context):
        # Pull to_receive_files from XCom if it's empty
        to_receive_files = self.to_receive_files
        if not to_receive_files:
            to_receive_files = context['task_instance'].xcom_pull(task_ids='prepare', key='to_receive_files')

        transport = AsyncSshTransport(machine=self.machine)
        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)
        with transport.open() as connection:
            for remotepath, localpath in to_receive_files.items():
                connection.getfile(remote_workdir / Path(remotepath), local_workdir / Path(localpath))



##########################
### REUSABLE TASK GROUP ###
##########################

def create_calcjob_taskgroup(
    group_id: str,
    machine: str,
    local_workdir: str,
    remote_workdir: str,
) -> TaskGroup:
    """
    Creates a reusable task group for the calcjob workflow:
    upload → submit → update → receive

    Each operator pulls its required values from XCom internally.
    """
    with TaskGroup(group_id=group_id) as calcjob_group:

        # Create operators that pull their own config from XCom
        upload_op = UploadOperator(
            task_id="upload",
            machine=machine,
            local_workdir=local_workdir,
            remote_workdir=remote_workdir,
            to_upload_files={}  # Will be pulled from XCom
        )

        submit_op = SubmitOperator(
            task_id="submit",
            machine=machine,
            local_workdir=local_workdir,
            remote_workdir=remote_workdir,
            submission_script=""  # Will be pulled from XCom
        )

        update_op = UpdateOperator(
            task_id="update",
            machine=machine,
            job_id=submit_op.output
        )

        receive_op = ReceiveOperator(
            task_id="receive",
            machine=machine,
            local_workdir=local_workdir,
            remote_workdir=remote_workdir,
            to_receive_files={}  # Will be pulled from XCom
        )

        # Set dependencies within the task group
        upload_op >> submit_op >> update_op >> receive_op

    return calcjob_group


#################
### CORE DAG ###
#################

def AiidDAG(**kwargs):
    kwargs['params'].update({
        # TODO move to nested transport params
        "machine": Param("localhost", type="string", section="Submission config"),
        "remote_workdir": Param("/Users/alexgo/code/airflow/remote_workdir", type="string", section="Submission config"),
        "local_workdir": Param("/Users/alexgo/code/airflow/local_workdir", type="string", section="Submission config"),
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
    def parse(local_workdir: str):
        """Parse results - pulls received_files from XCom instead of parameters"""
        from airflow.sdk import get_current_context
        context = get_current_context()
        task_instance = context['task_instance']

        received_files = task_instance.xcom_pull(task_ids='prepare', key='to_receive_files')
        for received_file in received_files.values():
            print(f"Final result: {(Path(local_workdir) / Path(received_file)).read_text()}")

    ##########################################################################
    ### THE CODE BELOW SHOULD BE AUTOMATICALLY CONNECTED TO THE CODE ABOVE ###

    # NOTE: no argument means all parms are passed
    prepare_op = prepare(x="{{ params.x }}", y="{{ params.y }}", sleep="{{ params.sleep }}")

    # Use the reusable task group (it will pull XCom values internally)
    calcjob_group = create_calcjob_taskgroup(
        group_id="calcjob",
        machine="{{ params.machine }}",
        local_workdir="{{ params.local_workdir }}",
        remote_workdir="{{ params.remote_workdir }}"
    )

    parse_op = parse(local_workdir="{{ params.local_workdir }}")

    prepare_op >> calcjob_group >> parse_op

    ##########################################################################


### END USER ###

# Information in conf is
# workdir

# https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#loading-dags

#example_asset = Asset(uri="data.xyz", name="my_dataset")

if __name__ == "__main__":
    dag.test(
        run_conf={"machine": "localhost",
                  "local_workdir": "/Users/alexgo/code/airflow/local_workdir",
                  "remote_workdir": "/Users/alexgo/code/airflow/remote_workdir",
                  }
    )
