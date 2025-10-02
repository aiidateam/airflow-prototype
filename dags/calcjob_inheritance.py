"""
CalcJob TaskGroup with Direct Inheritance

This demonstrates inheriting directly from TaskGroup instead of using a builder pattern.
Much cleaner and more Pythonic approach.
"""

# Import the existing operators and extend them for TaskGroup usage
import sys
from abc import ABC, abstractmethod
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict

from aiida.transports import AsyncSshTransport
from airflow import DAG
from airflow.models import BaseOperator, Param
from airflow.operators.python import PythonOperator
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.sensors.time_sensor import TimeSensor
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup

# sys.path.append(str(Path(__file__).parent))
# from calcjob import (
#     UploadOperator as BaseUploadOperator,
#     SubmitOperator as BaseSubmitOperator,
#     UpdateOperator,
#     ReceiveOperator as BaseReceiveOperator,
# )

import os
AIRFLOW_HOME_ = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
if AIRFLOW_HOME_ is None:
    raise ImportError("Could not find AIRFLOW_HOME.")
AIRFLOW_HOME = Path(AIRFLOW_HOME_)

LOCAL_WORKDIR = AIRFLOW_HOME / "storage" / "local_workdir"
LOCAL_WORKDIR.mkdir(exist_ok=True, parents=True)

REMOTE_WORKDIR = AIRFLOW_HOME / "storage" / "remote_workdir"
REMOTE_WORKDIR.mkdir(exist_ok=True, parents=True)


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



class TaskGroupUploadOperator(UploadOperator):
    """UploadOperator that can pull XCom from TaskGroup-scoped prepare task"""

    def __init__(self, task_group_id: str, **kwargs):
        super().__init__(**kwargs)
        self.task_group_id = task_group_id

    def execute(self, context):
        to_upload_files = self.to_upload_files
        if not to_upload_files:
            to_upload_files = context["task_instance"].xcom_pull(
                task_ids=f"{self.task_group_id}.prepare", key="to_upload_files"
            )

        if not to_upload_files:
            to_upload_files = {}

        remote_workdir = Path(self.remote_workdir)
        transport = AsyncSshTransport(machine=self.machine)
        with transport.open() as connection:
            for localpath, remotepath in to_upload_files.items():
                connection.putfile(
                    Path(localpath).absolute(), remote_workdir / Path(remotepath)
                )


class TaskGroupSubmitOperator(SubmitOperator):
    """SubmitOperator that can pull XCom from TaskGroup-scoped prepare task"""

    def __init__(self, task_group_id: str, **kwargs):
        super().__init__(**kwargs)
        self.task_group_id = task_group_id

    def execute(self, context):
        submission_script = self.submission_script
        if not submission_script:
            submission_script = context["task_instance"].xcom_pull(
                task_ids=f"{self.task_group_id}.prepare", key="submission_script"
            )

        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)
        transport = AsyncSshTransport(machine=self.machine)
        submission_script_path = local_workdir / Path("submit.sh")
        submission_script_path.write_text(submission_script)
        with transport.open() as connection:
            connection.putfile(submission_script_path, remote_workdir / "submit.sh")
            exit_code, stdout, stderr = connection.exec_command_wait(
                f"(bash {submission_script_path} > /dev/null 2>&1 & echo $!) &",
                workdir=remote_workdir,
            )
        if exit_code != 0:
            raise ValueError(f"Submission did not work, {stderr}")
        job_id = int(stdout.strip())
        self.log.info(f"Output of submission of process: {job_id}")
        return job_id


class TaskGroupReceiveOperator(ReceiveOperator):
    """ReceiveOperator that can pull XCom from TaskGroup-scoped prepare task"""

    def __init__(self, task_group_id: str, **kwargs):
        super().__init__(**kwargs)
        self.task_group_id = task_group_id

    def execute(self, context):
        to_receive_files = self.to_receive_files
        if not to_receive_files:
            to_receive_files = context["task_instance"].xcom_pull(
                task_ids=f"{self.task_group_id}.prepare", key="to_receive_files"
            )

        transport = AsyncSshTransport(machine=self.machine)
        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)
        with transport.open() as connection:
            for remotepath, localpath in to_receive_files.items():
                connection.getfile(
                    remote_workdir / Path(remotepath), local_workdir / Path(localpath)
                )


class CalcJobTaskGroup(TaskGroup, ABC):
    """
    Abstract TaskGroup for CalcJob workflows.

    Directly inherits from TaskGroup, so instances ARE TaskGroups.
    Subclasses implement prepare() and parse() methods.
    """

    def __init__(
        self,
        group_id: str,
        machine: str,
        local_workdir: str,
        remote_workdir: str,
        **kwargs,
    ):
        super().__init__(group_id=group_id, **kwargs)
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir

        # Build the task group when instantiated
        self._build_tasks()

    def _build_tasks(self):
        """Build all tasks within this task group"""

        # Create prepare task using the abstract method
        prepare_task = PythonOperator(
            task_id="prepare",
            python_callable=self.prepare,
            task_group=self,  # Important: assign to this task group
        )

        # Create the core calcjob workflow
        upload_op = TaskGroupUploadOperator(
            task_id="upload",
            task_group_id=self.group_id,
            machine=self.machine,
            local_workdir=self.local_workdir,
            remote_workdir=self.remote_workdir,
            to_upload_files={},  # Will be pulled from XCom
            task_group=self,
        )

        submit_op = TaskGroupSubmitOperator(
            task_id="submit",
            task_group_id=self.group_id,
            machine=self.machine,
            local_workdir=self.local_workdir,
            remote_workdir=self.remote_workdir,
            submission_script="",  # Will be pulled from XCom
            task_group=self,
        )

        update_op = UpdateOperator(
            task_id="update",
            machine=self.machine,
            job_id=submit_op.output,
            task_group=self,
        )

        receive_op = TaskGroupReceiveOperator(
            task_id="receive",
            task_group_id=self.group_id,
            machine=self.machine,
            local_workdir=self.local_workdir,
            remote_workdir=self.remote_workdir,
            to_receive_files={},  # Will be pulled from XCom
            task_group=self,
        )

        # Create parse task using the abstract method
        parse_task = PythonOperator(
            task_id="parse",
            python_callable=self.parse,
            op_kwargs={"local_workdir": self.local_workdir},
            task_group=self,
        )

        # Set up dependencies within the task group
        prepare_task >> upload_op >> submit_op >> update_op >> receive_op >> parse_task

    @abstractmethod
    def prepare(self, **context) -> Dict[str, Any]:
        """Abstract method to prepare job inputs"""
        pass

    @abstractmethod
    def parse(self, local_workdir: str, **context) -> tuple[int, Dict[str, Any]]:
        """Abstract method to parse job outputs

        Returns:
            tuple[int, Dict[str, Any]]: (exit_status, results)
                exit_status: 0 for success, non-zero for failure/error
                results: Dictionary containing parsed results
        """
        pass


class AddJobTaskGroup(CalcJobTaskGroup):
    """Addition job task group - directly IS a TaskGroup"""

    def __init__(
        self,
        group_id: str,
        machine: str,
        local_workdir: str,
        remote_workdir: str,
        x: int,
        y: int,
        sleep: int,
        **kwargs,
    ):
        self.x = x
        self.y = y
        self.sleep = sleep
        super().__init__(group_id, machine, local_workdir, remote_workdir, **kwargs)

    def prepare(self, **context) -> Dict[str, Any]:
        """Prepare addition job inputs"""
        to_upload_files = {}

        submission_script = f"""
sleep {self.sleep}
echo "$(({self.x}+{self.y}))" > result.out
        """

        to_receive_files = {"result.out": "addition_result.txt"}

        # Push to XCom for the calcjob operators to use
        context["task_instance"].xcom_push(key="to_upload_files", value=to_upload_files)
        context["task_instance"].xcom_push(
            key="submission_script", value=submission_script
        )
        context["task_instance"].xcom_push(
            key="to_receive_files", value=to_receive_files
        )

        return {
            "to_upload_files": to_upload_files,
            "submission_script": submission_script,
            "to_receive_files": to_receive_files,
        }

    def parse(self, local_workdir: str, **context) -> tuple[int, Dict[str, Any]]:
        """Parse addition job results"""
        to_receive_files = context["task_instance"].xcom_pull(
            task_ids=f"{self.group_id}.prepare", key="to_receive_files"
        )

        results = {}
        exit_status = 0  # Start with success

        try:
            for file_key, received_file in to_receive_files.items():
                file_path = Path(local_workdir) / Path(received_file)
                if not file_path.exists():
                    print(f"ERROR: Expected file {received_file} not found")
                    exit_status = 1
                    continue

                result_content = file_path.read_text().strip()
                print(f"Addition result ({self.x} + {self.y}): {result_content}")
                results[file_key] = int(result_content)

        except Exception as e:
            print(f"ERROR parsing results: {e}")
            exit_status = 2

        # Store both exit status and results in XCom
        final_result = (exit_status, results)
        context["task_instance"].xcom_push(key="final_result", value=final_result)
        return final_result


class MultiplyJobTaskGroup(CalcJobTaskGroup):
    """Multiplication job task group - directly IS a TaskGroup"""

    def __init__(
        self,
        group_id: str,
        machine: str,
        local_workdir: str,
        remote_workdir: str,
        x: int,
        y: int,
        sleep: int,
        **kwargs,
    ):
        self.x = x
        self.y = y
        self.sleep = sleep
        super().__init__(group_id, machine, local_workdir, remote_workdir, **kwargs)

    def prepare(self, **context) -> Dict[str, Any]:
        """Prepare multiplication job inputs"""
        to_upload_files = {}

        submission_script = f"""
sleep {self.sleep}
echo "$(({self.x}*{self.y}))" > multiply_result.out
echo "Operation: {self.x} * {self.y}" > operation.log
        """

        to_receive_files = {
            "multiply_result.out": "multiply_result.txt",
            "operation.log": "operation.log",
        }

        # Push to XCom
        context["task_instance"].xcom_push(key="to_upload_files", value=to_upload_files)
        context["task_instance"].xcom_push(
            key="submission_script", value=submission_script
        )
        context["task_instance"].xcom_push(
            key="to_receive_files", value=to_receive_files
        )

        return {
            "to_upload_files": to_upload_files,
            "submission_script": submission_script,
            "to_receive_files": to_receive_files,
        }

    def parse(self, local_workdir: str, **context) -> Dict[str, Any]:
        """Parse multiplication job results"""
        to_receive_files = context["task_instance"].xcom_pull(
            task_ids=f"{self.group_id}.prepare", key="to_receive_files"
        )

        results = {}
        for file_key, received_file in to_receive_files.items():
            file_path = Path(local_workdir) / Path(received_file)
            if file_path.exists():
                content = file_path.read_text().strip()
                print(f"File {file_key}: {content}")
                if file_key == "multiply_result.out":
                    results["result"] = int(content)
                else:
                    results["log"] = content

        print(
            f"Multiplication result ({self.x} * {self.y}): {results.get('result', 'N/A')}"
        )

        context["task_instance"].xcom_push(key="final_result", value=results)
        return results


# Create DAG
default_args = {
    "owner": "alexgo",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="calcjob_taskgroup_inheritance",
    default_args=default_args,
    description="CalcJob TaskGroup using direct inheritance",
    schedule=None,
    catchup=False,
    tags=["inheritance", "calcjob", "taskgroup"],
    params={
        "machine": Param("localhost", type="string"),
        "local_workdir": Param(
            str(LOCAL_WORKDIR), type="string"
        ),
        "remote_workdir": Param(
            str(REMOTE_WORKDIR), type="string"
        ),
    },
) as dag:
    # Create task groups directly - no builder pattern needed!
    # Use separate local and remote directories to avoid file conflicts
    add_job = AddJobTaskGroup(
        group_id="addition_job",
        machine="{{ params.machine }}",
        local_workdir="{{ params.local_workdir }}/addition_job",
        remote_workdir="{{ params.remote_workdir }}/addition_job",
        x=8,
        y=4,
        sleep=3,
    )

    multiply_job = MultiplyJobTaskGroup(
        group_id="multiplication_job",
        machine="{{ params.machine }}",
        local_workdir="{{ params.local_workdir }}/multiplication_job",
        remote_workdir="{{ params.remote_workdir }}/multiplication_job",
        x=6,
        y=9,
        sleep=2,
    )

    @task
    def combine_results():
        """Combine results from both job types"""
        from airflow.sdk import get_current_context

        context = get_current_context()
        task_instance = context["task_instance"]

        add_result = task_instance.xcom_pull(
            task_ids="addition_job.parse", key="final_result"
        )
        multiply_result = task_instance.xcom_pull(
            task_ids="multiplication_job.parse", key="final_result"
        )

        combined = {
            "addition": add_result,
            "multiplication": multiply_result,
        }

        print(f"Combined results: {combined}")
        return combined

    # Direct usage - add_job and multiply_job ARE TaskGroups!
    combine_task = combine_results()
    [add_job, multiply_job] >> combine_task


if __name__ == "__main__":
    print("Testing calcjob_taskgroup_inheritance DAG...")
    dag.test()
