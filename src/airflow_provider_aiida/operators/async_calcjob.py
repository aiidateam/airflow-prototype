from airflow.models import BaseOperator
from airflow.utils.context import Context
from pathlib import Path

from airflow_provider_aiida.triggers.async_calcjob import (
    UploadTrigger,
    SubmitTrigger,
    UpdateTrigger,
    ReceiveTrigger,
)

######################
### ASYNC OPERATORS ###
######################

class AsyncUploadOperator(BaseOperator):
    template_fields = ["machine", "local_workdir", "remote_workdir", "to_upload_files"]

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
            # Get the task group ID to construct the correct task_id
            task_group_id = self.task_group.group_id if self.task_group else None
            prepare_task_id = f'{task_group_id}.prepare' if task_group_id else 'prepare'
            to_upload_files = context['task_instance'].xcom_pull(task_ids=prepare_task_id, key='to_upload_files')

        if to_upload_files is None:
            raise ValueError("to_upload_files cannot be None. Either provide it as a parameter or ensure it's available in XCom.")

        self.defer(
            trigger=UploadTrigger(
                machine=self.machine,
                local_workdir=self.local_workdir,
                remote_workdir=self.remote_workdir,
                to_upload_files=to_upload_files,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        if event["status"] == "error":
            raise ValueError(f"Upload failed: {event['message']}")
        self.log.info("Upload completed successfully")


class AsyncSubmitOperator(BaseOperator):
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
            # Get the task group ID to construct the correct task_id
            task_group_id = self.task_group.group_id if self.task_group else None
            prepare_task_id = f'{task_group_id}.prepare' if task_group_id else 'prepare'
            submission_script = context['task_instance'].xcom_pull(task_ids=prepare_task_id, key='submission_script')

        if submission_script is None:
            raise ValueError("submission_script cannot be None. Either provide it as a parameter or ensure it's available in XCom.")

        self.defer(
            trigger=SubmitTrigger(
                machine=self.machine,
                local_workdir=self.local_workdir,
                remote_workdir=self.remote_workdir,
                submission_script=submission_script,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        if event["status"] == "error":
            raise ValueError(f"Submission failed: {event['message']}")

        job_id = event["job_id"]
        self.log.info(f"Output of submission of process: {job_id}")
        return job_id


class AsyncUpdateOperator(BaseOperator):
    template_fields = ["machine", "sleep"]

    def __init__(self, machine: str, job_id: int, sleep: int = 2, **kwargs):
        super().__init__(**kwargs)
        self.machine = machine
        self.job_id = job_id
        self.sleep = sleep

    def execute(self, context: Context):
        job_id = self.job_id.resolve(context) if hasattr(self.job_id, 'resolve') else self.job_id

        self.defer(
            trigger=UpdateTrigger(
                machine=self.machine,
                job_id=job_id,
                sleep=self.sleep,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        if event["status"] == "error":
            raise ValueError(f"Update check failed: {event['message']}")

        if event.get("job_completed"):
            self.log.info("Job completed successfully")


class AsyncReceiveOperator(BaseOperator):
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
            # Get the task group ID to construct the correct task_id
            task_group_id = self.task_group.group_id if self.task_group else None
            prepare_task_id = f'{task_group_id}.prepare' if task_group_id else 'prepare'
            to_receive_files = context['task_instance'].xcom_pull(task_ids=prepare_task_id, key='to_receive_files')

        if to_receive_files is None:
            raise ValueError("to_receive_files cannot be None. Either provide it as a parameter or ensure it's available in XCom.")

        self.defer(
            trigger=ReceiveTrigger(
                machine=self.machine,
                local_workdir=self.local_workdir,
                remote_workdir=self.remote_workdir,
                to_receive_files=to_receive_files,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        if event["status"] == "error":
            raise ValueError(f"Receive failed: {event['message']}")
        self.log.info("Receive completed successfully")