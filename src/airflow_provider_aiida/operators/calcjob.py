from datetime import timedelta
from airflow.models import BaseOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context
from pathlib import Path

from airflow_provider_aiida.hooks.ssh import SSHHook

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

        # If still empty, use empty dict
        if not to_upload_files:
            to_upload_files = {}

        # Use SSH Hook for connection management
        hook = SSHHook(machine=self.machine)
        remote_workdir = Path(self.remote_workdir)

        for localpath, remotepath in to_upload_files.items():
            remote_path = remote_workdir / Path(remotepath)
            hook.upload_file(localpath, str(remote_path))


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

        # Write submission script locally
        submission_script_path = local_workdir / Path("submit.sh")
        submission_script_path.write_text(submission_script)

        # Use SSH Hook for connection management
        hook = SSHHook(machine=self.machine)

        # Upload submission script
        remote_script_path = remote_workdir / "submit.sh"
        hook.upload_file(str(submission_script_path), str(remote_script_path))

        # Execute submission command
        exit_code, stdout, stderr = hook.execute_command(
            f"(bash submit.sh > /dev/null 2>&1 & echo $!) &",
            workdir=str(remote_workdir)
        )

        if exit_code != 0:
            raise ValueError(f"Submission did not work, {stderr}")

        job_id = int(stdout.strip())
        self.log.info(f"Output of submission of process: {job_id}")
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
        hook = SSHHook(machine=self.machine)
        job_id = self.job_id.resolve(context)

        # -0 does not kill the process, only verifies it
        retval, stdout_bytes, stderr_bytes = hook.execute_command(f"kill -0 {job_id}")

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

        # If still empty, use empty dict
        if not to_receive_files:
            to_receive_files = {}

        # Use SSH Hook for connection management
        hook = SSHHook(machine=self.machine)
        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)

        for remotepath, localpath in to_receive_files.items():
            remote_file = remote_workdir / Path(remotepath)
            local_file = local_workdir / Path(localpath)
            hook.download_file(str(remote_file), str(local_file))

