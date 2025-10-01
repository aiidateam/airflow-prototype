from datetime import timedelta
from airflow.models import BaseOperator
from airflow.triggers.temporal import TimeDeltaTrigger
#from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger 
from airflow.utils.context import Context
from pathlib import Path

from airflow_provider_aiida.hooks.ssh import SSHHook
from airflow_provider_aiida.aiida_core.transport import (
    get_transport_queue,
    get_authinfo_cached,
)

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
            # Handle both standalone and TaskGroup contexts
            task_group = context['task'].task_group
            prepare_task_id = f"{task_group.group_id}.prepare" if task_group else "prepare"
            to_upload_files = context['task_instance'].xcom_pull(task_ids=prepare_task_id, key='to_upload_files')

        # If still empty, use empty dict
        if not to_upload_files:
            to_upload_files = {}

        # Create local workdir if it doesn't exist
        local_workdir = Path(self.local_workdir)
        local_workdir.mkdir(parents=True, exist_ok=True)

        # Use TransportQueue for SSH operations
        import asyncio
        transport_queue = get_transport_queue()
        authinfo = get_authinfo_cached(self.machine or "localhost")
        remote_workdir = Path(self.remote_workdir)

        async def upload_files():
            with transport_queue.request_transport(authinfo) as request:
                connection = await request
                for localpath, remotepath in to_upload_files.items():
                    remote_path = remote_workdir / Path(remotepath)
                    connection.putfile(Path(localpath).absolute(), remote_path)

        loop =  asyncio.get_event_loop()
        loop.run_until_complete(upload_files())


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
            # Handle both standalone and TaskGroup contexts
            task_group = context['task'].task_group
            prepare_task_id = f"{task_group.group_id}.prepare" if task_group else "prepare"
            submission_script = context['task_instance'].xcom_pull(task_ids=prepare_task_id, key='submission_script')

        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)

        # Create local workdir if it doesn't exist
        local_workdir.mkdir(parents=True, exist_ok=True)

        # Write submission script locally
        submission_script_path = local_workdir / Path("submit.sh")
        submission_script_path.write_text(submission_script)

        # Use TransportQueue for SSH operations
        import asyncio
        transport_queue = get_transport_queue()
        authinfo = get_authinfo_cached(self.machine or "localhost")

        async def submit_job():
            with transport_queue.request_transport(authinfo) as request:
                connection = await request
                # Upload submission script
                connection.putfile(submission_script_path, remote_workdir / "submit.sh")
                # Execute submission command
                exit_code, stdout, stderr = connection.exec_command_wait(
                    f"(bash submit.sh > /dev/null 2>&1 & echo $!) &",
                    workdir=str(remote_workdir)
                )
                return exit_code, stdout, stderr

        loop =  asyncio.get_event_loop()
        exit_code, stdout, stderr = loop.run_until_complete(submit_job())

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
        import asyncio
        transport_queue = get_transport_queue()
        authinfo = get_authinfo_cached(self.machine or "localhost")
        job_id = self.job_id.resolve(context)

        async def check_job():
            with transport_queue.request_transport(authinfo) as request:
                connection = await request
                # -0 does not kill the process, only verifies it
                retval, stdout_bytes, stderr_bytes = connection.exec_command_wait(f"kill -0 {job_id}")
                return retval

        loop =  asyncio.get_event_loop()
        retval = loop.run_until_complete(check_job())

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
            # Handle both standalone and TaskGroup contexts
            task_group = context['task'].task_group
            prepare_task_id = f"{task_group.group_id}.prepare" if task_group else "prepare"
            to_receive_files = context['task_instance'].xcom_pull(task_ids=prepare_task_id, key='to_receive_files')

        # If still empty, use empty dict
        if not to_receive_files:
            to_receive_files = {}

        # Create local workdir if it doesn't exist
        local_workdir = Path(self.local_workdir)
        local_workdir.mkdir(parents=True, exist_ok=True)

        # Use TransportQueue for SSH operations
        import asyncio
        transport_queue = get_transport_queue()
        authinfo = get_authinfo_cached(self.machine or "localhost")
        remote_workdir = Path(self.remote_workdir)

        async def download_files():
            with transport_queue.request_transport(authinfo) as request:
                connection = await request
                for remotepath, localpath in to_receive_files.items():
                    remote_file = remote_workdir / Path(remotepath)
                    local_file = local_workdir / Path(localpath)
                    connection.getfile(remote_file, local_file)

        loop =  asyncio.get_event_loop()
        return loop.run_until_complete(download_files())
