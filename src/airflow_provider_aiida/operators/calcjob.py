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
                # Create remote working directory if it doesn't exist
                connection.makedirs(str(remote_workdir), ignore_existing=True)
                for localpath, remotepath in to_upload_files.items():
                    local_file = Path(localpath).absolute()
                    remote_path = remote_workdir / Path(remotepath)

                    # Debug: Check if local file exists
                    if not local_file.exists():
                        self.log.error(f"Local file does not exist: {local_file}")
                        raise FileNotFoundError(f"Local file not found: {local_file}")

                    # Create remote parent directory if needed
                    remote_parent = remote_path.parent
                    if remote_parent != remote_workdir:
                        connection.makedirs(str(remote_parent), ignore_existing=True)

                    self.log.info(f"Uploading {local_file} -> {remote_path}")
                    connection.putfile(local_file, remote_path)

        loop =  asyncio.get_event_loop()
        loop.run_until_complete(upload_files())


class SubmitOperator(BaseOperator):
    template_fields = ["machine", "local_workdir", "remote_workdir"]

    def __init__(self, machine: str, local_workdir: str, remote_workdir: str, submission_script: str, **kwargs):
        super().__init__(**kwargs)
        self.machine = machine or "localhost"
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
        self.log.info(f"Submission script written to: {submission_script_path}")

        # Use TransportQueue for SSH operations
        import asyncio
        transport_queue = get_transport_queue()
        authinfo = get_authinfo_cached(self.machine)

        async def submit_job():
            with transport_queue.request_transport(authinfo) as request:
                connection = await request
                # Create remote working directory if it doesn't exist
                connection.makedirs(str(remote_workdir), ignore_existing=True)
                self.log.info(f"Remote working directory created/verified: {remote_workdir}")
                # Upload submission script
                remote_script = remote_workdir / "submit.sh"
                connection.putfile(submission_script_path, remote_script)
                self.log.info(f"Uploaded submission script to: {remote_script}")
                # Execute submission command
                # TODO: this is a temporarly solution as we are not creating _aiida.sh file
                # this has to be fixed once we utalize aiida's scheduler layer
                if self.machine == "localhost":
                    command=f"(bash submit.sh > /dev/null 2>&1 & echo $!) &"
                else:
                    command="sbatch submit.sh"

                exit_code, stdout, stderr = connection.exec_command_wait(
                    command,
                    workdir=str(remote_workdir)
                )
                return exit_code, stdout, stderr

        loop =  asyncio.get_event_loop()
        exit_code, stdout, stderr = loop.run_until_complete(submit_job())

        if exit_code != 0:
            raise ValueError(f"Submission did not work, {stderr}")

        if self.machine == "localhost":
            job_id = int(stdout.strip())
            self.log.info(f"Output of submission of process: {job_id}")
        else:
            # This is supposedly SLURM, 
            # TODO: use aiida-core scheduler to handle this
            # Parse job ID from sbatch output (format: "Submitted batch job 12345")
            import re
            match = re.search(r'Submitted batch job (\d+)', stdout)
            if match:
                job_id = int(match.group(1))
            else:
                # Fallback: try to parse as direct integer (for non-SLURM schedulers)
                job_id = int(stdout.strip())
            self.log.info(f"Submitted SLURM job ID: {job_id}")
        return job_id

class UpdateOperator(BaseOperator):
    template_fields = ["machine"]

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

        # Handle both direct int and templated values
        if hasattr(self.job_id, 'resolve'):
            job_id = self.job_id.resolve(context)
        else:
            job_id = self.job_id

        async def check_job():
            with transport_queue.request_transport(authinfo) as request:
                connection = await request
                if self.machine == "localhost":
                    # -0 does not kill the process, only verifies it
                    retval, stdout_bytes, stderr_bytes = connection.exec_command_wait(f"kill -0 {job_id}")
                else:
                    # Check SLURM job status using squeue
                    retval, stdout_bytes, stderr_bytes = connection.exec_command_wait(
                        f"squeue -j {job_id} -h -o %T"
                    )
                return retval, stdout_bytes

        loop =  asyncio.get_event_loop()
        retval, stdout_bytes = loop.run_until_complete(check_job())

        if self.machine == "localhost":
            # TODO check why it is not alive
            self.log.info(f"retval={retval}")
            return bool(retval)
        else:
            # Check job state
            job_state = stdout_bytes.strip()
            self.log.info(f"Job {job_id} squeue returned: retval={retval}, state='{job_state}'")

            # If squeue returns non-zero OR empty output, job is not in queue (completed or failed)
            if retval != 0 or not job_state:
                self.log.info(f"Job {job_id} not in queue anymore (completed or failed)")
                return True  # Job is done (no longer alive)

            self.log.info(f"Job {job_id} current state: {job_state}")

            # Job is done if it's in COMPLETED, FAILED, CANCELLED, etc.
            # Still running if PENDING, RUNNING, etc.
            done_states = ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'NODE_FAIL', 'PREEMPTED']
            is_done = job_state in done_states

            return is_done


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
                # Remote directory should already exist at this point
                for remotepath, localpath in to_receive_files.items():
                    remote_file = remote_workdir / Path(remotepath)
                    local_file = local_workdir / Path(localpath)

                    # Check if remote file exists before trying to download
                    # TODO: this skips non existing files. To be investigated if that's how aiida/aiida-qe works
                    try:
                        if connection.isfile(str(remote_file)):
                            self.log.info(f"Downloading {remote_file} -> {local_file}")
                            connection.getfile(remote_file, local_file)
                        else:
                            self.log.warning(f"Remote file does not exist (skipping): {remote_file}")
                    except Exception as e:
                        self.log.warning(f"Failed to download {remote_file}: {e}")

        loop =  asyncio.get_event_loop()
        return loop.run_until_complete(download_files())
