import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Optional, Dict

from airflow.triggers.base import BaseTrigger, TriggerEvent
from aiida.engine.transports import TransportQueue


def get_authinfo_from_computer_label(computer_label: str):
    from aiida.orm import load_computer, User
    from aiida import load_profile

    load_profile()
    computer = load_computer(computer_label)
    user = User.collection.get_default()
    return computer.get_authinfo(user)


# ---------------------------
# Module-level singletons
# ---------------------------

_TRANSPORT_QUEUE: Optional[TransportQueue] = None
_AUTHINFO_CACHE: Dict[str, Any] = {}


def get_transport_queue() -> TransportQueue:
    """Return a per-process shared TransportQueue instance."""
    global _TRANSPORT_QUEUE
    if _TRANSPORT_QUEUE is None:
        _TRANSPORT_QUEUE = TransportQueue()
    return _TRANSPORT_QUEUE


def get_authinfo_cached(computer_label: str):
    """Cache authinfo objects per computer label to avoid repeated lookups."""
    global _AUTHINFO_CACHE
    auth = _AUTHINFO_CACHE.get(computer_label)
    if auth is None:
        auth = get_authinfo_from_computer_label(computer_label)
        _AUTHINFO_CACHE[computer_label] = auth
    return auth


class UploadTrigger(BaseTrigger):
    def __init__(
        self,
        machine: str,
        local_workdir: str,
        remote_workdir: str,
        to_upload_files: dict[str, str],
    ):
        super().__init__()
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir
        self.to_upload_files = to_upload_files

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_aiida.triggers.async_calcjob.UploadTrigger",
            {
                "machine": self.machine,
                "local_workdir": self.local_workdir,
                "remote_workdir": self.remote_workdir,
                "to_upload_files": self.to_upload_files,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Perform async operations with a shared TransportQueue instance."""
        # Lazily get shared resources inside the triggerer process
        transport_queue = get_transport_queue()
        authinfo = get_authinfo_cached(self.machine or "localhost")

        try:
            remote_workdir = Path(self.remote_workdir)

            with transport_queue.request_transport(authinfo) as request:
                connection = await request
                for localpath, remotepath in self.to_upload_files.items():
                    connection.putfile(
                        Path(localpath).absolute(),
                        remote_workdir / Path(remotepath)
                    )

            yield TriggerEvent({"status": "success"})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class SubmitTrigger(BaseTrigger):
    def __init__(
        self,
        machine: str,
        local_workdir: str,
        remote_workdir: str,
        submission_script: str,
    ):
        super().__init__()
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir
        self.submission_script = submission_script

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_aiida.triggers.async_calcjob.SubmitTrigger",
            {
                "machine": self.machine,
                "local_workdir": self.local_workdir,
                "remote_workdir": self.remote_workdir,
                "submission_script": self.submission_script,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            local_workdir = Path(self.local_workdir)
            remote_workdir = Path(self.remote_workdir)

            submission_script_path = local_workdir / Path("submit.sh")
            submission_script_path.write_text(self.submission_script)

            transport_queue = get_transport_queue()
            authinfo = get_authinfo_cached(self.machine or "localhost")
            with transport_queue.request_transport(authinfo) as request:
                connection = await request
                connection.putfile(
                    submission_script_path,
                    remote_workdir / "submit.sh"
                )
                exit_code, stdout, stderr = connection.exec_command_wait(
                    f"(bash {submission_script_path} > /dev/null 2>&1 & echo $!) &",
                    workdir=remote_workdir
                )

            if exit_code != 0:
                yield TriggerEvent({
                    "status": "error",
                    "message": f"Submission did not work, {stderr}"
                })
            else:
                job_id = int(stdout.strip())
                yield TriggerEvent({
                    "status": "success",
                    "job_id": job_id
                })
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class UpdateTrigger(BaseTrigger):
    def __init__(
        self,
        machine: str,
        job_id: int,
        sleep: int = 2,
    ):
        super().__init__()
        self.machine = machine
        self.job_id = job_id
        self.sleep = sleep

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_aiida.triggers.async_calcjob.UpdateTrigger",
            {
                "machine": self.machine,
                "job_id": self.job_id,
                "sleep": self.sleep,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            try:
                transport_queue = get_transport_queue()
                authinfo = get_authinfo_cached(self.machine or "localhost")
                with transport_queue.request_transport(authinfo) as request:
                    connection = await request
                    # -0 does not kill the process, only verifies it
                    retval, stdout_bytes, stderr_bytes = connection.exec_command_wait(
                        f"kill -0 {self.job_id}"
                    )

                # If retval is non-zero, the process is not alive
                if retval != 0:
                    yield TriggerEvent({"status": "success", "job_completed": True})
                    return

                # Process still running, sleep and check again
                await asyncio.sleep(self.sleep)

            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})
                return


class ReceiveTrigger(BaseTrigger):
    def __init__(
        self,
        machine: str,
        local_workdir: str,
        remote_workdir: str,
        to_receive_files: dict[str, str],
    ):
        super().__init__()
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir
        self.to_receive_files = to_receive_files

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_aiida.triggers.async_calcjob.ReceiveTrigger",
            {
                "machine": self.machine,
                "local_workdir": self.local_workdir,
                "remote_workdir": self.remote_workdir,
                "to_receive_files": self.to_receive_files,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            local_workdir = Path(self.local_workdir)
            remote_workdir = Path(self.remote_workdir)

            transport_queue = get_transport_queue()
            authinfo = get_authinfo_cached(self.machine or "localhost")
            with transport_queue.request_transport(authinfo) as request:
                connection = await request
                for remotepath, localpath in self.to_receive_files.items():
                    connection.getfile(
                        remote_workdir / Path(remotepath),
                        local_workdir / Path(localpath)
                    )

            yield TriggerEvent({"status": "success"})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
