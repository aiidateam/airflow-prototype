"""
Custom triggers for async file operations.
This module is in plugins/ so Airflow can properly import and deserialize triggers.
"""

import asyncio
import logging
import uuid
from pathlib import Path
from typing import Any, Optional, Dict

from airflow.triggers.base import TriggerEvent, BaseTrigger
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


class AsyncCalcJobTrigger(BaseTrigger):
    """Custom trigger that performs async file operations using a shared TransportQueue."""

    def __init__(
        self,
        machine: str,
        local_workdir: str,
        remote_workdir: str,
        to_upload_files: dict[str, str],
        to_receive_files: dict[str, str],
        submission_script: str,
    ):
        super().__init__()
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir
        self.to_upload_files = to_upload_files
        self.to_receive_files = to_receive_files
        self.submission_script = submission_script

        # IMPORTANT:
        # __init__ runs both in the task worker (at defer time) and again in the triggerer.
        # __init__ is run at least twice per DAG.
        # We will create/access shared resources lazily inside run().

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "async_calcjob_trigger.AsyncCalcJobTrigger",
            {
                "machine": self.machine,
                "local_workdir": self.local_workdir,
                "remote_workdir": self.remote_workdir,
                "to_receive_files": self.to_receive_files,
                "to_upload_files": self.to_upload_files,
                "submission_script": self.submission_script,
            },
        )

    async def _execute_upload(self, transport):
        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)
        submission_script_path = local_workdir / "submit.sh"
        # NOTE: Since this is not a user input it is passed as XComArg
        submission_script_path.write_text(self.submission_script)

        await transport.putfile_async(submission_script_path, remote_workdir / "submit.sh")

        exit_code, stdout, stderr = await transport.exec_command_wait_async(
            "(bash submit.sh > /dev/null 2>&1 & echo $!) &",
            workdir=remote_workdir,
        )

        if exit_code != 0:
            raise ValueError(f"Submission did not work, {stderr}")
        job_id = int(stdout.strip())
        logging.info(f"Output of submission of process: {job_id}")
        return job_id

    async def _execute_update(self, transport):
        job_id = self.job_id
        retval = 0  # 0 means process is alive

        while retval == 0:
            # -0 does not kill the process, only verifies it
            retval, stdout_bytes, stderr_bytes = await transport.exec_command_wait_async(f"kill -0 {job_id}")
            if retval == 0:
                await asyncio.sleep(5)

    async def _execute_receive(self, transport):
        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)

        for remotepath, localpath in self.to_receive_files.items():
            await transport.getfile_async(remote_workdir / Path(remotepath), local_workdir / Path(localpath))

    async def run(self):
        """Perform async operations with a shared TransportQueue instance."""
        # Lazily get shared resources inside the triggerer process
        transport_Q = get_transport_queue()
        authinfo = get_authinfo_cached(self.machine or "localhost")

        # Upload and submit
        with transport_Q.request_transport(authinfo) as request:
            transport = await request
            self.job_id = await self._execute_upload(transport)

        # Poll for completion
        with transport_Q.request_transport(authinfo) as request:
            transport = await request
            await self._execute_update(transport)

        # Retrieve results
        with transport_Q.request_transport(authinfo) as request:
            transport = await request
            await self._execute_receive(transport)

        yield TriggerEvent({"job_id": self.job_id})
