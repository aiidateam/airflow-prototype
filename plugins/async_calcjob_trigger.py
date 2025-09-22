
"""
Custom triggers for async file operations.
This module is in plugins/ so Airflow can properly import and deserialize triggers.
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from airflow.triggers.base import TriggerEvent, BaseTrigger

import sys
sys.path.append("/Users/alexgo/code/airflow/dags")
from transport.ssh import AsyncSshTransport


class AsyncCalcJobTrigger(BaseTrigger):
    """Custom trigger that performs async file write operations."""

    def __init__(self, machine: str, local_workdir: str, remote_workdir: str, to_upload_files: dict[str, str],
                 to_receive_files: dict[str, str], submission_script: str):
        super().__init__()
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir
        self.to_upload_files = to_upload_files
        self.to_receive_files = to_receive_files
        self.submission_script = submission_script

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

    async def _execute_upload(self):
        transport = AsyncSshTransport(machine=self.machine)
        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)
        submission_script_path = local_workdir / Path("submit.sh")
         # NOTE: Since this is not a user input it is passed as XComArg
        submission_script_path.write_text(self.submission_script)

        try:
            await transport.open_async()
            await transport.putfile_async(submission_script_path, remote_workdir / "submit.sh")
            exit_code, stdout, stderr = await transport.exec_command_wait_async(f"(bash {submission_script_path} > /dev/null 2>&1 & echo $!) &", workdir=remote_workdir)
        finally:
            await transport.close_async()

        if exit_code != 0:
            raise ValueError(f"Submission did not work, {stderr}")
        job_id = int(stdout.strip())
        logging.info(f"Output of submission of process: {job_id}") #parse out correction
        return job_id

    async def _execute_update(self):
        transport = AsyncSshTransport(machine=self.machine)
        job_id = self.job_id
        retval = 0  # 0 means process is alive

        try:
            await transport.open_async()
            while retval == 0:  # Continue while process is alive
                # -0 does not kill the process, only verifies it
                retval, stdout_bytes, stderr_bytes = await transport.exec_command_wait_async(f"kill -0 {job_id}")
                if retval == 0:  # Process still alive, wait before checking again
                    await asyncio.sleep(5)
        finally:
            await transport.close_async()

    async def _execute_receive(self):
        transport = AsyncSshTransport(machine=self.machine)
        local_workdir = Path(self.local_workdir)
        remote_workdir = Path(self.remote_workdir)

        try:
            await transport.open_async()
            for remotepath, localpath in self.to_receive_files.items():
                await transport.getfile_async(remote_workdir / Path(remotepath), local_workdir / Path(localpath))
        finally:
            await transport.close_async()

    async def run(self):
        """Perform async file write with artificial delay."""
        self.job_id = await self._execute_upload()
        await self._execute_update()
        await self._execute_receive()

        yield TriggerEvent({"job_id": self.job_id})
