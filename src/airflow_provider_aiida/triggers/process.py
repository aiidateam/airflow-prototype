"""Airflow triggers that wrap AiiDA CalcJob transport tasks.

These triggers directly execute the task functions from aiida-core's calcjob tasks module,
allowing CalcJob operations to be performed asynchronously in the Airflow triggerer.
"""

import logging
from typing import Any, AsyncIterator 

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow_provider_aiida.utils.airflow_control import load_process


logger = logging.getLogger(__name__)


class ProcStepUntilTerminatedTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_upload_job function."""

    def __init__(self, process_pk: int,
                 aiida_profile: str | None,
                 aiida_path: str | None):
        """Initialize the upload trigger.

        :param process_pk: Primary key of the CalcJobNode to upload
        """
        super().__init__()
        self.process_pk = process_pk
        self.aiida_profile = aiida_profile
        self.aiida_path = aiida_path

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.process.ProcStepUntilTerminatedTrigger",
            {"process_pk": self.process_pk,
            "aiida_profile": self.aiida_profile,
            "aiida_path": self.aiida_path,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the upload task."""
        try:
            proc = load_process(self.process_pk, self.aiida_profile, self.aiida_path)
            await proc.step_until_terminated()
            result = proc.future().result()

            yield TriggerEvent({
                "status": "success",
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Step until terminated task failed for node {self.process_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})

