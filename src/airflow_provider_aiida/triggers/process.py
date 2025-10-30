"""Airflow triggers that wrap AiiDA CalcJob transport tasks.

These triggers directly execute the task functions from aiida-core's calcjob tasks module,
allowing CalcJob operations to be performed asynchronously in the Airflow triggerer.
"""

import logging
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent

from aiida.engine.utils import InterruptableFuture
from aiida.orm import load_node
from airflow_provider_aiida.aiida_core.engine.runner import Runner

logger = logging.getLogger(__name__)

def load_process(node_pk: int):
    """reenters same state"""
    from aiida import load_profile
    load_profile()
    from aiida.engine import persistence
    from plumpy.persistence import LoadSaveContext
    persister = persistence.AiiDAPersister()
    saved_state = persister.load_checkpoint(node_pk)
    proc = saved_state.unbundle(LoadSaveContext())
    proc._runner = Runner.get_instance()
    return proc


class ProcStepUntilTerminatedTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_upload_job function."""

    def __init__(self, node_pk: int):
        """Initialize the upload trigger.

        :param node_pk: Primary key of the CalcJobNode to upload
        """
        super().__init__()
        self.node_pk = node_pk

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.process.ProcStepUntilTerminatedTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the upload task."""
        try:
            proc = load_process(self.node_pk)
            await proc.step_until_terminated()
            result = proc.future().result()

            yield TriggerEvent({
                "status": "success",
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Step until terminated task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})

