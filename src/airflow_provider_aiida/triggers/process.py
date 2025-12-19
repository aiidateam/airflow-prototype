"""Airflow triggers that wrap AiiDA CalcJob transport tasks.

These triggers directly execute the task functions from aiida-core's calcjob tasks module,
allowing CalcJob operations to be performed asynchronously in the Airflow triggerer.
"""

import logging
import asyncio
from typing import Any, AsyncIterator 
from plumpy.process_states import ProcessState

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
        from aiida.common import exceptions
        state = None
        try:
            proc = load_process(self.process_pk, self.aiida_profile, self.aiida_path)
            state = proc._state.LABEL 
            while not proc.has_terminated():
                if (state := proc._state.LABEL) != ProcessState.WAITING:
                    yield TriggerEvent({
                        "status": "success",
                        "state": f"{state}",
                    })

                if hasattr(proc, "_awaitables"):
                    # TODO not really nice to add workchain
                    if proc._awaitables:
                        from aiida.orm import load_node
                        while any([not load_node(awaitable.pk).is_terminated for awaitable in proc._awaitables]):
                            proc.report(f'Update asleep {[not load_node(awaitable.pk).is_terminated for awaitable in proc._awaitables]}')
                            await asyncio.sleep(1)

                        for awaitable in proc._awaitables:
                            proc.logger.info('received callback that awaitable %d has terminated', awaitable.pk)

                            try:
                                node = load_node(awaitable.pk)
                            except (exceptions.MultipleObjectsError, exceptions.NotExistent):
                                raise ValueError(f'provided pk<{awaitable.pk}> could not be resolved to a valid Node instance')
                            if awaitable.outputs:
                                value = {entry.link_label: entry.node for entry in node.base.links.get_outgoing()}
                            else:
                                value = node  # type: ignore[assignment]

                            proc._resolve_awaitable(awaitable, value)
                        proc.resume()
                await proc.step()

            yield TriggerEvent({
                "status": "success",
                "state": f"{state}",
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Step until terminated task failed for node {self.process_pk}")
            yield TriggerEvent({"status": "error", "state": f"{state}", "message": str(e), "traceback": tb})
