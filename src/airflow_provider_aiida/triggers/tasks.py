"""Airflow triggers that wrap AiiDA CalcJob transport tasks.

These triggers directly execute the task functions from aiida-core's calcjob tasks module,
allowing CalcJob operations to be performed asynchronously in the Airflow triggerer.
"""

import asyncio
import logging
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow_provider_aiida.aiida_core.engine.calcjob.tasks import (
    task_upload_job,
    task_submit_job,
    task_update_job,
    task_monitor_job,
    task_retrieve_job,
    task_stash_job,
    task_unstash_job,
    task_kill_job,
)
from aiida.engine.utils import InterruptableFuture
from aiida.orm import load_node
from airflow_provider_aiida.aiida_core.transport import get_transport_queue

logger = logging.getLogger(__name__)


class AiiDAUploadTrigger(BaseTrigger):
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
            "airflow_provider_aiida.triggers.async_aiida_calcjob.AiiDAUploadTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the upload task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            from aiida.common.datastructures import CalcInfo, CodeInfo

            # Load the CalcJobNode
            node = load_node(self.node_pk)

            # Reconstruct calc_info from node attributes
            calc_info = CalcInfo()
            calc_info.uuid = node.base.attributes.get('_calc_info_uuid', str(node.uuid))
            calc_info.skip_submit = node.base.attributes.get('_calc_info_skip_submit', False)

            # Reconstruct codes_info if present
            codes_info_data = node.base.attributes.get('_calc_info_codes_info', [])
            calc_info.codes_info = []
            for ci_data in codes_info_data:
                code_info = CodeInfo()
                code_info.code_uuid = ci_data.get('code_uuid')
                code_info.cmdline_params = ci_data.get('cmdline_params', [])
                code_info.stdin_name = ci_data.get('stdin_name')
                code_info.stdout_name = ci_data.get('stdout_name')
                code_info.stderr_name = ci_data.get('stderr_name')
                code_info.join_files = ci_data.get('join_files', False)
                calc_info.codes_info.append(code_info)

            transport_queue = get_transport_queue()
            cancellable = InterruptableFuture()

            skip_submit = await task_upload_job(node, transport_queue, cancellable, calc_info)

            yield TriggerEvent({
                "status": "success",
                "skip_submit": skip_submit,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Upload task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class AiiDASubmitTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_submit_job function."""

    def __init__(self, node_pk: int):
        """Initialize the submit trigger.

        :param node_pk: Primary key of the CalcJobNode to submit
        """
        super().__init__()
        self.node_pk = node_pk

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.async_aiida_calcjob.AiiDASubmitTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the submit task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = get_transport_queue()
            cancellable = InterruptableFuture()

            job_id = await task_submit_job(node, transport_queue, cancellable)

            yield TriggerEvent({
                "status": "success",
                "job_id": job_id,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Submit task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class AiiDAUpdateTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_update_job function.

    This trigger polls the job status until it's complete.
    """

    def __init__(self, node_pk: int, sleep_interval: int = 5):
        """Initialize the update trigger.

        :param node_pk: Primary key of the CalcJobNode to update
        :param sleep_interval: Seconds to sleep between update checks
        """
        super().__init__()
        self.node_pk = node_pk
        self.sleep_interval = sleep_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.async_aiida_calcjob.AiiDAUpdateTrigger",
            {
                "node_pk": self.node_pk,
                "sleep_interval": self.sleep_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the update task repeatedly until job is done."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = get_transport_queue()
            from aiida.engine.processes.calcjobs.manager import JobManager
            job_manager = JobManager(transport_queue)
            cancellable = InterruptableFuture()

            job_done = False
            while not job_done:
                job_done = await task_update_job(node, job_manager, cancellable)

                if not job_done:
                    await asyncio.sleep(self.sleep_interval)

            yield TriggerEvent({
                "status": "success",
                "job_done": True,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Update task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class AiiDAMonitorTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_monitor_job function."""

    def __init__(self, node_pk: int, monitors_pk: int | None = None):
        """Initialize the monitor trigger.

        :param node_pk: Primary key of the CalcJobNode to monitor
        :param monitors_pk: Primary key of the CalcJobMonitors node (if applicable)
        """
        super().__init__()
        self.node_pk = node_pk
        self.monitors_pk = monitors_pk

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.async_aiida_calcjob.AiiDAMonitorTrigger",
            {
                "node_pk": self.node_pk,
                "monitors_pk": self.monitors_pk,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the monitor task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = get_transport_queue()
            cancellable = InterruptableFuture()

            # Load monitors if provided
            from aiida.engine.processes.calcjobs.monitors import CalcJobMonitors
            monitors = None
            if self.monitors_pk:
                monitors_node = load_node(self.monitors_pk)
                monitors = CalcJobMonitors(monitors_node)

            monitor_result = await task_monitor_job(
                node, transport_queue, cancellable, monitors
            )

            result_dict = {"status": "success"}
            if monitor_result:
                result_dict["action"] = monitor_result.action
                result_dict["message"] = monitor_result.message
                result_dict["retrieve"] = monitor_result.retrieve
                result_dict["parse"] = monitor_result.parse

            yield TriggerEvent(result_dict)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Monitor task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class AiiDARetrieveTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_retrieve_job function."""

    def __init__(self, node_pk: int, retrieved_temporary_folder: str):
        """Initialize the retrieve trigger.

        :param node_pk: Primary key of the CalcJobNode to retrieve
        :param retrieved_temporary_folder: Path to temporary folder for retrieved files
        """
        super().__init__()
        self.node_pk = node_pk
        self.retrieved_temporary_folder = retrieved_temporary_folder

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.async_aiida_calcjob.AiiDARetrieveTrigger",
            {
                "node_pk": self.node_pk,
                "retrieved_temporary_folder": self.retrieved_temporary_folder,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the retrieve task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = get_transport_queue()
            cancellable = InterruptableFuture()

            # Create the retrieved_temporary_folder if it doesn't exist
            from pathlib import Path
            Path(self.retrieved_temporary_folder).mkdir(parents=True, exist_ok=True)

            retrieved = await task_retrieve_job(
                node, transport_queue, self.retrieved_temporary_folder, cancellable
            )

            yield TriggerEvent({
                "status": "success",
                "retrieved": retrieved is not None,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Retrieve task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class AiiDAStashTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_stash_job function."""

    def __init__(self, node_pk: int):
        """Initialize the stash trigger.

        :param node_pk: Primary key of the CalcJobNode to stash
        """
        super().__init__()
        self.node_pk = node_pk

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.async_aiida_calcjob.AiiDAStashTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the stash task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = get_transport_queue()
            cancellable = InterruptableFuture()

            await task_stash_job(node, transport_queue, cancellable)

            yield TriggerEvent({"status": "success"})
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Stash task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class AiiDAUnstashTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_unstash_job function."""

    def __init__(self, node_pk: int):
        """Initialize the unstash trigger.

        :param node_pk: Primary key of the CalcJobNode to unstash
        """
        super().__init__()
        self.node_pk = node_pk

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.async_aiida_calcjob.AiiDAUnstashTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the unstash task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = get_transport_queue()
            cancellable = InterruptableFuture()

            await task_unstash_job(node, transport_queue, cancellable)

            yield TriggerEvent({"status": "success"})
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Unstash task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class AiiDAKillTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_kill_job function."""

    def __init__(self, node_pk: int):
        """Initialize the kill trigger.

        :param node_pk: Primary key of the CalcJobNode to kill
        """
        super().__init__()
        self.node_pk = node_pk

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.async_aiida_calcjob.AiiDAKillTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the kill task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = get_transport_queue()
            cancellable = InterruptableFuture()

            result = await task_kill_job(node, transport_queue, cancellable)

            yield TriggerEvent({
                "status": "success",
                "killed": result,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Kill task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})
