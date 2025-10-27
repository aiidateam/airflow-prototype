"""Airflow triggers that wrap AiiDA CalcJob transport tasks.

These triggers directly execute the task functions from aiida-core's calcjob tasks module,
allowing CalcJob operations to be performed asynchronously in the Airflow triggerer.
"""

import logging
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent


from aiida.engine.processes.calcjobs.tasks import (
    task_upload_job,
    task_submit_job,
    task_update_job,
    task_retrieve_job,
    task_unstash_job,
    task_stash_job,
)

# TODO adapt these like above
#from airflow_provider_aiida.aiida_core.engine.calcjobs.tasks import (
#    task_monitor_job,
#    task_stash_job,
#    task_kill_job,
#)
from aiida.engine.utils import InterruptableFuture
from aiida.orm import load_node
from airflow_provider_aiida.aiida_core.engine.runner import Runner
import plumpy

logger = logging.getLogger(__name__)

def load_process(node_pk: int):
    """reenters same state"""
    from aiida import load_profile
    load_profile()
    from aiida.engine import persistence
    from plumpy.persistence import LoadSaveContext
    persister = persistence.AiiDAPersister()
    saved_state = persister.load_checkpoint(node_pk)
    return saved_state.unbundle(LoadSaveContext())

def load_process_to_waiting_state(node_pk: int):
    """reenters same state"""
    from aiida import load_profile
    load_profile()
    from aiida.engine import persistence
    from plumpy.persistence import LoadSaveContext
    persister = persistence.AiiDAPersister()
    saved_state = persister.load_checkpoint(node_pk)
    process = saved_state.unbundle(LoadSaveContext())
    new_state = plumpy.process_states.Waiting(process=process, done_callback=None)

    process.transition_to(new_state)

    return process

def save_checkpoint(process):
    try:
        process.update_outputs()
    except ValueError:
        raise
    process._save_checkpoint()

class CalcJobUploadTrigger(BaseTrigger):
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
            "airflow_provider_aiida.triggers.tasks.CalcJobUploadTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the upload task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            process = load_process_to_waiting_state(self.node_pk)

            transport_queue = Runner.get_instance().transport_queue
            cancellable = InterruptableFuture()

            skip_submit = await task_upload_job(process, transport_queue, cancellable)
            save_checkpoint(process)

            node = process.node
            if node.get_option('unstash') and node.process_type == 'aiida.calculations:core.unstash':
                await task_unstash_job(node, transport_queue, cancellable)

            yield TriggerEvent({
                "status": "success",
                "skip_submit": skip_submit,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Upload task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class CalcJobSubmitTrigger(BaseTrigger):
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
            "airflow_provider_aiida.triggers.tasks.CalcJobSubmitTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the submit task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida.engine.processes.exit_code import ExitCode
            from aiida import load_profile
            from aiida.orm import load_node
            load_profile()
            node = load_node(self.node_pk)
            transport_queue = Runner.get_instance().transport_queue
            cancellable = InterruptableFuture()

            result = await task_submit_job(node, transport_queue, cancellable)
            
            if isinstance(result, ExitCode):
                # The scheduler plugin returned an exit code from ``Scheduler.submit_job`` indicating the
                # job submission failed due to a non-transient problem and the job should be terminated.
                process = load_process(self.node_pk)
                new_state = plumpy.process_states.Finished(process=process, result=result, successful=False)
                process.transition_to(new_state)
                yield TriggerEvent({
                    "status": "success",
                    "successful": False,
                })

            yield TriggerEvent({
                "status": "success",
                "successful": True,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Submit task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class CalcJobUpdateTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_update_job function.

    This trigger polls the job status until it's complete.
    """

    def __init__(self, node_pk: int):
        """Initialize the update trigger.

        :param node_pk: Primary key of the CalcJobNode to update
        """
        super().__init__()
        self.node_pk = node_pk

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.tasks.CalcJobUpdateTrigger",
            {
                "node_pk": self.node_pk,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the update task repeatedly until job is done."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            from aiida.orm import load_node
            load_profile()
            node = load_node(self.node_pk)
            transport_queue = Runner.get_instance().transport_queue
            from aiida.engine.processes.calcjobs.manager import JobManager
            job_manager = JobManager(transport_queue)
            cancellable = InterruptableFuture()

            job_done = False
            while not job_done:
                job_done = await task_update_job(node, job_manager, cancellable)

            yield TriggerEvent({
                "status": "success",
                "job_done": True,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Update task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class CalcJobMonitorTrigger(BaseTrigger):
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
            "airflow_provider_aiida.triggers.tasks.CalcJobMonitorTrigger",
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
            transport_queue = Runner.get_instance().transport_queue
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


class CalcJobRetrieveTrigger(BaseTrigger):
    """Trigger that executes the AiiDA task_retrieve_job function."""

    def __init__(self, node_pk: int):
        """Initialize the retrieve trigger.

        :param node_pk: Primary key of the CalcJobNode to retrieve
        """
        super().__init__()
        self.node_pk = node_pk

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for persistence."""
        return (
            "airflow_provider_aiida.triggers.tasks.CalcJobRetrieveTrigger",
            {
                "node_pk": self.node_pk,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the retrieve task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            process = load_process_to_waiting_state(self.node_pk)
            transport_queue = Runner.get_instance().transport_queue
            # TODO check this dummy interruptable
            cancellable = InterruptableFuture()

            import tempfile
            temp_folder = tempfile.mkdtemp()
            retrieved = await task_retrieve_job(
                process, transport_queue, temp_folder, cancellable
            )
            save_checkpoint(process)

            yield TriggerEvent({
                "status": "success",
                "retrieved": retrieved is not None,
                "temp_folder": temp_folder,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Retrieve task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class CalcJobStashTrigger(BaseTrigger):
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
            "airflow_provider_aiida.triggers.tasks.CalcJobStashTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the stash task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = Runner.get_instance().transport_queue
            cancellable = InterruptableFuture()

            await task_stash_job(node, transport_queue, cancellable)

            yield TriggerEvent({"status": "success"})
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Stash task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class CalcJobUnstashTrigger(BaseTrigger):
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
            "airflow_provider_aiida.triggers.tasks.CalcJobUnstashTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the unstash task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = Runner.get_instance().transport_queue
            cancellable = InterruptableFuture()

            await task_unstash_job(node, transport_queue, cancellable)

            yield TriggerEvent({"status": "success"})
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.exception(f"Unstash task failed for node {self.node_pk}")
            yield TriggerEvent({"status": "error", "message": str(e), "traceback": tb})


class CalcJobKillTrigger(BaseTrigger):
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
            "airflow_provider_aiida.triggers.tasks.CalcJobKillTrigger",
            {"node_pk": self.node_pk},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Execute the kill task."""
        try:
            # Load AiiDA profile (triggers run in separate process)
            from aiida import load_profile
            load_profile()

            node = load_node(self.node_pk)
            transport_queue = Runner.get_instance().transport_queue
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
