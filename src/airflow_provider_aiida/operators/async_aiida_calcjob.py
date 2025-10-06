"""Airflow operators that defer to AiiDA CalcJob triggers.

These operators provide async execution of AiiDA CalcJob transport tasks by deferring
to the corresponding triggers that wrap aiida-core's task functions.
"""

from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context

from airflow_provider_aiida.triggers.async_aiida_calcjob import (
    AiiDAUploadTrigger,
    AiiDASubmitTrigger,
    AiiDAUpdateTrigger,
    AiiDAMonitorTrigger,
    AiiDARetrieveTrigger,
    AiiDAStashTrigger,
    AiiDAUnstashTrigger,
    AiiDAKillTrigger,
)


class AiiDAAsyncUploadOperator(BaseOperator):
    """Operator that defers to AiiDAUploadTrigger to upload CalcJob files.

    This operator executes the AiiDA task_upload_job function asynchronously.
    """

    template_fields = ["node_pk"]

    def __init__(self, node_pk: int, **kwargs):
        """Initialize the upload operator.

        :param node_pk: Primary key of the CalcJobNode to upload
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk

    def execute(self, context: Context):
        """Defer to the upload trigger."""
        self.defer(
            trigger=AiiDAUploadTrigger(node_pk=self.node_pk),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Handle the trigger completion."""
        if event["status"] == "error":
            error_msg = f"Upload failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        skip_submit = event.get("skip_submit", False)
        self.log.info(f"Upload completed successfully. Skip submit: {skip_submit}")
        return {"skip_submit": skip_submit}


class AiiDAAsyncSubmitOperator(BaseOperator):
    """Operator that defers to AiiDASubmitTrigger to submit a CalcJob.

    This operator executes the AiiDA task_submit_job function asynchronously.
    """

    template_fields = ["node_pk"]

    def __init__(self, node_pk: int, **kwargs):
        """Initialize the submit operator.

        :param node_pk: Primary key of the CalcJobNode to submit
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk

    def execute(self, context: Context):
        """Defer to the submit trigger."""
        self.defer(
            trigger=AiiDASubmitTrigger(node_pk=self.node_pk),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Handle the trigger completion."""
        if event["status"] == "error":
            error_msg = f"Submit failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        job_id = event.get("job_id")
        self.log.info(f"Submit completed successfully. Job ID: {job_id}")
        return {"job_id": job_id}


class AiiDAAsyncUpdateOperator(BaseOperator):
    """Operator that defers to AiiDAUpdateTrigger to monitor CalcJob status.

    This operator executes the AiiDA task_update_job function asynchronously,
    polling until the job is complete.
    """

    template_fields = ["node_pk", "sleep_interval"]

    def __init__(self, node_pk: int, sleep_interval: int = 5, **kwargs):
        """Initialize the update operator.

        :param node_pk: Primary key of the CalcJobNode to update
        :param sleep_interval: Seconds to sleep between update checks
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk
        self.sleep_interval = sleep_interval

    def execute(self, context: Context):
        """Defer to the update trigger."""
        self.defer(
            trigger=AiiDAUpdateTrigger(
                node_pk=self.node_pk,
                sleep_interval=self.sleep_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Handle the trigger completion."""
        if event["status"] == "error":
            error_msg = f"Update failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        job_done = event.get("job_done", False)
        self.log.info(f"Update completed successfully. Job done: {job_done}")
        return {"job_done": job_done}


class AiiDAAsyncMonitorOperator(BaseOperator):
    """Operator that defers to AiiDAMonitorTrigger to monitor CalcJob.

    This operator executes the AiiDA task_monitor_job function asynchronously.
    """

    template_fields = ["node_pk", "monitors_pk"]

    def __init__(self, node_pk: int, monitors_pk: int | None = None, **kwargs):
        """Initialize the monitor operator.

        :param node_pk: Primary key of the CalcJobNode to monitor
        :param monitors_pk: Primary key of the CalcJobMonitors node (if applicable)
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk
        self.monitors_pk = monitors_pk

    def execute(self, context: Context):
        """Defer to the monitor trigger."""
        self.defer(
            trigger=AiiDAMonitorTrigger(
                node_pk=self.node_pk,
                monitors_pk=self.monitors_pk,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Handle the trigger completion."""
        if event["status"] == "error":
            error_msg = f"Monitor failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        self.log.info("Monitor completed successfully")
        # Return monitor result details
        return {
            "action": event.get("action"),
            "message": event.get("message"),
            "retrieve": event.get("retrieve"),
            "parse": event.get("parse"),
        }


class AiiDAAsyncRetrieveOperator(BaseOperator):
    """Operator that defers to AiiDARetrieveTrigger to retrieve CalcJob files.

    This operator executes the AiiDA task_retrieve_job function asynchronously.
    """

    template_fields = ["node_pk", "retrieved_temporary_folder"]

    def __init__(self, node_pk: int, retrieved_temporary_folder: str, **kwargs):
        """Initialize the retrieve operator.

        :param node_pk: Primary key of the CalcJobNode to retrieve
        :param retrieved_temporary_folder: Path to temporary folder for retrieved files
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk
        self.retrieved_temporary_folder = retrieved_temporary_folder

    def execute(self, context: Context):
        """Defer to the retrieve trigger."""
        self.defer(
            trigger=AiiDARetrieveTrigger(
                node_pk=self.node_pk,
                retrieved_temporary_folder=self.retrieved_temporary_folder,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Handle the trigger completion."""
        if event["status"] == "error":
            error_msg = f"Retrieve failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        retrieved = event.get("retrieved", False)
        self.log.info(f"Retrieve completed successfully. Retrieved: {retrieved}")
        return {"retrieved": retrieved}


class AiiDAAsyncStashOperator(BaseOperator):
    """Operator that defers to AiiDAStashTrigger to stash CalcJob files.

    This operator executes the AiiDA task_stash_job function asynchronously.
    """

    template_fields = ["node_pk"]

    def __init__(self, node_pk: int, **kwargs):
        """Initialize the stash operator.

        :param node_pk: Primary key of the CalcJobNode to stash
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk

    def execute(self, context: Context):
        """Defer to the stash trigger."""
        self.defer(
            trigger=AiiDAStashTrigger(node_pk=self.node_pk),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Handle the trigger completion."""
        if event["status"] == "error":
            error_msg = f"Stash failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        self.log.info("Stash completed successfully")


class AiiDAAsyncUnstashOperator(BaseOperator):
    """Operator that defers to AiiDAUnstashTrigger to unstash CalcJob files.

    This operator executes the AiiDA task_unstash_job function asynchronously.
    """

    template_fields = ["node_pk"]

    def __init__(self, node_pk: int, **kwargs):
        """Initialize the unstash operator.

        :param node_pk: Primary key of the CalcJobNode to unstash
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk

    def execute(self, context: Context):
        """Defer to the unstash trigger."""
        self.defer(
            trigger=AiiDAUnstashTrigger(node_pk=self.node_pk),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Handle the trigger completion."""
        if event["status"] == "error":
            error_msg = f"Unstash failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        self.log.info("Unstash completed successfully")


class AiiDAAsyncKillOperator(BaseOperator):
    """Operator that defers to AiiDAKillTrigger to kill a CalcJob.

    This operator executes the AiiDA task_kill_job function asynchronously.
    """

    template_fields = ["node_pk"]

    def __init__(self, node_pk: int, **kwargs):
        """Initialize the kill operator.

        :param node_pk: Primary key of the CalcJobNode to kill
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk

    def execute(self, context: Context):
        """Defer to the kill trigger."""
        self.defer(
            trigger=AiiDAKillTrigger(node_pk=self.node_pk),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Handle the trigger completion."""
        if event["status"] == "error":
            error_msg = f"Kill failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        killed = event.get("killed", False)
        self.log.info(f"Kill completed successfully. Killed: {killed}")
        return {"killed": killed}
