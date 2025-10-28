"""Airflow operators that defer to AiiDA CalcJob triggers.

These operators provide async execution of AiiDA CalcJob transport tasks by deferring
to the corresponding triggers that wrap aiida-core's task functions.
"""

from airflow.exceptions import AirflowSkipException

from airflow.models import BaseOperator
from airflow.utils.context import Context

from airflow_provider_aiida.triggers.tasks import (
    CalcJobUploadTrigger,
    CalcJobSubmitTrigger,
    CalcJobUpdateTrigger,
    CalcJobMonitorTrigger,
    CalcJobRetrieveTrigger,
    CalcJobStashTrigger,
    CalcJobUnstashTrigger,
    CalcJobKillTrigger,
    ProcStepUntilTerminatedTrigger,
)

class ProcStepUntilTerminatedOperator(BaseOperator):

    template_fields = ["node_pk"]

    def __init__(self, node_pk: int, **kwargs):
        super().__init__(**kwargs)
        self.node_pk = node_pk

    def execute(self, context: Context):
        self.defer(
            trigger=ProcStepUntilTerminatedTrigger(node_pk=self.node_pk),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        if event["status"] == "error":
            error_msg = f"Step until terminated failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        self.log.info(f"Step until terminated completed successfully.")
        return None

class CalcJobUploadOperator(BaseOperator):
    """Operator that defers to CalcJobUploadTrigger to upload CalcJob files.

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
            trigger=CalcJobUploadTrigger(node_pk=self.node_pk),
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
        return skip_submit


class CalcJobSubmitOperator(BaseOperator):
    """Operator that defers to CalcJobSubmitTrigger to submit a CalcJob.

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
            trigger=CalcJobSubmitTrigger(node_pk=self.node_pk),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        """Handle the trigger completion."""
        if event["status"] == "error":
            error_msg = f"Submit failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)

        successful =  event.get("successful")
        self.log.info(f"Submit completed successfully: {successful}")
        return successful


class CalcJobUpdateOperator(BaseOperator):
    """Operator that defers to CalcJobUpdateTrigger to monitor CalcJob status.

    This operator executes the AiiDA task_update_job function asynchronously,
    polling until the job is complete.
    """

    template_fields = ["node_pk", "submit_successful"]

    def __init__(self, node_pk: int, submit_successful: bool, **kwargs):
        """Initialize the update operator.

        :param node_pk: Primary key of the CalcJobNode to update
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk
        self.submit_successful = submit_successful

    def execute(self, context: Context):
        """Defer to the update trigger."""
        if not self.submit_successful:
            raise AirflowSkipException("Submission was not successful. Skipping further execution of CalcJob.")
        self.defer(
            trigger=CalcJobUpdateTrigger(
                node_pk=self.node_pk,
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


class CalcJobMonitorOperator(BaseOperator):
    """Operator that defers to CalcJobMonitorTrigger to monitor CalcJob.

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
            trigger=CalcJobMonitorTrigger(
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


class CalcJobRetrieveOperator(BaseOperator):
    """Operator that defers to CalcJobRetrieveTrigger to retrieve CalcJob files.

    This operator executes the AiiDA task_retrieve_job function asynchronously.
    """

    template_fields = ["node_pk"]

    def __init__(self, node_pk: int, **kwargs):
        """Initialize the retrieve operator.

        :param node_pk: Primary key of the CalcJobNode to retrieve
        """
        super().__init__(**kwargs)
        self.node_pk = node_pk

    def execute(self, context: Context):
        """Defer to the retrieve trigger."""
        self.defer(
            trigger=CalcJobRetrieveTrigger(
                node_pk=self.node_pk,
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
        temp_folder = event.get("temp_folder", None)
        if temp_folder is None:
            raise ValueError() # TODO
        self.log.info(f"Retrieve completed successfully. Retrieved: {retrieved}")
        return {"retrieved": retrieved, "temp_folder": temp_folder}


class CalcJobStashOperator(BaseOperator):
    """Operator that defers to CalcJobStashTrigger to stash CalcJob files.

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
            trigger=CalcJobStashTrigger(node_pk=self.node_pk),
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


class CalcJobUnstashOperator(BaseOperator):
    """Operator that defers to CalcJobUnstashTrigger to unstash CalcJob files.

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
            trigger=CalcJobUnstashTrigger(node_pk=self.node_pk),
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


class CalcJobKillOperator(BaseOperator):
    """Operator that defers to CalcJobKillTrigger to kill a CalcJob.

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
            trigger=CalcJobKillTrigger(node_pk=self.node_pk),
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
