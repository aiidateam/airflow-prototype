"""Airflow operators that defer to AiiDA CalcJob triggers.

These operators provide async execution of AiiDA CalcJob transport tasks by deferring
to the corresponding triggers that wrap aiida-core's task functions.
"""
from airflow.models import BaseOperator
from airflow_provider_aiida.triggers.process import ProcStepUntilTerminatedTrigger

from airflow.utils.context import Context


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
