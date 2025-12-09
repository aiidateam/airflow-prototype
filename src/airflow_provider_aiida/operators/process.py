"""Airflow operators that defer to AiiDA CalcJob triggers.

These operators provide async execution of AiiDA CalcJob transport tasks by deferring
to the corresponding triggers that wrap aiida-core's task functions.
"""
from airflow.models import BaseOperator
from airflow_provider_aiida.triggers.process import ProcStepUntilTerminatedTrigger
from airflow_provider_aiida.utils.airflow_control import set_dag_run_id

from airflow.utils.context import Context


class ProcStepUntilTerminatedOperator(BaseOperator):

    template_fields = ["process_pk", "aiida_profile", "aiida_path"]

    def __init__(self,
                 process_pk: int,
                 aiida_profile: str | None,
                 aiida_path: str | None,
                 **kwargs):
        super().__init__(**kwargs)
        self.process_pk = process_pk
        self.aiida_profile = aiida_profile
        self.aiida_path = aiida_path

    def execute(self, context: Context):
        # Add dag_run_id to the process extras and attributes
        from aiida import load_profile
        load_profile(self.aiida_profile)
        from aiida.orm import load_node

        node = load_node(self.process_pk)

        # Try to get dag_run_id from context and set it on the node

        set_dag_run_id(node, context['run_id'])

        self.defer(
            trigger=ProcStepUntilTerminatedTrigger(
                process_pk=self.process_pk,
                aiida_profile=self.aiida_profile,
                aiida_path=self.aiida_path),
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
