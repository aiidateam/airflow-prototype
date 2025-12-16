"""Airflow operators that defer to AiiDA CalcJob triggers.

These operators provide async execution of AiiDA CalcJob transport tasks by deferring
to the corresponding triggers that wrap aiida-core's task functions.
"""
from airflow.models import BaseOperator
from plumpy.process_states import ProcessState
from airflow_provider_aiida.triggers.process import ProcStepUntilTerminatedTrigger
from airflow_provider_aiida.utils.airflow_control import set_dag_run_id, load_process

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
        from airflow_provider_aiida.aiida_core import load_profile
        load_profile(self.aiida_profile)
        from aiida.orm import load_node

        node = load_node(self.process_pk)

        # Try to get dag_run_id from context and set it on the node
        set_dag_run_id(node, context['run_id'])
        proc = load_process(self.process_pk, self.aiida_profile, self.aiida_path)
        coro = self._continue_run_aiida_process(proc)
        # TODO really not nice how runner is retrieved
        proc._runner.loop.run_until_complete(coro)

    def transition(self, context: Context, event: dict) -> None:
        if event["status"] == "error":
            error_msg = f"Step until terminated failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)
        proc = load_process(self.process_pk, self.aiida_profile, self.aiida_path)
        coro = self._continue_run_aiida_process(proc)
        # TODO really not nice how runner is retrieved
        proc._runner.loop.run_until_complete(coro)

    async def _continue_run_aiida_process(self, proc):
        while not proc.has_terminated():
            await proc.step()
            if proc._state.LABEL == ProcessState.WAITING:
                self.defer(
                    trigger=ProcStepUntilTerminatedTrigger(
                        process_pk=self.process_pk,
                        aiida_profile=self.aiida_profile,
                        aiida_path=self.aiida_path),
                    method_name="transition",
                )
