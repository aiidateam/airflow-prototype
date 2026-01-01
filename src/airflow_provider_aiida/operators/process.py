"""Airflow operators that defer to AiiDA CalcJob triggers.

These operators provide async execution of AiiDA CalcJob transport tasks by deferring
to the corresponding triggers that wrap aiida-core's task functions.
"""

import logging

from airflow.models import BaseOperator
from plumpy.process_states import ProcessState
from airflow_provider_aiida.triggers.process import ProcStepUntilTerminatedTrigger
from airflow_provider_aiida.utils.airflow_control import set_dag_run_id, load_process

from airflow.utils.context import Context
from airflow.utils.session import provide_session
from airflow.models import DagRun

logger = logging.getLogger(__name__)

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

    @staticmethod
    @provide_session
    def get_full_dag_run(dag_run_id: str, session=None) -> DagRun:
        if session is None:
            raise ValueError("Err")
        return session.query(DagRun).filter(DagRun.run_id == dag_run_id).one_or_none()

    def execute(self, context: Context):
        # Add dag_run_id to the process extras and attributes
        from airflow_provider_aiida.aiida_core import load_profile
        load_profile(self.aiida_profile)
        from aiida.orm import load_node

        # Try to get dag_run_id from context and set it on the node
        #from airflow.utils.types import DagRunTriggeredByType
        #from airflow.configuration import conf
        #if conf.get("database") == "airflow-db-not-allowed:///"
        #from airflow.sdk.execution_time.supervisor import BlockedDBSession
        #if isinstance 
 
        #broker_submit = self.get_full_dag_run(context['run_id']).triggered_by != DagRunTriggeredByType.TEST
        #try:
        #    broker_submit = self.get_full_dag_run(context['run_id']).triggered_by != DagRunTriggeredByType.TEST
        #except RuntimeError as e:
        #    if "Direct database access via the ORM is not allowed" in str(e):
        #        return False

        proc = load_process(self.process_pk, self.aiida_profile, self.aiida_path)
        coro = self._continue_run_aiida_process(proc)
        set_dag_run_id(proc.node, context["run_id"])

        # TODO really not nice how runner is retrieved
        proc._runner.loop.run_until_complete(coro)

    def transition(self, context: Context, event: dict) -> None:
        if event["status"] == "error":
            error_msg = f"Step until terminated failed: {event['message']}"
            if "traceback" in event:
                error_msg += f"\n\nFull traceback:\n{event['traceback']}"
            raise ValueError(error_msg)
        from aiida import load_profile
        from aiida.orm import load_node 
        load_profile()
        node = load_node(self.process_pk)
        if not node.is_terminated:
            proc = load_process(self.process_pk, self.aiida_profile, self.aiida_path)

            # If process is in WAITING state, recreate the waiting future in current loop
            #from plumpy.process_states import ProcessState
            #if hasattr(proc, '_state') and hasattr(proc._state, 'LABEL'):
            #    if proc._state.LABEL == ProcessState.WAITING:
            #        # Create new future in current event loop to replace old one
            #        proc._state._waiting_future = proc._runner.loop.create_future()

            # Only resolve awaitables for processes that have actually terminated
            #for awaitable in proc._awaitables:
            #    awaitable_node = load_node(awaitable.pk)
            #    if awaitable_node.is_terminated:
            #        proc._on_awaitable_finished(awaitable)

            coro = self._continue_run_aiida_process(proc)
            # TODO really not nice how runner is retrieved
            proc._runner.loop.run_until_complete(coro)

    async def _continue_run_aiida_process(self, proc):
        while not proc.has_terminated():
            # Log current state before stepping
            current_state = proc._state.LABEL if hasattr(proc._state, 'LABEL') else str(proc.state)

            # Get next step info for WorkChains
            next_step_info = "N/A"
            if hasattr(proc, '_stepper') and proc._stepper is not None:
                try:
                    # Try to get the current outline step
                    if hasattr(proc._stepper, '_fn') and proc._stepper._fn:
                        next_step_info = proc._stepper._fn.__name__
                except (AttributeError, TypeError):
                    pass

            self.log.info(
                f"Process {self.process_pk} - State: {current_state}, Next step: {next_step_info}, "
                f"Has awaitables: {len(proc._awaitables) if hasattr(proc, '_awaitables') else 0}"
            )

            await proc.step()

            # Log state after stepping
            new_state = proc._state.LABEL if hasattr(proc._state, 'LABEL') else str(proc.state)
            self.log.info(f"Process {self.process_pk} - After step, new state: {new_state}")

            if proc._state.LABEL == ProcessState.WAITING:
                self.log.info(f"Process {self.process_pk} - Entering WAITING state, deferring to triggerer")
                self.defer(
                    trigger=ProcStepUntilTerminatedTrigger(
                        process_pk=self.process_pk,
                        aiida_profile=self.aiida_profile,
                        aiida_path=self.aiida_path),
                    method_name="transition",
                )
