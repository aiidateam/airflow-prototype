"""Utility functions for interacting with Airflow."""
from __future__ import annotations

from typing import TYPE_CHECKING, Type

from airflow_provider_aiida.aiida_core.engine.processes.process import AirflowAttributeKey
from airflow_provider_aiida.aiida_core.engine.runner import AirflowRunner

if TYPE_CHECKING:
    from aiida.engine.processes import Process
    from asyncio import AbstractEventLoop

def get_current_event_loop() -> 'AbstractEventLoop':
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop - create a new one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop

def load_process(process_pk: int, aiida_profile: str | None, aiida_path: str | None):
    """reenters same state"""
    import os
    # TODO find a solution that gives understandable error message
    # NOTE: this conflicts if profiles from different aiida paths are used
    if aiida_path is not None:
        os.environ["AIIDA_PATH"] = aiida_path
    from airflow_provider_aiida.aiida_core import load_profile
    load_profile(aiida_profile)
    from plumpy.persistence import LoadSaveContext
    loop = get_current_event_loop()
    runner = AirflowRunner(loop=loop)
    saved_state = runner.persister.load_checkpoint(process_pk)
    proc = saved_state.unbundle(LoadSaveContext())
    proc._runner = runner
    # NOTE: Overwrite persisted loop since loop might have changed
    proc._loop = loop
    return proc


def set_dag_run_id(node, dag_run_id: str):
    node.base.extras.set(AirflowAttributeKey.DAG_RUN_ID, dag_run_id)

def get_dag_run_id(node) -> str:
    return node.base.extras.get(AirflowAttributeKey.DAG_RUN_ID)

def dag_id_from_process(process: str | Type[Process]) -> str:
    """
    Convert an AiiDA process name to an Airflow DAG ID.

    By convention, the DAG ID matches the process class name or entry point.
    For example:
    - 'ArithmeticAddCalculation' -> 'ArithmeticAddCalculation'
    - ArithmeticAddCalculation -> 'ArithmeticAddCalculation'
    - 'aiida.calculations:core.arithmetic.add' -> 'ArithmeticAddCalculation'

    Args:
        process: The AiiDA process class or name or entry point

    Returns:
        The corresponding Airflow DAG ID

    Note:
        Currently, this assumes the DAG ID is the same as the process class name.
        If your DAGs use different naming conventions, this function can be extended.
    """
    # Extract class name if it's a full entry point
    if isinstance(process, str):
        if ':' in process:
            # Handle entry point format like 'aiida.calculations:core.arithmetic.add'
            # This would need to be resolved to the actual class name
            # For now, just use the part after the last dot
            # TODO are there any aiida utils doing this?
            process = process.split(':')[-1].split('.')[-1]
            # Capitalize first letter to match class name convention
            return process.title().replace('_', '')
        else:
            return process
    else:
        return process.__name__

def clear_dag_run(dag_id: str, dag_run_id: str, dry_run: bool = False, only_failed: bool = False, run_on_latest_version: bool = False):
    """
    Clear a DAG run by clearing its task instances using REST API.

    This function clears task instances in a DAG run, setting them to a state
    that allows them to be re-run.

    Args:
        dag_id: The DAG ID
        dag_run_id: The DAG run ID to clear
        dry_run: If True, only return what would be cleared without actually clearing
        only_failed: If True, only clear failed tasks

    Returns:
        Response from the clear operation

    Example:
        >>> from airflow_provider_aiida.utils.airflow_control import clear_dag_run
        >>> clear_dag_run('ArithmeticAddCalculation', 'manual__2024-01-01T00:00:00+00:00')
    """
    from airflow.api_fastapi.core_api.routes.public.task_instances import post_clear_task_instances
    from airflow.api_fastapi.core_api.datamodels.task_instances import ClearTaskInstancesBody
    from airflow.settings import Session
    from airflow.models.dagbag import DBDagBag
    from airflow.models.taskinstance import clear_task_instances
    from airflow.utils.state import DagRunState
    from airflow.api_fastapi.core_api.routes.public.dag_run import clear_dag_run
    from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunClearBody

    # Get a session
    session = Session()

    try:
        # Get a DBDagBag
        dag_bag = DBDagBag()

        body = DAGRunClearBody(dry_run=dry_run, only_failed=only_failed, run_on_latest_version=run_on_latest_version)
        result = clear_dag_run(dag_id, dag_run_id, body, dag_bag, session)
        session.commit()

        return result

        # BACKUP code for clearing task_instances
        #dag_bag._dags
        # Get the DAG run to find task IDs
        #from airflow.models import DagRun
        #dag_run = session.query(DagRun).filter(
        #    DagRun.dag_id == dag_id,
        #    DagRun.run_id == dag_run_id
        #).first()
        #dag_bag.get_dag_for_run(dag_run, session)

        #if dag_run is None:
        #    raise ValueError(f"DAG run not found: dag_id={dag_id}, run_id={dag_run_id}")
        ## Get all task instances and extract their task IDs
        #task_instances = dag_run.get_task_instances(session=session)
        ##task_ids = [ti.task_id for ti in task_instances]
        #clear_task_instances(
        #    tis = task_instances,
        #    session = session,
        #    dag_run_state = DagRunState.QUEUED,
        #    run_on_latest_version = False,
        #)

        ## Create the request body for clearing task instances
        #body = ClearTaskInstancesBody(
        #    dry_run=dry_run,
        #    only_failed=only_failed,
        #    dag_run_id=dag_run_id,
        #    task_ids=task_ids,
        #)

        ## Call the Airflow REST API function to clear task instances
        #result = post_clear_task_instances(
        #    dag_id=dag_id,
        #    body=body,
        #    dag_bag=dag_bag,
        #    session=session,
        #)

    finally:
        session.close()


def clear_aiida_process_dag_run(process : str | Type['Process'], dag_run_id: str, dry_run: bool = False, only_failed: bool = False, run_on_latest_version: bool = False):

    # Check if process_name is a class or a string
    dag_id = dag_id_from_process(process)

    # Clear the DAG run
    return clear_dag_run(dag_id=dag_id, dag_run_id=dag_run_id, dry_run=dry_run, only_failed=only_failed, run_on_latest_version=run_on_latest_version)


def mark_dag_run_failed(dag_id: str, dag_run_id: str):
    """
    Mark a DAG run as failed.

    This function sets the state of a DAG run to 'failed' and marks all
    non-terminal task instances as failed to prevent further execution.

    Args:
        dag_id: The DAG ID
        dag_run_id: The DAG run ID to mark as failed

    Returns:
        The updated DAG run object

    Example:
        >>> from airflow_provider_aiida.utils.airflow_control import mark_dag_run_failed
        >>> mark_dag_run_failed('ArithmeticAddCalculation', 'manual__2024-01-01T00:00:00+00:00')
    """
    from airflow.models import DagRun
    from airflow.settings import Session
    from airflow.utils.state import DagRunState, TaskInstanceState

    # Get a session
    session = Session()

    try:
        # Get the DAG run
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.run_id == dag_run_id
        ).first()

        if dag_run is None:
            raise ValueError(f"DAG run not found: dag_id={dag_id}, run_id={dag_run_id}")

        # Mark all non-terminal task instances as failed
        # This includes: running, queued, scheduled, deferred, up_for_retry, up_for_reschedule
        non_terminal_states = [
            TaskInstanceState.RUNNING,
            TaskInstanceState.QUEUED,
            TaskInstanceState.SCHEDULED,
            TaskInstanceState.DEFERRED,
            TaskInstanceState.UP_FOR_RETRY,
            TaskInstanceState.UP_FOR_RESCHEDULE,
            TaskInstanceState.RESTARTING,
        ]

        for ti in dag_run.get_task_instances(session=session):
            if ti.state in non_terminal_states:
                ti.set_state(TaskInstanceState.FAILED, session=session)
            # Also mark tasks that haven't started yet (None state) as skipped
            # so they don't run in the future
            elif ti.state is None:
                ti.set_state(TaskInstanceState.SKIPPED, session=session)

        # Mark the DAG run as failed
        dag_run.set_state(DagRunState.FAILED)

        session.commit()
        return dag_run

    finally:
        session.close()


def mark_aiida_process_dag_run_failed(process: str | Type['Process'], dag_run_id: str):
    # Check if process is a class or a string
    dag_id = dag_id_from_process(process)

    # Mark the DAG run as failed
    return mark_dag_run_failed(dag_id=dag_id, dag_run_id=dag_run_id)
