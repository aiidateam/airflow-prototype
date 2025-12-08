"""Utility functions for interacting with Airflow."""
from typing import TYPE_CHECKING, Type

from airflow_provider_aiida.aiida_core.engine.processes.process import AirflowAttributeKey

if TYPE_CHECKING:
    from aiida.engine.processes import Process

def set_dag_run_id(node, dag_run_id: str):
    node.base.extras.set(AirflowAttributeKey.DAG_RUN_ID, dag_run_id)

def get_dag_run_id(node) -> str:
    return node.base.extras.get(AirflowAttributeKey.DAG_RUN_ID)

def clear_dag_run(dag_id: str, dag_run_id: str, dry_run: bool = False, only_failed: bool = False, run_on_latest_version: bool = False):
    """
    Clear a DAG run using Airflow's API.

    This function clears task instances in a DAG run, setting them to a state
    that allows them to be re-run.

    Args:
        dag_id: The DAG ID
        dag_run_id: The DAG run ID to clear
        dry_run: If True, only return what would be cleared without actually clearing
        only_failed: If True, only clear failed tasks
        run_on_latest_version: If True, run on the latest bundle version after clearing (experimental)

    Returns:
        List of task instances that were/would be cleared if dry_run=True,
        or the updated DAG run if dry_run=False

    Example:
        >>> from airflow_provider_aiida.utils.pause import clear_dag_run
        >>> clear_dag_run('ArithmeticAddCalculation', 'manual__2024-01-01T00:00:00+00:00')
    """
    from airflow.api_fastapi.core_api.routes.public.dag_run import clear_dag_run as airflow_clear_dag_run
    from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunClearBody
    from airflow.settings import Session
    from airflow.models import DagBag

    # Create the request body
    body = DAGRunClearBody(
        dry_run=dry_run,
        only_failed=only_failed,
        run_on_latest_version=run_on_latest_version
    )

    # Get a DagBag
    dag_bag = DagBag()

    # Get a session
    session = Session()

    try:
        # Call the Airflow API function (Airflow 3.1.0 API)
        result = airflow_clear_dag_run(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            body=body,
            dag_bag=dag_bag,
            session=session,
        )
        return result
    finally:
        session.close()


def clear_aiida_process_dag_run(process : str | Type['Process'], dag_run_id: str, dry_run: bool = False, only_failed: bool = False, run_on_latest_version: bool = False):
    """
    Clear a DAG run using an AiiDA process name or process class.

    This function converts the AiiDA process name to a DAG ID and clears
    the corresponding DAG run. It accepts either a string (process name) or
    a process class (extracts __name__).

    Args:
        process: The AiiDA process class name (e.g., 'ArithmeticAddCalculation')
                     or the process class itself (e.g., ArithmeticAddCalculation)
        dag_run_id: The DAG run ID to clear
        dry_run: If True, only return what would be cleared without actually clearing
        only_failed: If True, only clear failed tasks
        run_on_latest_version: If True, run on the latest bundle version after clearing (experimental)

    Returns:
        List of task instances that were/would be cleared if dry_run=True,
        or the updated DAG run if dry_run=False

    Example:
        >>> from airflow_provider_aiida.utils.pause import clear_aiida_process_dag_run
        >>> # Using string
        >>> clear_aiida_process_dag_run('ArithmeticAddCalculation', 'manual__2024-01-01T00:00:00+00:00')
        >>> # Using process class
        >>> from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
        >>> clear_aiida_process_dag_run(ArithmeticAddCalculation, 'manual__2024-01-01T00:00:00+00:00')
    """
    from airflow_provider_aiida.utils.dags import dag_id_from_process

    # Check if process_name is a class or a string
    dag_id = dag_id_from_process(process)

    # Clear the DAG run
    return clear_dag_run(dag_id=dag_id, dag_run_id=dag_run_id, dry_run=dry_run, only_failed=only_failed, run_on_latest_version=run_on_latest_version)


def mark_dag_run_failed(dag_id: str, dag_run_id: str):
    """
    Mark a DAG run as failed.

    This function sets the state of a DAG run to 'failed' using Airflow's internal API.
    All running task instances in the DAG run will also be marked as failed.

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

        # Mark the DAG run as failed
        dag_run.set_state(DagRunState.FAILED)

        # Mark all running task instances as failed
        for ti in dag_run.get_task_instances(session=session):
            if ti.state in [TaskInstanceState.RUNNING, TaskInstanceState.QUEUED, TaskInstanceState.SCHEDULED]:
                ti.set_state(TaskInstanceState.FAILED, session=session)

        session.commit()
        return dag_run

    finally:
        session.close()


def mark_aiida_process_dag_run_failed(process: str | Type['Process'], dag_run_id: str):
    """
    Mark a DAG run as failed using an AiiDA process name or process class.

    This function converts the AiiDA process name to a DAG ID and marks
    the corresponding DAG run as failed.

    Args:
        process: The AiiDA process class name (e.g., 'ArithmeticAddCalculation')
                 or the process class itself (e.g., ArithmeticAddCalculation)
        dag_run_id: The DAG run ID to mark as failed

    Returns:
        The updated DAG run object

    Example:
        >>> from airflow_provider_aiida.utils.airflow_control import mark_aiida_process_dag_run_failed
        >>> # Using string
        >>> mark_aiida_process_dag_run_failed('ArithmeticAddCalculation', 'manual__2024-01-01T00:00:00+00:00')
        >>> # Using process class
        >>> from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
        >>> mark_aiida_process_dag_run_failed(ArithmeticAddCalculation, 'manual__2024-01-01T00:00:00+00:00')
    """
    from airflow_provider_aiida.utils.dags import dag_id_from_process

    # Check if process is a class or a string
    dag_id = dag_id_from_process(process)

    # Mark the DAG run as failed
    return mark_dag_run_failed(dag_id=dag_id, dag_run_id=dag_run_id)
