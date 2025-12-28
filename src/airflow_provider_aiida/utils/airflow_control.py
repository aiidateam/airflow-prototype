"""Utility functions for interacting with Airflow."""
from __future__ import annotations

import logging
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

from plumpy.base.utils import super_check

class DBLogHandler(logging.Handler):
    """A custom db log handler for writing logs tot he database"""

    def emit(self, record):
        if record.exc_info:
            # We do this because if there is exc_info this will put an appropriate string in exc_text.
            # See:
            # https://github.com/python/cpython/blob/1c2cb516e49ceb56f76e90645e67e8df4e5df01a/Lib/logging/handlers.py#L590
            self.format(record)

        from aiida import orm
        from aiida.manage import get_manager

        backend = record.__dict__.pop('backend', None)
        backend = get_manager().get_profile_storage()
        orm.Log.get_collection(backend).create_entry_from_record(record)

def remove_from_aiida_logger_streaming_handler():
    """
    Removes stderr StreamHandlers and replaces with stdout StreamHandler.

    This prevents Airflow from capturing logs as ERROR level (stderr),
    while still allowing terminal output via stdout.
    """
    import sys
    from aiida.common.log import AIIDA_LOGGER
    logger = AIIDA_LOGGER

    # Remove all stderr StreamHandlers
    handlers_to_remove = []
    has_stdout_handler = False

    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            if handler.stream == sys.stderr:
                # Remove stderr handlers
                handlers_to_remove.append(handler)
            elif handler.stream == sys.stdout:
                # Already has stdout handler
                has_stdout_handler = True

    for handler in handlers_to_remove:
        logger.removeHandler(handler)
        logging.debug(f"Removed stderr StreamHandler from '{logger.name}' logger")

    # Add stdout StreamHandler if we don't have one
    if not has_stdout_handler:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)

        # Use the same formatter as the removed handler if possible
        if handlers_to_remove and handlers_to_remove[0].formatter:
            stdout_handler.setFormatter(handlers_to_remove[0].formatter)
        else:
            # Default formatter
            formatter = logging.Formatter(
                '%(asctime)s <%(process)d> %(name)s: [%(levelname)s] %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S %p'
            )
            stdout_handler.setFormatter(formatter)

        logger.addHandler(stdout_handler)
        logging.debug(f"Added stdout StreamHandler to '{logger.name}' logger")

def ensure_aiida_db_log_handler(aiida_logger: logging.Logger):
    """Ensure AiiDA's DBLogHandler is configured and remove stderr handlers.

    This function:
    1. Removes StreamHandlers from ALL parent loggers (prevents ERROR-level stderr capture)
    2. Adds DBLogHandler if missing
    3. Ensures logs go through Airflow's structured logging system
    """
    import sys
    from aiida.manage.configuration import get_config_option

    # Walk up the logger hierarchy and remove StreamHandlers from all parent loggers
    # This is necessary because handlers on parent loggers propagate to child loggers
    current_logger = aiida_logger
    while current_logger:
        handlers_to_remove = []
        for handler in current_logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                # Remove handlers that write to stdout/stderr
                if handler.stream in (sys.stdout, sys.stderr):
                    handlers_to_remove.append(handler)

        for handler in handlers_to_remove:
            current_logger.removeHandler(handler)
            logging.debug(f"Removed StreamHandler from '{current_logger.name}' logger to prevent stderr logging")

        # Move to parent logger
        current_logger = current_logger.parent

    # Check if DBLogHandler is already present on the provided logger
    for handler in aiida_logger.handlers:
        if isinstance(handler, DBLogHandler):
            return  # Already configured

    # DBLogHandler not found, add it
    try:
        db_log_level = get_config_option('logging.db_loglevel')
        db_handler = DBLogHandler()
        db_handler.setLevel(db_log_level)
        aiida_logger.addHandler(db_handler)
        logging.debug(f"Re-added DBLogHandler to '{aiida_logger.name}' logger at level {db_log_level}")
    except Exception as e:
        logging.warning(f"Could not add DBLogHandler: {e}")

class LogRecordInspector(logging.Filter):

    def filter(self, record: logging.LogRecord) -> bool:
        from aiida import load_profile
        load_profile()
        from aiida.manage import get_manager
        from aiida import orm
        record.__dict__.pop('backend', None)
        backend = get_manager().get_profile_storage()
        orm.Log.get_collection(backend).create_entry_from_record(record)
        # Immediately access the raw LogRecord
        #print(f"Level: {record.levelno}")
        #print(f"Message: {record.getMessage()}")
        #print(f"Has dbnode_id: {hasattr(record, 'dbnode_id')}")

        # Access all attributes
        #for key, value in record.__dict__.items():
        #    print(f"  {key}: {value}")
        if record.levelno == 23:
            record.levelno = logging.INFO
            record.levelname = 'INFO'

        return True  # Allow through

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
    from aiida.engine.persistence import AiiDAPersister
    saved_state = AiiDAPersister().load_checkpoint(process_pk)
    proc = saved_state.unbundle(LoadSaveContext())

    # TODO this property will be added to process 
    if hasattr(proc, "_context"):
        broker_submit = proc._context['_airflow_provider_aiida__broker_submit']
    else:
        broker_submit = False
    proc._runner = AirflowRunner(loop=loop, broker_submit=broker_submit)
    # NOTE: Overwrite persisted loop since loop might have changed
    proc._loop = loop

    def on_waiting() -> None:
        proc.__class__.__bases__[0].on_waiting(proc)
        pass

    on_waiting.__self__ = proc
    proc.on_waiting = on_waiting

    def on_wait(awaitables):
        proc.__class__.__bases__[0].on_wait(proc, awaitables)
        pass
    on_wait.__self__ = proc
    proc.on_wait = on_wait

    # TODO bug seem to not appear anyomre?
    def report(msg: str, *args, **kwargs) -> None:
        import inspect
        message = f'[{proc.node.pk}|{proc.__class__.__name__}|{inspect.stack()[1][3]}]: {msg}'
        # TODO seems not to work?
        #proc.logger.log(23, message, *args, **kwargs)
        #proc.logger.info(message, *args, **kwargs)
        proc.logger.report(message, *args, **kwargs)
        #proc.logger.warning(message, *args, **kwargs)
        #proc.logger.info(message, *args, **kwargs)
        # TODO seems to work?
        #proc.logger.report(message, *args, **kwargs)

    proc.report = report
    # TODO does not work
    #ensure_aiida_db_log_handler(proc.logger.logger)
    remove_from_aiida_logger_streaming_handler()
    proc.logger.logger.addFilter(LogRecordInspector())

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
