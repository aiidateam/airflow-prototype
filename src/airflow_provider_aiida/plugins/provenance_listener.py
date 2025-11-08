"""Airflow listener plugin for updating AiiDA provenance node states.

This module implements Airflow listeners that update AiiDA CalcJobNode and WorkChainNode
states when DAG runs and task instances change state.
"""

import logging
import os
from typing import Optional, TYPE_CHECKING

from airflow.listeners import hookimpl
from airflow.models import DagRun, TaskInstance
from airflow.plugins_manager import AirflowPlugin

from airflow_provider_aiida.common.utils import (
    _get_workchain_node,
    _get_or_create_workchain_node,
    _get_or_create_calcjob_node,
    _sanitize_link_label
)

from aiida.common.links import LinkType

if TYPE_CHECKING:
    from aiida.orm import WorkChainNode

logger = logging.getLogger(__name__)

# NOTE: Even though the scheduler runs the listener, the logs in the hooks are not shown in the output of the scheduler.
#       Only the code that is executed when running the file is piped to the output of the scheduler.
#       Therefore we create a dedicated log file.

# Add file handler for detailed provenance listener logging
import tempfile
log_file = os.path.join(tempfile.gettempdir(), 'airflow_provider_aiida_provenance_listener.log')
file_handler = logging.FileHandler(log_file, mode='a')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)

logger.info(f"Provenance listener logging initialized, writing to: {log_file}")



def add_incoming(self, source: 'Node', link_type: LinkType, link_label: str) -> None:
    """Add a link of the given type from a given node to ourself. Skipping validation.

    :param source: the node from which the link is coming
    :param link_type: the link type
    :param link_label: the link label
    :raise TypeError: if `source` is not a Node instance or `link_type` is not a `LinkType` enum
    :raise ValueError: if the proposed link is invalid
    """
    if self._node.is_stored and source.is_stored:
        self._node.backend_entity.add_incoming(source.backend_entity, link_type, link_label)
    else:
        self._add_incoming_cache(source, link_type, link_label)

def _should_skip_provenance_from_dag_id(dag_id: str) -> bool:
    """Check if a DAG has the 'not_store_provenance' tag.

    Uses SerializedDagModel to avoid loading the full DAG and causing DetachedInstanceError.

    :param dag_id: The DAG ID to check
    :return: True if provenance tracking should be skipped
    """
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow_provider_aiida.dag import AiidaDAG

    try:
        # Query SerializedDagModel directly by dag_id (primary key)
        serialized_dag = SerializedDagModel.get(dag_id)
        if serialized_dag and serialized_dag.data:
            tags = serialized_dag.data.get('dag', {}).get('tags', [])
            logger.info(f"Provenance check via SerializedDagModel: dag_id={dag_id}, tags={tags}, has_tag={AiidaDAG.PROVENANCE_TAG in tags}")
            if AiidaDAG.PROVENANCE_TAG in tags:
                return True
        else:
            logger.warning(f"SerializedDagModel not found or has no data for dag_id={dag_id}")
    except Exception as e:
        logger.warning(f"Could not check DAG tags for {dag_id}: {e}")

    return False

def _from_dag_run_should_skip_provenance(dag_run) -> bool:
    """Check if a DAG has the 'not_store_provenance' tag by accessing dag_run.dag.

    WARNING: This may cause DetachedInstanceError if dag_run.dag triggers lazy loading.
    Prefer using _should_skip_provenance_from_dag_id instead.
    """
    try:
        from airflow_provider_aiida.dag import AiidaDAG
        tags = dag_run.dag.tags
        logger.info(f"Provenance check via dag_run.dag: dag_id={dag_run.dag_id}, tags={tags}, has_tag={AiidaDAG.PROVENANCE_TAG in tags}")
        return AiidaDAG.PROVENANCE_TAG in tags
    except Exception as e:
        logger.warning(f"Could not check DAG tags via dag_run for {dag_run.dag_id}: {e}")
        return False


def _add_dag_params_as_inputs(dag_run: DagRun, workchain_node: 'WorkChainNode'):
    """Add DAG params as INPUT_WORK links to the WorkChainNode.

    :param dag_run: The Airflow DAG run
    """
    from airflow.models import DagBag
    from aiida.orm import to_aiida_type
    from aiida import load_profile
    import functools

    # Extract all needed attributes immediately to avoid DetachedInstanceError
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id

    try:
        # Check if this DAG should skip provenance tracking
        if _from_dag_run_should_skip_provenance(dag_run):
            logger.debug(f"Skipping param input links for DAG with 'not_store_provenance' tag: {dag_id}")
            return


        # Get DAG params - accessing dag_run.dag may cause DetachedInstanceError, so handle carefully
        dag = None
        try:
            dag = DagBag().get_dag(dag_id)
        except Exception as e:
            logger.warning(f"Could not access dag because of exception: {e}. Skipping adding parameters to WorkChainNode {workchain_node}.")

        if dag is None:
            return

        dag_params = {key: dag_run.conf.get(key, value) for key, value in dag.params.items()}

        # If workchain_node is already stored, we need to use monkey-patched add_incoming to avoid validation
        if workchain_node.is_stored:
            partial_add_incoming = functools.partial(add_incoming, workchain_node.base.links)
            workchain_node.base.links.add_incoming = partial_add_incoming

    
        # Convert each param to an AiiDA Data node and create INPUT_WORK link
        for param_name in dag_params:
            param_value = dag_params[param_name]
            try:
                # Check if input link already exists
                link_label = _sanitize_link_label(param_name)
                existing_inputs = workchain_node.base.links.get_incoming(
                    link_type=LinkType.INPUT_WORK, link_label_filter=link_label
                ).all()

                if existing_inputs:
                    logger.debug(f"INPUT_WORK link '{link_label}' already exists for WorkChainNode {workchain_node.pk}")
                    continue

                # Convert param value to AiiDA Data node
                try:
                    data_node = to_aiida_type(param_value.value)
                except Exception as e:
                    logger.warning(f"Could not convert param '{param_name}' to AiiDA type: {e}")
                    continue

                # Store the data node if not already stored
                if not data_node.is_stored:
                    data_node.store()

                # Create INPUT_WORK link
                workchain_node.base.links.add_incoming(
                    data_node, link_type=LinkType.INPUT_WORK, link_label=link_label
                )
                logger.info(f"Created INPUT_WORK link '{link_label}' -> WorkChainNode {workchain_node.pk}")

            except Exception as e:
                logger.warning(f"Failed to add param '{param_name}' as input to WorkChainNode: {e}")

        # Store workchain_node if it wasn't already stored
        if not workchain_node.is_stored:
            workchain_node.store()

    except Exception as e:
        import traceback
        logger.warning(f"Failed to add DAG params as inputs for {dag_id}/{run_id}: {e}")
        logger.warning(f"Traceback:\n{traceback.format_exc()}")


def _update_workchain_from_dag_run(dag_run: DagRun):
    """Update WorkChainNode state based on DAG run state.

    Creates the WorkChainNode if it doesn't exist yet.

    :param dag_run: The Airflow DAG run
    """
    from airflow.utils.state import DagRunState
    from plumpy.process_states import ProcessState
    from aiida import load_profile

    # Extract all needed attributes immediately to avoid DetachedInstanceError
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    dag_state = dag_run.state

    try:
        # Check if this DAG should skip provenance tracking
        if _from_dag_run_should_skip_provenance(dag_run):
            logger.debug(f"Skipping WorkChainNode creation for DAG with 'not_store_provenance' tag: {dag_id}")
            return

        load_profile()

        # Get or create the WorkChainNode
        workchain_node, created = _get_or_create_workchain_node(dag_id, run_id)

        # TODO globals
        # Map DAG run state to AiiDA ProcessState
        dag_state_mapping = {
            DagRunState.QUEUED: ProcessState.CREATED,
            DagRunState.RUNNING: ProcessState.RUNNING,
            DagRunState.SUCCESS: ProcessState.FINISHED,
            DagRunState.FAILED: ProcessState.EXCEPTED,
        }

        aiida_state = dag_state_mapping.get(dag_state)
        if aiida_state:
            # Store the node if not already stored
            if not workchain_node.is_stored:
                workchain_node.store()

            workchain_node.set_process_state(aiida_state)
            workchain_node.base.extras.set('airflow_dag_run_state', str(dag_state))
            logger.info(f"Updated WorkChainNode {workchain_node.pk} to state {aiida_state} (DAG state: {dag_state})")

    except Exception as e:
        logger.warning(f"Failed to update WorkChainNode for DAG run {dag_id}/{run_id}: {e}")


def _ensure_call_calc_link(task_instance: TaskInstance):
    """Ensure CALL_CALC link exists from WorkChainNode to CalcJobNode.

    This function checks if a WorkChainNode exists for the DAG run, creates or retrieves
    the CalcJobNode, and ensures a CALL_CALC link is created between them if it doesn't
    already exist.

    :param task_instance: The Airflow task instance
    :return: True if processing should continue, False if should skip (e.g., for AiidaDAG)
    """
    from aiida.common.links import LinkType
    from aiida import load_profile

    # Extract all needed attributes immediately to avoid DetachedInstanceError
    dag_id = task_instance.dag_id
    run_id = task_instance.run_id
    task_id = task_instance.task_id
    map_index = getattr(task_instance, 'map_index', -1)

    try:
        load_profile()

        # Try to find existing WorkChainNode for this DAG run
        workchain_node = _get_workchain_node(dag_id, run_id)

        if workchain_node is None:
            # No WorkChainNode exists = this is an AiidaDAG, skip
            logger.info(f"No WorkChainNode found for {dag_id}/{run_id}, skipping (likely AiidaDAG)")
            return False

        # Get or create the CalcJobNode for the task
        calc_node = _get_or_create_calcjob_node(
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            map_index=map_index
        )

        # Check if the calc_node already has an incoming CALL_CALC link
        existing_call_links = calc_node.base.links.get_incoming(
            link_type=LinkType.CALL_CALC
        ).all()

        has_call_calc_link = len(existing_call_links) > 0

        if not has_call_calc_link:
            # Create CALL_CALC link from WorkChain to CalcJob
            logger.info(f"Creating CALL_CALC link: WorkChain {workchain_node.pk} -> CalcJob {calc_node.pk}")

            # If calc_node is already stored, we need to use the monkey-patched add_incoming
            if calc_node.is_stored:
                import functools
                partial_add_incoming = functools.partial(add_incoming, calc_node.base.links)
                calc_node.base.links.add_incoming = partial_add_incoming

            call_label = _sanitize_link_label(task_id)
            calc_node.base.links.add_incoming(
                workchain_node, link_type=LinkType.CALL_CALC, link_label=call_label
            )
        else:
            logger.debug(f"CALL_CALC link already exists for CalcJob {calc_node.pk}")

        # Store nodes if not already stored
        if not workchain_node.is_stored:
            workchain_node.store()

        if not calc_node.is_stored:
            calc_node.store()

        return True

    except Exception as e:
        logger.warning(f"Failed to ensure CALL_CALC link for task {dag_id}/{run_id}/{task_id}: {e}")
        return True  # Continue with state update even if link creation failed


def _update_calcjob_from_task_instance(task_instance: TaskInstance):
    """Update CalcJobNode state based on task instance state.

    Creates the CalcJobNode if it doesn't exist yet.

    :param task_instance: The Airflow task instance
    """
    from airflow.utils.state import TaskInstanceState
    from airflow.models import DagBag
    from airflow_provider_aiida.dag import AiidaDAG
    from plumpy.process_states import ProcessState
    from aiida.common.links import LinkType
    from aiida import load_profile

    # Extract all needed attributes immediately to avoid DetachedInstanceError
    dag_id = task_instance.dag_id
    run_id = task_instance.run_id
    task_id = task_instance.task_id
    map_index = getattr(task_instance, 'map_index', -1)
    task_state = task_instance.state

    try:
        load_profile()

        # Try to find existing WorkChainNode for this DAG run
        # If it doesn't exist, this is an AiidaDAG and we should skip tracking
        workchain_node = _get_workchain_node(dag_id, run_id)

        if workchain_node is None:
            # No WorkChainNode exists = this is an AiidaDAG, skip CalcJobNode creation
            logger.info(f"No WorkChainNode found for {dag_id}/{run_id}, skipping CalcJobNode creation (likely AiidaDAG)")
            return

        # Get or create the CalcJobNode for the task
        calcjob_node = _get_or_create_calcjob_node(
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            map_index=map_index
        )

        # Create CALL_CALC link from WorkChain to CalcJob if both are unstored
        if not workchain_node.is_stored and not calcjob_node.is_stored:
            call_label = _sanitize_link_label(task_id)
            calcjob_node.base.links.add_incoming(
                workchain_node, link_type=LinkType.CALL_CALC, link_label=call_label
            )

        # Store nodes if not already stored
        if not workchain_node.is_stored:
            workchain_node.store()

        if not calcjob_node.is_stored:
            calcjob_node.store()

        # TODO globals
        # Map task instance state to AiiDA ProcessState
        task_state_mapping = {
            TaskInstanceState.QUEUED: ProcessState.CREATED,
            TaskInstanceState.SCHEDULED: ProcessState.CREATED,
            TaskInstanceState.RUNNING: ProcessState.RUNNING,
            TaskInstanceState.DEFERRED: ProcessState.WAITING,
            TaskInstanceState.SUCCESS: ProcessState.FINISHED,
            TaskInstanceState.FAILED: ProcessState.EXCEPTED,
            TaskInstanceState.UPSTREAM_FAILED: ProcessState.EXCEPTED,
            TaskInstanceState.SKIPPED: ProcessState.KILLED,
            TaskInstanceState.REMOVED: ProcessState.KILLED,
            TaskInstanceState.UP_FOR_RETRY: ProcessState.WAITING,
            TaskInstanceState.UP_FOR_RESCHEDULE: ProcessState.WAITING,
            TaskInstanceState.RESTARTING: ProcessState.WAITING,
        }

        aiida_state = task_state_mapping.get(task_state)
        if aiida_state:
            calcjob_node.set_process_state(aiida_state)
            calcjob_node.base.extras.set('airflow_task_state', str(task_state))
            logger.info(f"Updated CalcJobNode {calcjob_node.pk} to state {aiida_state} (task state: {task_state})")

    except Exception as e:
        logger.warning(f"Failed to update CalcJobNode for task {dag_id}/{run_id}/{task_id}: {e}")


class ProvenanceListener:
    """Listener class for AiiDA provenance updates."""

    # NOTE: not executed in testruns
    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
        """Called when a DAG run starts running.

        Updates the WorkChainNode state to RUNNING.

        :param dag_run: The DAG run that started running
        :param msg: Additional message
        """
        logger.info(f"DAG run running: {dag_run.dag_id}/{dag_run.run_id}")
        _update_workchain_from_dag_run(dag_run)

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        """Called when a DAG run completes successfully.

        Updates the WorkChainNode state to FINISHED and adds DAG params as INPUT_WORK links.

        :param dag_run: The DAG run that succeeded
        :param msg: Additional message
        """
        logger.info(f"DAG run succeeded: {dag_run.dag_id}/{dag_run.run_id}")
        _update_workchain_from_dag_run(dag_run)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
        """Called when a DAG run fails.

        Updates the WorkChainNode state to EXCEPTED.

        :param dag_run: The DAG run that failed
        :param msg: Additional message
        """
        logger.info(f"DAG run failed: {dag_run.dag_id}/{dag_run.run_id}")
        _update_workchain_from_dag_run(dag_run)


    @hookimpl
    def on_task_instance_success(
        self,
        previous_state: Optional[str],
        task_instance: TaskInstance,
    ):
        """Called when a task instance completes successfully.

        Updates the CalcJobNode state to FINISHED.

        :param previous_state: The previous state of the task instance
        :param task_instance: The task instance that succeeded
        """
        logger.info(f"Task succeeded: {task_instance.dag_id}/{task_instance.run_id}/{task_instance.task_id}")

        # Ensure CALL_CALC link exists (skip if AiidaDAG)
        if not _ensure_call_calc_link(task_instance):
            return

        # Update the CalcJobNode state
        _update_calcjob_from_task_instance(task_instance)
    
    # TODO streamline usage of id and object
    @hookimpl
    def on_task_instance_running(
        self,
        previous_state: Optional[str],
        task_instance: TaskInstance,
    ):
        """Called when a task instance starts running.
 
        Updates the CalcJobNode state to RUNNING.
 
        :param previous_state: The previous state of the task instance
        :param task_instance: The task instance that started running
        """
        logger.debug(f"Task running: {task_instance.dag_id}/{task_instance.run_id}/{task_instance.task_id}")

        # NOTE: we create the workchain node in the task since the dag running hook is skipped in test runs
        if not _should_skip_provenance_from_dag_id(task_instance.dag_id):
            workchain_node, workchain_node_created = _get_or_create_workchain_node(task_instance.dag_id, task_instance.run_id)
            # if the node was created we need to connect the dag input parameter
            if workchain_node_created:
                from airflow.models import DagRun
                # TODO error handling
                dag_run = DagRun.find(dag_id=task_instance.dag_id, run_id=task_instance.run_id)[0]
                _add_dag_params_as_inputs(dag_run, workchain_node)

        # Ensure CALL_CALC link exists (skip if AiidaDAG)
        if not _ensure_call_calc_link(task_instance):
            return

        # Update the CalcJobNode state
        _update_calcjob_from_task_instance(task_instance)

    @hookimpl
    def on_task_instance_failed(
        self,
        previous_state: Optional[str],
        task_instance: TaskInstance,
    ):
        """Called when a task instance fails.

        Updates the CalcJobNode state to EXCEPTED.

        :param previous_state: The previous state of the task instance
        :param task_instance: The task instance that failed
        """
        logger.info(f"Task failed: {task_instance.dag_id}/{task_instance.run_id}/{task_instance.task_id}")

        # Ensure CALL_CALC link exists (skip if AiidaDAG)
        if not _ensure_call_calc_link(task_instance):
            return

        # Update the CalcJobNode state
        _update_calcjob_from_task_instance(task_instance)


class ProvenanceListenerPlugin(AirflowPlugin):
    """Airflow plugin to register the AiiDA provenance listener."""

    name = "aiida_provenance_listener"
    listeners = [ProvenanceListener()]
