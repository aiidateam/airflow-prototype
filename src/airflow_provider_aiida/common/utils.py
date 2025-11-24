"""Common ORM utilities for AiiDA nodes in the Airflow provider."""

import re
import logging
import os

logger = logging.getLogger(__name__)

# Add file handler for detailed ORM logging
log_file = os.path.expanduser('~/airflow_provider_aiida_orm.log')
file_handler = logging.FileHandler(log_file, mode='a')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
# TODO for prototyping phase we put it on DEBUG
logger.setLevel(logging.DEBUG)

logger.info(f"ORM logging initialized, writing to: {log_file}")


def _sanitize_link_label(label: str) -> str:
    """Sanitize a string to be a valid AiiDA link label.

    AiiDA link labels must contain only alphanumeric characters and underscores.

    :param label: The label to sanitize
    :return: Sanitized label with only valid characters
    """
    # Replace any non-alphanumeric, non-underscore characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', label)
    # Ensure it doesn't start with a number (prepend 'x' if it does)
    if sanitized and sanitized[0].isdigit():
        sanitized = 'x' + sanitized
    return sanitized or 'result'


def _get_workchain_node(dag_id: str, run_id: str):
    """Get an existing WorkChainNode for an Airflow DAG run.

    Returns None if no node exists.

    :param dag_id: Airflow DAG ID
    :param run_id: Airflow run ID
    :return: WorkChainNode instance or None
    """
    from aiida.orm import WorkChainNode, QueryBuilder

    # Create a unique identifier for this DAG run
    unique_id = f"{dag_id}__{run_id}"

    # Check if node already exists using the unique_id stored in extras
    qb = QueryBuilder()
    qb.append(WorkChainNode, filters={'extras.airflow_unique_id': unique_id})
    results = qb.all()

    if results:
        existing_node = results[0][0]
        logger.debug(f"Found existing WorkChainNode: dag_id={dag_id}, run_id={run_id}, pk={existing_node.pk}")
        return existing_node

    logger.debug(f"No WorkChainNode found: dag_id={dag_id}, run_id={run_id}")
    return None

def _get_or_create_workchain_node(dag_id: str, run_id: str):
    """Get or create a WorkChainNode representing an Airflow DAG run.

    Sets initial state to CREATED when creating a new node.

    :param dag_id: Airflow DAG ID
    :param run_id: Airflow run ID
    :return: Tuple of (WorkChainNode instance, created: bool)
             created is True if a new node was created, False if existing node was found
    """
    from aiida import load_profile
    from aiida.orm import WorkChainNode, QueryBuilder
    from plumpy.process_states import ProcessState
    load_profile()

    # Create a unique identifier for this DAG run
    unique_id = f"{dag_id}__{run_id}"

    # Check if node already exists using the unique_id stored in extras
    qb = QueryBuilder()
    qb.append(WorkChainNode, filters={'extras.airflow_unique_id': unique_id})
    results = qb.all()

    if results:
        existing_node = results[0][0]
        logger.debug(f"Found existing WorkChainNode: dag_id={dag_id}, run_id={run_id}, pk={existing_node.pk}")
        return existing_node, False

    # Create new WorkChainNode for the DAG run
    logger.debug(f"Creating NEW WorkChainNode: dag_id={dag_id}, run_id={run_id}, unique_id={unique_id}")
    workchain_node = WorkChainNode()
    workchain_node.set_process_label(f"{dag_id}[{run_id}]")
    workchain_node.label = dag_id
    workchain_node.description = f"Airflow DAG run: {dag_id}, run_id: {run_id}"
    workchain_node.base.extras.set('airflow_unique_id', unique_id)
    workchain_node.base.extras.set('airflow_dag_id', dag_id)
    workchain_node.base.extras.set('airflow_run_id', run_id)
    workchain_node.base.extras.set('airflow_xcom_backend', True)

    # Set initial process state to CREATED
    workchain_node.set_process_state(ProcessState.CREATED)

    logger.info(f"Created NEW WorkChainNode: dag_id={dag_id}, run_id={run_id}, node={workchain_node}")

    return workchain_node, True


def _get_or_create_calcjob_node(task_id: str, dag_id: str, run_id: str, map_index: int = -1):
    """Get or create a CalcJobNode representing an Airflow task.

    Sets initial state to CREATED when creating a new node.

    :param task_id: Airflow task ID
    :param dag_id: Airflow DAG ID
    :param run_id: Airflow run ID
    :param map_index: Airflow map index for mapped tasks
    :return: CalcJobNode instance
    """
    from aiida import load_profile
    from aiida.orm import CalcJobNode, QueryBuilder
    from plumpy.process_states import ProcessState
    load_profile()

    # Create a unique identifier for this task execution (used in extras for lookup)
    unique_id = f"{dag_id}__{task_id}__{run_id}"
    if map_index >= 0:
        unique_id += f"__{map_index}"

    # Check if node already exists using the unique_id stored in extras
    qb = QueryBuilder()
    qb.append(CalcJobNode, filters={'extras.airflow_unique_id': unique_id})
    results = qb.all()

    if results:
        existing_node = results[0][0]
        logger.debug(f"Found existing CalcJobNode: dag_id={dag_id}, task_id={task_id}, run_id={run_id}, map_index={map_index}, pk={existing_node.pk}")
        return existing_node

    # Create new CalcJobNode with task_id as label (more readable)
    logger.warning(f"Creating NEW CalcJobNode: dag_id={dag_id}, task_id={task_id}, run_id={run_id}, map_index={map_index}, unique_id={unique_id}")
    calc_node = CalcJobNode()
    # Use task_id as label for readability, with map_index if applicable
    calc_node.set_process_label(f"{task_id}[{map_index}]" if map_index >= 0 else task_id)
    calc_node.label = task_id
    calc_node.description = f"Airflow task: {task_id} from DAG: {dag_id}, run: {run_id}"
    calc_node.base.extras.set('airflow_unique_id', unique_id)
    calc_node.base.extras.set('airflow_dag_id', dag_id)
    calc_node.base.extras.set('airflow_task_id', task_id)
    calc_node.base.extras.set('airflow_run_id', run_id)
    calc_node.base.extras.set('airflow_map_index', map_index)
    calc_node.base.extras.set('airflow_xcom_backend', True)

    # Set initial process state to CREATED
    calc_node.set_process_state(ProcessState.CREATED)

    logger.info(f"Created NEW CalcJobNode: dag_id={dag_id}, task_id={task_id}, run_id={run_id}, map_index={map_index}, node={calc_node}")

    return calc_node
