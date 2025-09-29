import logging
from airflow.models import DagRun, TaskInstance
from airflow.plugins_manager import AirflowPlugin
from airflow.sdk.definitions.param import Param
from airflow.models import Param as ModelsParam
from airflow.listeners import hookimpl
from aiida import load_profile, orm
from aiida.common.links import LinkType
from pathlib import Path
from typing import Any, Dict, Optional
import json

load_profile()

logger = logging.getLogger(__name__)


def _param_to_python(param) -> Any:
    """
    Convert an Airflow Param object to a Python native value.

    Args:
        param: Airflow Param object or any other value

    Returns:
        Python native value (int, float, bool, str, dict, list, etc.)
    """

    # Check if it's a Param object
    if not isinstance(param, (Param, ModelsParam)):
        return param

    # Get the actual value
    actual_value = param.value

    # Get schema type if available
    schema = getattr(param, "schema", {})
    param_type = schema.get("type", None)

    # Convert based on schema type
    if param_type == "integer":
        try:
            return int(actual_value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert Param value '{actual_value}' to int")
            return actual_value

    elif param_type == "number":
        try:
            return float(actual_value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert Param value '{actual_value}' to float")
            return actual_value

    elif param_type == "boolean":
        if isinstance(actual_value, bool):
            return actual_value
        # Handle string representations
        if isinstance(actual_value, str):
            return actual_value.lower() in ("true", "1", "yes", "on")
        return bool(actual_value)

    elif param_type == "string":
        return str(actual_value)

    elif param_type == "object":
        # Should already be a dict
        return actual_value if isinstance(actual_value, dict) else {}

    elif param_type == "array":
        # Should already be a list
        return actual_value if isinstance(actual_value, (list, tuple)) else []

    else:
        # No type specified or unknown type - return as-is
        return actual_value


def _convert_to_aiida_data(value: Any) -> Optional[orm.Data]:
    """
    Convert a Python value to the appropriate AiiDA Data node.

    Returns None if the value type is not supported or should be skipped.
    """
    # First check if it's an Airflow Param and convert it
    if isinstance(value, (Param, ModelsParam)):
        value = _param_to_python(value)

    # Handle basic types (check bool BEFORE int, since bool is subclass of int)
    if isinstance(value, bool):
        return orm.Bool(value)
    elif isinstance(value, int):
        return orm.Int(value)
    elif isinstance(value, float):
        return orm.Float(value)
    elif isinstance(value, str):
        return orm.Str(value)

    # Handle collections - store as Dict or List nodes
    elif isinstance(value, dict):
        return orm.Dict(dict=value)
    elif isinstance(value, (list, tuple)):
        return orm.List(list=list(value))

    # Handle Path objects
    elif isinstance(value, Path):
        return orm.Str(str(value))

    # For complex objects, try JSON serialization
    else:
        try:
            json_str = json.dumps(value)
            return orm.Str(json_str)
        except (TypeError, ValueError):
            logger.warning(
                f"Could not convert value of type {type(value)} to AiiDA node"
            )
            return None


def _store_params_as_aiida_inputs(
    node: orm.Node, params: Dict[str, Any], prefix: str = ""
) -> None:
    """
    Store parameters as AiiDA data nodes and link them as inputs.

    Args:
        node: The AiiDA node to link inputs to
        params: Dictionary of parameters to store
        prefix: Optional prefix for link labels (e.g., 'dag_params', 'conf')
    """
    for key, value in params.items():
        # Create link label with optional prefix
        link_label = f"{prefix}_{key}" if prefix else key

        # Skip None values
        if value is None:
            continue

        # Convert to AiiDA data node
        aiida_data = _convert_to_aiida_data(value)

        if aiida_data is not None:
            try:
                # Store the data node first
                aiida_data.store()
                # Then add the link
                node.base.links.add_incoming(
                    aiida_data, link_type=LinkType.INPUT_CALC, link_label=link_label
                )
            except ValueError as e:
                # Link already exists or other constraint violation
                logger.debug(f"Could not link {link_label}: {e}")


def should_create_calcjob_node(task_instance: TaskInstance) -> bool:
    """
    Determine if a task instance should be converted to a CalcJobNode.

    This is where you can implement your logic for identifying which tasks
    should be stored in AiiDA. Options:
    1. Check operator type
    2. Check task_id pattern
    3. Check for specific XCom keys
    4. Use task tags/metadata
    """
    # Option 1: Check operator type
    if "CalcJobTaskOperator" in task_instance.operator:
        return True

    # Option 2: Check task_id pattern
    if "calcjob" in task_instance.task_id.lower():
        return True

    # Option 3: Check for marker in task metadata
    task = task_instance.task
    if hasattr(task, "params") and task.params.get("aiida_store", False):
        return True

    return False


def _store_task_inputs(node: orm.CalcJobNode, task_instance: TaskInstance) -> None:
    """
    Store all inputs for a task: params and conf.

    Args:
        node: The CalcJobNode to link inputs to
        task_instance: The Airflow task instance
    """
    # import ipdb; ipdb.set_trace()
    # Store task params (static parameters defined in DAG)
    if hasattr(task_instance.task, "params") and task_instance.task.params:
        _store_params_as_aiida_inputs(
            node, task_instance.task.params, prefix="task_param"
        )

    # Store DAG run conf (dynamic parameters from trigger)
    if task_instance.dag_run and task_instance.dag_run.conf:
        _store_params_as_aiida_inputs(node, task_instance.dag_run.conf, prefix="conf")


def _store_task_outputs(node: orm.CalcJobNode, task_instance: TaskInstance) -> None:
    """
    Store all outputs from a task: XCom values.

    Args:
        node: The CalcJobNode to link outputs to
        task_instance: The Airflow task instance
    """
    # TODO: continue here, how to get outputs, XComs?
    import ipdb; ipdb.set_trace()
    try:
        # Get all XCom keys for this task
        xcom_data = task_instance.xcom_pull(task_ids=task_instance.task_id, key=None)

        if not xcom_data:
            return

        for key, value in xcom_data.items():
            # Skip return_value or handle it separately if needed
            if key == "return_value":
                continue

            # Convert to AiiDA data node
            aiida_data = _convert_to_aiida_data(value)
            if aiida_data:
                aiida_data.store()
                # Link as output (CREATE link)
                aiida_data.base.links.add_incoming(
                    node, link_type=LinkType.CREATE, link_label=f"xcom_{key}"
                )

    except Exception as e:
        logger.warning(
            f"Could not retrieve XCom outputs for task {task_instance.task_id}: {e}"
        )


def _create_calcjob_node_from_task(
    task_instance: TaskInstance, parent_workchain_node: orm.WorkChainNode
) -> orm.CalcJobNode:
    """
    Create an AiiDA CalcJobNode from an Airflow task instance.
    """
    node = orm.CalcJobNode()
    node.label = f"airflow_calcjob_{task_instance.task_id}"
    node.description = f"CalcJob from Airflow task {task_instance.task_id}"

    # Store Airflow metadata in extras
    node.base.extras.set("airflow_dag_id", task_instance.dag_id)
    node.base.extras.set("airflow_run_id", task_instance.run_id)
    node.base.extras.set("airflow_task_id", task_instance.task_id)

    # Set process metadata
    node.set_process_type(f"airflow.{task_instance.operator}")
    node.set_process_state("finished")
    node.set_exit_status(0 if task_instance.state == "success" else 1)

    # Link to parent WorkChainNode (before storing)
    if parent_workchain_node:
        node.base.links.add_incoming(
            parent_workchain_node,
            link_type=LinkType.CALL_CALC,
            link_label=task_instance.task_id,
        )

    # Add inputs BEFORE storing the node
    _store_task_inputs(node, task_instance)

    # Now store the node (inputs are locked in)
    node.store()

    # Outputs can be added after storing
    # import ipdb; ipdb.set_trace()
    _store_task_outputs(node, task_instance)

    logger.info(f"Created CalcJobNode {node.pk} for task {task_instance.task_id}")
    return node


def _should_integrate_dag_with_aiida(dag_run: DagRun) -> bool:
    """Check if this DAG should be stored in AiiDA"""
    dag_tags = getattr(dag_run.dag, "tags", [])
    return "aiida" in dag_tags


def _create_workchain_node_from_dag(dag_run: DagRun) -> None:
    """
    Create a WorkChainNode from a successful Airflow DAG run.

    Now completely generic - stores all params and conf without assumptions.
    """
    # Create the WorkChainNode for the entire DAG
    workchain_node = orm.WorkChainNode()
    workchain_node.label = f"airflow_dag_{dag_run.dag_id}"
    workchain_node.description = f"Workflow from Airflow DAG {dag_run.dag_id}"

    workchain_node.base.extras.set("airflow_dag_id", dag_run.dag_id)
    workchain_node.base.extras.set("airflow_run_id", dag_run.run_id)

    # Store ALL DAG parameters generically
    dag_params = getattr(dag_run.dag, "params", {})
    if dag_params:
        _store_params_as_aiida_inputs(workchain_node, dag_params, prefix="dag_param")

    # Store ALL DAG configuration generically
    dag_conf = getattr(dag_run, "conf", {})
    if dag_conf:
        _store_params_as_aiida_inputs(workchain_node, dag_conf, prefix="conf")

    workchain_node.set_process_state("finished")
    workchain_node.set_exit_status(0)
    workchain_node.store()

    # Process each task in the DAG
    task_instances = dag_run.get_task_instances()

    for ti in task_instances:
        # Generic detection: process any successful task
        # You can add filters here if needed (e.g., by operator type)
        if ti.state == "success":
            # Check if this is a CalcJob-like task
            # You might want to add metadata to your tasks to identify them
            if should_create_calcjob_node(ti):
                _create_calcjob_node_from_task(ti, workchain_node)

    logger.info(f"Created WorkChainNode {workchain_node.pk} for DAG {dag_run.dag_id}")


# Airflow Listener Plugin
class AiiDAIntegrationListener:
    """Listener that integrates Airflow DAG runs with AiiDA provenance"""

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
        """Called when a DAG run enters the running state."""
        logger.info(f"DAG run started: {dag_run.dag_id}/{dag_run.run_id}")

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        """Called when a DAG run completes successfully."""
        logger.info(f"DAG run succeeded: {dag_run.dag_id}/{dag_run.run_id}")

        if _should_integrate_dag_with_aiida(dag_run):
            logger.info(f"Creating WorkChainNode for DAG {dag_run.dag_id}")
            try:
                _create_workchain_node_from_dag(dag_run)
            except Exception as e:
                logger.error(f"Failed to create AiiDA provenance: {e}", exc_info=True)
        else:
            logger.info(
                f"Skipping AiiDA integration for DAG {dag_run.dag_id} (no 'aiida' tag)"
            )

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
        """Called when a DAG run fails."""
        logger.info(f"DAG run failed: {dag_run.dag_id}/{dag_run.run_id}")
        # Optionally store failed runs in AiiDA with appropriate exit status


# Create listener instance
aiida_listener = AiiDAIntegrationListener()


# Plugin registration
class AiiDAIntegrationPlugin(AirflowPlugin):
    name = "aiida_integration_plugin"
    listeners = [aiida_listener]
