import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from airflow.models import DagRun, TaskInstance
from airflow.plugins_manager import AirflowPlugin
from airflow.sdk.definitions.param import Param
from airflow.models import Param as ModelsParam
from airflow.listeners import hookimpl
from aiida import load_profile, orm
from aiida.common.links import LinkType
import json

# Add dags directory to path for CalcJobTaskGroup import
# sys.path.append('/home/geiger_j/aiida_projects/aiida-airflow/git-repos/airflow-prototype/dags/')
# from calcjob_inheritance import CalcJobTaskGroup

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
        if isinstance(node, orm.WorkflowNode):
            link_type = LinkType.INPUT_WORK
        elif isinstance(node, orm.CalculationNode):
            link_type = LinkType.INPUT_CALC

        if aiida_data is not None:
            try:
                # Store the data node first
                aiida_data.store()
                # Then add the link
                node.base.links.add_incoming(
                    aiida_data, link_type=link_type, link_label=link_label
                )
            except ValueError as e:
                # Link already exists or other constraint violation
                logger.debug(f"Could not link {link_label}: {e}")


def should_create_calcjob_node_for_taskgroup(task_instance: TaskInstance) -> bool:
    """
    Determine if a task instance is part of a CalcJobTaskGroup.

    This checks if the task is the "parse" task of a CalcJobTaskGroup,
    which signals completion of the entire group.

    Args:
        task_instance: Airflow task instance

    Returns:
        bool: True if this is a parse task from a CalcJobTaskGroup
    """
    # Check if task_id indicates it's a parse task in a task group
    if ".parse" in task_instance.task_id:
        # Verify parent group exists and has the expected structure
        group_id = task_instance.task_id.rsplit(".parse", 1)[0]

        # Check if this is likely a CalcJobTaskGroup by looking for sibling tasks
        dag_run = task_instance.dag_run
        if dag_run:
            task_instances = dag_run.get_task_instances()
            # Look for the prepare task in the same group
            for ti in task_instances:
                if ti.task_id == f"{group_id}.prepare":
                    return True

    return False


def _get_taskgroup_id_from_parse_task(task_instance: TaskInstance) -> str:
    """Extract the task group ID from a parse task's task_id"""
    return task_instance.task_id.rsplit(".parse", 1)[0]


def _store_taskgroup_inputs(
    node: orm.CalcJobNode, task_instance: TaskInstance, dag_run: DagRun
) -> None:
    """
    Store all inputs for a CalcJobTaskGroup.

    Inputs come from:
    1. The prepare task's XCom outputs (to_upload_files, submission_script, to_receive_files)
    2. The CalcJobTaskGroup instance's parameters (x, y, sleep, etc.)
    3. DAG-level params and conf

    Args:
        node: The CalcJobNode to link inputs to
        task_instance: The parse task instance (end of the group)
        dag_run: The DAG run containing the task
    """
    group_id = _get_taskgroup_id_from_parse_task(task_instance)
    prepare_task_id = f"{group_id}.prepare"

    # Get the prepare task instance to access its XCom data
    prepare_ti = None
    for ti in dag_run.get_task_instances():
        if ti.task_id == prepare_task_id:
            prepare_ti = ti
            break

    if not prepare_ti:
        logger.warning(f"Could not find prepare task {prepare_task_id}")
        return

    # Store prepare task outputs as inputs to the CalcJob
    try:
        to_upload_files = task_instance.xcom_pull(
            task_ids=prepare_task_id, key="to_upload_files"
        )
        if to_upload_files:
            aiida_data = _convert_to_aiida_data(to_upload_files)
            if aiida_data:
                aiida_data.store()
                node.base.links.add_incoming(
                    aiida_data,
                    link_type=LinkType.INPUT_CALC,
                    link_label="to_upload_files",
                )
    except Exception as e:
        logger.debug(f"Could not store to_upload_files: {e}")

    try:
        submission_script = task_instance.xcom_pull(
            task_ids=prepare_task_id, key="submission_script"
        )
        if submission_script:
            aiida_data = _convert_to_aiida_data(submission_script)
            if aiida_data:
                aiida_data.store()
                node.base.links.add_incoming(
                    aiida_data,
                    link_type=LinkType.INPUT_CALC,
                    link_label="submission_script",
                )
    except Exception as e:
        logger.debug(f"Could not store submission_script: {e}")

    try:
        to_receive_files = task_instance.xcom_pull(
            task_ids=prepare_task_id, key="to_receive_files"
        )
        if to_receive_files:
            aiida_data = _convert_to_aiida_data(to_receive_files)
            if aiida_data:
                aiida_data.store()
                node.base.links.add_incoming(
                    aiida_data,
                    link_type=LinkType.INPUT_CALC,
                    link_label="to_receive_files",
                )
    except Exception as e:
        logger.debug(f"Could not store to_receive_files: {e}")

    # Store DAG-level params and conf
    if dag_run.conf:
        _store_params_as_aiida_inputs(node, dag_run.conf, prefix="conf")

    dag_params = getattr(dag_run.dag, "params", {})
    if dag_params:
        _store_params_as_aiida_inputs(node, dag_params, prefix="dag_param")


def _store_taskgroup_outputs(
    node: orm.CalcJobNode, task_instance: TaskInstance
) -> None:
    """
    Store all outputs from a CalcJobTaskGroup.

    Outputs come from the parse task's XCom data (final_result).

    Args:
        node: The CalcJobNode to link outputs to
        task_instance: The parse task instance
    """
    try:
        # Get the final_result from the parse task
        final_result = task_instance.xcom_pull(
            task_ids=task_instance.task_id, key="final_result"
        )

        if final_result:
            # Handle tuple format (exit_status, results) from AddJobTaskGroup
            if isinstance(final_result, tuple) and len(final_result) == 2:
                exit_status, results = final_result

                # Store exit status
                exit_status_node = orm.Int(exit_status)
                exit_status_node.store()
                exit_status_node.base.links.add_incoming(
                    node, link_type=LinkType.CREATE, link_label="exit_status"
                )

                # Store results dict
                if results:
                    for key, value in results.items():
                        aiida_data = _convert_to_aiida_data(value)
                        if aiida_data:
                            aiida_data.store()
                            aiida_data.base.links.add_incoming(
                                node,
                                link_type=LinkType.CREATE,
                                link_label=f"result_{key}",
                            )

            # Handle dict format from MultiplyJobTaskGroup
            elif isinstance(final_result, dict):
                for key, value in final_result.items():
                    aiida_data = _convert_to_aiida_data(value)
                    if aiida_data:
                        aiida_data.store()
                        aiida_data.base.links.add_incoming(
                            node, link_type=LinkType.CREATE, link_label=f"result_{key}"
                        )

            # Handle other formats
            else:
                aiida_data = _convert_to_aiida_data(final_result)
                if aiida_data:
                    aiida_data.store()
                    aiida_data.base.links.add_incoming(
                        node, link_type=LinkType.CREATE, link_label="final_result"
                    )

    except Exception as e:
        logger.warning(
            f"Could not retrieve outputs for task {task_instance.task_id}: {e}"
        )


def _create_calcjob_node_from_taskgroup(
    task_instance: TaskInstance,
    parent_workchain_node: orm.WorkChainNode,
    dag_run: DagRun,
) -> orm.CalcJobNode:
    """
    Create an AiiDA CalcJobNode from a CalcJobTaskGroup (represented by its parse task).

    Args:
        task_instance: The parse task instance (end of the TaskGroup)
        parent_workchain_node: The parent WorkChainNode for the DAG
        dag_run: The DAG run

    Returns:
        The created and stored CalcJobNode
    """
    group_id = _get_taskgroup_id_from_parse_task(task_instance)

    node = orm.CalcJobNode()
    node.label = f"airflow_calcjob_group_{group_id}"
    node.description = f"CalcJob from Airflow TaskGroup {group_id}"

    # Store Airflow metadata in extras
    node.base.extras.set("airflow_dag_id", task_instance.dag_id)
    node.base.extras.set("airflow_run_id", task_instance.run_id)
    node.base.extras.set("airflow_task_group_id", group_id)

    # Set process metadata
    node.set_process_type(f"airflow.CalcJobTaskGroup")
    node.set_process_state("finished")

    # Determine exit status from parse task result
    exit_status = 0
    try:
        final_result = task_instance.xcom_pull(
            task_ids=task_instance.task_id, key="final_result"
        )
        if isinstance(final_result, tuple) and len(final_result) == 2:
            exit_status = final_result[0]
    except Exception:
        pass

    node.set_exit_status(exit_status if task_instance.state == "success" else 1)

    # Link to parent WorkChainNode (before storing)
    if parent_workchain_node:
        node.base.links.add_incoming(
            parent_workchain_node,
            link_type=LinkType.CALL_CALC,
            link_label=group_id,
        )

    # Add inputs BEFORE storing the node
    _store_taskgroup_inputs(node, task_instance, dag_run)

    # Now store the node (inputs are locked in)
    node.store()

    # Outputs can be added after storing
    _store_taskgroup_outputs(node, task_instance)

    logger.info(f"Created CalcJobNode {node.pk} for TaskGroup {group_id}")
    return node


def _should_integrate_dag_with_aiida(dag_run: DagRun) -> bool:
    """Check if this DAG should be stored in AiiDA"""
    dag_tags = getattr(dag_run.dag, "tags", [])
    # Look for tags that indicate this is a CalcJob workflow
    return any(tag in dag_tags for tag in ["aiida", "calcjob", "taskgroup"])


def _create_workchain_node_with_inputs(dag_run: DagRun) -> orm.WorkChainNode:
    """
    Create a WorkChainNode from a running Airflow DAG and store its inputs.

    Returns:
        The created and stored WorkChainNode
    """
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

    workchain_node.set_process_state("running")
    workchain_node.store()

    logger.info(f"Created WorkChainNode {workchain_node.pk} for DAG {dag_run.dag_id}")
    breakpoint()
    return workchain_node


def _finalize_workchain_node_with_outputs(dag_run: DagRun) -> None:
    """
    Find the WorkChainNode for a completed DAG run and add outputs (CalcJobNodes from TaskGroups).
    If the WorkChainNode doesn't exist yet, create it first.
    """
    from aiida.orm import QueryBuilder

    # Try to find the WorkChainNode created in on_dag_run_running
    qb = QueryBuilder()
    qb.append(
        orm.WorkChainNode,
        filters={"extras.airflow_run_id": dag_run.run_id},
        tag="workchain",
    )
    results = qb.all()

    if not results:
        # WorkChainNode doesn't exist yet - create it now with inputs
        logger.warning(
            f"WorkChainNode not found for run_id {dag_run.run_id}. "
            f"Creating it now (on_dag_run_running may not have been called)."
        )
        workchain_node = _create_workchain_node_with_inputs(dag_run)
    else:
        workchain_node = results[0][0]

    # Update process state to finished
    workchain_node.set_process_state("finished")
    workchain_node.set_exit_status(0)

    # Process each task in the DAG to find CalcJobTaskGroup parse tasks
    task_instances = dag_run.get_task_instances()
    for ti in task_instances:
        if ti.state == "success" and should_create_calcjob_node_for_taskgroup(ti):
            _create_calcjob_node_from_taskgroup(ti, workchain_node, dag_run)

    logger.info(f"Finalized WorkChainNode {workchain_node.pk} for DAG {dag_run.dag_id}")


# Airflow Listener Plugin
class AiiDATaskGroupIntegrationListener:
    """Listener that integrates Airflow CalcJobTaskGroups with AiiDA provenance"""

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
        """Called when a DAG run enters the running state."""
        logger.info(f"DAG run started: {dag_run.dag_id}/{dag_run.run_id}")

        if _should_integrate_dag_with_aiida(dag_run):
            logger.info(f"Creating WorkChainNode for DAG {dag_run.dag_id}")
            try:
                _create_workchain_node_with_inputs(dag_run)
            except Exception as e:
                logger.error(
                    f"Failed to create AiiDA WorkChainNode: {e}", exc_info=True
                )

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        """Called when a DAG run completes successfully."""
        logger.info(f"DAG run succeeded: {dag_run.dag_id}/{dag_run.run_id}")

        if _should_integrate_dag_with_aiida(dag_run):
            logger.info(f"Finalizing WorkChainNode for DAG {dag_run.dag_id}")
            try:
                _finalize_workchain_node_with_outputs(dag_run)
            except Exception as e:
                logger.error(f"Failed to finalize AiiDA provenance: {e}", exc_info=True)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
        """Called when a DAG run fails."""
        logger.info(f"DAG run failed: {dag_run.dag_id}/{dag_run.run_id}")
        # Optionally store failed runs in AiiDA with appropriate exit status


# Create listener instance
aiida_taskgroup_listener = AiiDATaskGroupIntegrationListener()


# Plugin registration
class AiidaDagRunListener(AirflowPlugin):
    name = "aiida_dag_run_listener"
    listeners = [aiida_taskgroup_listener]
