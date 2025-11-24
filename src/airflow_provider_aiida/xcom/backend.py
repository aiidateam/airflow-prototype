from airflow.models.xcom import BaseXCom
from typing import Any
import json
import logging

from airflow_provider_aiida.common.utils import (
    _sanitize_link_label,
    _get_or_create_workchain_node,
    _get_or_create_calcjob_node,
)

from aiida.common.links import LinkType

logger = logging.getLogger(__name__)


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


# TODO think about restarts with clear task, then the node already exists and one cannot change input. in principle we would create a new node
# TODO if there is a node that cannot be aiida serialized it than maybe we create a placeholder node?
class AiidaBackend(BaseXCom):

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> str:
        """Serialize XCom value to JSON str and create AiiDA provenance."""
        from airflow.serialization.serde import serialize
        from airflow.models import DagBag
        from airflow_provider_aiida.dag import AiidaDAG
        from aiida.orm import to_aiida_type
        from aiida import load_profile

        # Check if this is an AiidaDAG - if so, use standard serialization
        if dag_id:
            try:
                dagbag = DagBag()
                dag = dagbag.get_dag(dag_id)
                logger.info(f"XCom serialize_value: dag_id={dag_id}, dag_type={type(dag).__name__}, is_AiidaDAG={isinstance(dag, AiidaDAG)}")
                if isinstance(dag, AiidaDAG):
                    # AiidaDAG manages its own provenance - use standard XCom
                    logger.info(f"Using standard XCom serialization for AiidaDAG: {dag_id}")
                    return serialize(value)  # type: ignore[return-value]
            except Exception as e:
                logger.warning(f"Could not check DAG type for {dag_id}: {e}")

        load_profile()

        # Check if value is already an AiiDA Node
        from aiida.orm import Node
        if isinstance(value, Node):
            # Value is already an AiiDA node - just reference it
            if not value.is_stored:
                logger.warning(f"AiiDA node {value} is not stored. Storing it now.")
                value.store()

            data_node = value
            # Set extras if not already set
            if 'airflow_map_index' not in data_node.base.extras.all:
                data_node.base.extras.set('airflow_map_index', map_index)
        else:
            # Try to convert value to AiiDA Data node
            try:
                data_node = to_aiida_type(value)
                data_node.base.extras.set('airflow_map_index', map_index)
            except Exception as e:
                logger.warning(f"Could not convert value to AiiDA type: {e}. Falling back to standard serialization.")
                return serialize(value)  # type: ignore[return-value]

        # Ensure we have task metadata
        if not all([task_id, dag_id, run_id]):
            logger.warning("Missing task metadata for AiiDA provenance. Falling back to standard serialization.")
            return serialize(value)  # type: ignore[return-value]

        try:
            # Get or create WorkChainNode for the DAG run
            workchain_node, created = _get_or_create_workchain_node(dag_id, run_id)

            # Get or create CalcJobNode for the producing task
            # Use explicit None check for map_index since 0 is a valid value
            calc_node = _get_or_create_calcjob_node(
                task_id=task_id,
                dag_id=dag_id,
                run_id=run_id,
                map_index=map_index if map_index is not None else -1
            )

            # Create CALL_CALC link from WorkChain to CalcJob (if not already exists)
            if not calc_node.is_stored:
                # Check if CALL_CALC link already exists
                call_label = _sanitize_link_label(task_id)
                existing_calls = workchain_node.base.links.get_outgoing(
                    link_type=LinkType.CALL_CALC, link_label_filter=call_label
                ).all()

                if not existing_calls:
                    calc_node.base.links.add_incoming(
                        workchain_node, link_type=LinkType.CALL_CALC, link_label=call_label
                    )

            # Link the data node as output of the calc node (CREATE link)
            # Note: We need to set the link before storing
            if not data_node.is_stored:
                # Sanitize the link label to ensure it's valid for AiiDA
                # TODO or 'result' needs to better handled
                #      try except block with logger message, use return_value
                link_label = _sanitize_link_label(key or 'result')
                data_node.base.links.add_incoming(calc_node, link_type=LinkType.CREATE, link_label=link_label)

            # Store the workchain node first if not already stored
            if not workchain_node.is_stored:
                workchain_node.store()

            # Store the calc node (which also validates/stores the CALL_CALC link)
            if not calc_node.is_stored:
                calc_node.store()

            # Store the data node (this also stores the link)
            if not data_node.is_stored:
                data_node.store()

            # Return a reference to the AiiDA node
            # Include flag to indicate if original value was a Node (to return Node in deserialize)
            serialized_data = {
                'aiida_node_pk': data_node.pk,
                'aiida_node_uuid': data_node.uuid,
                'producer_task_id': task_id,
                'producer_dag_id': dag_id,
                'producer_run_id': run_id,
                'xcom_key': key,
                'return_node': isinstance(value, Node),  # Flag to return Node object vs value
            }

            return json.dumps(serialized_data)

        except Exception as e:
            logger.exception(f"Failed to create AiiDA provenance for XCom: {e}")
            return serialize(value)  # type: ignore[return-value]

    @staticmethod
    def deserialize_value(result) -> Any:
        """Deserialize XCom value from str objects and create AiiDA input links."""
        from airflow.serialization.serde import deserialize
        from airflow.sdk import get_current_context
        from airflow.models import DagBag
        from airflow_provider_aiida.dag import AiidaDAG
        from aiida.orm import load_node
        from aiida.common.links import LinkType
        from aiida import load_profile

        # Check if this is an AiidaDAG - if so, use standard deserialization
        try:
            context = get_current_context()
            ti = context["ti"]
            dag_id = ti.dag_id

            dagbag = DagBag()
            dag = dagbag.get_dag(dag_id)
            logger.info(f"XCom deserialize_value: dag_id={dag_id}, dag_type={type(dag).__name__}, is_AiidaDAG={isinstance(dag, AiidaDAG)}")
            if isinstance(dag, AiidaDAG):
                # AiidaDAG manages its own provenance - use standard XCom
                logger.info(f"Using standard XCom deserialization for AiidaDAG: {dag_id}")
                return deserialize(result.value)
        except Exception as e:
            logger.warning(f"Could not check DAG type during deserialization: {e}")

        load_profile()

        # Try to parse as AiiDA reference
        try:
            data = json.loads(result.value)
            if not isinstance(data, dict) or 'aiida_node_pk' not in data:
                # Not an AiiDA reference, fall back to standard deserialization
                return deserialize(result.value)
        except (json.JSONDecodeError, TypeError):
            # Not JSON or not our format, fall back
            return deserialize(result.value)

        try:
            # Get current task context
            context = get_current_context()
            ti = context["ti"]
            consumer_task_id = ti.task_id
            consumer_dag_id = ti.dag_id
            consumer_run_id = ti.run_id
            consumer_map_index = getattr(ti, 'map_index', -1)

            # Load the AiiDA data node
            aiida_node_pk = data['aiida_node_pk']
            data_node = load_node(pk=aiida_node_pk)

            # Get or create WorkChainNode for the DAG run
            workchain_node, created = _get_or_create_workchain_node(consumer_dag_id, consumer_run_id)

            # Get or create CalcJobNode for the consuming task
            calc_node = _get_or_create_calcjob_node(
                task_id=consumer_task_id,
                dag_id=consumer_dag_id,
                run_id=consumer_run_id,
                map_index=consumer_map_index
            )

            # Create CALL_CALC link from WorkChain to CalcJob (if not already exists)
            if not calc_node.is_stored:
                # Check if CALL_CALC link already exists
                call_label = _sanitize_link_label(consumer_task_id)
                existing_calls = workchain_node.base.links.get_outgoing(
                    link_type=LinkType.CALL_CALC, link_label_filter=call_label
                ).all()

                if not existing_calls:
                    calc_node.base.links.add_incoming(
                        workchain_node, link_type=LinkType.CALL_CALC, link_label=call_label
                    )

            
            # Monkey patching node related links to overwrite validation
            if calc_node.is_stored:
                import functools
                partial_add_incoming = functools.partial(add_incoming, calc_node.base.links)
                calc_node.base.links.add_incoming = partial_add_incoming

            # Link the data node as input to the consuming calc node
            # AiiDA constraint: input links can only be added to unstored process nodes
            # Determine link label based on task type
            from airflow.providers.standard.operators.python import PythonOperator
            import inspect
            import uuid
            import hashlib

            link_label = None

            # Since XCom backend is sometimes multiple times called in the same task we use use the id to ensure that only one link is created per data-node-to-task connection
            links = calc_node.base.links.get_incoming(link_type=LinkType.INPUT_CALC).all()
            link_exists = False
            for link in links:
                if (link_exists := link.node.pk == data_node.pk):
                    break
            
            if not link_exists:
                if issubclass(ti.task.operator_class, PythonOperator):
                    try:
                        # Get the callable's signature
                        callable_func = ti.task.python_callable
                        sig = inspect.signature(callable_func)
                        param_names = list(sig.parameters.keys())

                        # Get current input index (how many inputs already linked)
                        arg_index = len(links)

                        # Use the parameter name at this index if it exists
                        if arg_index < len(param_names):
                            link_label = _sanitize_link_label(param_names[arg_index])
                            logger.debug(f"Using parameter name '{param_names[arg_index]}' as link label (index {arg_index})")
                        else:
                            raise ValueError("Number of parameters of python callable is less than the estimated argument index.")
                    except Exception as e:
                        logger.warning(f"Failed to extract parameter name from PythonOperator: {e}")
                        link_label = _sanitize_link_label(f"input_{len(links)}")
                else:
                    # For non-Python operators, use a deterministic hash based on the producer task info
                    logger.warning(f"Failed to extract parameter name from PythonOperator")
                    link_label = _sanitize_link_label(f"input_{len(links)}")

                existing_inputs = calc_node.base.links.get_incoming(link_label_filter=link_label).all()

                if not any(link.node.pk == data_node.pk for link in existing_inputs):
                    calc_node.base.links.add_incoming(
                        data_node,
                        link_type=LinkType.INPUT_CALC,
                        link_label=link_label
                    )
                    logger.info(
                        f"Created AiiDA provenance link: {data['producer_task_id']} -> {consumer_task_id} "
                        f"(node pk={aiida_node_pk})"
                    )

            # Store the workchain node first if not already stored
            if not workchain_node.is_stored:
                workchain_node.store()

            # Store the calc node with its inputs (and CALL_CALC link)
            calc_node.store()

            # Return either the Node object or its Python value based on serialization flag
            return_node = data.get('return_node', False)
            if return_node:
                # Original value was an AiiDA Node - return the node itself
                logger.debug(f"Returning AiiDA node (pk={aiida_node_pk}) as Node object")
                return data_node
            else:
                # Original value was a Python value - return the extracted value
                logger.debug(f"Returning AiiDA node (pk={aiida_node_pk}) as Python value")
                return data_node.value

        except Exception as e:
            logger.exception(f"Failed to create AiiDA input link during deserialization: {e}")
            # Fall back to standard deserialization
            return deserialize(result.value)


