"""Runner singleton that manages a shared TransportQueue for the triggerer."""

import asyncio
import logging
import signal
from typing import Optional, Any, Callable, Dict, Tuple, Union, Type

from aiida.engine.persistence import AiiDAPersister
from aiida.engine.transports import TransportQueue
from aiida.engine.runners import Runner
from aiida.engine import utils
from aiida.plugins.utils import PluginVersionProvider
from aiida.engine.processes.calcjobs import manager
from aiida.orm import ProcessNode 
from aiida.common import exceptions
from aiida.engine.processes import Process, ProcessBuilder
from aiida.engine.runners import Runner, ResultAndNode
from aiida.common.exceptions import ConfigurationError

from plumpy.persistence import Persister
from plumpy.events import set_event_loop_policy



_LOGGER = logging.getLogger(__name__)
# TODO remove after prototype phase
logging.basicConfig(level=logging.DEBUG)

TYPE_RUN_PROCESS = Union[Process, Type[Process], ProcessBuilder]


class AirflowRunner(Runner):
    """Singleton runner that owns the TransportQueue for the triggerer process.

    Since each triggerer has only one event loop, we only need one Runner instance
    that all triggers share. This enables transport connection reuse across all
    triggers running in the same triggerer.
    """

    _persister: Optional[Persister] = None

    def __init__(
        self,
        poll_interval: Union[int, float] = 0,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        broker_submit = True,
    ):
        """Construct a new runner.

        :param poll_interval: interval in seconds between polling for status of active sub processes
        :param loop: an asyncio event loop, if none is suppled a new one will be created
        :param communicator: the communicator to use
        :param broker_submit: if True, processes will be submitted to the broker, otherwise they will be scheduled here
        :param persister: the persister to use to persist processes

        """
        set_event_loop_policy()
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._poll_interval = poll_interval
        self._transport = TransportQueue(self._loop)
        self._job_manager = manager.JobManager(self._transport)
        self._persister = AiiDAPersister()
        self._plugin_version_provider = PluginVersionProvider()
        #from airflow.configuration import conf
        #self._broker_submit = conf.get("database", "sql_alchemy_conn", None) == "airflow-db-not-allowed:///"
        self._broker_submit = broker_submit

    def _run(
        self, process: TYPE_RUN_PROCESS, inputs: dict[str, Any] | None = None, **kwargs: Any
    ) -> Tuple[Dict[str, Any], ProcessNode]:
        """Run the process with the supplied inputs in this runner that will block until the process is completed.

        The return value will be the results of the completed process

        :param process: the process class or process function to run
        :param inputs: the inputs to be passed to the process
        :return: tuple of the outputs of the process and the calculation node
        """
        inputs = utils.prepare_inputs(inputs, **kwargs)

        if utils.is_process_function(process):
            # TODO this needs to be considered
            result, node = process.run_get_node(**inputs)  # type: ignore[union-attr]
            return result, node

        process_inited = self.instantiate_process(process, **inputs)
        if hasattr(process_inited, "_context"):
            process_inited._context['_airflow_provider_aiida__broker_submit'] = False
        process_inited.runner.persister.save_checkpoint(process_inited)

        from airflow.models import DagBag
        dag_id = process.__name__
        dag = DagBag().get_dag(dag_id)

        if dag is None:
            raise ValueError(f"Could not find DAG corresponding to process class {dag_id!r}")

        from aiida import get_profile

        try:
            aiida_profile = get_profile()
        except ConfigurationError:
            from airflow_provider_aiida.aiida_core import load_profile
            aiida_profile = load_profile()

        import os
        aiida_path = os.getenv("AIIDA_PATH", None)
        conf = {"process_pk": process_inited.node.pk,
                "aiida_profile": aiida_profile.name,
                "aiida_path": aiida_path
                }

        dag.test(run_conf=conf)
        return process_inited.outputs, process_inited.node


    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Get the event loop of this runner."""
        return self._loop

    @property
    def transport(self) -> TransportQueue:
        return self._transport

    @property
    def persister(self) -> Optional[Persister]:
        """Get the persister used by this runner."""
        return self._persister

    @property
    def communicator(self) -> None:
        """Get the communicator used by this runner."""
        return None

    @property
    def plugin_version_provider(self) -> PluginVersionProvider:
        return self._plugin_version_provider

    @property
    def job_manager(self) -> manager.JobManager:
        return self._job_manager

    @property
    def controller(self) -> None:
        """Get the controller used by this runner."""
        return None

    def instantiate_process(self, process, **inputs):
        return utils.instantiate_process(self, process, **inputs)

    def submit(self, process, inputs: dict[str, Any] | None = None, **kwargs: Any):
        """Submit the process with the supplied inputs to this runner immediately returning control to the interpreter.

        The return value will be the calculation node of the submitted process

        :param process: the process class to submit
        :param inputs: the inputs to be passed to the process
        :return: the calculation node of the process
        """
        assert not utils.is_process_function(process), 'Cannot submit a process function'

        inputs = utils.prepare_inputs(inputs, **kwargs)
        process_inited = self.instantiate_process(process, **inputs)

        # TODO this property will be added to process 
        if hasattr(process_inited, "_context"):
            process_inited._context['_airflow_provider_aiida__broker_submit'] = self._broker_submit

        if not process_inited.metadata.store_provenance:
            raise exceptions.InvalidOperation('cannot submit a process with `store_provenance=False`')

        if process_inited.metadata.get('dry_run', False):
            raise exceptions.InvalidOperation('cannot submit a process from within another with `dry_run=True`')

        self.persister.save_checkpoint(process_inited)
        process_inited_dag_id = process_inited.__class__.__name__ # TODO .build_process_type().replace(":", "-")

        # Use sync REST API client to trigger DAG
        from aiida import get_profile
        from aiida.common.exceptions import ConfigurationError
        import os

        # Get AiiDA profile
        try:
            aiida_profile = get_profile()
        except ConfigurationError:
            from airflow_provider_aiida.aiida_core import load_profile
            aiida_profile = load_profile()

        # Get AIIDA_PATH if set
        aiida_path = os.getenv("AIIDA_PATH", None)

        # Prepare DAG trigger configuration
        conf = {
            'process_pk': process_inited.pid,
            'aiida_profile': aiida_profile.name,
            'aiida_path': aiida_path
        }

        # Get sync REST API client
        # Trigger the DAG
        trigger_dag_kwargs = dict(
            dag_id=process_inited_dag_id,
            run_id=None,
            conf=conf,
            logical_date=None,
        )

        try:
            _LOGGER.info(f"Triggering DAG {process_inited_dag_id} for process {process_inited.pid}")
            if self._broker_submit:
                trigger_dag_kwargs.update(dict(note=f"Triggered by AiiDA process {process_inited.pid}"))
                from airflow_provider_aiida.utils.airflow_restapi import get_airflow_rest_api_client_sync
                client = get_airflow_rest_api_client_sync(aiida_profile.name)
                response = client.trigger_dag(**trigger_dag_kwargs)
                _LOGGER.info(f"DAG {process_inited_dag_id} triggered successfully: {response.get('dag_run_id', 'unknown')}")
            else:
                # TODO need to do something else
                #self.loop.create_task(process_inited.step_until_terminated())

                # Run dag.test() in a subprocess to avoid blocking
                import subprocess
                import sys
                import json

                # Serialize conf to JSON for passing to subprocess
                conf_json = json.dumps(conf)

                # Python code to execute in subprocess
                test_code = f"""
import sys
import json
from airflow.models.dagbag import DagBag

dag_id = {process_inited_dag_id!r}
conf = json.loads({conf_json!r})

dag_bag = DagBag()
dag = dag_bag.get_dag(dag_id)

if dag is None:
    print(f"ERROR: DAG '{{dag_id}}' not found in DagBag", file=sys.stderr)
    sys.exit(1)

print(f"Running dag.test() for DAG: {{dag_id}}")
dag.test(run_conf=conf)
print(f"dag.test() completed for DAG: {{dag_id}}")
"""

                # Start the subprocess
                proc = subprocess.Popen(
                    [sys.executable, "-c", test_code],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                #output = proc.communicate()

                _LOGGER.info(f"Started dag.test() for {process_inited_dag_id} in subprocess (PID: {proc.pid})")

                # Non-blocking: subprocess runs independently
                # Output will be captured but not waited for

                #from airflow.api.common.trigger_dag import trigger_dag
                #from airflow.utils.types import DagRunTriggeredByType
                #trigger_dag_kwargs.update(dict(triggered_by=DagRunTriggeredByType.TEST))
                #result = trigger_dag(**trigger_dag_kwargs)
                #_LOGGER.info(f"DAG {process_inited_dag_id} triggered successfully: {result}")
        except Exception as e:
            import traceback
            _LOGGER.error(f"Failed to trigger DAG {process_inited_dag_id}: {e}. Full traceback: {traceback.format_exc()}")
            raise
        return process_inited.node

    # TODO not needed anymore
    def call_on_process_finish(self, pk: int, callback: Callable[[], Any]) -> None:
        # TODO not needed
        import functools
        from aiida.orm import load_node
        import uuid
        import threading

        node = load_node(pk=pk)
        subscriber_identifier = str(uuid.uuid4())
        event = threading.Event()

        def inline_callback(event, *args, **kwargs):
            """Callback to wrap the actual callback, that will always remove the subscriber that will be registered.

            As soon as the callback is called successfully once, the `event` instance is toggled, such that if this
            inline callback is called a second time, the actual callback is not called again.
            """
            if event.is_set():
                return

            try:
                callback()
            finally:
                event.set()
                if self.communicator:
                    self.communicator.remove_broadcast_subscriber(subscriber_identifier)

        self._poll_process(node, functools.partial(inline_callback, event))

    # TODO not needed anymore
    def _poll_process(self, node, callback):
        """Check whether the process state of the node is terminated and call the callback or reschedule it.

        :param node: the process node
        :param callback: callback to be called when process is terminated
        """
        if node.is_terminated:
            args = [node.__class__.__name__, node.pk]
            _LOGGER.info('%s<%d> confirmed to be terminated by backup polling mechanism', *args)
            self._loop.call_soon(callback)
        else:
            self._loop.call_later(self._poll_interval, self._poll_process, node, callback)
