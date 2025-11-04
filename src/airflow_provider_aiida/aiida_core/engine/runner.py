"""Runner singleton that manages a shared TransportQueue for the triggerer."""

import asyncio
import logging
import signal
from typing import Optional, Any, Callable, NamedTuple, Dict, Tuple, Union, Type
from aiida.engine.persistence import AiiDAPersister
from aiida.engine.transports import TransportQueue
from aiida.engine import utils
from aiida.plugins.utils import PluginVersionProvider
from aiida.engine.processes.calcjobs import manager
from aiida.orm import ProcessNode 
from aiida.common import exceptions
from aiida.engine.processes import Process, ProcessBuilder
from plumpy.persistence import Persister


_LOGGER = logging.getLogger(__name__)
# TODO remove after prototype phase
logging.basicConfig(level=logging.DEBUG)

TYPE_RUN_PROCESS = Union[Process, Type[Process], ProcessBuilder]

class ResultAndNode(NamedTuple):
    result: Dict[str, Any]
    node: ProcessNode

class Runner:
    """Singleton runner that owns the TransportQueue for the triggerer process.

    Since each triggerer has only one event loop, we only need one Runner instance
    that all triggers share. This enables transport connection reuse across all
    triggers running in the same triggerer.
    """

    _instance: Optional['Runner'] = None

    def __new__(cls):
        """Create or return the single Runner instance."""
        if cls._instance is None:
            instance = super().__new__(cls)

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running loop - create a new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            from airflow.configuration import conf
            sql_conn = conf.get('database', 'sql_alchemy_conn', fallback='NOT SET')
            _LOGGER.debug(f"SQL connection: {sql_conn if sql_conn != 'NOT SET' else 'NOT SET'}")
            instance._loop = loop
            instance._poll_interval = 0
            # NOTE: A triggerer set the sql connection variable to "airflow-db-not-allowed:///"
            #       while in a test run this is set to a poper sql connection
            instance._broker_submit = True #sql_conn == "airflow-db-not-allowed:///"
            instance._transport = TransportQueue(instance._loop)
            instance._job_manager = manager.JobManager(instance._transport)
            instance._persister = AiiDAPersister()
            instance._plugin_version_provider = PluginVersionProvider()
            instance._communicator = None
            instance._controller = None

            cls._instance = instance
            _LOGGER.debug(f"Runner initialized with event loop {id(loop)}")

        return cls._instance

    def __init__(self):
        pass

    @classmethod
    def get_instance(cls) -> 'Runner':
        """Get the singleton Runner instance."""
        return cls()

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

    @classmethod
    def clear(cls):
        """Clear the singleton instance (useful for testing)."""
        cls._instance = None
        _LOGGER.debug("Cleared Runner singleton")

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
        #assert not self._closed

        inputs = utils.prepare_inputs(inputs, **kwargs)
        process_inited = self.instantiate_process(process, **inputs)

        if not process_inited.metadata.store_provenance:
            raise exceptions.InvalidOperation('cannot submit a process with `store_provenance=False`')

        if process_inited.metadata.get('dry_run', False):
            raise exceptions.InvalidOperation('cannot submit a process from within another with `dry_run=True`')

        self.persister.save_checkpoint(process_inited)
        process_inited_dag_id = process_inited.__class__.__name__ # TODO .build_process_type().replace(":", "-")

        if self._broker_submit:

            import subprocess
            import sys
            code_snippet = f"""
import os
import sys

# Debug: Print environment info
print(f"AIRFLOW_HOME: {{os.environ.get('AIRFLOW_HOME', 'NOT SET')}}", file=sys.stdout)
print(f"Working dir: {{os.getcwd()}}", file=sys.stdout)
print(f"Python: {{sys.executable}}", file=sys.stdout)

# Check what SQL connection Airflow is trying to use
from airflow.configuration import conf
sql_conn = conf.get('database', 'sql_alchemy_conn', fallback='NOT SET')
print(f"SQL connection: {{sql_conn if sql_conn != 'NOT SET' else 'NOT SET'}}", file=sys.stderr)

from airflow.api.client import get_current_api_client
client = get_current_api_client()

# Trigger the DAG run
client.trigger_dag(
    dag_id='{process_inited_dag_id}',
    conf={{'node_pk': {process_inited.pid}}}
)
"""
            from pathlib import Path
            import os
            import tempfile

            # Create a temporary file for the trigger script
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                _LOGGER.info(f"Creating temporary file in {f.name}")
                f.write(code_snippet)
                code_py = Path(f.name)

            # Call the trigger script via subprocess
            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(code_py),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={}, # NOTE: the triggerer has enviroment variables that clash with triggering api
                cwd=os.getcwd()
            )

            stdout, stderr = proc.communicate()
            if stdout:
                _LOGGER.debug(f"DAG trigger {process_inited_dag_id} output: {stdout}")
            if proc.returncode != 0:
                _LOGGER.error(f"DAG trigger {process_inited_dag_id} failed with return code {proc.returncode}")
            if stderr:
                _LOGGER.error(f"DAG trigger {process_inited_dag_id} error: {stderr}")

        else:
            self.loop.create_task(process_inited.step_until_terminated())
        return process_inited.node

    def call_on_process_finish(self, pk: int, callback: Callable[[], Any]) -> None:
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
            result, node = process.run_get_node(**inputs)  # type: ignore[union-attr]
            return result, node

        with utils.loop_scope(self.loop):
            process_inited = self.instantiate_process(process, **inputs)

            def kill_process(_num, _frame):
                """Send the kill signal to the process in the current scope."""
                if process_inited.is_killing:
                    LOGGER.warning('runner received interrupt, process %s already being killed', process_inited.pid)
                    return
                LOGGER.critical('runner received interrupt, killing process %s', process_inited.pid)
                process_inited.kill(msg_text='Process was killed because the runner received an interrupt')

            original_handler_int = signal.getsignal(signal.SIGINT)
            original_handler_term = signal.getsignal(signal.SIGTERM)

            try:
                signal.signal(signal.SIGINT, kill_process)
                signal.signal(signal.SIGTERM, kill_process)
                process_inited.execute()
            finally:
                signal.signal(signal.SIGINT, original_handler_int)
                signal.signal(signal.SIGTERM, original_handler_term)

            return process_inited.outputs, process_inited.node

    def run(self, process: TYPE_RUN_PROCESS, inputs: dict[str, Any] | None = None, **kwargs: Any) -> Dict[str, Any]:
        """Run the process with the supplied inputs in this runner that will block until the process is completed.

        The return value will be the results of the completed process

        :param process: the process class or process function to run
        :param inputs: the inputs to be passed to the process
        :return: the outputs of the process
        """
        result, _ = self._run(process, inputs, **kwargs)
        return result

    def run_get_node(
        self, process: TYPE_RUN_PROCESS, inputs: dict[str, Any] | None = None, **kwargs: Any
    ) -> ResultAndNode:
        """Run the process with the supplied inputs in this runner that will block until the process is completed.

        The return value will be the results of the completed process

        :param process: the process class or process function to run
        :param inputs: the inputs to be passed to the process
        :return: tuple of the outputs of the process and the calculation node
        """
        result, node = self._run(process, inputs, **kwargs)
        return ResultAndNode(result, node)

