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
        broker_submit = False,
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
            result, node = process.run_get_node(**inputs)  # type: ignore[union-attr]
            return result, node

        process_inited = self.instantiate_process(process, **inputs)

        from airflow.models import DagBag
        dag_id = process.__name__
        dag = DagBag().get_dag(dag_id)

        if dag is None:
            raise ValueError(f"Could not find DAG corresponding to process class {dag_id!r}")

        from aiida import get_profile

        try:
            aiida_profile = get_profile()
        except ConfigurationError:
            from aiida import load_profile
            aiida_profile = load_profile()

        import os
        aiida_path = os.getenv("AIIDA_PATH", None)
        conf = {"process_pk": process_inited.node.pk,
                "aiida_profile": aiida_profile.name,
                "aiida_path": aiida_path
                }

        dag.test(
            run_conf=conf
        )
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

        if not process_inited.metadata.store_provenance:
            raise exceptions.InvalidOperation('cannot submit a process with `store_provenance=False`')

        if process_inited.metadata.get('dry_run', False):
            raise exceptions.InvalidOperation('cannot submit a process from within another with `dry_run=True`')

        self.persister.save_checkpoint(process_inited)
        process_inited_dag_id = process_inited.__class__.__name__ # TODO .build_process_type().replace(":", "-")

        if self._broker_submit:
            from aiida import get_profile
            from aiida.common.exceptions import ConfigurationError

            try:
                aiida_profile = get_profile()
            except ConfigurationError:
                from aiida import load_profile
                aiida_profile = load_profile()

            import os
            aiida_path = os.getenv("AIIDA_PATH", None)

            import subprocess
            import sys
            # TODO we need to pass the environ from the operator
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

# NOTE: Raises error when not successfull, the typehint None is a bit confusing, it should not happen 
from airflow.api.common import trigger_dag
from airflow.utils.types import DagRunTriggeredByType

conf={{'process_pk': {process_inited.pid},
       'aiida_profile': {aiida_profile!r},
       'aiida_path': {aiida_path!r}
}}

trigger_dag.trigger_dag(
    dag_id={process_inited_dag_id!r},
    triggered_by=DagRunTriggeredByType.CLI,
    run_id=None,
    conf=conf,
    logical_date=None,
    replace_microseconds=True,
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
