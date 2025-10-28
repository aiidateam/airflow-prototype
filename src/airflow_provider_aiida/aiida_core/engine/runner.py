"""Runner singleton that manages a shared TransportQueue for the triggerer."""

import asyncio
import logging
from typing import Optional, Any, Callable
from aiida.engine.persistence import AiiDAPersister
from aiida.engine.transports import TransportQueue
from aiida.engine import utils
from aiida.plugins.utils import PluginVersionProvider



_LOGGER = logging.getLogger(__name__)


class Runner:
    """Singleton runner that owns the TransportQueue for the triggerer process.

    Since each triggerer has only one event loop, we only need one Runner instance
    that all triggers share. This enables transport connection reuse across all
    triggers running in the same triggerer.
    """

    _instance: Optional['Runner'] = None

    def __new__(cls, loop: Optional[asyncio.AbstractEventLoop]):
        """Create or return the single Runner instance."""
        if cls._instance is None:
            _LOGGER.info("Creating singleton Runner instance")
            instance = super().__new__(cls)

            if loop is None:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    # No running loop - create a new one
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

            instance._loop = loop
            instance._transport_queue = TransportQueue(loop=loop)
            instance._persister = AiiDAPersister()
            # TODO JobManager?
            instance._plugin_version_provider = PluginVersionProvider()


            cls._instance = instance
            _LOGGER.debug(f"Runner initialized with event loop {id(loop)}")

        return cls._instance

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop]):
        """Initialize is a no-op since __new__ handles everything."""
        pass

    @property
    def transport_queue(self) -> TransportQueue:
        """Get the shared TransportQueue."""
        return self._transport_queue

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Get the event loop."""
        return self._loop

    @classmethod
    def get_instance(cls, loop: Optional[asyncio.AbstractEventLoop] = None) -> 'Runner':
        """Get the singleton Runner instance."""
        return cls(loop=loop)

    @classmethod
    def clear(cls):
        """Clear the singleton instance (useful for testing)."""
        cls._instance = None
        _LOGGER.debug("Cleared Runner singleton")

    @classmethod
    def instantiate_process(cls, process, **inputs):
        return utils.instantiate_process(cls._instance, process, **inputs)

    @classmethod
    def submit(cls, process, inputs: dict[str, Any] | None = None, **kwargs: Any):
        """Submit the process with the supplied inputs to this runner immediately returning control to the interpreter.

        The return value will be the calculation node of the submitted process

        :param process: the process class to submit
        :param inputs: the inputs to be passed to the process
        :return: the calculation node of the process
        """
        assert not utils.is_process_function(process), 'Cannot submit a process function'
        #assert not self._closed

        inputs = utils.prepare_inputs(inputs, **kwargs)
        process_inited = cls.instantiate_process(process, **inputs)

        if not process_inited.metadata.store_provenance:
            raise exceptions.InvalidOperation('cannot submit a process with `store_provenance=False`')

        if process_inited.metadata.get('dry_run', False):
            raise exceptions.InvalidOperation('cannot submit a process from within another with `dry_run=True`')

        #if self._broker_submit:
        #assert self.persister is not None, 'runner does not have a persister'
        #assert self.controller is not None, 'runner does not have a controller'
        cls._instance._persister.save_checkpoint(process_inited)
        #process_inited.close()

        from airflow.api.client import get_current_api_client
        client = get_current_api_client()

# Trigger the DAG run
        client.trigger_dag(
            dag_id=process_inited.__class__.__name__,
            conf={'node_pk': process_inited.pid}
        )

        #self.controller.continue_process(process_inited.pid)
        #else:
        #    self.loop.create_task(process_inited.step_until_terminated())

        return process_inited.node

    def call_on_process_finish(self, pk: int, callback: Callable[[], Any]) -> None:
        pass

    @classmethod
    @property
    def loop(cls) -> asyncio.AbstractEventLoop:
        """Get the event loop of this runner."""
        return cls._instance._loop

    @classmethod
    @property
    def transport(cls) -> TransportQueue:
        return cls._instance._transport

    @classmethod
    @property
    def persister(cls): # TODO -> Optional[Persister]:
        """Get the persister used by this runner."""
        return cls._instance._persister

    @property
    def communicator(self): # TODO -> Optional[Persister]:
        return None

    @property
    def plugin_version_provider(self) -> PluginVersionProvider:
        return self._instance._plugin_version_provider
