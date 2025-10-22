"""Runner singleton that manages a shared TransportQueue for the triggerer."""

import asyncio
import logging
from typing import Optional
from aiida.engine.transports import TransportQueue


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

