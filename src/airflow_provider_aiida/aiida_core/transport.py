"""Transport utilities for AiiDA integration without requiring AiiDA database."""

from typing import Optional, Dict, Any

from airflow.hooks.base import BaseHook
from aiida.engine.transports import TransportQueue


class DummyComputer:
    """Dummy Computer object that mimics AiiDA Computer for duck typing."""

    def __init__(self, label: str, hostname: str, transport_type: str, scheduler_type: str):
        self.label = label
        self.hostname = hostname
        self.transport_type = transport_type
        self.scheduler_type = scheduler_type
        self.pk = hash(label)  # Use label hash as unique identifier


class DummyUser:
    """Dummy User object that mimics AiiDA User for duck typing."""

    def __init__(self, email: str):
        self.email = email
        self.pk = hash(email)


class DummyAuthInfo:
    """Dummy AuthInfo object that mimics AiiDA AuthInfo for duck typing with TransportQueue."""

    def __init__(self, computer: DummyComputer, user: DummyUser):
        self.computer = computer
        self.user = user
        self._auth_params = {}
        # Use computer pk as unique identifier for transport caching
        self.pk = computer.pk

    def set_auth_params(self, auth_params: dict):
        """Set authentication parameters."""
        self._auth_params = auth_params

    def get_auth_params(self) -> dict:
        """Get authentication parameters."""
        return self._auth_params

    def get_transport(self):
        """Return an AiiDA SSH transport configured from auth params."""
        from aiida.plugins import TransportFactory

        transport_class = TransportFactory(self.computer.transport_type)
        # Create transport with machine parameter (hostname) and auth params
        transport = transport_class(machine=self.computer.hostname, **self._auth_params)
        return transport


def get_authinfo_from_airflow_connection(conn_id: str):
    """
    Create AiiDA AuthInfo-like object from Airflow connection metadata.

    This bypasses the AiiDA database and creates the AuthInfo on-the-fly
    from Airflow's connection configuration using duck-typed dummy classes.

    :param conn_id: Airflow connection ID (typically machine name)
    :return: DummyAuthInfo object compatible with TransportQueue
    """
    # Get connection from Airflow metadata database
    conn = BaseHook.get_connection(conn_id)

    # Create dummy Computer object (not stored in AiiDA DB)
    computer = DummyComputer(
        label=conn_id,
        hostname=conn.host or 'localhost',
        transport_type='core.ssh',
        scheduler_type='core.direct',
    )

    # Create dummy User object (not stored in AiiDA DB)
    user = DummyUser(email=conn.login or 'airflow@localhost')

    # Create dummy AuthInfo with connection metadata
    authinfo = DummyAuthInfo(computer=computer, user=user)

    # Set SSH authentication parameters
    # By default, use AutoAdd policy to automatically add unknown hosts
    auth_params = {
        'username': conn.login or 'airflow',
        'port': conn.port or 22,
        'load_system_host_keys': True,
        'key_policy': 'AutoAddPolicy',  # Automatically add unknown hosts
        # Add other SSH params from extras if needed
        **conn.extra_dejson
    }

    authinfo.set_auth_params(auth_params)

    return authinfo


# ---------------------------
# Module-level singletons
# ---------------------------

_TRANSPORT_QUEUE: Optional[TransportQueue] = None
_AUTHINFO_CACHE: Dict[str, Any] = {}


def get_transport_queue() -> TransportQueue:
    """Return a TransportQueue instance using the current event loop.

    Note: Always creates a new TransportQueue to ensure it uses the current
    event loop. This is necessary because Airflow triggers run in different
    async contexts with different event loops.
    """
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    return TransportQueue(loop=loop)


def get_authinfo_cached(conn_id: str):
    """
    Cache authinfo objects per connection ID to avoid repeated lookups.

    Uses Airflow connection metadata instead of AiiDA database.
    """
    global _AUTHINFO_CACHE
    auth = _AUTHINFO_CACHE.get(conn_id)
    if auth is None:
        auth = get_authinfo_from_airflow_connection(conn_id)
        _AUTHINFO_CACHE[conn_id] = auth
    return auth
