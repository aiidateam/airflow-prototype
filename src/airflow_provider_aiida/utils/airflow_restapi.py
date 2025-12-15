"""Async REST API client for Airflow API.

This module provides async functions for calling the Airflow REST API
from triggers and other async contexts.
"""
from __future__ import annotations

import configparser
import logging
import os
import platform
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class NetworkUtils:
    """Network utility functions.

    This class contains static utility methods for network operations.
    All methods are static - no instance needed.
    """

    @staticmethod
    def get_free_port() -> int:
        """Get a free port from the OS.

        This function asks the OS to allocate an ephemeral port by binding
        to port 0. The OS assigns a free port, which we then return.

        Returns:
            Port number assigned by the OS (typically in range 49152-65535)

        Note:
            This is fast (~1ms) and uses the OS's port allocation mechanism.
            There's a small race condition where the port could be taken between
            when we release it and when the caller uses it, but this is rare
            for ephemeral ports.
        """
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            s.listen(1)
            port = s.getsockname()[1]
        return port


class AirflowRestApiClient:
    """Async REST API client for Airflow.

    This is a pure HTTP client that doesn't know about AiiDA profiles or config files.
    Use AirflowRestApiClientFactory.get_client() to create instances.

    Args:
        host: Airflow API host (e.g., 'localhost')
        port: Airflow API port (e.g., 8080)
        jwt_secret: JWT secret for authentication (optional)
        core_api_jwt_audience: JWT audience for core API endpoints (e.g., DAG management)
        execution_api_jwt_audience: JWT audience for execution API endpoints (e.g., task execution)
        timezone_str: Timezone string for JWT timestamps (default: 'system')
    """

    def __init__(
        self,
        host: str,
        port: int | str,
        jwt_secret: str | None,
        core_api_jwt_audience: str,
        execution_api_jwt_audience: str,
        timezone_str: str,
    ):
        # Store connection info
        self._host = host
        self._port = int(port)
        self._api_url = f'http://{host}:{port}/api/v2'

        # Store JWT settings
        self._jwt_secret = jwt_secret
        self._core_api_jwt_audience = core_api_jwt_audience
        self._execution_api_jwt_audience = execution_api_jwt_audience
        self._timezone_str = timezone_str

        # Fix for macOS: Disable proxy detection to avoid fork-safety issues
        # See: https://github.com/python/cpython/issues/58037
        if platform.system() == 'Darwin':
            logger.debug("Detected macOS - disabling proxy for REST API to avoid fork-safety issues")
            os.environ['no_proxy'] = '*'

        # Create reusable async HTTP client
        self._client = httpx.AsyncClient()

        logger.info(f"Initialized AirflowRestApiClient for {self._api_url}")

    async def open(self):
        """Close the HTTP client and cleanup resources."""
        if self._client:
            await self._client.aclose()
            logger.debug("Closed AirflowRestApiClient")

    async def close(self):
        """Close the HTTP client and cleanup resources."""
        if self._client:
            await self._client.aclose()
            logger.debug("Closed AirflowRestApiClient")

    def _generate_jwt_headers(self, audience: str) -> dict[str, str]:
        """Generate JWT authentication headers.

        Args:
            audience: JWT audience claim (e.g., self._core_api_jwt_audience)

        Returns:
            Dictionary with Content-Type and Authorization headers
        """
        import jwt
        import datetime
        import uuid
        from zoneinfo import ZoneInfo

        headers = {
            'Content-Type': 'application/json',
        }

        # Generate JWT token if we have the secret
        if self._jwt_secret:
            # Get current time in the configured timezone
            if self._timezone_str.lower() == 'system':
                # Use system local timezone
                now_dt = datetime.datetime.now()
            else:
                # Use specified timezone
                try:
                    tz = ZoneInfo(self._timezone_str)
                    now_dt = datetime.datetime.now(tz)
                except Exception:
                    # Fallback to UTC if timezone is invalid
                    logger.warning(f"Invalid timezone {self._timezone_str}, using UTC")
                    now_dt = datetime.datetime.now(datetime.timezone.utc)

            # Convert to UTC timestamp
            now = int(now_dt.timestamp())

            payload_jwt = {
                'jti': uuid.uuid4().hex,  # JWT ID
                'iss': 'airflow',  # Issuer
                'aud': audience,  # Audience
                'sub': 'airflow',  # Subject (user identity)
                'role': 'admin',  # User role (Admin for full permissions)
                'nbf': now - 10,  # Not before (10 seconds ago to account for clock skew)
                'exp': now + 300,  # Expiration (5 minutes from now)
                'iat': now,  # Issued at
            }

            logger.debug(f"JWT token config: audience={audience}, timezone={self._timezone_str}")
            logger.debug(f"JWT token timestamps: nbf={payload_jwt['nbf']}, iat={payload_jwt['iat']}, exp={payload_jwt['exp']}")

            # Encode with HS512 algorithm (Airflow's default)
            token = jwt.encode(payload_jwt, self._jwt_secret, algorithm='HS512', headers={'alg': 'HS512'})
            headers['Authorization'] = f'Bearer {token}'
            logger.info("Generated JWT token for authentication (HS512)")
        else:
            logger.warning("No JWT secret configured, trying without authentication")

        return headers

    async def clear_task_instances_async(
        self,
        task_ids_to_clear: list[str],
        dag_id: str,
        dag_run_id: str,
        dry_run: bool = False,
        only_failed: bool = False,
        reset_dag_runs: bool = False,
    ) -> dict[str, Any]:
        """
        Clear task instances via Airflow REST API (async).

        This function can be safely called from triggers and other async contexts.

        Args:
            task_ids_to_clear: List of task IDs to clear
            dag_id: DAG ID
            dag_run_id: DAG run ID
            dry_run: If True, only return what would be cleared
            only_failed: If True, only clear failed tasks
            reset_dag_runs: If True, reset the DAG run state

        Returns:
            Response from API as dict
        """
        # Call the REST API to clear task instances
        url = f"{self._api_url}/dags/{dag_id}/clearTaskInstances"
        payload = {
            "dry_run": dry_run,
            "only_failed": only_failed,
            "dag_run_id": dag_run_id,
            "task_ids": task_ids_to_clear,
            "reset_dag_runs": reset_dag_runs,
        }

        logger.info(f"Calling REST API to clear tasks: {url}")
        logger.info(f"Payload: {payload}")

        try:
            # Generate headers with JWT authentication
            headers = self._generate_jwt_headers(self._core_api_jwt_audience)

            # Use the reusable client
            response = await self._client.post(url, json=payload, headers=headers)

            logger.info(f"Response status code: {response.status_code}")
            logger.debug(f"Response headers: {response.headers}")
            logger.debug(f"Response text: {response.text}")

            response.raise_for_status()
            result = response.json()
            logger.info(f"Successfully cleared tasks: {result}")
            return result

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Error: {e}")
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Failed to clear tasks via REST API: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    async def trigger_dag(
        self,
        dag_id: str,
        run_id: str | None = None,
        conf: dict[str, Any] | None = None,
        logical_date: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """
        Trigger a DAG run via Airflow REST API (async).

        This function can be safely called from triggers and other async contexts.

        Args:
            dag_id: The DAG ID to trigger
            run_id: Optional custom run ID (Airflow will generate one if not provided)
            conf: Optional configuration JSON to pass to the DAG
            logical_date: Optional execution/logical date (ISO 8601 format)
            note: Optional note/description for this DAG run

        Returns:
            Response from API as dict containing DAG run details

        Example:
            >>> response = await client.trigger_dag(
            ...     dag_id="ArithmeticAddCalculation",
            ...     conf={"process_pk": 12345, "aiida_profile": "my-profile"}
            ... )
        """
        # Build the API endpoint URL
        url = f"{self._api_url}/dags/{dag_id}/dagRuns"

        # Build request payload with only non-None values
        payload = {}
        if run_id is not None:
            payload["dag_run_id"] = run_id
        if conf is not None:
            payload["conf"] = conf
        if logical_date is not None:
            payload["logical_date"] = logical_date
        if note is not None:
            payload["note"] = note

        logger.info(f"Triggering DAG via REST API: {url}")
        logger.debug(f"Payload: {payload}")

        try:
            # Generate headers with JWT authentication
            headers = self._generate_jwt_headers(self._core_api_jwt_audience)

            # Use the reusable client
            response = await self._client.post(url, json=payload, headers=headers)

            logger.info(f"Response status code: {response.status_code}")
            logger.debug(f"Response headers: {response.headers}")
            logger.debug(f"Response text: {response.text}")

            response.raise_for_status()
            result = response.json()
            logger.info(f"Successfully triggered DAG run: {result.get('dag_run_id', 'unknown')}")
            return result

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Error: {e}")
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Failed to trigger DAG via REST API: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise


class AirflowRestApiClientManager:
    """Manager for creating and caching AirflowRestApiClient instances.

    This class handles all the AiiDA-specific logic for reading config files
    and determining the correct parameters for the client. It maintains a cache
    of clients per profile for connection reuse.

    Usage:
        client = AirflowRestApiClientManager.get_client()
        client = AirflowRestApiClientManager.get_client('my-profile')
    """

    _clients: dict[str, AirflowRestApiClient] = {}

    @classmethod
    def get_client(cls, aiida_profile: str | None = None) -> AirflowRestApiClient:
        """Get or create a cached AirflowRestApiClient for the given profile.

        This method reads all configuration from airflow.cfg and creates a client
        with the appropriate parameters. Clients are cached per profile.

        Args:
            aiida_profile: AiiDA profile name (optional, uses default if None)

        Returns:
            AirflowRestApiClient instance for the profile
        """
        from airflow_provider_aiida.aiida_core.manage.configuration.config import get_airflow_home
        from airflow_provider_aiida.aiida_core import load_profile

        # Load the profile
        profile = load_profile(aiida_profile)

        # Return cached client if exists
        if profile.name in cls._clients:
            logger.debug(f"Reusing cached AirflowRestApiClient for profile {profile.name}")
            return cls._clients[profile.name]

        # Get airflow_home and read config
        airflow_home = get_airflow_home(profile)
        config_file = airflow_home / 'airflow.cfg'

        config = configparser.ConfigParser()
        config.read(config_file)

        # Read webserver host and port (REST API is served by webserver)
        host = config.get('api', 'host', fallback=None)
        port_str = config.get('api', 'port', fallback=None)

        # Host is required
        if host is None:
            raise ValueError(
                f"Missing 'web_server_host' in [webserver] section of {config_file}. "
                "Please configure the Airflow webserver host."
            )

        # If port not configured, get a free port from OS
        if port_str is None:
            port = NetworkUtils.get_free_port()
            logger.info(f"No port configured, allocated free port {port} from OS")
        else:
            port = int(port_str)

        # Read JWT settings
        jwt_secret = config.get('api', 'jwt_secret', fallback=None)
        core_api_jwt_audience = config.get('api_auth', 'jwt_audience', fallback='apache-airflow')
        execution_api_jwt_audience = config.get('execution_api', 'jwt_audience', fallback='apache-airflow')

        # Read timezone
        timezone_str = config.get('core', 'default_timezone', fallback='system')

        logger.info(f"Creating AirflowRestApiClient for profile {profile.name} at {host}:{port}")

        # Create and cache the client
        client = AirflowRestApiClient(
            host=host,
            port=port,
            jwt_secret=jwt_secret,
            core_api_jwt_audience=core_api_jwt_audience,
            execution_api_jwt_audience=execution_api_jwt_audience,
            timezone_str=timezone_str,
        )

        cls._clients[profile.name] = client
        return client

    @classmethod
    def clear_cache(cls, aiida_profile: str | None = None):
        """Clear cached client(s).

        Args:
            aiida_profile: Profile to clear (if None, clears all)
        """
        if aiida_profile is None:
            logger.info("Clearing all cached AirflowRestApiClient instances")
            cls._clients.clear()
        elif aiida_profile in cls._clients:
            logger.info(f"Clearing cached AirflowRestApiClient for profile {aiida_profile}")
            del cls._clients[aiida_profile]


# Convenience function for backward compatibility and easier usage
def get_airflow_rest_api_client(aiida_profile: str | None = None) -> AirflowRestApiClient:
    """Get or create a cached AirflowRestApiClient for the given profile.

    This is a convenience wrapper around AirflowRestApiClientManager.get_client().

    Args:
        aiida_profile: AiiDA profile name (optional, uses default if None)

    Returns:
        AirflowRestApiClient instance for the profile
    """
    return AirflowRestApiClientManager.get_client(aiida_profile)
