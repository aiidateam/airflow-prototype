"""Async REST API client for Airflow API.

This module provides async functions for calling the Airflow REST API
from triggers and other async contexts.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
import configparser
import logging
import json
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


class AirflowRestApiClientBase(ABC):
    """Base class for Airflow REST API clients.

    This base class contains shared logic for both sync and async clients:
    - JWT authentication token generation
    - Request payload building
    - Connection configuration

    Subclasses must implement the actual HTTP operations (sync or async).

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
        core_api_jwt_audience: str | None,
        execution_api_jwt_audience: str | None,
        timezone_str: str,
        basic_auth_username: str | None = None,
        basic_auth_password: str | None = None,
    ):
        # Store connection info
        self._host = host
        self._port = int(port)
        self._api_url = f'http://{host}:{port}/api/v2'
        self._auth_url = f'http://{host}:{port}/auth/token'

        # Store JWT settings
        self._jwt_secret = jwt_secret
        self._core_api_jwt_audience = core_api_jwt_audience
        self._execution_api_jwt_audience = execution_api_jwt_audience
        self._timezone_str = timezone_str

        # Store Basic Auth settings (for SimpleAuthManager)
        self._basic_auth_username = basic_auth_username
        self._basic_auth_password = basic_auth_password

        # Token cache for token exchange authentication
        self._cached_jwt_token: str | None = None

        # Fix for macOS: Disable proxy detection to avoid fork-safety issues
        # See: https://github.com/python/cpython/issues/58037
        if platform.system() == 'Darwin':
            logger.debug("Detected macOS - disabling proxy for REST API to avoid fork-safety issues")
            os.environ['no_proxy'] = '*'

        # Subclasses will set _client
        self._client = None

    def _generate_auth_headers(self, audience: str | None) -> dict[str, str]:
        """Generate authentication headers with token exchange support.

        Args:
            audience: JWT audience claim (e.g., self._core_api_jwt_audience)

        Returns:
            Dictionary with Content-Type and Authorization headers
        """
        headers = {
            'Content-Type': 'application/json',
        }

        # Use cached JWT token if available
        if self._cached_jwt_token:
            headers['Authorization'] = f'Bearer {self._cached_jwt_token}'
            logger.debug("Using cached JWT token")
            return headers
        # If no JWT secret but we have Basic Auth credentials, get token from server
        elif not self._jwt_secret and self._basic_auth_username and self._basic_auth_password:
            logger.debug("No JWT secret configured, attempting token exchange with credentials")
            try:
                token = self._obtain_jwt_token_from_credentials()
                if token:
                    self._cached_jwt_token = token
                    headers['Authorization'] = f'Bearer {token}'
                    logger.debug("Successfully obtained and cached JWT token via token exchange")
                    return headers
            except Exception as e:
                logger.error(f"Failed to obtain JWT token: {e}")
                raise
        elif self._jwt_secret: # TODO need also audience in this case
            import jwt
            import datetime
            import uuid
            from zoneinfo import ZoneInfo
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
                    logger.debug(f"Invalid timezone {self._timezone_str}, using UTC")
                    now_dt = datetime.datetime.now(datetime.timezone.utc)

            # Convert to UTC timestamp
            now = int(now_dt.timestamp())

            # TODO i think audience should be in default
            payload_jwt = {
                'jti': uuid.uuid4().hex,  # JWT ID
                'iss': 'airflow',  # Issuer
                'sub': 'airflow',  # Subject (user identity)
                'aud': audience,  # Audience
                'role': 'admin',  # User role (Admin for full permissions)
                'nbf': now - 10,  # Not before (10 seconds ago to account for clock skew)
                'exp': now + 300,  # Expiration (5 minutes from now)
                'iat': now,  # Issued at
            }

            logger.debug(f"JWT token config: audience={audience}, timezone={self._timezone_str}")
            logger.debug(f"JWT token timestamps: nbf={payload_jwt['nbf']}, iat={payload_jwt['iat']}, exp={payload_jwt['exp']}")

            # Encode with HS512 algorithm (Airflow's default)
            token = jwt.encode(payload_jwt, self._jwt_secret, algorithm='HS512', headers={'alg': 'HS512'})
            self._cached_jwt_token = token
            headers['Authorization'] = f'Bearer {token}'
            logger.debug("Generated JWT token for authentication (HS512)")
        else:
            logger.debug("No JWT secret or credentials configured, trying without authentication")

        return headers

    def clear_token_cache(self):
        """Clear the cached JWT token to force re-authentication.

        This method can be called to manually clear the cached JWT token,
        forcing the client to obtain a new token on the next API call.
        This is useful for testing or when you know the token has been invalidated.
        """
        self._cached_jwt_token = None
        logger.debug("Cleared cached JWT token")

    @abstractmethod
    def _obtain_jwt_token_from_credentials(self) -> str | None:
        """Obtain JWT token from Airflow by exchanging credentials.

        This method POSTs username/password to /auth/token endpoint
        and returns the JWT token from the response.

        Returns:
            JWT token string if successful, None otherwise

        Raises:
            httpx.HTTPStatusError: If authentication fails
        """
        pass

    def _build_clear_task_instances_payload(
        self,
        dag_id: str,
        dag_run_id: str,
        task_ids_to_clear: list[str],
        dry_run: bool,
        only_failed: bool,
        reset_dag_runs: bool,
    ) -> tuple[str, dict[str, Any]]:
        """Build URL and payload for clear task instances request.

        Args:
            dag_id: DAG ID
            dag_run_id: DAG run ID
            task_ids_to_clear: List of task IDs to clear
            dry_run: If True, only return what would be cleared
            only_failed: If True, only clear failed tasks
            reset_dag_runs: If True, reset the DAG run state

        Returns:
            Tuple of (url, payload)
        """
        url = f"{self._api_url}/dags/{dag_id}/clearTaskInstances"
        payload = {
            "dry_run": dry_run,
            "only_failed": only_failed,
            "dag_run_id": dag_run_id,
            "task_ids": task_ids_to_clear,
            "reset_dag_runs": reset_dag_runs,
        }
        return url, payload

    def _build_trigger_dag_payload(
        self,
        dag_id: str,
        run_id: str | None,
        conf: dict[str, Any] | None,
        logical_date: str | None,
        note: str | None,
    ) -> tuple[str, dict[str, Any]]:
        """Build URL and payload for trigger DAG request.

        Args:
            dag_id: The DAG ID to trigger
            run_id: Optional custom run ID
            conf: Optional configuration JSON to pass to the DAG
            logical_date: Optional execution/logical date (ISO 8601 format). If None, uses current UTC time.
            note: Optional note/description for this DAG run

        Returns:
            Tuple of (url, payload)
        """
        url = f"{self._api_url}/dags/{dag_id}/dagRuns"
        payload = {}
        if run_id is not None:
            payload["dag_run_id"] = run_id
        if conf is not None:
            payload["conf"] = conf

        # logical_date is required by Airflow API - use current UTC time if not provided
        if logical_date is not None:
            payload["logical_date"] = logical_date
        else:
            import datetime
            payload["logical_date"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

        if note is not None:
            payload["note"] = note
        return url, payload

    @abstractmethod
    def clear_task_instances(
        self,
        task_ids_to_clear: list[str],
        dag_id: str,
        dag_run_id: str,
        dry_run: bool = False,
        only_failed: bool = False,
        reset_dag_runs: bool = False,
    ) -> dict[str, Any]:
        """Clear task instances via Airflow REST API.

        Subclasses must implement this as sync or async.
        """
        pass

    @abstractmethod
    def trigger_dag(
        self,
        dag_id: str,
        run_id: str | None = None,
        conf: dict[str, Any] | None = None,
        logical_date: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Trigger a DAG run via Airflow REST API.

        Subclasses must implement this as sync or async.
        """
        pass

    @abstractmethod
    def close(self):
        """Close the HTTP client and cleanup resources.

        Subclasses must implement this as sync or async.
        """
        pass


class AirflowRestApiClientAsync(AirflowRestApiClientBase):
    """Async REST API client for Airflow.

    This is a pure HTTP client that doesn't know about AiiDA profiles or config files.
    Use AirflowRestApiClientManager.get_async_client() to create instances.

    Args:
        host: Airflow API host (e.g., 'localhost')
        port: Airflow API port (e.g., 8080)
        jwt_secret: JWT secret for authentication (optional)
        core_api_jwt_audience: JWT audience for core API endpoints (e.g., DAG management)
        execution_api_jwt_audience: JWT audience for execution API endpoints (e.g., task execution)
        timezone_str: Timezone string for JWT timestamps (default: 'system')
        basic_auth_username: Username for Basic Authentication (optional, for SimpleAuthManager)
        basic_auth_password: Password for Basic Authentication (optional, for SimpleAuthManager)
    """

    def __init__(
        self,
        host: str,
        port: int | str,
        jwt_secret: str | None,
        core_api_jwt_audience: str,
        execution_api_jwt_audience: str,
        timezone_str: str,
        basic_auth_username: str | None = None,
        basic_auth_password: str | None = None,
    ):
        # Call base class constructor
        super().__init__(host, port, jwt_secret, core_api_jwt_audience, execution_api_jwt_audience, timezone_str, basic_auth_username, basic_auth_password)

        # Create reusable async HTTP client with trust_env=False to avoid macOS proxy issues
        self._client = httpx.AsyncClient(trust_env=False)

        logger.debug(f"Initialized AirflowRestApiClientAsync for {self._api_url}")

    async def close(self):
        """Close the HTTP client and cleanup resources."""
        if self._client:
            await self._client.aclose()
            logger.debug("Closed AirflowRestApiClientAsync")

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def _obtain_jwt_token_from_credentials(self) -> str | None:
        """Obtain JWT token by exchanging credentials (async).

        Returns:
            JWT token string if successful, None otherwise

        Raises:
            httpx.HTTPStatusError: If authentication fails
        """
        if not self._basic_auth_username or not self._basic_auth_password:
            return None

        try:
            logger.debug(f"Obtaining JWT token for user: {self._basic_auth_username}")
            response = await self._client.post(
                self._auth_url,
                json={
                    "username": self._basic_auth_username,
                    "password": self._basic_auth_password,
                },
                headers={"Content-Type": "application/json"},
            )

            response.raise_for_status()
            token_data = response.json()
            token = token_data.get('access_token') or token_data.get('token')

            if not token:
                logger.error("Token response did not contain 'access_token' or 'token' field")
                return None

            logger.debug("Successfully obtained JWT token")
            return token

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to obtain token: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error during token exchange: {e}")
            raise

    async def clear_task_instances(
        self,
        task_ids_to_clear: list[str],
        dag_id: str,
        dag_run_id: str,
        dry_run: bool = False,
        only_failed: bool = False,
        reset_dag_runs: bool = False,
    ) -> dict[str, Any]:
        """Clear task instances via Airflow REST API (async).

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
        # Build URL and payload using base class helper
        url, payload = self._build_clear_task_instances_payload(
            dag_id, dag_run_id, task_ids_to_clear, dry_run, only_failed, reset_dag_runs
        )

        logger.debug(f"Calling REST API to clear tasks: {url}")
        logger.debug(f"Payload: {payload}")

        # Try the request, with one retry on 401
        for attempt in range(2):
            try:
                # Generate headers with JWT authentication
                headers = self._generate_auth_headers(self._core_api_jwt_audience)

                # Use the reusable client
                response = await self._client.post(url, json=payload, headers=headers)

                logger.debug(f"Response status code: {response.status_code}")

                # Handle 401 on first attempt by clearing token and retrying
                if response.status_code == 401 and attempt == 0 and self._cached_jwt_token:
                    logger.debug("Received 401 Unauthorized, token may be expired. Clearing cache and retrying...")
                    self._cached_jwt_token = None
                    continue

                logger.debug(f"Response headers: {response.headers}")
                logger.debug(f"Response text: {response.text}")

                response.raise_for_status()
                result = response.json()
                logger.debug(f"Successfully cleared tasks: {result}")
                return result

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401 and attempt == 0 and self._cached_jwt_token:
                    logger.debug("Received 401 Unauthorized, token may be expired. Clearing cache and retrying...")
                    self._cached_jwt_token = None
                    continue
                else:
                    # Create more informative error message with response details
                    error_msg = (
                        f"HTTP {e.response.status_code} error when clearing tasks for DAG '{dag_id}' run '{dag_run_id}':\n"
                        f"  Error: {e}\n"
                        f"  Response body: {e.response.text}\n"
                        f"  Request URL: {e.request.url}\n"
                        f"  Request payload: {payload}"
                    )
                    logger.error(error_msg)
                    # Re-raise with enhanced message
                    raise RuntimeError(error_msg) from e
            except Exception as e:
                logger.error(f"Failed to clear tasks via REST API: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise

        raise RuntimeError("Failed to clear tasks after retrying with fresh token")

    async def trigger_dag(
        self,
        dag_id: str,
        run_id: str | None = None,
        conf: dict[str, Any] | None = None,
        logical_date: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Trigger a DAG run via Airflow REST API (async).

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
        # Build URL and payload using base class helper
        url, payload = self._build_trigger_dag_payload(dag_id, run_id, conf, logical_date, note)

        logger.debug(f"Triggering DAG via REST API: {url}")
        logger.debug(f"Payload: {payload}")

        # Try the request, with one retry on 401
        for attempt in range(2):
            try:
                # Generate headers with JWT authentication
                headers = self._generate_auth_headers(self._core_api_jwt_audience)

                # Use the reusable client
                response = await self._client.post(url, json=payload, headers=headers)

                logger.debug(f"Response status code: {response.status_code}")

                # Handle 401 on first attempt by clearing token and retrying
                if response.status_code == 401 and attempt == 0 and self._cached_jwt_token:
                    logger.debug("Received 401 Unauthorized, token may be expired. Clearing cache and retrying...")
                    self._cached_jwt_token = None
                    continue

                logger.debug(f"Response headers: {response.headers}")
                logger.debug(f"Response text: {response.text}")

                response.raise_for_status()
                result = response.json()
                logger.debug(f"Successfully triggered DAG run: {result.get('dag_run_id', 'unknown')}")
                return result

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401 and attempt == 0 and self._cached_jwt_token:
                    logger.debug("Received 401 Unauthorized, token may be expired. Clearing cache and retrying...")
                    self._cached_jwt_token = None
                    continue
                else:
                    # Create more informative error message with response details
                    error_msg = (
                        f"HTTP {e.response.status_code} error when triggering DAG '{dag_id}':\n"
                        f"  Error: {e}\n"
                        f"  Response body: {e.response.text}\n"
                        f"  Request URL: {e.request.url}\n"
                        f"  Request payload: {payload}"
                    )
                    logger.error(error_msg)
                    # Re-raise with enhanced message
                    raise RuntimeError(error_msg) from e
            except Exception as e:
                logger.error(f"Failed to trigger DAG via REST API: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise

        raise RuntimeError("Failed to trigger DAG after retrying with fresh token")


class AirflowRestApiClientSync(AirflowRestApiClientBase):
    """Sync REST API client for Airflow.

    This is a pure HTTP client that doesn't know about AiiDA profiles or config files.
    Use AirflowRestApiClientManager.get_sync_client() to create instances.

    Args:
        host: Airflow API host (e.g., 'localhost')
        port: Airflow API port (e.g., 8080)
        jwt_secret: JWT secret for authentication (optional)
        core_api_jwt_audience: JWT audience for core API endpoints (e.g., DAG management)
        execution_api_jwt_audience: JWT audience for execution API endpoints (e.g., task execution)
        timezone_str: Timezone string for JWT timestamps (default: 'system')
        basic_auth_username: Username for Basic Authentication (optional, for SimpleAuthManager)
        basic_auth_password: Password for Basic Authentication (optional, for SimpleAuthManager)
    """

    def __init__(
        self,
        host: str,
        port: int | str,
        jwt_secret: str | None,
        core_api_jwt_audience: str,
        execution_api_jwt_audience: str,
        timezone_str: str,
        basic_auth_username: str | None = None,
        basic_auth_password: str | None = None,
    ):
        # Call base class constructor
        super().__init__(host, port, jwt_secret, core_api_jwt_audience, execution_api_jwt_audience, timezone_str, basic_auth_username, basic_auth_password)

        # Create reusable sync HTTP client with trust_env=False to avoid macOS proxy issues
        self._client = httpx.Client(trust_env=False)

        logger.debug(f"Initialized AirflowRestApiClientSync for {self._api_url}")

    def close(self):
        """Close the HTTP client and cleanup resources."""
        if self._client:
            self._client.close()
            logger.debug("Closed AirflowRestApiClientSync")

    def __enter__(self):
        """Sync context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit."""
        self.close()

    def _obtain_jwt_token_from_credentials(self) -> str | None:
        """Obtain JWT token by exchanging credentials (sync).

        Returns:
            JWT token string if successful, None otherwise

        Raises:
            httpx.HTTPStatusError: If authentication fails
        """
        if not self._basic_auth_username or not self._basic_auth_password:
            return None

        try:
            logger.debug(f"Obtaining JWT token for user: {self._basic_auth_username}")
            response = self._client.post(
                self._auth_url,
                json={
                    "username": self._basic_auth_username,
                    "password": self._basic_auth_password,
                },
                headers={"Content-Type": "application/json"},
            )

            response.raise_for_status()
            token_data = response.json()
            token = token_data.get('access_token') or token_data.get('token')

            if not token:
                logger.error("Token response did not contain 'access_token' or 'token' field")
                return None

            logger.debug("Successfully obtained JWT token")
            return token

        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to obtain token: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error during token exchange: {e}")
            raise

    def clear_task_instances(
        self,
        task_ids_to_clear: list[str],
        dag_id: str,
        dag_run_id: str,
        dry_run: bool = False,
        only_failed: bool = False,
        reset_dag_runs: bool = False,
    ) -> dict[str, Any]:
        """Clear task instances via Airflow REST API (sync).

        This function can be safely called from synchronous contexts.

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
        # Build URL and payload using base class helper
        url, payload = self._build_clear_task_instances_payload(
            dag_id, dag_run_id, task_ids_to_clear, dry_run, only_failed, reset_dag_runs
        )

        logger.debug(f"Calling REST API to clear tasks: {url}")
        logger.debug(f"Payload: {payload}")

        # Try the request, with one retry on 401
        for attempt in range(2):
            try:
                # Generate headers with JWT authentication
                headers = self._generate_auth_headers(self._core_api_jwt_audience)

                # Use the reusable client (sync call)
                response = self._client.post(url, json=payload, headers=headers)

                logger.debug(f"Response status code: {response.status_code}")

                # Handle 401 on first attempt by clearing token and retrying
                if response.status_code == 401 and attempt == 0 and self._cached_jwt_token:
                    logger.debug("Received 401 Unauthorized, token may be expired. Clearing cache and retrying...")
                    self._cached_jwt_token = None
                    continue

                logger.debug(f"Response headers: {response.headers}")
                logger.debug(f"Response text: {response.text}")

                response.raise_for_status()
                result = response.json()
                logger.debug(f"Successfully cleared tasks: {result}")
                return result

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401 and attempt == 0 and self._cached_jwt_token:
                    logger.debug("Received 401 Unauthorized, token may be expired. Clearing cache and retrying...")
                    self._cached_jwt_token = None
                    continue
                else:
                    # Create more informative error message with response details
                    error_msg = (
                        f"HTTP {e.response.status_code} error when clearing tasks for DAG '{dag_id}' run '{dag_run_id}':\n"
                        f"  Error: {e}\n"
                        f"  Response body: {e.response.text}\n"
                        f"  Request URL: {e.request.url}\n"
                        f"  Request payload: {payload}"
                    )
                    logger.error(error_msg)
                    # Re-raise with enhanced message
                    raise RuntimeError(error_msg) from e
            except Exception as e:
                logger.error(f"Failed to clear tasks via REST API: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise

        raise RuntimeError("Failed to clear tasks after retrying with fresh token")

    def trigger_dag(
        self,
        dag_id: str,
        run_id: str | None = None,
        conf: dict[str, Any] | None = None,
        logical_date: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Trigger a DAG run via Airflow REST API (sync).

        This function can be safely called from synchronous contexts.

        Args:
            dag_id: The DAG ID to trigger
            run_id: Optional custom run ID (Airflow will generate one if not provided)
            conf: Optional configuration JSON to pass to the DAG
            logical_date: Optional execution/logical date (ISO 8601 format)
            note: Optional note/description for this DAG run

        Returns:
            Response from API as dict containing DAG run details

        Example:
            >>> response = client.trigger_dag(
            ...     dag_id="ArithmeticAddCalculation",
            ...     conf={"process_pk": 12345, "aiida_profile": "my-profile"}
            ... )
        """
        # Build URL and payload using base class helper
        url, payload = self._build_trigger_dag_payload(dag_id, run_id, conf, logical_date, note)

        logger.debug(f"Triggering DAG via REST API: {url}")
        logger.debug(f"Payload: {payload}")

        # Try the request, with one retry on 401
        for attempt in range(2):
            try:
                # Generate headers with JWT authentication
                headers = self._generate_auth_headers(self._core_api_jwt_audience)

                # Use the reusable client (sync call)
                response = self._client.post(url, json=payload, headers=headers)

                logger.debug(f"Response status code: {response.status_code}")

                # Handle 401 on first attempt by clearing token and retrying
                if response.status_code == 401 and attempt == 0 and self._cached_jwt_token:
                    logger.debug("Received 401 Unauthorized, token may be expired. Clearing cache and retrying...")
                    self._cached_jwt_token = None
                    continue

                logger.debug(f"Response headers: {response.headers}")
                logger.debug(f"Response text: {response.text}")

                response.raise_for_status()
                result = response.json()
                logger.debug(f"Successfully triggered DAG run: {result.get('dag_run_id', 'unknown')}")
                return result

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401 and attempt == 0 and self._cached_jwt_token:
                    logger.debug("Received 401 Unauthorized, token may be expired. Clearing cache and retrying...")
                    self._cached_jwt_token = None
                    continue
                else:
                    # Create more informative error message with response details
                    error_msg = (
                        f"HTTP {e.response.status_code} error when triggering DAG '{dag_id}':\n"
                        f"  Error: {e}\n"
                        f"  Response body: {e.response.text}\n"
                        f"  Request URL: {e.request.url}\n"
                        f"  Request payload: {payload}"
                    )
                    logger.error(error_msg)
                    # Re-raise with enhanced message
                    raise RuntimeError(error_msg) from e
            except Exception as e:
                logger.error(f"Failed to trigger DAG via REST API: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise

        raise RuntimeError("Failed to trigger DAG after retrying with fresh token")


class AirflowRestApiClientManager:
    """Manager for creating and caching AirflowRestApiClient instances.

    This class handles all the AiiDA-specific logic for reading config files
    and determining the correct parameters for the client. It maintains separate
    caches for sync and async clients per profile for connection reuse.

    Usage:
        # Get async client (default)
        client = AirflowRestApiClientManager.get_async_client()
        client = AirflowRestApiClientManager.get_async_client('my-profile')

        # Get sync client
        client = AirflowRestApiClientManager.get_sync_client()
        client = AirflowRestApiClientManager.get_sync_client('my-profile')

        # Backward compatible (returns async by default)
        client = AirflowRestApiClientManager.get_client()
        client = AirflowRestApiClientManager.get_client('my-profile', async_mode=False)
    """

    _async_clients: dict[str, AirflowRestApiClientAsync] = {}
    _sync_clients: dict[str, AirflowRestApiClientSync] = {}

    @classmethod
    def _read_client_config(cls, aiida_profile: str | None = None) -> dict[str, Any]:
        """Read configuration for creating an Airflow REST API client.

        This helper method extracts all the common configuration reading logic
        used by both get_async_client and get_sync_client.

        Args:
            aiida_profile: AiiDA profile name (optional, uses default if None)

        Returns:
            Dictionary containing all client configuration parameters
        """
        from airflow_provider_aiida.aiida_core.manage.configuration.config import get_airflow_home
        from airflow_provider_aiida.aiida_core import load_profile

        # Load the profile
        profile = load_profile(aiida_profile)

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
                f"Missing 'host' in [api] section of {config_file}. "
                "Please configure the Airflow API host."
            )

        # If port not configured, get a free port from OS
        # TODO is_numeric check
        if port_str is None:
            port = NetworkUtils.get_free_port()
            logger.debug(f"No port configured, allocated free port {port} from OS")
        else:
            port = int(port_str)

        # Read JWT settings
        jwt_secret = config.get('api_auth', 'jwt_secret', fallback=None) 
        # NOTE: the fallback is the same as for the
        #       airflow.api_fastapi.auth.managers.base_auth_managerBaseAuthManager
        core_api_jwt_audience = config.get('api_auth', 'jwt_audience', fallback="apache-airflow")
        execution_api_jwt_audience = config.get('execution_api', 'jwt_audience', fallback=None)
        if jwt_secret and not core_api_jwt_audience:
            raise ValueError("Airflow config provided JWT secret but not an audience for core api")
        if jwt_secret and not execution_api_jwt_audience:
            raise ValueError("Airflow config provided JWT secret but not an audience for execution api")

        # Read Basic Auth settings (for SimpleAuthManager)
        basic_auth_username = None
        basic_auth_password = None

        auth_manager_passwords_file = airflow_home / 'simple_auth_manager_passwords.json.generated'
        try:
            with open(auth_manager_passwords_file, 'r') as f:
                admin_info = json.load(f)
                basic_auth_username, basic_auth_password =  next(iter(admin_info.items()))
                if basic_auth_password:
                    logger.debug(f"Using Basic Auth with username: {basic_auth_username}")
                else:
                    logger.debug(f"Password not found for user {basic_auth_username} in {auth_manager_passwords_file}")
        except Exception as e:
            import traceback
            logger.debug(f"Failed to read password file {auth_manager_passwords_file}: {e}. Full traceback:\n{traceback.format_exc()}")


        if (not basic_auth_username or not basic_auth_password) and (not jwt_secret and (core_api_jwt_audience or execution_api_jwt_audience)):
            raise ValueError("Cannot authenticate. Even password file with admin user and admin password must exist (usually created on start of api-server) or JWT secret with audience for core and execution api must be provided.")



        # Read timezone
        timezone_str = config.get('core', 'default_timezone', fallback=None)
        if timezone_str is None:
            raise ValueError("core.default_timezone is not defined in airflow config")

        return {
            'profile': profile,
            'host': host,
            'port': port,
            'jwt_secret': jwt_secret,
            'core_api_jwt_audience': core_api_jwt_audience,
            'execution_api_jwt_audience': execution_api_jwt_audience,
            'timezone_str': timezone_str,
            'basic_auth_username': basic_auth_username,
            'basic_auth_password': basic_auth_password,
        }

    # TODO duplicate
    @classmethod
    def get_async_client(cls, aiida_profile: str | None = None) -> AirflowRestApiClientAsync:
        """Get or create a cached async AirflowRestApiClient for the given profile.

        This method reads all configuration from airflow.cfg and creates an async client
        with the appropriate parameters. Clients are cached per profile.

        Args:
            aiida_profile: AiiDA profile name (optional, uses default if None)

        Returns:
            AirflowRestApiClientAsync instance for the profile
        """
        # Read configuration
        config = cls._read_client_config(aiida_profile)
        profile = config['profile']

        # Return cached client if exists and is still open
        if profile.name in cls._async_clients:
            cached_client = cls._async_clients[profile.name]
            # Check if the underlying httpx client is still open
            if not cached_client._client.is_closed:
                logger.debug(f"Reusing cached AirflowRestApiClientAsync for profile {profile.name}")
                return cached_client
            else:
                # Client was closed, remove from cache and create new one
                logger.debug(f"Cached AirflowRestApiClientAsync for profile {profile.name} was closed, creating new one")
                del cls._async_clients[profile.name]

        logger.debug(f"Creating AirflowRestApiClientAsync for profile {profile.name} at {config['host']}:{config['port']}")
        logger.debug(f"Auth config: jwt_secret={'set' if config['jwt_secret'] else 'not set'}, basic_auth={'set' if config['basic_auth_username'] and config['basic_auth_password'] else 'not set'}")

        # Create and cache the async client
        client = AirflowRestApiClientAsync(
            host=config['host'],
            port=config['port'],
            jwt_secret=config['jwt_secret'],
            core_api_jwt_audience=config['core_api_jwt_audience'],
            execution_api_jwt_audience=config['execution_api_jwt_audience'],
            timezone_str=config['timezone_str'],
            basic_auth_username=config['basic_auth_username'],
            basic_auth_password=config['basic_auth_password'],
        )

        cls._async_clients[profile.name] = client
        return client

    @classmethod
    def get_sync_client(cls, aiida_profile: str | None = None) -> AirflowRestApiClientSync:
        """Get or create a cached sync AirflowRestApiClient for the given profile.

        This method reads all configuration from airflow.cfg and creates a sync client
        with the appropriate parameters. Clients are cached per profile.

        Args:
            aiida_profile: AiiDA profile name (optional, uses default if None)

        Returns:
            AirflowRestApiClientSync instance for the profile
        """
        # Read configuration
        config = cls._read_client_config(aiida_profile)
        profile = config['profile']

        # Return cached client if exists and is still open
        if profile.name in cls._sync_clients:
            cached_client = cls._sync_clients[profile.name]
            # Check if the underlying httpx client is still open
            if not cached_client._client.is_closed:
                logger.debug(f"Reusing cached AirflowRestApiClientSync for profile {profile.name}")
                return cached_client
            else:
                # Client was closed, remove from cache and create new one
                logger.debug(f"Cached AirflowRestApiClientSync for profile {profile.name} was closed, creating new one")
                del cls._sync_clients[profile.name]

        logger.debug(f"Creating AirflowRestApiClientSync for profile {profile.name} at {config['host']}:{config['port']}")
        logger.debug(f"Auth config: jwt_secret={'set' if config['jwt_secret'] else 'not set'}, basic_auth={'set' if config['basic_auth_username'] and config['basic_auth_password'] else 'not set'}")

        # Create and cache the sync client
        client = AirflowRestApiClientSync(
            host=config['host'],
            port=config['port'],
            jwt_secret=config['jwt_secret'],
            core_api_jwt_audience=config['core_api_jwt_audience'],
            execution_api_jwt_audience=config['execution_api_jwt_audience'],
            timezone_str=config['timezone_str'],
            basic_auth_username=config['basic_auth_username'],
            basic_auth_password=config['basic_auth_password'],
        )

        cls._sync_clients[profile.name] = client
        return client

    @classmethod
    def get_client(cls, aiida_profile: str | None = None, async_mode: bool = True):
        """Get or create a cached AirflowRestApiClient for the given profile.

        This method provides backward compatibility by returning async clients by default.
        Use get_async_client() or get_sync_client() for explicit control.

        Args:
            aiida_profile: AiiDA profile name (optional, uses default if None)
            async_mode: If True (default), return async client; if False, return sync client

        Returns:
            AirflowRestApiClientAsync or AirflowRestApiClientSync instance
        """
        if async_mode:
            return cls.get_async_client(aiida_profile)
        else:
            return cls.get_sync_client(aiida_profile)

    @classmethod
    def clear_cache(cls, aiida_profile: str | None = None):
        """Clear cached client(s).

        Args:
            aiida_profile: Profile to clear (if None, clears all)
        """
        if aiida_profile is None:
            logger.debug("Clearing all cached AirflowRestApiClient instances")
            cls._async_clients.clear()
            cls._sync_clients.clear()
        else:
            if aiida_profile in cls._async_clients:
                logger.debug(f"Clearing cached AirflowRestApiClientAsync for profile {aiida_profile}")
                del cls._async_clients[aiida_profile]
            if aiida_profile in cls._sync_clients:
                logger.debug(f"Clearing cached AirflowRestApiClientSync for profile {aiida_profile}")
                del cls._sync_clients[aiida_profile]


# Backward compatibility - point to async version
AirflowRestApiClient = AirflowRestApiClientAsync


# Convenience functions for easier usage
def get_airflow_rest_api_client(aiida_profile: str | None = None) -> AirflowRestApiClientAsync:
    """Get or create a cached async AirflowRestApiClient for the given profile.

    This is a convenience wrapper around AirflowRestApiClientManager.get_async_client().
    For backward compatibility, this returns the async client by default.

    Args:
        aiida_profile: AiiDA profile name (optional, uses default if None)

    Returns:
        AirflowRestApiClientAsync instance for the profile
    """
    return AirflowRestApiClientManager.get_async_client(aiida_profile)


def get_airflow_rest_api_client_async(aiida_profile: str | None = None) -> AirflowRestApiClientAsync:
    """Get or create a cached async AirflowRestApiClient for the given profile.

    This is a convenience wrapper around AirflowRestApiClientManager.get_async_client().

    Args:
        aiida_profile: AiiDA profile name (optional, uses default if None)

    Returns:
        AirflowRestApiClientAsync instance for the profile
    """
    return AirflowRestApiClientManager.get_async_client(aiida_profile)


def get_airflow_rest_api_client_sync(aiida_profile: str | None = None) -> AirflowRestApiClientSync:
    """Get or create a cached sync AirflowRestApiClient for the given profile.

    This is a convenience wrapper around AirflowRestApiClientManager.get_sync_client().

    Args:
        aiida_profile: AiiDA profile name (optional, uses default if None)

    Returns:
        AirflowRestApiClientSync instance for the profile
    """
    return AirflowRestApiClientManager.get_sync_client(aiida_profile)
