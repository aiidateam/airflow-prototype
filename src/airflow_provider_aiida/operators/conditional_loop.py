"""Conditional loop operator that executes steps while a condition is true.

This operator implements a loop pattern where:
1. A condition is checked
2. If true, a series of steps are executed
3. After steps complete, tasks are cleared to re-check the condition
4. Loop continues until condition is false or max iterations reached
"""
from __future__ import annotations

from typing import Callable, Sequence

from airflow.models import BaseOperator
from airflow.models.taskinstance import clear_task_instances
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException


class ConditionalLoopTaskGroup(TaskGroup):
    """
    Task group that implements a conditional loop pattern.

    This creates three types of tasks:
    1. Condition check task - evaluates whether to continue
    2. Step tasks - execute the actual work
    3. Loop control task - decides whether to clear tasks and loop again

    Args:
        group_id: ID for the task group
        condition_callable: Function that returns True to continue, False to stop
        step_callables: List of functions to execute when condition is True
        max_iterations: Maximum number of iterations
        **kwargs: Additional arguments passed to TaskGroup
    """

    def __init__(
        self,
        group_id: str,
        condition_callable: Callable,
        step_callables: list[Callable],
        max_iterations: int = 3,
        **kwargs,
    ):
        super().__init__(group_id=group_id, **kwargs)

        self.condition_callable = condition_callable
        self.step_callables = step_callables
        self.max_iterations = max_iterations

        # Create tasks within this group context
        with self:
            self._create_tasks()

    def _exit(self):
        pass

    def _create_tasks(self):
        """Create the condition, step, and loop control tasks."""

        # Task 1: Check condition
        #check_condition = PythonOperator(
        #    task_id='check_condition',
        #    python_callable=self._check_condition_wrapper,
        #)
        check_condition = BranchPythonOperator(
            task_id='check_condition',
            python_callable=self._check_condition_wrapper,
        )
        exit_task = PythonOperator(
            task_id='exit',
            python_callable=self._exit,
        )
        check_condition >> exit_task

        # Task 2: Execute steps (only if condition is true)
        step_tasks = []
        last_step_task = None
        for i, step_callable in enumerate(self.step_callables):
            step_task = PythonOperator(
                task_id=f'step_{i}',
                python_callable=self._step_wrapper,
                op_kwargs={'step_callable': step_callable, 'step_index': i},
            )
            step_tasks.append(step_task)
            if last_step_task is None:
                check_condition >> step_task
            else:
                last_step_task >> step_task
            last_step_task = step_task

        # Task 3: Loop control (clear tasks if should continue)
        loop_control = PythonOperator(
            task_id='loop_control',
            python_callable=self._loop_control,
        )

        # Set up dependencies
        step_task >> loop_control

    def _check_condition_wrapper(self, **context):
        """
        Wrapper for the condition callable that handles iteration tracking.

        Stores the condition result and iteration count in XCom.
        Raises AirflowSkipException if condition is False to skip downstream tasks.
        """
        ti = context['ti']

        # Get current iteration count
        iteration = ti.xcom_pull(key='iteration_count', task_ids=f"{self.group_id}.loop_control") or 0
        ti.xcom_push(key='iteration_count', value=iteration)

        self.log.info(f"Checking condition (iteration {iteration})")

        # Call the actual condition function
        if iteration < self.max_iterations:
            try:
                should_continue = self.condition_callable(**context)
                self.log.info(f"Condition callable result: {should_continue}")
            except Exception as e:
                should_continue = False
                self.log.error(f"Condition callable failed: {e}")
                ti.xcom_push(key='condition_result', value=False)
                ti.xcom_push(key='should_continue', value=False)
                raise
        else:
            self.log.info(f"Max number of iterations {self.max_iterations} reached.")
            should_continue = False


        # Store result in XCom
        ti.xcom_push(key='condition_result', value=should_continue)

        dag_run_id = context['run_id']
        dag_id = context['dag'].dag_id

        if should_continue:
            # TODO  do cleaner
            task_ids_to_clear = [f'{self.group_id}.step_{i}' for i in range(len(self.step_callables))]
            task_ids_to_clear.append(f'{self.group_id}.loop_control')
            self._send_clear_tasks_message(task_ids_to_clear, dag_id, dag_run_id)
            return f"{self.group_id}.step_0"
        else:
            self._send_clear_tasks_message([f'{self.group_id}.exit'], dag_id, dag_run_id)
            return f"{self.group_id}.exit"

    def _step_wrapper(self, step_callable: Callable, step_index: int, **context):
        """
        Wrapper for step callables.

        Executes the step and tracks progress.
        """
        ti = context['ti']
        iteration = ti.xcom_pull(key='iteration_count') or 0

        self.log.info(f"Executing step {step_index} (iteration {iteration})")

        try:
            result = step_callable(**context)
            self.log.info(f"Step {step_index} completed successfully")
            return result
        except Exception as e:
            self.log.error(f"Step {step_index} failed: {e}")
            raise

    def _loop_control(self, **context):
        """
        Control whether to loop again.

        This task:
        1. Checks if condition was True and steps completed
        2. Increments iteration counter
        3. Clears condition and step tasks if should continue
        4. Otherwise, ends the loop
        """
        from airflow.settings import Session
        from airflow.models import DagRun

        ti = context['ti']
        dag_run_id = context['run_id']
        dag_id = context['dag'].dag_id

        # Get current state
        iteration = ti.xcom_pull(key='iteration_count', task_ids=f"{self.group_id}.check_condition")
        condition_result = ti.xcom_pull(key='condition_result')

        self.log.info(f"Loop control - iteration {iteration}, condition: {condition_result}")

        # Increment iteration counter for next loop
        next_iteration = iteration + 1
        ti.xcom_push(key='iteration_count', value=next_iteration)

        self.log.info(f"Condition is True, clearing tasks to start iteration {next_iteration}")
        task_ids_to_clear = [f'{self.group_id}.check_condition']
        self._send_clear_tasks_message(task_ids_to_clear, dag_id, dag_run_id)

    def _send_clear_tasks_message(self, task_ids_to_clear, dag_id, dag_run_id):
        import requests
        from airflow_provider_aiida.aiida_core.manage.configuration.config import get_airflow_home
        import configparser
        import jwt
        import datetime

        # Get Airflow API credentials from environment or config
        from airflow_provider_aiida.aiida_core import load_profile
        profile = load_profile()

        # TODO figure out ports from config
        airflow_api_url = 'http://0.0.0.0:8080/api/v2'
        # Build the task IDs to clear

        # Call the REST API to clear task instances
        url = f"{airflow_api_url}/dags/{dag_id}/clearTaskInstances"
        payload = {
            "dry_run": False,
            "only_failed": False,
            "dag_run_id": dag_run_id,
            "task_ids": task_ids_to_clear,
            "reset_dag_runs": False,
        }

        self.log.info(f"Calling REST API to clear tasks: {url}")
        self.log.info(f"Payload: {payload}")

        try:
            # Generate JWT token for authentication
            airflow_home = get_airflow_home(profile)
            config_file = airflow_home / 'airflow.cfg'

            # Read JWT secret from config
            config = configparser.ConfigParser()
            config.read(config_file)

            jwt_secret = config.get('api_auth', 'jwt_secret', fallback=None)

            # Get timezone from airflow.cfg
            timezone_str = config.get('core', 'default_timezone', fallback='utc')

            # Get JWT audience from airflow.cfg (defaults to 'apache-airflow')
            jwt_audience = config.get('api_auth', 'jwt_audience', fallback='apache-airflow')

            headers = {
                'Content-Type': 'application/json',
            }

            # Generate JWT token if we have the secret
            if jwt_secret:
                # Create JWT token matching Airflow's format
                import uuid
                import time
                from zoneinfo import ZoneInfo

                # Get current time in the configured timezone
                if timezone_str.lower() == 'system':
                    # Use system local timezone
                    now_dt = datetime.datetime.now()
                else:
                    # Use specified timezone
                    try:
                        tz = ZoneInfo(timezone_str)
                        now_dt = datetime.datetime.now(tz)
                    except Exception:
                        # Fallback to UTC if timezone is invalid
                        self.log.warning(f"Invalid timezone {timezone_str}, using UTC")
                        now_dt = datetime.datetime.now(datetime.timezone.utc)

                # Convert to UTC timestamp
                now = int(now_dt.timestamp())

                payload_jwt = {
                    'jti': uuid.uuid4().hex,  # JWT ID
                    'iss': 'airflow',  # Issuer
                    'aud': jwt_audience,  # Audience (from config, defaults to 'apache-airflow')
                    'sub': 'airflow',  # Subject (user identity)
                    'role': 'admin',  # User role (Admin for full permissions)
                    'nbf': now - 10,  # Not before (10 seconds ago to account for clock skew)
                    'exp': now + 300,  # Expiration (5 minutes from now)
                    'iat': now,  # Issued at
                }

                self.log.info(f"JWT token config: audience={jwt_audience}, timezone={timezone_str}")
                self.log.info(f"JWT token timestamps: nbf={payload_jwt['nbf']}, iat={payload_jwt['iat']}, exp={payload_jwt['exp']}")
                self.log.info(f"Current time: {now}")

                # Encode with HS512 algorithm (Airflow's default)
                token = jwt.encode(payload_jwt, jwt_secret, algorithm='HS512', headers={'alg': 'HS512'})
                headers['Authorization'] = f'Bearer {token}'
                self.log.info("Generated JWT token for authentication (HS512)")
            else:
                self.log.warning("No JWT secret found in config, trying without authentication")

            self.log.info(f"Request headers: {headers}")
            self.log.info(f"Request URL: {url}")
            self.log.info(f"Request payload: {payload}")

            response = requests.post(url, json=payload, headers=headers)

            self.log.info(f"Response status code: {response.status_code}")
            self.log.info(f"Response headers: {response.headers}")
            self.log.info(f"Response text: {response.text}")

            response.raise_for_status()
            self.log.info(f"Successfully cleared tasks: {response.json()}")
        except requests.exceptions.HTTPError as e:
            self.log.error(f"HTTP Error: {e}")
            self.log.error(f"Response status: {response.status_code}")
            self.log.error(f"Response body: {response.text}")
            # Don't fail the task, just log the error
            pass
        except Exception as e:
            self.log.error(f"Failed to clear tasks via REST API: {e}")
            import traceback
            self.log.error(traceback.format_exc())
            # Don't fail the task, just log the error
            pass
