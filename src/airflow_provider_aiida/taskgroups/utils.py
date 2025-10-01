"""
Utils TaskGroup with Direct Inheritance
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.models import Param
from airflow.sdk import task

from airflow_provider_aiida.operators.calcjob import (
    UploadOperator,
    SubmitOperator,
    UpdateOperator,
    ReceiveOperator
)



class WhileTaskGroup(TaskGroup, ABC):
    """
    Abstract TaskGroup for while loop workflows.

    Executes a body task repeatedly while a condition is true.
    Subclasses implement condition() and body() methods.
    """

    def __init__(self, group_id: str, max_iterations: int = 100, **kwargs):
        super().__init__(group_id=group_id, **kwargs)
        self.max_iterations = max_iterations

        # Build the task group when instantiated
        self._build_tasks()

    def _build_tasks(self):
        """Build all tasks within this task group"""

        @task(task_id='while_loop', task_group=self)
        def while_loop_task(**context):
            """Execute while loop logic"""
            iteration = 0
            prev_result = None

            while iteration < self.max_iterations:
                # Pull previous iteration result if exists
                if iteration > 0:
                    prev_result = context['task_instance'].xcom_pull(
                        task_ids=f'{self.group_id}.while_loop',
                        key=f'iteration_{iteration - 1}'
                    )

                # Check condition (pass previous result)
                should_continue = self.condition(
                    iteration=iteration,
                    prev_result=prev_result,
                    **context
                )

                if not should_continue:
                    print(f"While loop condition is false at iteration {iteration}")
                    break

                # Execute body (pass previous result)
                result = self.body(
                    iteration=iteration,
                    prev_result=prev_result,
                    **context
                )

                # Push iteration result to XCom
                context['task_instance'].xcom_push(
                    key=f'iteration_{iteration}',
                    value={'iteration': iteration, 'result': result}
                )

                iteration += 1

            if iteration >= self.max_iterations:
                print(f"While loop reached maximum iterations: {self.max_iterations}")

            return {"iterations": iteration, "completed": iteration < self.max_iterations}

        # Create the while loop task
        while_loop_task()

    @abstractmethod
    def condition(self, iteration: int, prev_result: Any = None, **context) -> bool:
        """
        Abstract method to check if loop should continue.

        Args:
            iteration: Current iteration number (starts at 0)
            prev_result: Result from previous iteration (None for first iteration)
            **context: Airflow context

        Returns:
            bool: True to continue loop, False to break
        """
        pass

    @abstractmethod
    def body(self, iteration: int, prev_result: Any = None, **context) -> Any:
        """
        Abstract method to execute loop body.

        Args:
            iteration: Current iteration number (starts at 0)
            prev_result: Result from previous iteration (None for first iteration)
            **context: Airflow context

        Returns:
            Any: Result from body execution
        """
        pass
    
