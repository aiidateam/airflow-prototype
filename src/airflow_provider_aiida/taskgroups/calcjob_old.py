"""
CalcJob TaskGroup with Direct Inheritance

This demonstrates inheriting directly from TaskGroup instead of using a builder pattern.
Much cleaner and more Pythonic approach.
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


class CalcJobTaskGroup(TaskGroup, ABC):
    """
    Abstract TaskGroup for CalcJob workflows.

    Directly inherits from TaskGroup, so instances ARE TaskGroups.
    Subclasses implement prepare() and parse() methods.
    """

    def __init__(self, group_id: str, machine: str, local_workdir: str, remote_workdir: str, **kwargs):
        super().__init__(group_id=group_id, **kwargs)
        self.machine = machine
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir

        # Build the task group when instantiated
        self._build_tasks()

    def _build_tasks(self):
        """Build all tasks within this task group"""

        # Create prepare task using the abstract method
        prepare_task = PythonOperator(
            task_id='prepare',
            python_callable=self.prepare,
            task_group=self,  # Important: assign to this task group
        )

        # Create the core calcjob workflow
        upload_op = UploadOperator(
            task_id="upload",
            machine=self.machine,
            local_workdir=self.local_workdir,
            remote_workdir=self.remote_workdir,
            to_upload_files={},  # Will be pulled from XCom
            task_group=self,
        )

        submit_op = SubmitOperator(
            task_id="submit",
            machine=self.machine,
            local_workdir=self.local_workdir,
            remote_workdir=self.remote_workdir,
            submission_script="",  # Will be pulled from XCom
            task_group=self,
        )

        update_op = UpdateOperator(
            task_id="update",
            machine=self.machine,
            job_id=submit_op.output,
            task_group=self,
        )

        receive_op = ReceiveOperator(
            task_id="receive",
            machine=self.machine,
            local_workdir=self.local_workdir,
            remote_workdir=self.remote_workdir,
            to_receive_files={},  # Will be pulled from XCom
            task_group=self,
        )

        # Create parse task using the abstract method
        parse_task = PythonOperator(
            task_id='parse',
            python_callable=self.parse,
            op_kwargs={'local_workdir': self.local_workdir},
            task_group=self,
        )

        # Set up dependencies within the task group
        prepare_task >> upload_op >> submit_op >> update_op >> receive_op >> parse_task

    @abstractmethod
    def prepare(self, **context) -> Dict[str, Any]:
        """Abstract method to prepare job inputs"""
        pass

    @abstractmethod
    def parse(self, local_workdir: str, **context) -> tuple[int, Dict[str, Any]]:
        """Abstract method to parse job outputs

        Returns:
            tuple[int, Dict[str, Any]]: (exit_status, results)
                exit_status: 0 for success, non-zero for failure/error
                results: Dictionary containing parsed results
        """
        pass
