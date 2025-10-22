"""
Async AiiDA CalcJob TaskGroup using AiiDA Core Task Functions

This taskgroup uses the AiiDA async operators that directly wrap aiida-core's
calcjob task functions, providing native AiiDA CalcJob execution in Airflow.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow_provider_aiida.aiida_core.engine.calcjobs.calcjob import CalcJob
if TYPE_CHECKING:
    from airflow_provider_aiida.aiida_core.engine.processes.process_spec import CalcJobProcessSpec

from airflow_provider_aiida.operators.tasks import (
    CalcJobUploadOperator,
    CalcJobSubmitOperator,
    CalcJobUpdateOperator,
    CalcJobMonitorOperator,
    CalcJobRetrieveOperator,
    CalcJobStashOperator,
    CalcJobUnstashOperator,
)


class CalcJobTaskGroup(TaskGroup, ABC):
    """
    Abstract TaskGroup for async AiiDA CalcJob workflows using deferrable operators.

    This version directly uses AiiDA's calcjob task functions through triggers,
    providing native AiiDA CalcJob execution with Airflow's async capabilities.

    The workflow follows AiiDA's CalcJob state machine:
    - UPLOAD -> SUBMIT -> UPDATE -> STASH -> RETRIEVE -> PARSE

    Or if skip_submit is True:
    - UPLOAD -> STASH -> RETRIEVE -> PARSE

    Subclasses must implement define() class method and create_calcjob() and parse() methods.
    """

    def __init__(
        self,
        group_id: str,
        process,
        **inputs
    ):
        """Initialize the AiiDA CalcJob TaskGroup.

        :param group_id: Unique identifier for this task group
        """
        super().__init__(group_id=group_id)
        self.process = process

        self._build_tasks()

    def _create_calcjob(self, **context):
        breakpoint()
        calcjob = self.process.__init__(inputs=context['params'])

        #if self.inputs.metadata.dry_run:
        #    await self._perform_dry_run()
        #    return plumpy.process_states.Stop(None, True)

        #if 'remote_folder' in self.inputs:
        #    prepare_task = PythonOperator(
        #        task_id='prepare_calcjob',
        #        python_callable=self._perform_import,
        #        op_kwargs={'inputs': self.inputs},
        #        task_group=self,
        #    )
        #    PythonOperator(
        #        self._self._perform_import
        #            )
        #    exit_code = await self._perform_import()
        #    # TODO(migration) returned exit_code logic?
        #    #return exit_code

        ## The following conditional is required for the caching to properly work. Even if the source node has a process
        ## state of `Finished` the cached process will still enter the running state. The process state will have then
        ## been overridden by the engine to `Running` so we cannot check that, but if the `exit_status` is anything other
        ## than `None`, it should mean this node was taken from the cache, so the process should not be rerun.
        #if self.node.exit_status is not None:
        #    # Normally the outputs will be attached to the process by a ``Parser``, if defined in the inputs. But in
        #    # this case, the parser will not be called. The outputs will already have been added to the process node
        #    # though, so all that needs to be done here is just also assign them to the process instance. This such that
        #    # when the process returns its results, it returns the actual outputs and not an empty dictionary.
        #    self._outputs = self.node.base.links.get_outgoing(link_type=LinkType.CREATE).nested()
        #    return self.node.exit_status
        return calcjob.pk

    def _prepare_calcjob_task(self):
        pass


    @classmethod
    def define(cls, spec: CalcJobProcessSpec) -> None:
        """Define the input/output specification using AiiDA's CalcJobProcessSpec.

        Subclasses should override this to specify their inputs.

        :param spec: CalcJobProcessSpec to define inputs/outputs on
        """
        CalcJob.define(spec)


    def _build_tasks(self):
        """Build all tasks within this task group following AiiDA's CalcJob workflow."""

        # Task to create CalcJobNode and prepare for submission (only if node_pk not provided)
        create_calcjob_task = PythonOperator(
            task_id='create_calcjob',
            python_callable=self._create_calcjob,
            task_group=self,
        )
        # Get the node_pk to use downstream
        node_pk_ref = create_calcjob_task.output

        prepare_calcjob_task = PythonOperator(
            task_id='run',
            python_callable=self._prepare_calcjob_task,
            task_group=self,
        )
        
        # TODO unstash
        #if self.enable_unstash:
        #    unstash_op = CalcJobUnstashOperator(
        #        task_id="unstash",
        #        node_pk=node_pk_ref,
        #        task_group=self,
        #    )

        # Upload task
        upload_op = CalcJobUploadOperator(
            task_id="upload",
            node_pk=node_pk_ref,
            task_group=self,
        )

        # TODO
        # Branch based on skip_submit flag
        #branch_task = BranchPythonOperator(
        #    task_id="check_skip_submit",
        #    python_callable=self._check_skip_submit,
        #    task_group=self,
        #)

        # Submit task (only if not skipping)
        submit_op = CalcJobSubmitOperator(
            task_id="submit",
            node_pk=node_pk_ref,
            task_group=self,
        )

        # Update task (monitor job status)
        update_op = CalcJobUpdateOperator(
            task_id="update",
            node_pk=node_pk_ref,
            task_group=self,
        )

        # Optional: Monitor task
        # TODO
        #if self.enable_monitors:
        #    monitor_op = CalcJobMonitorOperator(
        #        task_id="monitor",
        #        node_pk=node_pk_ref,
        #        monitors_pk=self.monitors_pk,
        #        task_group=self,
        #    )

        # TODO stash
        # Optional: Stash task
        #if self.enable_stash:
        #    stash_op = CalcJobStashOperator(
        #        task_id="stash",
        #        node_pk=node_pk_ref,
        #        task_group=self,
        #    )

        # Retrieve task
        retrieve_op = CalcJobRetrieveOperator(
            task_id="retrieve",
            node_pk=node_pk_ref,
            task_group=self,
        )

        # Parse task
        # TODO
        #parse_task = PythonOperator(
        #    task_id='parse',
        #    python_callable=self.parse,
        #    op_kwargs={'retrieved_temporary_folder': self.retrieved_temporary_folder},
        #    task_group=self,
        #)

        create_calcjob_task >> prepare_calcjob_task >> upload_op >> submit_op >> update_op >> retrieve_op
