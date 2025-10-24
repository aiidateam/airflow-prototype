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
from plumpy.base.utils import call_with_super_check 
import plumpy

#from airflow_provider_aiida.aiida_core.engine.calcjobs.calcjob import CalcJob
from aiida.engine.processes.calcjobs.calcjob import CalcJob
if TYPE_CHECKING:
    #from airflow_provider_aiida.aiida_core.engine.processes.process_spec import CalcJobProcessSpec
    from aiida.engine.processes.process_spec import CalcJobProcessSpec

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

    Subclasses must implement define() class method and created() and parse() methods.
    """

    def __init__(
        self,
        group_id: str,
        process_class,
        **inputs
    ):
        """Initialize the AiiDA CalcJob TaskGroup.

        :param group_id: Unique identifier for this task group
        """
        super().__init__(group_id=group_id)
        self.process_class = process_class
        self._build_tasks()

    def _create_calcjob(self, **context):
        from aiida import load_profile
        from aiida.orm import load_code
        load_profile()
        inputs = context['params']
        if 'code' in inputs:
            if isinstance(inputs['code'], str):
                inputs['code'] = load_code(inputs['code'])
            else:
                raise ValueError() # TODO
        else:
            raise ValueError() # TODO

        process = self.process_class(inputs=inputs)
        # For creating pesistence checkpoints and other database related actions
        process.on_entering(process._state)
        process.on_entered(None)

        # NOTE: I don't know aiida internals good enough if this is always given but is assumed in the rest of the code
        assert process.pid == process.node.pk
        return process.node.pk

    def _run_calcjob(self, pk: int):
        """Run the calculation job.

        This means invoking the `presubmit` and storing the temporary folder in the node's repository. Then we move the
        process in the `Wait` state, waiting for the `UPLOAD` transport task to be started.

        :returns: the `Stop` command if a dry run, int if the process has an exit status,
            `Wait` command if the calcjob is to be uploaded

        """
        process = self.load_process(pk) 
        old_state = process._state
        process._state = plumpy.process_states.Running(process=process, run_fn=process.run)
        process.on_entering(process._state)
        process.on_entered(old_state)

        if process.inputs.metadata.dry_run:
            return self.get_absolute_task_id("perform_dry_run")

        if 'remote_folder' in process.inputs:
            return self.get_absolute_task_id("perform_import")

        # The following conditional is required for the caching to properly work. Even if the source node has a process
        # state of `Finished` the cached process will still enter the running state. The process state will have then
        # been overridden by the engine to `Running` so we cannot check that, but if the `exit_status` is anything other
        # than `None`, it should mean this node was taken from the cache, so the process should not be rerun.
        if process.node.exit_status is not None:
            return self.get_absolute_task_id("cached_calcjob")

        # Launch the wait operation
        return self.get_absolute_task_id("wait_calcjob")

    def _wait_calcjob(self, pk: int):
        """Run the calculation job.

        This means invoking the `presubmit` and storing the temporary folder in the node's repository. Then we move the
        process in the `Wait` state, waiting for the `UPLOAD` transport task to be started.

        :returns: the `Stop` command if a dry run, int if the process has an exit status,
            `Wait` command if the calcjob is to be uploaded

        """
        process = self.load_process(pk) 
        # TODO check done_callback
        old_state = process._state
        process._state = plumpy.process_states.Waiting(process=process, done_callback=None)
        process.on_entering(process._state)
        process.on_entered(old_state)

    def _finish_calcjob(self, pk: int):
        process = self.load_process(pk) 
        # TODO result is probably
        old_state = process._state
        process._state = plumpy.process_states.Finished(process=process, result=0, successful=True)
        process.on_entering(process._state)
        process.on_entered(old_state)

    def _perform_dry_run(self, pk: int):
        calcjob = self.load_process(pk)
        return calcjob._perform_dry_run()

    def _perform_import(self, pk: int):
        calcjob = self.load_process(pk) 
        exit_code = calcjob._perform_import()
        return exit_code

    def _cached_calcjob(self, pk: int):
        # Normally the outputs will be attached to the process by a ``Parser``, if defined in the inputs. But in
        # this case, the parser will not be called. The outputs will already have been added to the process node
        # though, so all that needs to be done here is just also assign them to the process instance. This such that
        # when the process returns its results, it returns the actual outputs and not an empty dictionary.
        from aiida import load_profile
        from aiida.orm import load_node
        from aiida.common.links import LinkType
        load_profile()
        node = load_node(pk)
        self._outputs = node.base.links.get_outgoing(link_type=LinkType.CREATE).nested()
        return node.exit_status

    def _parse(self, pk: int, retrieve_op_output: dict, **context):
        temp_folder = retrieve_op_output['temp_folder']
        calcjob = self.load_process(pk) 
        result = calcjob.parse(temp_folder)
        return result

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
        create_op = PythonOperator(
            task_id='create_calcjob',
            python_callable=self._create_calcjob,
            task_group=self,
        )
        # Get the node_pk to use downstream
        node_pk_ref = create_op.output

        run_op = BranchPythonOperator(
            task_id="run_calcjob",
            python_callable=self._run_calcjob,
            op_kwargs={"pk": node_pk_ref},
            task_group=self,
        )

        wait_op = PythonOperator(
            task_id='wait_calcjob',
            python_callable=self._wait_calcjob,
            op_kwargs={'pk': node_pk_ref},
            task_group=self,
        )
        
        finish_op = PythonOperator(
            task_id='finish_calcjob',
            python_callable=self._finish_calcjob,
            op_kwargs={'pk': node_pk_ref},
            task_group=self,
        )

        perform_import_op = PythonOperator(
            task_id='perform_import',
            python_callable=self._perform_import,
            op_kwargs={'pk': node_pk_ref},
            task_group=self,
        )

        perform_dry_run_op = PythonOperator(
            task_id='perform_dry_run',
            python_callable=self._perform_dry_run,
            op_kwargs={'pk': node_pk_ref},
            task_group=self,
        )

        cached_calcjob_op = PythonOperator(
            task_id='cached_calcjob',
            python_callable=self._cached_calcjob,
            op_kwargs={'pk': node_pk_ref},
            task_group=self,
        )
        
        # Upload task
        upload_op = CalcJobUploadOperator(
            task_id="upload",
            node_pk=node_pk_ref,
            task_group=self,
        )
        
        # TODO unstash
        #if self.enable_unstash:
        #    unstash_op = CalcJobUnstashOperator(
        #        task_id="unstash",
        #        node_pk=node_pk_ref,
        #        task_group=self,
        #    )


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

        parse_op = PythonOperator(
            task_id='parse',
            python_callable=self._parse,
            op_kwargs={'pk': node_pk_ref,
                       'retrieve_op_output': retrieve_op.output},
            task_group=self,
        )

        # CREATED
        create_op >> run_op 
        ## RUNNING
        run_op >> [perform_dry_run_op, perform_import_op, cached_calcjob_op, wait_op]
        ## WAITING
        wait_op >> upload_op >> submit_op >> update_op >> retrieve_op >> parse_op
        parse_op >> finish_op

    ### UTILS ###
    def get_absolute_task_id(self, task_id: str) -> str:
        return ".".join([self.group_id, task_id])
    
    @staticmethod
    def load_process(node_pk: int) -> CalcJob:
        """Loads the CalcJob from the checkpoint in the CalcJobNode"""
        from aiida import load_profile
        from aiida.orm import load_node
        # is the calcjob node referring to node?
        load_profile()
        #node = load_node(node_pk)
        from aiida.engine import persistence
        from plumpy.persistence import LoadSaveContext
        persister = persistence.AiiDAPersister()
        saved_state = persister.load_checkpoint(node_pk)
        process = saved_state.unbundle(LoadSaveContext())
        return process
        #create_op >> run_op >> [perform_dry_run_op, perform_import_op, cached_calcjob_op] >> wait_op >> upload_op >> submit_op >> update_op >> retrieve_op >> finish_op
