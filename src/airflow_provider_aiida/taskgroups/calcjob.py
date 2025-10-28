"""
Async AiiDA CalcJob TaskGroup using AiiDA Core Task Functions

This taskgroup uses the AiiDA async operators that directly wrap aiida-core's
calcjob task functions, providing native AiiDA CalcJob execution in Airflow.
"""

from __future__ import annotations

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


import plumpy

from aiida.engine.processes.calcjobs.calcjob import CalcJob

from airflow_provider_aiida.operators.tasks import (
    CalcJobUploadOperator,
    CalcJobSubmitOperator,
    CalcJobUpdateOperator,
    CalcJobMonitorOperator,
    CalcJobRetrieveOperator,
    CalcJobStashOperator,
    CalcJobUnstashOperator,
)

# TODO
# - stashing monitoring
# - exit code, needs to be done in the class, need to finish this for every operation that ends with Stop or returns not Wait in Running state 
#   - perform import
#   - self._monitor_result
# - check if transition_to is used everywhere from the same utills 
# TODO(low-prio)
# - cancellable, I don't know if really needed because it is related to pause/kill
# - connected to everything not so important

class CalcJobTaskGroup(TaskGroup):
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
        process_class,
        node_pk: int
    ):
        """Initialize the AiiDA CalcJob TaskGroup.

        :param group_id: Unique identifier for this task group
        """
        super().__init__(group_id=process_class.__name__)
        self.process_class = process_class
        self.node_pk = node_pk
        self._build_tasks()

    # TODO I think we should not introduce it in the PR since we will not autotranslate builder
    #def _create_calcjob(self, **context):
    #    from aiida import load_profile
    #    from aiida.orm import load_code
    #    load_profile()

    #    inputs = context['params']
    #    # For REST API submission we pass the node_pk
    #    if 'node_pk' in inputs:
    #        # TODO check if checkpoint is actually available
    #        process = self.load_process(inputs['node_pk'])
    #        # TODO check state process._state ==
    #        return inputs['node_pk']
    #    
    #    # TODO we need to do this more strictly, quite some effort to streamline
    #    if 'code' in inputs:
    #        if isinstance(inputs['code'], str):
    #            inputs['code'] = load_code(inputs['code'])
    #        else:
    #            raise ValueError() # TODO
    #    else:
    #        raise ValueError() # TODO

    #    process = self.process_class(inputs=inputs)

    #    # NOTE: I don't know aiida internals good enough if this is always given but is assumed in the rest of the code
    #    assert process.pid == process.node.pk
    #    return process.node.pk

    def _calcjob_run(self, pk: int):
        """Run the calculation job.

        This means invoking the `presubmit` and storing the temporary folder in the node's repository. Then we move the
        process in the `Wait` state, waiting for the `UPLOAD` transport task to be started.

        :returns: the `Stop` command if a dry run, int if the process has an exit status,
            `Wait` command if the calcjob is to be uploaded

        """
        process = self.load_process(pk)
        # NOTE: process.run is never executed since we never launch it but a function of process needs to passed due to serialization 
        new_state = plumpy.process_states.Running(process=process, run_fn=process.run)
        process.transition_to(new_state)


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
        return self.get_absolute_task_id("upload")

    def _check_if_unstash(self, node_pk: int):
        """TODO"""
        node = self.load_process(node_pk).node
        if node.get_option('unstash') and node.process_type == 'aiida.calculations:core.unstash':
            return self.get_absolute_task_id("unstash")
        return self.get_absolute_task_id("check_if_skip_submit")

    def _check_if_stash(self, node_pk: int):
        """TODO"""
        node = self.load_process(node_pk).node
        if node.get_option('stash'):
            return self.get_absolute_task_id("stash")
        return self.get_absolute_task_id("retrieve")

    def _check_if_skip_submit(self, **context):
        skip_submit = context["ti"].xcom_pull(self.get_absolute_task_id("upload"))
        if not isinstance(skip_submit, bool):
            raise TypeError(f"skip_submit is not bool but {type(skip_submit)}")

        if skip_submit:
            return self.get_absolute_task_id("stash")
        else:
            return self.get_absolute_task_id("submit")

    def _perform_dry_run(self, pk: int):
        calcjob = self.load_process(pk)
        result = calcjob._perform_dry_run()
        # TODO exit code
        raise NotImplementedError()

    def _perform_import(self, pk: int):
        calcjob = self.load_process(pk) 
        exit_code = calcjob._perform_import()
        # TODO exit code transition
        raise NotImplementedError()
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
        process = self.load_process(pk)
        # NOTE: process.run is never executed since we never launch it but a function of process needs to passed due to serialization 
        new_state = plumpy.process_states.Running(process=process, run_fn=process.run)
        process.transition_to(new_state)

        result = process.parse(temp_folder)

        new_state = plumpy.process_states.Finished(process=process, result=result, successful=True)
        process.transition_to(new_state)
        return result


    def terminate(self, pk: int, retrieve_op_output: dict, **context):
        if isinstance(result, ExitCode):
            # The scheduler plugin returned an exit code from ``Scheduler.submit_job`` indicating the
            # job submission failed due to a non-transient problem and the job should be terminated.
            return self.create_state(ProcessState.RUNNING, self.process.terminate, result)

    def _build_tasks(self):
        """Build all tasks within this task group following AiiDA's CalcJob workflow."""

        # Task to create CalcJobNode and prepare for submission (only if node_pk not provided)
        # TODO I think we should not introduce it in the PR since we will not autotranslate builder
        #create_op = PythonOperator(
        #    task_id='create_calcjob',
        #    python_callable=self._create_calcjob,
        #    task_group=self,
        #)
        # Get the node_pk to use downstream
        node_pk_ref = self.node_pk

        calcjob_run_op = BranchPythonOperator(
            task_id="calcjob_run",
            python_callable=self._calcjob_run,
            op_kwargs={"pk": node_pk_ref},
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

        check_if_unstash_op = BranchPythonOperator(
            task_id="check_if_unstash",
            python_callable=self._check_if_unstash,
            op_kwargs={"node_pk": node_pk_ref},
            task_group=self,
        )
        
        check_if_skip_submit_op = BranchPythonOperator(
            task_id="check_if_skip_submit",
            python_callable=self._check_if_skip_submit,
            # TODO remember that you can pass arguments like this but this creates deps
            #op_kwargs={"skip_submit": upload_op.output},
            #op_kwargs={"skip_submit": "{{ ti.xcom_pull(task_ids='ArithmeticAddCalculation.upload') }}"},
            #op_kwargs={"upload_output": upload_op.output},
            #op_kwargs={"skip_submit": "{{ ti.xcom_pull(task_ids='ArithmeticAddCalculation.upload', key='skip_submit') }}"},
            task_group=self,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
 
        unstash_op = CalcJobUnstashOperator(
            task_id="unstash",
            node_pk=node_pk_ref,
            task_group=self,
        )

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
            submit_successful=submit_op.output,
            task_group=self,
        )

        # Optional: Monitor task
        # TODO
        #monitor_op = CalcJobMonitorOperator(
        #    task_id="monitor",
        #    node_pk=node_pk_ref,
        #    monitors_pk=self.monitors_pk,
        #    task_group=self,
        #)

        check_if_stash_op = BranchPythonOperator(
            task_id="check_if_stash",
            python_callable=self._check_if_stash,
            op_kwargs={"node_pk": node_pk_ref},
            task_group=self,
        )

        stash_op = CalcJobStashOperator(
            task_id="stash",
            node_pk=node_pk_ref,
            task_group=self,
        )

        # Retrieve task
        retrieve_op = CalcJobRetrieveOperator(
            task_id="retrieve",
            node_pk=node_pk_ref,
            task_group=self,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        parse_op = PythonOperator(
            task_id='parse',
            python_callable=self._parse,
            op_kwargs={'pk': node_pk_ref,
                       'retrieve_op_output': retrieve_op.output},
            task_group=self,
        )
        #unstash_noop_op = EmptyOperator(task_id="unstash_noop", task_group=self)

        ## RUNNING
        calcjob_run_op >> [perform_dry_run_op, perform_import_op, cached_calcjob_op, upload_op]
        ## WAITING
        upload_op >> check_if_unstash_op
        check_if_unstash_op >> [unstash_op, check_if_skip_submit_op]
        unstash_op >> check_if_skip_submit_op
        check_if_skip_submit_op >> [stash_op, submit_op]
        submit_op >> update_op >> check_if_stash_op
        check_if_stash_op >> stash_op >> retrieve_op
        check_if_stash_op >> retrieve_op
        ## RUNNING 
        retrieve_op  >> parse_op


    ### UTILS ###
    def get_absolute_task_id(self, task_id: str) -> str:
        return ".".join([self.group_id, task_id])
    
    # TODO remove, not important
    def transition_process_to_state(self, process: plumpy.Process, state: plumpy.ProcessState) -> CalcJob:
        if state == plumpy.ProcessState.CREATED:
            raise NotImplementedError() # TODO
            process.on_entering(process._state)
            process.on_entered(None)
        elif state == plumpy.ProcessState.RUNNING:
            # NOTE: process.run is never executed since we never launch it but a function of process needs to passed due to serialization 
            new_state = plumpy.process_states.Running(process=process, run_fn=process.run)
        elif state == plumpy.ProcessState.WAITING:
            new_state = plumpy.process_states.Waiting(process=process, done_callback=None)
        elif state == plumpy.ProcessState.FINISHED:
            raise ValueError("You should not be here") # TODO refactor
            new_state = plumpy.process_states.Finished(process=process, result=0, successful=True)
        else:
            raise ValueError()
        process.transition_to(new_state)

    @staticmethod
    def load_process(node_pk: int) -> CalcJob:
        """Loads the CalcJob from the checkpoint in the CalcJobNode"""
        from aiida import load_profile
        load_profile()
        from aiida.engine import persistence
        from plumpy.persistence import LoadSaveContext
        persister = persistence.AiiDAPersister()
        saved_state = persister.load_checkpoint(node_pk)
        return saved_state.unbundle(LoadSaveContext())

    # TODO still needed?
    @staticmethod
    def save_checkpoint(process):
        # this is a copy from on_entered, this only stores the outputs before serialization so we can serialize the uuid
        try:
            process.update_outputs()
        except ValueError:
            raise
        process._save_checkpoint()
