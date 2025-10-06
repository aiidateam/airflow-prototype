"""
Async AiiDA CalcJob TaskGroup using AiiDA Core Task Functions

This taskgroup uses the AiiDA async operators that directly wrap aiida-core's
calcjob task functions, providing native AiiDA CalcJob execution in Airflow.
"""

from abc import ABC, abstractmethod
from typing import Any

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator

from aiida.engine import CalcJobProcessSpec

from airflow_provider_aiida.operators.async_aiida_calcjob import (
    AiiDAAsyncUploadOperator,
    AiiDAAsyncSubmitOperator,
    AiiDAAsyncUpdateOperator,
    AiiDAAsyncMonitorOperator,
    AiiDAAsyncRetrieveOperator,
    AiiDAAsyncStashOperator,
    AiiDAAsyncUnstashOperator,
)


class AsyncAiiDACalcJobTaskGroup(TaskGroup, ABC):
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
        node_pk: int | None = None,
        retrieved_temporary_folder: str | None = None,
        enable_stash: bool = False,
        enable_unstash: bool = False,
        enable_monitors: bool = False,
        monitors_pk: int | None = None,
        update_sleep_interval: int = 5,
        submit_script_filename: str = '_aiidasubmit.sh',
        **kwargs
    ):
        """Initialize the AiiDA CalcJob TaskGroup.

        :param group_id: Unique identifier for this task group
        :param node_pk: Primary key of existing CalcJobNode (if reusing existing node)
        :param retrieved_temporary_folder: Path for retrieved files
        :param enable_stash: Whether to enable stashing (optional)
        :param enable_unstash: Whether to enable unstashing (optional)
        :param enable_monitors: Whether to enable monitoring
        :param monitors_pk: Primary key of CalcJobMonitors node
        :param update_sleep_interval: Seconds between update checks
        :param submit_script_filename: Name of the submit script file
        """
        super().__init__(group_id=group_id)
        self.node_pk = node_pk
        self.retrieved_temporary_folder = retrieved_temporary_folder or f"/tmp/aiida_retrieved_{group_id}"
        self.enable_stash = enable_stash
        self.enable_unstash = enable_unstash
        self.enable_monitors = enable_monitors
        self.monitors_pk = monitors_pk
        self.update_sleep_interval = update_sleep_interval
        self.submit_script_filename = submit_script_filename

        # Initialize spec and store inputs using AiiDA's CalcJobProcessSpec
        self._spec = CalcJobProcessSpec()
        self.define(self._spec)

        # Store inputs from kwargs based on spec
        # These may be template strings that will be rendered at task execution time
        self.inputs = {}
        for port_name in self._spec.inputs.ports.keys():
            if port_name in kwargs:
                self.inputs[port_name] = kwargs[port_name]

        # Build the task group when instantiated
        self._build_tasks()

    def _create_calcjob_and_prepare(self, inputs: dict, **context) -> int:
        """Create CalcJobNode, prepare for submission, and store everything.

        This combines node creation and preparation into one task so we can
        store files in the repository before the node is stored.

        Args:
            inputs: Dictionary of inputs (templates already rendered by Airflow)

        Returns:
            int: Primary key of the created CalcJobNode
        """
        from aiida.orm import CalcJobNode, load_computer, to_aiida_type
        from aiida.common.datastructures import CalcJobState
        from aiida.common.folders import SandboxFolder
        from aiida.manage.configuration import get_config_option
        from types import SimpleNamespace

        # Get computer from inputs (should be in spec)
        if 'computer' not in inputs:
            raise ValueError("CalcJob requires 'computer' input in spec")

        # Convert inputs to AiiDA nodes using to_aiida_type
        # At this point, templates are already rendered by Airflow
        resolved_inputs = {}
        for key, value in inputs.items():
            # If it's already an AiiDA node, use it
            if hasattr(value, 'value'):
                resolved_inputs[key] = value
            else:
                # Convert to AiiDA type (value is now the rendered string)
                resolved_inputs[key] = to_aiida_type(value)

        computer_label = resolved_inputs['computer'].value
        computer = load_computer(computer_label)

        # Create CalcJobNode (but don't store yet)
        node = CalcJobNode(computer=computer)
        node.set_attribute('process_label', self.__class__.__name__)

        # Set CalcJob metadata options (required for proper job submission)
        node.set_option('submit_script_filename', self.submit_script_filename)
        node.set_option('scheduler_stdout', '_scheduler-stdout.txt')
        node.set_option('scheduler_stderr', '_scheduler-stderr.txt')
        node.set_option('resources', {'num_machines': 1, 'num_mpiprocs_per_machine': 1})

        # Store all inputs as attributes
        for key, aiida_node in resolved_inputs.items():
            if key != 'computer':  # Computer is already set
                # Extract value from AiiDA Data nodes
                attr_value = aiida_node.value if hasattr(aiida_node, 'value') else aiida_node
                node.base.attributes.set(key, attr_value)

        # Set initial state
        node.set_state(CalcJobState.UPLOADING)

        # Now prepare for submission BEFORE storing the node
        filepath_sandbox = get_config_option('storage.sandbox') or None

        # Create a namespace with resolved inputs (similar to AiiDA's self.inputs)
        resolved_inputs_ns = SimpleNamespace()
        for key, aiida_node in resolved_inputs.items():
            if key == 'computer':
                value = computer_label
            else:
                value = aiida_node.value if hasattr(aiida_node, 'value') else aiida_node
            setattr(resolved_inputs_ns, key, value)

        # Store resolved inputs on self so prepare_for_submission can access them
        self.resolved_inputs = resolved_inputs_ns

        with SandboxFolder(filepath_sandbox) as folder:
            # Call the user-defined prepare_for_submission
            calc_info = self.prepare_for_submission(folder)

            # Set uuid on calc_info (required by execmanager) - this is what presubmit does
            calc_info.uuid = str(node.uuid)

            # Update retrieve lists on the node (similar to presubmit)
            # This adds scheduler stdout/stderr to the retrieve list
            retrieve_list = calc_info.retrieve_list or []
            node.set_retrieve_list(retrieve_list)

            retrieve_temporary_list = calc_info.retrieve_temporary_list or []
            node.set_retrieve_temporary_list(retrieve_temporary_list)

            # Store the folder contents in the node's repository (node not stored yet)
            node.put_object_from_tree(folder.abspath, '')

            # Store CalcInfo details as node attributes
            node.base.attributes.set('_calc_info_uuid', calc_info.uuid if hasattr(calc_info, 'uuid') else None)
            node.base.attributes.set('_calc_info_skip_submit', calc_info.skip_submit or False)
            node.base.attributes.set('_calc_info_codes_info', [
                {
                    'code_uuid': str(ci.code_uuid) if hasattr(ci, 'code_uuid') and ci.code_uuid else None,
                    'cmdline_params': ci.cmdline_params,
                    'stdin_name': ci.stdin_name,
                    'stdout_name': ci.stdout_name,
                    'stderr_name': ci.stderr_name,
                    'join_files': ci.join_files,
                } for ci in (calc_info.codes_info or [])
            ])

            # Store retrieve lists
            node.set_retrieve_list(calc_info.retrieve_list or [])
            node.set_retrieve_temporary_list(calc_info.retrieve_temporary_list or [])

        # NOW store the node (after repository is populated)
        node.store()

        return node.pk


    @classmethod
    def define(cls, spec: CalcJobProcessSpec):
        """Define the input/output specification using AiiDA's CalcJobProcessSpec.

        Subclasses should override this to specify their inputs.

        :param spec: CalcJobProcessSpec to define inputs/outputs on
        """
        pass

    @abstractmethod
    def prepare_for_submission(self, folder) -> 'CalcInfo':
        """Prepare the calculation for submission.

        This method should be implemented by subclasses to:
        1. Write input files to the folder
        2. Create and return a CalcInfo object with job submission details

        Access inputs via self.resolved_inputs.x, self.resolved_inputs.y, etc.

        Args:
            folder: A SandboxFolder where input files should be written

        Returns:
            CalcInfo: Object containing submission details (codes to execute, files to copy, etc.)
        """
        pass

    def _build_tasks(self):
        """Build all tasks within this task group following AiiDA's CalcJob workflow."""

        # Task to create CalcJobNode and prepare for submission (only if node_pk not provided)
        if not self.node_pk:
            prepare_task = PythonOperator(
                task_id='prepare_calcjob',
                python_callable=self._create_calcjob_and_prepare,
                op_kwargs={'inputs': self.inputs},
                task_group=self,
            )
            # Get the node_pk to use downstream
            node_pk_ref = prepare_task.output
        else:
            node_pk_ref = self.node_pk

        # Optional: Unstash task
        if self.enable_unstash:
            unstash_op = AiiDAAsyncUnstashOperator(
                task_id="unstash",
                node_pk=node_pk_ref,
                task_group=self,
            )

        # Upload task
        upload_op = AiiDAAsyncUploadOperator(
            task_id="upload",
            node_pk=node_pk_ref,
            task_group=self,
        )

        # Branch based on skip_submit flag
        branch_task = BranchPythonOperator(
            task_id="check_skip_submit",
            python_callable=self._check_skip_submit,
            task_group=self,
        )

        # Submit task (only if not skipping)
        submit_op = AiiDAAsyncSubmitOperator(
            task_id="submit",
            node_pk=node_pk_ref,
            task_group=self,
        )

        # Update task (monitor job status)
        update_op = AiiDAAsyncUpdateOperator(
            task_id="update",
            node_pk=node_pk_ref,
            sleep_interval=self.update_sleep_interval,
            task_group=self,
        )

        # Optional: Monitor task
        if self.enable_monitors:
            monitor_op = AiiDAAsyncMonitorOperator(
                task_id="monitor",
                node_pk=node_pk_ref,
                monitors_pk=self.monitors_pk,
                task_group=self,
            )

        # Optional: Stash task
        if self.enable_stash:
            stash_op = AiiDAAsyncStashOperator(
                task_id="stash",
                node_pk=node_pk_ref,
                task_group=self,
            )

        # Retrieve task
        retrieve_op = AiiDAAsyncRetrieveOperator(
            task_id="retrieve",
            node_pk=node_pk_ref,
            retrieved_temporary_folder=self.retrieved_temporary_folder,
            task_group=self,
        )

        # Parse task
        parse_task = PythonOperator(
            task_id='parse',
            python_callable=self.parse,
            op_kwargs={'retrieved_temporary_folder': self.retrieved_temporary_folder},
            task_group=self,
        )

        # Set up dependencies
        if not self.node_pk:
            if self.enable_unstash:
                prepare_task >> unstash_op >> upload_op
            else:
                prepare_task >> upload_op
        else:
            if self.enable_unstash:
                unstash_op >> upload_op

        upload_op >> branch_task

        # Full workflow: upload -> submit -> update -> [monitor] -> [stash] -> retrieve -> parse
        branch_task >> submit_op >> update_op

        if self.enable_monitors:
            update_op >> monitor_op
            next_task = monitor_op
        else:
            next_task = update_op

        if self.enable_stash:
            next_task >> stash_op >> retrieve_op
        else:
            next_task >> retrieve_op

        # Skip submit workflow: upload -> [stash] -> retrieve -> parse
        if self.enable_stash:
            branch_task >> stash_op
        else:
            branch_task >> retrieve_op

        retrieve_op >> parse_task

    def _create_calcjob_wrapper(self, **context):
        """Wrapper to create the CalcJobNode generically from spec inputs."""
        if self.node_pk:
            return {"node_pk": self.node_pk}

        # Generic CalcJobNode creation using the spec inputs
        from aiida.orm import CalcJobNode, load_computer, to_aiida_type
        from aiida.common.datastructures import CalcJobState

        # Get computer from inputs (should be in spec)
        if 'computer' not in self.inputs:
            raise ValueError("CalcJob requires 'computer' input in spec")

        # Convert inputs to AiiDA nodes using to_aiida_type
        resolved_inputs = {}
        for key, value in self.inputs.items():
            # If it's already an AiiDA node, use it
            if hasattr(value, 'value'):
                resolved_inputs[key] = value
            else:
                # Convert to AiiDA type
                resolved_inputs[key] = to_aiida_type(value)

        computer_label = resolved_inputs['computer'].value
        computer = load_computer(computer_label)

        # Create CalcJobNode
        node = CalcJobNode(computer=computer)
        node.set_attribute('process_label', self.__class__.__name__)

        # Store all inputs as attributes
        for key, aiida_node in resolved_inputs.items():
            if key != 'computer':  # Computer is already set
                # Extract value from AiiDA Data nodes
                attr_value = aiida_node.value if hasattr(aiida_node, 'value') else aiida_node
                node.base.attributes.set(key, attr_value)

        # Set initial state
        node.set_state(CalcJobState.UPLOADING)

        # Store the node
        node.store()

        return {"node_pk": node.pk}

    def _check_skip_submit(self, **context):
        """Check if we should skip the submit step based on upload results."""
        ti = context['task_instance']
        upload_result = ti.xcom_pull(task_ids=f'{self.group_id}.upload')

        if upload_result and upload_result.get('skip_submit'):
            # Skip to stash (if enabled) or retrieve
            if self.enable_stash:
                return f'{self.group_id}.stash'
            return f'{self.group_id}.retrieve'
        else:
            # Continue with submit
            return f'{self.group_id}.submit'

    @abstractmethod
    def parse(self, retrieved_temporary_folder: str, **context) -> dict[str, Any]:
        """Abstract method to parse job outputs.

        This method should:
        1. Read the retrieved files from the temporary folder
        2. Parse the results
        3. Store results in the AiiDA database if needed
        4. Return parsed results

        Args:
            retrieved_temporary_folder: Path to folder containing retrieved files

        Returns:
            dict: Dictionary containing parsed results
        """
        pass


class SimpleAsyncAiiDACalcJobTaskGroup(AsyncAiiDACalcJobTaskGroup):
    """
    Simplified version of AsyncAiiDACalcJobTaskGroup without optional features.

    This provides a minimal workflow: UPLOAD -> SUBMIT -> UPDATE -> RETRIEVE -> PARSE
    """

    def __init__(
        self,
        group_id: str,
        node_pk: int | None = None,
        retrieved_temporary_folder: str | None = None,
        update_sleep_interval: int = 5,
        submit_script_filename: str = '_aiidasubmit.sh',
        **kwargs
    ):
        """Initialize the simple AiiDA CalcJob TaskGroup.

        :param group_id: Unique identifier for this task group
        :param node_pk: Primary key of existing CalcJobNode (if reusing existing node)
        :param retrieved_temporary_folder: Path for retrieved files
        :param update_sleep_interval: Seconds between update checks
        :param submit_script_filename: Name of the submit script file
        """
        super().__init__(
            group_id=group_id,
            node_pk=node_pk,
            retrieved_temporary_folder=retrieved_temporary_folder,
            enable_stash=False,
            enable_unstash=False,
            enable_monitors=False,
            update_sleep_interval=update_sleep_interval,
            submit_script_filename=submit_script_filename,
            **kwargs
        )
