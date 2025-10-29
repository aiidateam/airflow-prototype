from __future__ import annotations


from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_provider_aiida.operators.tasks import (
    ProcStepUntilTerminatedOperator,
)

import plumpy


from aiida.engine.processes.workchains.workchain import WorkChainSpec, if_, while_, return_


class WorkChainTaskGroup(TaskGroup):
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

        #self._spec = WorkChainSpec()
        #process_class.define(self._spec)
        self._build_tasks()

        ## Store inputs from kwargs based on spec
        ## These may be template strings that will be rendered at task execution time
        ## Use _wc_inputs to avoid conflict with TaskGroup.inputs property
        #self._wc_inputs = {}
        #for port_name in self._spec.inputs.ports.keys():
        #    if port_name in kwargs:
        #        self._wc_inputs[port_name] = kwargs[port_name]

        ## Create context proxy for use in step methods (use self.group_id from parent)
        #self._context_proxy = WorkChainContext(self.group_id)

        ## Build the task group when instantiated
        #self._build_tasks()

    def _build_tasks(self):
        exec_op = ProcStepUntilTerminatedOperator(
            task_id="execute_until_terminate",
            node_pk=self.node_pk,
            task_group=self,
        )


    def _build_tasks_old(self):
        """Build all tasks within this task group by walking the outline.

        This method:
        1. Creates an initialization task to set up context/inputs
        2. Walks the outline AST and creates operators for each step
        3. Wires dependencies based on outline order and control flow
        """
        # Task to initialize context and inputs in XCom
        init_task = EmptyOperator(task_id="noop", task_group=self)

        # Get the outline from the spec
        outline = self._spec.get_outline()

        # Build tasks from outline
        if outline:
            # Create workflow from outline steps (functions and control flow)
            steps = self._get_outline_steps()

            prev_task = init_task
            for i, (step_type, step_name, instruction) in enumerate(steps):
                if step_type == 'function_call':
                    # Simple function call
                    step_task = self._create_step_task(step_name, i)
                    prev_task >> step_task
                    prev_task = step_task
                elif step_type == 'if':
                    # if_ control flow - create branch operator
                    branch_task, merge_task = self._create_if_branch(instruction, i)
                    prev_task >> branch_task
                    prev_task = merge_task  # Continue from merge point
                elif step_type == 'while':
                    # while_ control flow - not yet implemented
                    import logging
                    logging.warning(f"while_ construct at index {i} not yet implemented")
                else:
                    import logging
                    logging.warning(f"Unknown construct type '{step_type}' at index {i}")

    def _get_outline_steps(self) -> list[tuple[str, str | None, Any]]:
        """Extract step methods from the outline.

        :return: List of tuples (step_type, step_name, instruction) where:
                 step_type is 'function_call', 'if', 'while', or 'unknown'
                 step_name is the function name for function_call, None for control flow
                 instruction is the plumpy instruction object
        """
        outline = self._spec.get_outline()
        if not outline:
            return []

        # The outline._instruction contains a list of _FunctionCall objects (for simple outlines)
        # or other control flow constructs (if_, while_)
        steps = []

        try:
            if hasattr(outline, '_instruction'):
                for instruction in outline._instruction:
                    # Check the type of instruction
                    instruction_type = type(instruction).__name__

                    if instruction_type == '_FunctionCall':
                        # Simple function call - extract the function name from _fn attribute
                        if hasattr(instruction, '_fn'):
                            steps.append(('function_call', instruction._fn.__name__, instruction))
                        else:
                            # Fallback to string representation
                            steps.append(('function_call', str(instruction), instruction))
                    elif instruction_type == '_If':
                        # if_ control flow construct
                        steps.append(('if', None, instruction))
                    elif instruction_type == '_While':
                        # while_ control flow construct
                        steps.append(('while', None, instruction))
                    else:
                        # Unknown instruction type
                        steps.append(('unknown', None, instruction))
        except Exception as e:
            import logging
            logging.warning(f"Failed to parse outline: {e}")

        return steps

    def _create_if_branch(self, if_instruction: Any, index: int) -> tuple[BranchPythonOperator, PythonOperator]:
        """Create Airflow tasks for an if_ control flow construct.

        This creates:
        1. A BranchPythonOperator that evaluates conditions and chooses branch
        2. PythonOperators for each branch body
        3. A dummy join task to merge branches

        :param if_instruction: The _If instruction from plumpy
        :param index: The instruction index in the outline
        :return: Tuple of (branch_operator, join_operator) for dependency wiring
        """
        from airflow.operators.empty import EmptyOperator

        branch_id = f'if_branch_{index}'

        # Extract conditionals from the if_ instruction
        # if_instruction._ifs contains a list of _Conditional objects
        conditionals = if_instruction._ifs if hasattr(if_instruction, '_ifs') else []

        # Create branch evaluation function
        def evaluate_branches(**context):
            """Evaluate conditions and return the task_id to execute."""
            import logging

            ti = context['task_instance']
            task = ti.task

            # Get upstream task instances to pull context from them
            upstream_task_ids = [t.task_id for t in task.upstream_list]

            ctx_data = {}
            # Try to pull context from upstream tasks (most recent first)
            for upstream_id in reversed(upstream_task_ids):
                pulled_ctx = ti.xcom_pull(key=f'{self.group_id}_context', task_ids=upstream_id)
                if pulled_ctx:
                    ctx_data = pulled_ctx
                    break

            inputs_data = ti.xcom_pull(key=f'{self.group_id}_inputs', task_ids=f'{self.group_id}.init_workchain') or {}

            # Create namespace for predicate evaluation
            from types import SimpleNamespace
            namespace = SimpleNamespace()
            namespace.ctx = SimpleNamespace(**ctx_data)
            namespace.inputs = SimpleNamespace(**inputs_data)

            # Store on self temporarily
            self._temp_ctx = namespace.ctx
            self._temp_inputs = namespace.inputs

            # Evaluate each condition in order
            for i, conditional in enumerate(conditionals):
                predicate = conditional.predicate

                # Call the predicate function
                try:
                    result = predicate(self)
                    logging.info(f"Branch {i} predicate returned: {result}")

                    if result:
                        # This branch should execute
                        # Return the first task ID in the branch body
                        body = conditional.body
                        if body and len(body) > 0:
                            first_step = body[0]
                            if hasattr(first_step, '_fn'):
                                branch_task_id = f'{self.group_id}.{branch_id}_branch{i}_{first_step._fn.__name__}'
                            else:
                                branch_task_id = f'{self.group_id}.{branch_id}_branch{i}_0'
                            logging.info(f"Choosing branch {i}: {branch_task_id}")

                            # Clean up temporary attributes
                            delattr(self, '_temp_ctx')
                            delattr(self, '_temp_inputs')

                            return branch_task_id
                except Exception as e:
                    logging.error(f"Error evaluating predicate for branch {i}: {e}")
                    import traceback
                    traceback.print_exc()

            # Clean up temporary attributes
            if hasattr(self, '_temp_ctx'):
                delattr(self, '_temp_ctx')
            if hasattr(self, '_temp_inputs'):
                delattr(self, '_temp_inputs')

            # No branch matched - go directly to join
            join_task_id = f'{self.group_id}.{branch_id}_join'
            logging.info(f"No branch matched, going to join: {join_task_id}")
            return join_task_id

        # Create the branch operator
        branch_op = BranchPythonOperator(
            task_id=branch_id,
            python_callable=evaluate_branches,
            task_group=self,
        )

        # Create tasks for each branch body
        branch_end_tasks = []
        for branch_idx, conditional in enumerate(conditionals):
            body = conditional.body

            prev_task = branch_op
            for step_idx, step_instr in enumerate(body):
                if hasattr(step_instr, '_fn'):
                    step_name = step_instr._fn.__name__
                    task_id = f'{branch_id}_branch{branch_idx}_{step_name}'
                else:
                    task_id = f'{branch_id}_branch{branch_idx}_{step_idx}'

                step_task = PythonOperator(
                    task_id=task_id,
                    python_callable=self._execute_step,
                    op_kwargs={'step_method': step_name if hasattr(step_instr, '_fn') else str(step_instr)},
                    task_group=self,
                )

                # Wire dependencies within the branch
                if prev_task == branch_op:
                    # First task in branch connects to branch operator
                    branch_op >> step_task
                else:
                    prev_task >> step_task

                prev_task = step_task

            # Track the last task in this branch
            branch_end_tasks.append(prev_task)

        # Create join/merge task
        join_op = EmptyOperator(
            task_id=f'{branch_id}_join',
            task_group=self,
            trigger_rule='none_failed_min_one_success',  # Succeeds if any upstream succeeds
        )

        # Connect all branch ends to join
        for end_task in branch_end_tasks:
            end_task >> join_op

        # Also connect branch operator directly to join (for no-match case)
        branch_op >> join_op

        return branch_op, join_op

    def _create_step_task(self, step_name: str, index: int) -> PythonOperator:
        """Create a PythonOperator for a WorkChain step.

        :param step_name: The name of the step method to execute
        :param index: The step index in the outline
        :return: PythonOperator wrapping the step
        """
        return PythonOperator(
            task_id=step_name,
            python_callable=self._execute_step,
            op_kwargs={'step_method': step_name},
            task_group=self,
        )

    def _execute_step(self, step_method: str, **context):
        """Execute a WorkChain step method.

        This wrapper:
        1. Loads context and inputs from XCom
        2. Calls the step method (which can access self.ctx and self.inputs)
        3. Handles ToContext return values
        4. Handles exit codes
        5. Saves updated context to XCom

        :param step_method: Name of the step method to execute
        """
        import logging

        ti = context['task_instance']

        # Load context and inputs from XCom
        # Context needs to be pulled from upstream tasks (tasks that this depends on)
        # Inputs are static and always pulled from init_workchain

        # Get upstream task instances to pull context from them
        # For branches, we need to look beyond immediate upstream (the branch operator)
        # and find tasks that have actually pushed context
        task = ti.task

        # Get ALL upstream task IDs (including transitive upstreams)
        def get_all_upstream_ids(task, visited=None):
            if visited is None:
                visited = set()
            upstream_ids = []
            for upstream_task in task.upstream_list:
                if upstream_task.task_id not in visited:
                    visited.add(upstream_task.task_id)
                    upstream_ids.append(upstream_task.task_id)
                    # Recursively get upstreams
                    upstream_ids.extend(get_all_upstream_ids(upstream_task, visited))
            return upstream_ids

        upstream_task_ids = get_all_upstream_ids(task)

        ctx_data = {}
        # Try to pull context from upstream tasks (most recent first = reverse order)
        for upstream_id in reversed(upstream_task_ids):
            if upstream_id.startswith(f'{self.group_id}.'):
                pulled_ctx = ti.xcom_pull(key=f'{self.group_id}_context', task_ids=upstream_id)
                if pulled_ctx:
                    ctx_data = pulled_ctx
                    logging.info(f"Pulled context from {upstream_id}: {ctx_data}")
                    break  # Use the first (most recent) non-empty context found

        inputs_data = ti.xcom_pull(key=f'{self.group_id}_inputs', task_ids=f'{self.group_id}.init_workchain') or {}

        logging.info(f"Loaded context: {ctx_data}")
        logging.info(f"Loaded inputs: {inputs_data}")

        # Create a namespace object that mimics AiiDA's WorkChain interface
        # This allows step methods to access self.ctx and self.inputs
        from types import SimpleNamespace

        # Create namespace with context and inputs
        namespace = SimpleNamespace()
        namespace.ctx = SimpleNamespace(**ctx_data)
        namespace.inputs = SimpleNamespace(**inputs_data)
        namespace.report = lambda msg: logging.info(msg)

        # Store namespace on self temporarily for step method access
        self._temp_ctx = namespace.ctx
        self._temp_inputs = namespace.inputs
        self._temp_report = namespace.report

        # Get the step method from the class
        step_func = getattr(self.__class__, step_method, None)
        if not step_func:
            raise ValueError(f"Step method '{step_method}' not found on {self.__class__.__name__}")

        # Execute the step method
        result = step_func(self)

        # Handle different return types
        if isinstance(result, ToContext):
            # Update context with ToContext values
            for key, value in result.items():
                setattr(namespace.ctx, key, value)
        elif isinstance(result, int) and result > 0:
            # Exit code
            raise ValueError(f"WorkChain exited with code {result}")
        elif hasattr(result, 'status') and result.status > 0:
            # ExitCode object
            raise ValueError(f"WorkChain exited: {result.message}")

        # Convert namespace back to dict for XCom storage
        updated_ctx = vars(namespace.ctx)

        # Save updated context
        logging.info(f"Saving updated context to XCom: {updated_ctx}")
        ti.xcom_push(key=f'{self.group_id}_context', value=updated_ctx)

        # Clean up temporary attributes
        delattr(self, '_temp_ctx')
        delattr(self, '_temp_inputs')
        delattr(self, '_temp_report')

        return {'step': step_method, 'completed': True}
