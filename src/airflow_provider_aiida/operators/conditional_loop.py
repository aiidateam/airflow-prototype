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

        from airflow.settings import Session
        from airflow.models import DagRun
        dag_run_id = context['run_id']
        dag_id = context['dag'].dag_id
        session = Session()
        if should_continue:
            try:
                # Get the DAG run
                dag_run = session.query(DagRun).filter(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == dag_run_id
                ).first()

                if dag_run is None:
                    self.log.error(f"DAG run not found: {dag_id}/{dag_run_id}")
                    return "DAG run not found"

                # Get all task instances in this task group that need to be cleared
                task_instances = dag_run.get_task_instances(session=session)

                # Filter to only tasks in this group (condition + steps)
                tasks_to_clear = [
                    task_instance for task_instance in task_instances
                    if task_instance.task_id.startswith(f"{self.group_id}.step_") or
                       task_instance.task_id.startswith(f"{self.group_id}.loop_control")
                ]

                if tasks_to_clear:
                    self.log.info(f"Clearing {len(tasks_to_clear)} tasks for next iteration")
                    from airflow.utils.state import DagRunState

                    clear_task_instances(
                        tis = tasks_to_clear,
                        session = session,
                        dag_run_state = DagRunState.QUEUED,
                        run_on_latest_version = False,
                    )
                    session.commit()


            finally:
                session.close()
            return f"{self.group_id}.step_0"
        else:
            try:
                # Get the DAG run
                dag_run = session.query(DagRun).filter(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == dag_run_id
                ).first()

                if dag_run is None:
                    self.log.error(f"DAG run not found: {dag_id}/{dag_run_id}")
                    return "DAG run not found"

                # Get all task instances in this task group that need to be cleared
                task_instances = dag_run.get_task_instances(session=session)

                # Filter to only tasks in this group (condition + steps)
                tasks_to_clear = [
                    task_instance for task_instance in task_instances
                    if task_instance.task_id.startswith(f"{self.group_id}.exit")
                ]

                if tasks_to_clear:
                    self.log.info(f"Clearing {len(tasks_to_clear)} tasks for next iteration")
                    from airflow.utils.state import DagRunState

                    clear_task_instances(
                        tis = tasks_to_clear,
                        session = session,
                        dag_run_state = DagRunState.QUEUED,
                        run_on_latest_version = False,
                    )
                    session.commit()


            finally:
                session.close()
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

        # Determine if we should continue looping
        should_continue = (
            #condition_result is True and
            next_iteration < self.max_iterations
        )

        ti.xcom_push(key='should_continue', value=should_continue)

        if should_continue:
            self.log.info(f"Condition is True, clearing tasks to start iteration {next_iteration}")
            from airflow.api_fastapi.core_api.routes.public.task_instances import post_clear_task_instances
            from airflow.api_fastapi.core_api.datamodels.task_instances import ClearTaskInstancesBody
            from airflow.models.dagbag import DBDagBag

            #dag_bag = DBDagBag()

            #body = ClearTaskInstancesBody(
            #    dry_run=False,
            #    only_failed=False,
            #    dag_run_id=context['dag_run'].run_id,
            #    task_ids=['my_loop.check_condition',
            #              'my_loop.step_0',
            #              'my_loop.step_1',
            #              'my_loop.step_2',
            #              'my_loop.loop_control'],
            #)
            #
            ## Call the Airflow REST API function to clear task instances
            #session = Session()
            #try:
            #    result = post_clear_task_instances(
            #        dag_id=dag_id,
            #        body=body,
            #        dag_bag=dag_bag,
            #        session=session,
            #    )
            #    session.commit()
            #finally:
            #    session.close()

            # Create a new session
            session = Session()
            
            try:
                # Get the DAG run
                dag_run = session.query(DagRun).filter(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == dag_run_id
                ).first()

                if dag_run is None:
                    self.log.error(f"DAG run not found: {dag_id}/{dag_run_id}")
                    return "DAG run not found"

                # Get all task instances in this task group that need to be cleared
                task_instances = dag_run.get_task_instances(session=session)

                # Filter to only tasks in this group (condition + steps)
                tasks_to_clear = [
                    task_instance for task_instance in task_instances
                    if task_instance.task_id.startswith(f"{self.group_id}.check_condition")
                ]

                if tasks_to_clear:
                    self.log.info(f"Clearing {len(tasks_to_clear)} tasks for next iteration")
                    from airflow.utils.state import DagRunState

                    clear_task_instances(
                        tis = tasks_to_clear,
                        session = session,
                        dag_run_state = DagRunState.QUEUED,
                        run_on_latest_version = False,
                    )
                    session.commit()

                return f"Cleared tasks for iteration {next_iteration}"

            finally:
                session.close()
        else:
            self.log.info("Loop completed - condition is False or max iterations reached")
            return "Loop completed"
