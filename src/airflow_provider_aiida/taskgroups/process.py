from __future__ import annotations


from airflow.utils.task_group import TaskGroup

from airflow_provider_aiida.operators.process import ProcStepUntilTerminatedOperator


class ProcessTaskGroup(TaskGroup):
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

    def _build_tasks(self):
        ProcStepUntilTerminatedOperator(
            task_id="step_until_terminate",
            node_pk=self.node_pk,
            task_group=self,
        )

