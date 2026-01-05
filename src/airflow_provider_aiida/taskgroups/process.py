from __future__ import annotations


from airflow.sdk import TaskGroup

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
        process_pk: int,
        aiida_profile: str | None,
        aiida_path: str | None,
    ):
        """Initialize the AiiDA CalcJob TaskGroup.

        :param group_id: Unique identifier for this task group
        """
        super().__init__(group_id=process_class.__name__)
        self.process_class = process_class
        self.process_pk = process_pk
        self.aiida_profile = aiida_profile
        self.aiida_path = aiida_path
        self._build_tasks()

    def _build_tasks(self):
        ProcStepUntilTerminatedOperator(
            task_id="step_until_terminate",
            process_pk=self.process_pk,
            aiida_profile=self.aiida_profile,
            aiida_path=self.aiida_path,
            task_group=self,
        )

