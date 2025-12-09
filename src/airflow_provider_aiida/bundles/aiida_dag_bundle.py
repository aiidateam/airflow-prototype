"""AiiDA DAG Bundle - generates DAGs from AiiDA calculation entry points."""

import logging
import tempfile
from pathlib import Path

from airflow.dag_processing.bundles.local import LocalDagBundle

log = logging.getLogger(__name__)


DAG_TEMPLATE = '''"""Auto-generated DAG for {calculation_name}."""
from airflow_provider_aiida.taskgroups.process import ProcessTaskGroup
from {module_path} import {class_name}

from airflow import DAG
from airflow.models.param import Param

with DAG(
    '{dag_id}',
    params={{
        "process_pk": Param("", type="integer"),
        "aiida_profile": Param(None, type=["null", "string"]),
        "aiida_path": Param(None, type=["null", "string"])
    }},
    render_template_as_native_obj=True
) as dag:
    ProcessTaskGroup(
        process_class={class_name},
        process_pk="{{{{ params.process_pk }}}}",
        aiida_profile="{{{{ params.aiida_profile }}}}",
        aiida_path="{{{{ params.aiida_path }}}}",
    )
'''


class AiidaDagBundle(LocalDagBundle):
    """
    DAG bundle that generates DAGs from AiiDA calculation entry points.

    This bundle automatically discovers all registered AiiDA calculations via the
    'aiida.calculations' entry point group and generates corresponding Airflow DAGs.

    For example, if a calculation is registered as:
    [project.entry-points.'aiida.calculations']
    'core.arithmetic.add' = 'aiida.calculations.arithmetic.add:ArithmeticAddCalculation'

    This bundle will generate a DAG file that creates an Airflow DAG for running
    that calculation.

    :param name: Name of the bundle (default: 'aiida_dags')
    :param refresh_interval: How often to refresh the bundle in seconds (default: 300)
    :param output_dir: Directory where DAG files are generated (default: temp directory)
    """

    def __init__(
        self,
        *,
        name: str = "aiida_dags",
        refresh_interval: int = 300,
        output_dir: str | Path | None = None,
        **kwargs,
    ):
        # Generate DAG files from AiiDA entry points
        path = self._generate_dag_files(output_dir)

        # Initialize the parent LocalDagBundle with the generated path
        super().__init__(
            name=name,
            path=path,
            refresh_interval=refresh_interval,
            **kwargs
        )

    def _generate_dag_files(self, output_dir: str | Path | None = None) -> str:
        """
        Generate DAG files from AiiDA calculation and workflow entry points.

        Args:
            output_dir: Directory where DAG files should be written.
                       If None, creates a temporary directory.

        Returns:
            Path to the directory containing generated DAG files
        """
        if output_dir is None:
            output_dir = Path(tempfile.mkdtemp(prefix='aiida_dags_'))
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)

        # Get all calculation and workflow entry points
        calculation_entry_points = self._get_calculation_entry_points()
        workflow_entry_points = self._get_workflow_entry_points()

        # Combine both dictionaries
        all_entry_points = {**calculation_entry_points, **workflow_entry_points}

        if not all_entry_points:
            log.warning("No AiiDA calculation or workflow entry points found")
            # Fall back to built-in example DAGs
            from airflow_provider_aiida import example_dags
            return str(Path(example_dags.__file__).parent)

        # Generate a DAG file for each entry point
        generated_count = 0
        for entry_point_name, entry_point in all_entry_points.items():
            try:
                self._generate_dag_file(entry_point_name, entry_point, output_dir)
                generated_count += 1
                log.debug(f"Generated DAG for {entry_point_name}")
            except Exception as e:
                log.warning(f"Failed to generate DAG for {entry_point_name}: {e}")

        log.info(f"Generated {generated_count} DAG files in {output_dir}")
        return str(output_dir)

    def _get_calculation_entry_points(self) -> dict:
        """Get all registered AiiDA calculation entry points.

        Returns:
            Dictionary mapping entry point names to EntryPoint objects
        """
        try:
            from aiida.plugins.entry_point import get_entry_point_names, get_entry_point
        except ImportError:
            log.error("AiiDA is not installed, cannot load calculation entry points")
            return {}

        entry_points = {}

        # Get all calculation entry points
        try:
            for entry_point_name in get_entry_point_names('aiida.calculations'):
                entry_point = get_entry_point('aiida.calculations', entry_point_name)
                entry_points[entry_point_name] = entry_point
        except Exception as e:
            log.error(f"Failed to get calculation entry points: {e}")

        return entry_points

    def _get_workflow_entry_points(self) -> dict:
        """Get all registered AiiDA workflow entry points.

        Returns:
            Dictionary mapping entry point names to EntryPoint objects
        """
        try:
            from aiida.plugins.entry_point import get_entry_point_names, get_entry_point
        except ImportError:
            log.error("AiiDA is not installed, cannot load workflow entry points")
            return {}

        entry_points = {}

        # Get all workflow entry points
        try:
            for entry_point_name in get_entry_point_names('aiida.workflows'):
                entry_point = get_entry_point('aiida.workflows', entry_point_name)
                entry_points[entry_point_name] = entry_point
        except Exception as e:
            log.error(f"Failed to get workflow entry points: {e}")

        return entry_points

    def _generate_dag_file(
        self,
        entry_point_name: str,
        entry_point,
        output_dir: Path
    ) -> Path:
        """Generate a DAG file for a specific calculation entry point.

        Args:
            entry_point_name: Name of the entry point (e.g., 'core.arithmetic.add')
            entry_point: The EntryPoint object
            output_dir: Directory where the DAG file should be written

        Returns:
            Path to the generated DAG file
        """
        # Load the calculation class to get its name
        calculation_class = entry_point.load()
        class_name = calculation_class.__name__
        module_path = calculation_class.__module__

        # Use class name as DAG ID
        dag_id = class_name

        # Create safe filename from entry point name
        safe_name = entry_point_name.replace('.', '_').replace(':', '_')
        filename = f"{safe_name}.py"

        # Generate DAG content from template
        dag_content = DAG_TEMPLATE.format(
            calculation_name=entry_point_name,
            class_name=class_name,
            module_path=module_path,
            dag_id=dag_id
        )

        # Write to file
        output_path = output_dir / filename
        output_path.write_text(dag_content)

        return output_path
