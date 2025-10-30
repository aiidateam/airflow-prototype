"""AiiDA DAG Bundle - loads DAGs from entry points."""

import logging
from pathlib import Path

from airflow.dag_processing.bundles.local import LocalDagBundle

from importlib_metadata import entry_points
log = logging.getLogger(__name__)


class AiidaDagBundle(LocalDagBundle):
    """
    DAG bundle that loads DAGs from AiiDA provider entry points.

    This bundle discovers and loads DAG modules registered via the 'aiida.dags'
    entry point group. This allows other packages to contribute workflows by adding
    entry points like:

    [project.entry-points.'aiida.dags']
    my_workflow = 'my_package.dags'

    The bundle simply inherits from LocalDagBundle and points to the package
    directory specified in the entry point.

    :param name: Name of the bundle (default: 'aiida_dags')
    :param refresh_interval: How often to refresh the bundle in seconds (default: 300)
    """

    def __init__(
        self,
        *,
        name: str = "aiida_dags",
        refresh_interval: int = 300,
        **kwargs,
    ):
        # Discover the path from entry points
        path = self._discover_path_from_entry_points()

        # Initialize the parent LocalDagBundle with the discovered path
        super().__init__(
            name=name,
            path=path,
            refresh_interval=refresh_interval,
            **kwargs
        )

    def _discover_path_from_entry_points(self) -> str:
        """
        Discover the DAG directory path from entry points.

        Returns the path to the first 'aiida.dags' entry point package.
        Falls back to built-in example_dags if no entry points found.
        """
        eps = entry_points()

        # Handle both old and new entry_points() API
        aiida_dags = eps.select(group='aiida.dags')

        if not aiida_dags:
            log.warning("No 'aiida.dags' entry points found, using built-in example DAGs")
            # Fall back to built-in example DAGs
            from airflow_provider_aiida import example_dags
            return str(Path(example_dags.__file__).parent)

        # Use the first entry point
        ep = next(iter(aiida_dags))
        log.info(f"Loading DAGs from entry point: {ep.name} -> {ep.value}")

        try:
            module = ep.load()

            # Get the package path
            if hasattr(module, '__path__'):
                # It's a package, use its directory
                path = Path(module.__file__).parent
            else:
                # It's a single module, use its parent directory
                path = Path(module.__file__).parent

            log.info(f"Using DAG path: {path}")
            return str(path)

        except Exception as e:
            log.error(f"Failed to load entry point {ep.name}: {e}", exc_info=True)
            # Fall back to built-in example DAGs
            from airflow_provider_aiida import example_dags
            return str(Path(example_dags.__file__).parent)
