"""AiiDA DAG Bundle - loads DAGs from entry points."""

import logging
import shutil
import tempfile
from pathlib import Path
from typing import Optional

from airflow.dag_processing.bundles.base import BaseDagBundle

try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points

log = logging.getLogger(__name__)


class AiidaDagBundle(BaseDagBundle):
    """
    DAG bundle that loads DAGs from AiiDA provider entry points.

    This bundle discovers and loads DAG modules registered via the 'aiida.dags'
    entry point group. This allows other packages to contribute workflows by adding
    entry points like:

    [project.entry-points.'aiida.dags']
    my_workflow = 'my_package.dags.my_workflow'

    The bundle creates a temporary directory and copies the discovered DAG modules.

    :param name: Name of the bundle (default: 'aiida_dags')
    :param refresh_interval: How often to refresh the bundle in seconds (default: 300)
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        name: str = "aiida_dags",
        refresh_interval: int = 300,
        **kwargs,
    ):
        super().__init__(name=name, refresh_interval=refresh_interval, **kwargs)
        self._path: Optional[Path] = None
        self._initialized = False

    @property
    def path(self) -> Path:
        """Path for this bundle."""
        if self._path is None:
            # Create a temporary directory for the bundle
            # This will be populated during initialize()
            self._path = Path(tempfile.mkdtemp(prefix=f"airflow_aiida_bundle_{self.name}_"))
            log.info(f"Created bundle directory at {self._path}")
        return self._path

    def get_current_version(self) -> None:
        """No versioning support for this bundle."""
        return None

    def initialize(self) -> None:
        """
        Initialize the bundle by discovering and copying DAG files from entry points.

        This method:
        1. Discovers all 'aiida.dags' entry points
        2. Loads each entry point to get the module path
        3. Copies/symlinks the DAG files to the bundle directory
        """
        if self._initialized:
            return

        with self.lock():
            if self._initialized:
                return

            log.info(f"Initializing AiiDA DAG bundle '{self.name}'")

            # Discover entry points
            eps = entry_points()

            # Handle both old and new entry_points() API
            if hasattr(eps, 'select'):
                # Python 3.10+ API
                aiida_dags = eps.select(group='aiida.dags')
            else:
                # Python 3.9 API
                aiida_dags = eps.get('aiida.dags', [])

            if not aiida_dags:
                log.warning("No 'aiida.dags' entry points found")
                # Include the built-in example DAGs from this package
                self._load_builtin_dags()
            else:
                # Load DAGs from entry points
                for ep in aiida_dags:
                    try:
                        log.info(f"Loading DAG from entry point: {ep.name}")
                        # Load the module
                        module = ep.load()

                        # Get the module's file path
                        if hasattr(module, '__file__') and module.__file__:
                            source_file = Path(module.__file__)
                            if source_file.exists():
                                # Copy to bundle directory
                                dest_file = self.path / source_file.name
                                shutil.copy2(source_file, dest_file)
                                log.info(f"Copied {source_file.name} to bundle")
                        else:
                            log.warning(f"Entry point {ep.name} has no __file__ attribute")
                    except Exception as e:
                        log.error(f"Failed to load entry point {ep.name}: {e}", exc_info=True)

            self._initialized = True
            super().initialize()

    def _load_builtin_dags(self) -> None:
        """Load the built-in example DAGs from airflow_provider_aiida.example_dags."""
        try:
            from airflow_provider_aiida import example_dags

            example_dags_dir = Path(example_dags.__file__).parent
            log.info(f"Loading built-in DAGs from {example_dags_dir}")

            # Copy all Python files from example_dags to the bundle
            for dag_file in example_dags_dir.glob("*.py"):
                if dag_file.name != "__init__.py":
                    dest_file = self.path / dag_file.name
                    shutil.copy2(dag_file, dest_file)
                    log.info(f"Copied built-in DAG {dag_file.name} to bundle")
        except Exception as e:
            log.error(f"Failed to load built-in DAGs: {e}", exc_info=True)

    def refresh(self) -> None:
        """
        Refresh the bundle by rediscovering entry points.

        This allows new workflows to be discovered without restarting Airflow.
        """
        with self.lock():
            log.info(f"Refreshing AiiDA DAG bundle '{self.name}'")

            # Clear the existing bundle directory
            if self._path and self._path.exists():
                for item in self._path.iterdir():
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        shutil.rmtree(item)

            # Re-initialize
            self._initialized = False
            self.initialize()
