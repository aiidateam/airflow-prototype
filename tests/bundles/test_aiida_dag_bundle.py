"""Test AiiDA DAG Bundle."""

import pytest
from pathlib import Path
from airflow_provider_aiida.bundles.aiida_dag_bundle import AiidaDagBundle


def test_aiida_dag_bundle_initialization():
    """Test that the AiiDA DAG bundle initializes correctly."""
    bundle = AiidaDagBundle(name="test_bundle")

    # Check initial state
    assert bundle.name == "test_bundle"
    assert bundle.supports_versioning is False
    assert bundle.get_current_version() is None

    # Check that the path exists and points to example_dags folder
    assert bundle.path.exists()
    assert bundle.path.is_dir()

    # Should have the Python DAG files from example_dags folder
    dag_files = list(bundle.path.glob("*.py"))
    dag_files = [f for f in dag_files if f.name != "__init__.py"]
    assert len(dag_files) > 0, "Bundle should contain at least one DAG file"

    # Check that the expected DAGs from example_dags folder are present (hardcoded names)
    dag_names = [f.stem for f in dag_files]  # Get names without .py extension
    expected_dags = ["arithmetic_add", "async_arithmetic_add", "test_while_loop"]

    for expected_dag in expected_dags:
        assert expected_dag in dag_names, f"Expected DAG file '{expected_dag}.py' should be present in bundle"


def test_aiida_dag_bundle_refresh():
    """Test that the bundle can be refreshed."""
    bundle = AiidaDagBundle(name="test_refresh")

    initial_files = set(f.name for f in bundle.path.glob("*.py") if f.name != "__init__.py")

    # Refresh should work without errors
    bundle.refresh()

    refreshed_files = set(f.name for f in bundle.path.glob("*.py") if f.name != "__init__.py")

    # Files should be the same after refresh (no entry points changed)
    assert initial_files == refreshed_files


def test_aiida_dag_bundle_path_property():
    """Test that the path property points to the package directory."""
    bundle = AiidaDagBundle(name="test_path")

    # Path should point to the entry point package directory
    path = bundle.path
    assert path.exists()
    assert path.is_dir()
    assert "example_dags" in str(path)


def test_multiple_bundles_independent():
    """Test that multiple bundles can be created."""
    bundle1 = AiidaDagBundle(name="bundle1")
    bundle2 = AiidaDagBundle(name="bundle2")

    bundle1.initialize()
    bundle2.initialize()

    # Both bundles should point to the same entry point path
    # (they read from the same entry point)
    assert bundle1.path == bundle2.path
    assert bundle1.path.exists()
    assert bundle2.path.exists()
