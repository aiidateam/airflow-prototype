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

    # Initialize the bundle
    bundle.initialize()

    # Check that the path exists and contains DAG files
    assert bundle.path.exists()
    assert bundle.path.is_dir()

    # Should have at least the built-in example DAGs
    dag_files = list(bundle.path.glob("*.py"))
    assert len(dag_files) > 0, "Bundle should contain at least one DAG file"

    # Check that arithmetic_add.py is present
    dag_names = [f.name for f in dag_files]
    assert "arithmetic_add.py" in dag_names, "Built-in arithmetic_add DAG should be present"


def test_aiida_dag_bundle_refresh():
    """Test that the bundle can be refreshed."""
    bundle = AiidaDagBundle(name="test_refresh")
    bundle.initialize()

    initial_files = set(f.name for f in bundle.path.glob("*.py"))

    # Refresh should work without errors
    bundle.refresh()

    refreshed_files = set(f.name for f in bundle.path.glob("*.py"))

    # Files should be the same after refresh (no entry points changed)
    assert initial_files == refreshed_files


def test_aiida_dag_bundle_path_property():
    """Test that the path property creates a temporary directory."""
    bundle = AiidaDagBundle(name="test_path")

    # Path should be created on access
    path = bundle.path
    assert path.exists()
    assert path.is_dir()
    assert "airflow_aiida_bundle_test_path" in str(path)


def test_multiple_bundles_independent():
    """Test that multiple bundles have independent paths."""
    bundle1 = AiidaDagBundle(name="bundle1")
    bundle2 = AiidaDagBundle(name="bundle2")

    bundle1.initialize()
    bundle2.initialize()

    # Paths should be different
    assert bundle1.path != bundle2.path
    assert bundle1.path.exists()
    assert bundle2.path.exists()
