"""Tests for the ArithmeticRestartWorkChain."""
import pytest
from aiida.orm import Int, Float
from airflow_provider_aiida.workflows.restart import ArithmeticRestartWorkChain
from airflow_provider_aiida.aiida_core.engine.launch import run_get_node, submit


def test_restart_workchain_success_dag_test_run(bash_code):
    """Test the ArithmeticRestartWorkChain DAG in testing runtime environment - success case."""

    inputs = {
        'code': bash_code,
        'x': Int(5),
        'y': Int(10),
        'fail_threshold': Float(0.0),  # Never fail
        'max_iterations': Int(5),
    }

    _, node = run_get_node(ArithmeticRestartWorkChain, inputs)

    assert node.is_finished_ok, f"Workchain failed, exit status: {node.exit_status}, exit message: {node.exit_message}"
    assert node.outputs.result.value == 15, f"Expected 15, got {node.outputs.result.value}"
    assert node.base.extras.get('iteration', 1) == 1, "Should have succeeded on first iteration"


def test_restart_workchain_with_retries_dag_test_run(bash_code):
    """Test the ArithmeticRestartWorkChain DAG with random failures - testing restart mechanism."""

    inputs = {
        'code': bash_code,
        'x': Int(3),
        'y': Int(7),
        'fail_threshold': Float(0.7),  # 70% chance to fail
        'max_iterations': Int(20),  # High enough to eventually succeed
    }

    _, node = run_get_node(ArithmeticRestartWorkChain, inputs)

    # Should eventually succeed (with 20 tries, probability of all failing is ~0.7^20 ≈ 0.0008)
    assert node.is_finished_ok, f"Workchain failed, exit status: {node.exit_status}, exit message: {node.exit_message}"
    assert node.outputs.result.value == 10, f"Expected 10, got {node.outputs.result.value}"
    # Can't assert exact iteration count due to randomness, but should be > 1
    # Just check that it completed


def test_restart_workchain_max_iterations_exceeded_dag_test_run(bash_code):
    """Test the ArithmeticRestartWorkChain DAG exceeds max iterations."""

    inputs = {
        'code': bash_code,
        'x': Int(2),
        'y': Int(8),
        'fail_threshold': Float(1.0),  # Always fail
        'max_iterations': Int(3),
    }

    _, node = run_get_node(ArithmeticRestartWorkChain, inputs)

    assert node.is_failed, "Workchain should have failed"
    assert node.exit_status == 401, f"Expected exit status 401 (max iterations), got {node.exit_status}"


@pytest.mark.integration
def test_restart_workchain_success_trigger_run(bash_code):
    """Test the triggered ArithmeticRestartWorkChain with airflow services - success case."""

    inputs = {
        'code': bash_code,
        'x': Int(5),
        'y': Int(10),
        'fail_threshold': Float(0.0),  # Never fail
        'max_iterations': Int(5),
    }

    node = submit(ArithmeticRestartWorkChain, inputs, wait=True)

    assert node.is_finished_ok, f"Workchain failed, exit status: {node.exit_status}, exit message: {node.exit_message}"
    assert node.outputs.result.value == 15, f"Expected 15, got {node.outputs.result.value}"


@pytest.mark.integration
def test_restart_workchain_with_retries_trigger_run(bash_code):
    """Test the triggered ArithmeticRestartWorkChain with random failures and retries."""

    inputs = {
        'code': bash_code,
        'x': Int(3),
        'y': Int(7),
        'fail_threshold': Float(0.7),  # 70% chance to fail
        'max_iterations': Int(20),  # High enough to eventually succeed
    }

    node = submit(ArithmeticRestartWorkChain, inputs, wait=True)

    # Should eventually succeed
    assert node.is_finished_ok, f"Workchain failed, exit status: {node.exit_status}, exit message: {node.exit_message}"
    assert node.outputs.result.value == 10, f"Expected 10, got {node.outputs.result.value}"


@pytest.mark.integration
def test_restart_workchain_max_iterations_trigger_run(bash_code):
    """Test the triggered ArithmeticRestartWorkChain exceeds max iterations."""

    inputs = {
        'code': bash_code,
        'x': Int(2),
        'y': Int(8),
        'fail_threshold': Float(1.0),  # Always fail
        'max_iterations': Int(3),
    }

    node = submit(ArithmeticRestartWorkChain, inputs, wait=True)

    assert node.is_failed, "Workchain should have failed"
    assert node.exit_status == 401, f"Expected exit status 401 (max iterations), got {node.exit_status}"
