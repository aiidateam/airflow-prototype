import pytest
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from airflow_provider_aiida.aiida_core.engine.launch import run_get_node, submit
from aiida.orm import Int


def test_arithmetic_add_dag_test_run(bash_code):
    """Test the ArithmeticAddCalculation DAG in testing runtime environment"""

    inputs = {
        'code': bash_code,
        'x': Int(5),
        'y': Int(10),
    }
    # TODO result is empty dict is this normal?
    _, node = run_get_node(ArithmeticAddCalculation, inputs)

    assert not node.is_failed, "Calculation failed, exit status: {node.exit_status}, exit message: {node.exit_message}"
    assert node.outputs.sum.value == 15


@pytest.mark.integration
def test_arithmetic_add_trigger_run(bash_code):
    """Test the triggered ArithmeticAddCalculation DAG with airflow services"""

    inputs = {
        'code': bash_code,
        'x': Int(5),
        'y': Int(10),
    }
    # TODO: add timeout
    node = submit(ArithmeticAddCalculation, inputs, wait=True)

    assert not node.is_failed, "Calculation failed, exit status: {node.exit_status}, exit message: {node.exit_message}"
    assert node.outputs.sum.value == 15
