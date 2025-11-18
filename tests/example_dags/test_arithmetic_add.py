from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from airflow_provider_aiida.aiida_core.engine.launch import run_get_node
from aiida.orm import Int

# Import the DAG


def test_arithmetic_add_dag(aiida_code_installed):
    """Test the ArithmeticAddCalculation DAG"""

    inputs = {
        'code': aiida_code_installed(default_calc_job_plugin='core.arithmetic.add'),
        'x': Int(5),
        'y': Int(10),
    }
    result, node = run_get_node(ArithmeticAddCalculation, inputs)

    assert not node.is_failed, "Calculation failed, exit status: {node.exit_status}, exit message: {node.exit_message}" 
    assert result.sum.value == 15
