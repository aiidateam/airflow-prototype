from airflow_provider_aiida.aiida_core.engine.launch import create
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida.orm import Int

# Import the DAG
from airflow_provider_aiida.example_dags.arithmetic_add import dag


def test_arithmetic_add_dag(aiida_code_installed):
    """Test the ArithmeticAddCalculation DAG"""

    inputs = {
        'code': aiida_code_installed(default_calc_job_plugin='core.arithmetic.add'),
        'x': Int(5),
        'y': Int(10),
    }

    process = ArithmeticAddCalculation(inputs=inputs)
    process._save_checkpoint()
    node = process.node

    dag.test(
        run_conf={
            "node_pk": node.pk
        }
    )

    assert not node.is_failed, "Calculation failed, exit status: {node.exit_status}, exit message: {node.exit_message}" 
    assert node.outputs.sum.value == 15
