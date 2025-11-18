from airflow_provider_aiida.taskgroups.process import ProcessTaskGroup 
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation

from airflow import DAG
from airflow.models.param import Param

with DAG(
    'ArithmeticAddCalculation',
    params={
        "node_pk": Param("", type="integer")
    },
    render_template_as_native_obj = True
) as dag:
    ProcessTaskGroup(
        process_class=ArithmeticAddCalculation,
        node_pk="{{ params.node_pk }}",
    )


if __name__ == "__main__":

    from airflow_provider_aiida.aiida_core.engine.launch import run_get_node
    from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
    from aiida import load_profile
    from aiida.orm import load_code, Int

    load_profile()

    code = load_code('bash@localhost')
    inputs = {
        'code': code,
        'x': Int(0),
        'y': Int(1),
        #'metadata': {'options': {'sleep': 5}} 
    }
    result, node = run_get_node(ArithmeticAddCalculation, inputs)
    print(result, node)

