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

    # Create Process
    from airflow_provider_aiida.aiida_core.engine.launch import create
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

    # TODO Does not work yet
    #node = create(ArithmeticAddCalculation, inputs)

    process = ArithmeticAddCalculation(inputs=inputs)
    process._save_checkpoint()
    node = process.node


    dag.test(
        run_conf={
            "node_pk": node.pk
        }
    )

    print("\n" + "=" * 60)
    print("DAG test completed!")
    print("=" * 60)
