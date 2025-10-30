from __future__ import annotations

from airflow_provider_aiida.taskgroups.process import ProcessTaskGroup 
from aiida.workflows.arithmetic.multiply_add import MultiplyAddWorkChain 


from airflow import DAG
from airflow.models.param import Param

with DAG(
    'MultiplyAddWorkChain',
    params={
        "node_pk": Param("", type="integer")
    },
    render_template_as_native_obj = True
) as dag:
    ProcessTaskGroup(
        process_class=MultiplyAddWorkChain,
        node_pk="{{ params.node_pk }}",
    )

if __name__ == "__main__":
    from aiida import load_profile
    load_profile()

    print("=" * 60)
    print("Testing arithmetic_aiida_native_single DAG")
    print("=" * 60)

    # Test the DAG with default parameters
    from aiida.orm import load_code, Int
    code = load_code('bash@localhost')
    inputs = {
        'code': code,
        'x': Int(0),
        'y': Int(1),
        'z': Int(2),
    }
    
    process = MultiplyAddWorkChain(inputs=inputs)
    # For creating pesistence checkpoints and other database related actions
    process._save_checkpoint()

    dag.test(
        run_conf={
            "node_pk": process.node.pk
        }
    )

    print("\n" + "=" * 60)
    print("DAG test completed!")
    print("=" * 60)
