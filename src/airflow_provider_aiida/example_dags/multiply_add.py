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
    from airflow_provider_aiida.aiida_core.engine.launch import create
    from aiida import load_profile
    from aiida.orm import load_code, Int

    load_profile()

    code = load_code('bash@localhost')
    inputs = {
        'code': code,
        'x': Int(0),
        'y': Int(1),
        'z': Int(2),
    }
    
    # TODO Does not work yet
    #node = create(MultiplyAddWorkChain, inputs)

    process = MultiplyAddWorkChain(inputs=inputs)
    process._save_checkpoint()
    node = process.node

    dag.test(
        run_conf={
            "node_pk": node.pk
        }
    )
