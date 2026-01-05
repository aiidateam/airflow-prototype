from airflow_provider_aiida.taskgroups.process import ProcessTaskGroup 
from aiida.workflows.arithmetic.multiply_add import MultiplyAddWorkChain 

from airflow import DAG
from airflow.sdk import Param

with DAG(
    'MultiplyAddWorkChain',
    params={
        "process_pk": Param("", type="integer"),
        "aiida_profile": Param(None, type=["null", "string"]),
        "aiida_path": Param(None, type=["null", "string"])
    },
    render_template_as_native_obj = True
) as dag:
    ProcessTaskGroup(
        process_class=MultiplyAddWorkChain,
        process_pk="{{ params.process_pk }}",
        aiida_profile="{{ params.aiida_profile }}",
        aiida_path="{{ params.aiida_path }}",
    )

if __name__ == "__main__":
    from airflow_provider_aiida.aiida_core.engine.launch import run_get_node
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
    
    output, node = run_get_node(MultiplyAddWorkChain, inputs)
    print(output, node)
