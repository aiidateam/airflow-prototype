# Create Process
from airflow_provider_aiida.aiida_core.engine.launch import submit
from aiida.workflows.arithmetic.multiply_add import MultiplyAddWorkChain 
from airflow_provider_aiida.aiida_core import load_profile
load_profile()
from aiida.orm import load_code, Int
code = load_code('bash@localhost')
inputs = {
    'code': code,
    'x': Int(0),
    'y': Int(1),
    'z': Int(2),
}

dag_run = submit(MultiplyAddWorkChain, inputs)

print(f"Triggered DAG run: {dag_run}")
