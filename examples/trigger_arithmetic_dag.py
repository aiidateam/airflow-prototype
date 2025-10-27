"""
Trigger an Airflow DAG using the internal API client.
This script must be run from within the Airflow environment.
"""

from airflow.api.client import get_current_api_client

# DAG configuration
DAG_ID = "arithmetic_add_calcjob"


# Create Process
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
from aiida import load_profile
load_profile()
from aiida.orm import load_code, Int
code = load_code('bash@localhost')
inputs = {
    'code': code,
    'x': Int(0),
    'y': Int(1),
    #'metadata': {'options': {'sleep': 5}} 
}

process = ArithmeticAddCalculation(inputs=inputs)
process._save_checkpoint()

# DAG run configuration
conf = {
    "node_pk": process.node.pk
}

# Get the Airflow API client
# This works when running from within the Airflow environment
# (e.g., from a task, scheduler, or worker)
client = get_current_api_client()

# Trigger the DAG run
dag_run = client.trigger_dag(
    dag_id=DAG_ID,
    conf=conf
)

print(f"Triggered DAG run: {dag_run}")
