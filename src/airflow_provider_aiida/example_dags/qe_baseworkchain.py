"""
Example DAG showing backward-compatible PwBaseWorkChain in Airflow.

This demonstrates how the same AiiDA PwBaseWorkChain builder pattern
works seamlessly in Airflow, maintaining full backward compatibility.
"""

from datetime import datetime
from airflow import DAG
from aiida import orm, load_profile
from ase.build import bulk
from aiida_quantumespresso.workflows.pw.base import PwBaseWorkChain as AiiDaPwBaseWorkChain

from airflow_provider_aiida.taskgroups.workchains import PwBaseWorkChain

# Load AiiDA profile (same as before)
load_profile()

# Create the DAG
with DAG(
    dag_id='qe_pw_base_workchain',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
    tags=['quantum-espresso', 'pw', 'base-workchain'],
) as dag:

    # ========== SAME AS BEFORE: Build using AiiDA builder pattern ==========
    structure = orm.StructureData(ase=bulk('Si', 'fcc', 5.43))
    code = orm.load_code('pw-7.3@thor')

    # Use the exact same builder interface as AiiDA
    builder = AiiDaPwBaseWorkChain.get_builder_from_protocol(
        code=code,
        structure=structure,
        protocol='fast'
    )

    # ========== NEW: Create Airflow TaskGroup from builder ==========
    # Instead of engine.run(builder), create a PwBaseWorkChain TaskGroup
    pw_base_wc = PwBaseWorkChain.from_builder(
        builder=builder,
        group_id='pw_base_workchain',
        machine='thor',
        local_workdir='/tmp/airflow/pw_base',
        remote_workdir='/mnt/home/khosra_a/aiida/airflow_remote'
    )

    # The TaskGroup is now part of the DAG and will execute when triggered
    # It includes:
    # - Automatic error handling and restarts
    # - K-points validation
    # - Electronic/ionic convergence checks
    # - All the same features as AiiDA's PwBaseWorkChain