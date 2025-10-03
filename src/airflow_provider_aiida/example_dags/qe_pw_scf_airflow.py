"""
Example DAG showing how to use the Airflow-compatible PwCalculation.

This demonstrates backward compatibility - the same AiiDA builder pattern works,
but now executes as an Airflow DAG.
"""

from datetime import datetime
from airflow import DAG
from aiida.orm import Dict, KpointsData, StructureData, load_code, load_group
from aiida import load_profile
from ase.build import bulk

from airflow_provider_aiida.taskgroups.plugins import PwCalculation

# Load AiiDA profile (same as before)
load_profile()

# Create the DAG
with DAG(
    dag_id='qe_pw_scf_airflow',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
) as dag:

    # ========== SAME AS BEFORE: Build using AiiDA builder pattern ==========
    # Load the code configured for ``pw.x``
    code = load_code('pw-7.3@thor')
    builder = code.get_builder()

    # Create a silicon fcc crystal
    structure = StructureData(ase=bulk('Si', 'fcc', 5.43))
    builder.structure = structure

    # Load the pseudopotential family
    pseudo_family = load_group('SSSP/1.3/PBEsol/efficiency')
    builder.pseudos = pseudo_family.get_pseudos(structure=structure)

    # Request the recommended wavefunction and charge density cutoffs
    cutoff_wfc, cutoff_rho = pseudo_family.get_recommended_cutoffs(
        structure=structure,
        unit='Ry'
    )

    parameters = Dict({
        'CONTROL': {
            'calculation': 'scf'
        },
        'SYSTEM': {
            'ecutwfc': cutoff_wfc,
            'ecutrho': cutoff_rho,
        }
    })
    builder.parameters = parameters

    # Generate a 2x2x2 Monkhorst-Pack mesh
    kpoints = KpointsData()
    kpoints.set_kpoints_mesh([2, 2, 2])
    builder.kpoints = kpoints

    # Run the calculation on 1 CPU
    builder.metadata.options = {
        'resources': {
            'num_machines': 1,
        },
        'max_wallclock_seconds': 1800,
        'withmpi': False,
    }

    # ========== NEW: Create Airflow TaskGroup from builder ==========
    # Instead of run.get_node(builder), create a PwCalculation TaskGroup
    pw_calc = PwCalculation.from_builder(
        builder=builder,
        group_id='pw_scf_calculation',
        machine='thor',  # Uses the same computer as the code
        local_workdir='/tmp/airflow/pw_scf',
        remote_workdir='/mnt/home/khosra_a/aiida/airflow_remote'
    )

    # The TaskGroup is now part of the DAG and will execute when triggered
