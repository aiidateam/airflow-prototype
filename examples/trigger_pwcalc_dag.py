"""
Trigger an Airflow DAG using the internal API client.
This script must be run from within the Airflow environment.
"""

from airflow.api.client import get_current_api_client

# DAG configuration
DAG_ID = "pw_calcjob"


# Create Process
from aiida import load_profile
load_profile()

from aiida_quantumespresso.calculations.pw import PwCalculation
from ase.build import bulk
from aiida.orm import Dict, StructureData, KpointsData, load_group, load_code

code = load_code('qe-7.3-gf-pw@thor')
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
        'num_mpiprocs_per_machine': 1,
    },
    'max_wallclock_seconds': 1800,
    'withmpi': True,
}


process = PwCalculation(inputs=builder)
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
