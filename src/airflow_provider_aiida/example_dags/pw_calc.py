from __future__ import annotations
from airflow_provider_aiida.taskgroups.calcjob import CalcJobTaskGroup
from aiida_quantumespresso.calculations.pw import PwCalculation
from ase.build import bulk

from airflow import DAG
from airflow.sdk import Param

with DAG(
    'PwCalculation',
    params={
        "node_pk": Param("", type="integer")
    },
    render_template_as_native_obj = True
) as dag:
    add_job = CalcJobTaskGroup(
        process_class=PwCalculation,
        node_pk="{{ params.node_pk }}",
    )

    add_job


if __name__ == "__main__":
    from aiida import load_profile
    load_profile()
    from aiida.orm import Dict, StructureData, KpointsData, load_group, load_code

    print("=" * 60)
    print("Testing arithmetic_aiida_native_single DAG")
    print("=" * 60)

    # Test the DAG with default parameters
    #code = load_code('pw-7.3@thor')
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
