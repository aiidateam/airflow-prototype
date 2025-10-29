from __future__ import annotations
from airflow_provider_aiida.taskgroups.workchain import WorkChainTaskGroup
from aiida_quantumespresso.workflows.pw.base import PwBaseWorkChain
from ase.build import bulk

from airflow import DAG
from airflow.sdk import Param

with DAG(
    'PwBaseWorkChain',
    params={
        "node_pk": Param("", type="integer")
    },
    render_template_as_native_obj = True
) as dag:
    pw_base_wc = WorkChainTaskGroup(
        process_class=PwBaseWorkChain,
        node_pk="{{ params.node_pk }}",
    )

    pw_base_wc


if __name__ == "__main__":
    from aiida import load_profile
    load_profile()
    from aiida.orm import Dict, StructureData, KpointsData, load_group, load_code

    print("=" * 60)
    print("Testing PwBaseWorkChain DAG")
    print("=" * 60)

    # Test the DAG with default parameters
    #code = load_code('pw-7.3@thor')
    code = load_code('pw-7.3@thor')

    # Create a silicon fcc crystal
    structure = StructureData(ase=bulk('Si', 'fcc', 5.43))

    # Load the pseudopotential family
    pseudo_family = load_group('SSSP/1.3/PBEsol/efficiency')
    pseudos = pseudo_family.get_pseudos(structure=structure)

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

    # Generate a 2x2x2 Monkhorst-Pack mesh
    kpoints = KpointsData()
    kpoints.set_kpoints_mesh([2, 2, 2])

    # Build inputs for PwBaseWorkChain
    # Note: kpoints is at the top level, not nested under 'pw'
    inputs = {
        'pw': {
            'code': code,
            'structure': structure,
            'pseudos': pseudos,
            'parameters': parameters,
            'metadata': {
                'options': {
                    'resources': {
                        'num_machines': 1,
                        'num_mpiprocs_per_machine': 1,
                    },
                    'max_wallclock_seconds': 1800,
                    'withmpi': True,
                }
            }
        },
        'kpoints': kpoints,
    }

    process = PwBaseWorkChain(inputs=inputs)
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
