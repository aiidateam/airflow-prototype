"""
Airflow DAG for Equation of State (EOS) workflow using TaskFlow API.

This DAG:
1. Relaxes an atomic structure to minimum energy (optional)
2. Creates strained structures with different scaling factors
3. Calculates energy and volume for each strained structure in parallel
4. Fits the E-V data to Birch-Murnaghan EOS model
"""

from aiida_quantumespresso.calculations.pw import PwCalculation
from airflow.decorators import task, task_group
from airflow.sdk import DAG
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


### XCom backend BEGIN ###
from typing import TYPE_CHECKING, Optional

from airflow_provider_aiida.aiida_core.airflow.ui import nodes_to_uuids, uuids_to_nodes
if TYPE_CHECKING:
    import ase

def serialize_atoms(mol: 'ase.Atoms') -> dict:
    """Serialize ASE Atoms to JSON."""
    return {
        'symbols': mol.get_chemical_symbols(),
        'positions': mol.get_positions().tolist(),
        'cell': mol.get_cell().tolist(),
        'pbc': mol.get_pbc().tolist(),
    }

def deserialize_atoms(mol_dict: dict) -> 'ase.Atoms':
    """Deserialize JSON to ASE Atoms."""
    from ase import Atoms
    return Atoms(**mol_dict)
### XCom backend END ###


@task
def create_strained_structures(atoms_json: dict, scales: list) -> list[dict]:
    """Generate a series of strained structures from a list of scaling factors."""
    atoms = deserialize_atoms(atoms_json)
    result = []
    for scale in scales:
        strained_atoms = atoms.copy()
        strained_atoms.set_cell(atoms.get_cell() * scale, scale_atoms=True)
        result.append(serialize_atoms(strained_atoms))
    return result


@task
def prepare(atoms_json: dict, pwcalc_params: Optional[dict]):
    from aiida import load_profile
    from aiida.orm import StructureData
    load_profile()
    inputs = get_default_pwcalc_param() if pwcalc_params is None else uuids_to_nodes(pwcalc_params)
    atoms = deserialize_atoms(atoms_json)
    si = StructureData()
    si.set_ase(atoms)
    inputs['structure'] = si
    process = PwCalculation(inputs=inputs)
    process._save_checkpoint()
    return {'node_pk': process.node.pk}

@task
def get_output(conf: dict):
    from aiida import load_profile
    from aiida.orm import load_node
    load_profile()
    cj = load_node(conf['node_pk'])
    return cj.outputs.output_parameters

@task(multiple_outputs=True)
def extract_energy_volume(output_parameters):
    return {
        'energy': output_parameters.value['energy'],
        'volume': output_parameters.value['volume'],
    }


@task(multiple_outputs=True)
def fit_eos_model(data: list) -> dict:
    """Fit Energy-Volume data to a Birch-Murnaghan Equation of State."""
    from ase.eos import EquationOfState
    from ase.units import kJ

    # Unpack the energies and volumes from the input data dictionary
    volumes_list = [value['volume'] for value in data]
    energies_list = [value['energy'] for value in data]

    eos = EquationOfState(volumes_list, energies_list)
    try:
        v0, e0, B = eos.fit()

        # The bulk modulus B is converted from eV/Å³ to GPa.
        B_GPa = B / kJ * 1.0e24
        return {'v0_A^3': v0, 'e0_eV': e0, 'B_GPa': B_GPa}
    except:
        return {'v0_A^3': 0., 'e0_eV': 0., 'B_GPa': 0.}


@task_group
def eos_taskgroup(atoms_json: dict, scales: list, pwcalc_params: Optional[dict]):
    """The complete EOS workflow as a task group."""

    scaled_structures = create_strained_structures(atoms_json, scales)

    pwcalc_conf = prepare.partial(pwcalc_params=pwcalc_params).expand(atoms_json=scaled_structures)
    pwcalc_run_task = TriggerDagRunOperator.partial(
            task_id="run_pwcalc", trigger_dag_id="PwCalculation",
            wait_for_completion=True, poke_interval=5).expand(conf=pwcalc_conf)
    pwcalc_outputs = get_output.expand(conf=pwcalc_conf)
    pwcalc_run_task >> pwcalc_outputs
    energy_volume = extract_energy_volume.expand(output_parameters=pwcalc_outputs)
    return fit_eos_model(energy_volume)


def get_default_pwcalc_param():
    from aiida import load_profile
    from aiida.common.exceptions import NotExistent


    from aiida.orm import (
        Dict,
        load_code,
        load_group,
        StructureData,
        KpointsData,
    )
    from ase.build import bulk

    #
    load_profile()
    # create pw code
    pw_code = load_code("qe-7.3-gf-pw@thor")
    si = StructureData(ase=bulk("Si"))
    pw_paras = Dict(
        {
            "CONTROL": {
                "calculation": "scf",
            },
            "SYSTEM": {
                "ecutwfc": 30,
                "ecutrho": 240,
                "occupations": "smearing",
                "smearing": "gaussian",
                "degauss": 0.1,
            },
        }
    )
    # Load the pseudopotential family.
    pseudo_family = load_group("SSSP/1.3/PBEsol/efficiency")
    pseudos = pseudo_family.get_pseudos(structure=si)
    #
    metadata = {
        "options": {
            "resources": {
                "num_machines": 1,
                "num_mpiprocs_per_machine": 1,
            },
        }
    }
    #
    kpoints = KpointsData()
    kpoints.set_kpoints_mesh([3, 3, 3])
    pseudos = pseudo_family.get_pseudos(structure=si)
    scf_inputs = {
        "code": pw_code,
        "parameters": pw_paras,
        "kpoints": kpoints,
        "pseudos": pseudos,
        "metadata": metadata,
    }

    return scf_inputs

with DAG(
    dag_id='eos_pwcalc_workflow',
    params={
        'atoms': {
            'numbers': [14, 14],
             'positions': [[0.    , 0.    , 0.    ],
                           [1.3575, 1.3575, 1.3575]],
             'cell': [[0.   , 2.715, 2.715],
                      [2.715, 0.   , 2.715],
                      [2.715, 2.715, 0.   ]],
             'pbc': [ True,  True,  True],
        },
        'scales': Param([0.95], type="array"),
        'pwcalc_params': Param(None, type=["object", "null"])
    },
    render_template_as_native_obj = True
) as eos_dag:

    eos_taskgroup("{{ params.atoms }}",
                  "{{ params.scales }}",
                  "{{ params.pwcalc_params }}")

if __name__ == "__main__":
    # if you want to change default params you need to do nodes_to_uuids, maybe moved to xcom backend?
    eos_dag.test()
