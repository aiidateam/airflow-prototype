"""
Airflow DAG for atomization energy workflow using TaskFlow API.

This DAG computes the atomization energy by:
1. Calculating energy of an isolated atom
2. Calculating energy of a molecule
3. Computing atomization energy: 2 * E(atom) - E(molecule)
"""

from airflow.decorators import dag, task, task_group


### XCom backend BEGIN ###
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import ase

def serialize_atoms(mol: 'ase.Atoms') -> dict:
    import json
    return json.dumps({
        'symbols': mol.get_chemical_symbols(),
        'positions': mol.get_positions().tolist(),
        'cell': mol.get_cell().tolist(),
        'pbc': mol.get_pbc().tolist(),
    })

def deserialize_atoms(mol_dict: dict) -> 'ase.Atoms':
    import json
    from ase import Atoms
    return Atoms(**json.loads(mol_dict))
### XCom backend END ###

@task
def calculate_energy(atoms_dict):
    """Calculate the total energy of an atomic structure using ASE."""
    from ase import Atoms
    from ase.calculators.emt import EMT

    # Reconstruct Atoms object from dict
    atoms = deserialize_atoms(atoms_dict)
    atoms.calc = EMT()
    atoms.get_potential_energy()
    energy = atoms.calc.results['energy']
    print(f"Energy: {energy} eV")
    return energy


@task
def compute_atomization_energy(energy_atom, energy_molecule):
    """Calculate the atomization energy from atomic and molecular energies."""
    atomization_energy = 2 * energy_atom - energy_molecule
    print(f"Atomization energy: {atomization_energy} eV")
    return atomization_energy


@task_group
def atomization_energy_taskgroup(molecule_dict, atom_dict):
    """Define the workflow graph to compute atomization energy."""
    e_atom = calculate_energy(atom_dict)
    e_molecule = calculate_energy(molecule_dict)
    result = compute_atomization_energy(e_atom, e_molecule)
    return result


@dag(
    dag_id='atomization_energy_workflow',
    params={
        'molecule_dict': {
            'symbols': ['H', 'H'],
            'positions': [[0.0, 0.0, 0.0], [0.0, 0.0, 0.74]],
            'cell': [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]],
            'pbc': [False, False, False],
        },
        'atom_dict': { 
            'symbols': ['H'],
            'positions': [[0.0, 0.0, 0.0]],
            'cell': [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]],
            'pbc': [False, False, False],
        },
    },
)
def atomization_energy_dag():
    return atomization_energy_taskgroup("{{ params.molecule_dict | tojson }}", "{{ params.atom_dict | tojson }}")



# Instantiate the DAG
dag_instance = atomization_energy_dag()


if __name__ == "__main__":
    dag_instance.test()
