"""
Airflow DAG for Equation of State (EOS) workflow using TaskFlow API.

This DAG:
1. Relaxes an atomic structure to minimum energy (optional)
2. Creates strained structures with different scaling factors
3. Calculates energy and volume for each strained structure in parallel
4. Fits the E-V data to Birch-Murnaghan EOS model
"""

from airflow.decorators import task, task_group
from airflow.sdk import DAG
from airflow.models.param import Param


### XCom backend BEGIN ###
from typing import TYPE_CHECKING
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
def relax_structure(run_relax: bool, atoms_json: dict):
    """Relax the atomic structure to its minimum energy configuration using ASE."""
    if not run_relax:
        return atoms_json

    from ase.calculators.emt import EMT
    from ase.optimize import BFGS
    atoms = deserialize_atoms(atoms_json)
    atoms.calc = EMT()
    optimizer = BFGS(atoms)
    optimizer.run(fmax=0.01)
    return serialize_atoms(atoms)


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

# TODO try multiple_outputs=True
@task(multiple_outputs=True)
def calculate_energy_and_volume(atoms_json: dict):
    """Calculate the energy and volume for a single atomic structure."""
    from ase.calculators.emt import EMT

    atoms = deserialize_atoms(atoms_json)
    atoms.calc = EMT()
    atoms.get_potential_energy()

    return {
        'energy': atoms.calc.results['energy'],
        'volume': atoms.get_volume(),
    }

# TODO remove or use
@task
def collect_results(results: list) -> dict:
    """Collect results from parallel energy/volume calculations into a dict."""
    collected = {}
    for i, result in enumerate(results):
        collected[f'strain_{i}'] = result
    return collected


@task(multiple_outputs=True)
def fit_eos_model(data: list) -> dict:
    """Fit Energy-Volume data to a Birch-Murnaghan Equation of State."""
    from ase.eos import EquationOfState
    from ase.units import kJ

    # Unpack the energies and volumes from the input data dictionary
    volumes_list = [value['volume'] for value in data]
    energies_list = [value['energy'] for value in data]

    eos = EquationOfState(volumes_list, energies_list)
    v0, e0, B = eos.fit()

    # The bulk modulus B is converted from eV/Å³ to GPa.
    B_GPa = B / kJ * 1.0e24
    return {'v0_A^3': v0, 'e0_eV': e0, 'B_GPa': B_GPa}


@task_group
def eos_taskgroup(atoms_json: dict, scales: list, run_relax: bool = True):
    """The complete EOS workflow as a task group."""
    # NOTE: We cannot use a python if-logic within a task group. We can only do it within a task 
    # or we use a specfic task.branch here to make it explicit (also in the UI represented)
    atoms_json = relax_structure(run_relax, atoms_json)
    scaled_structures = create_strained_structures(atoms_json, scales)
    # NOTE: We cannot use a python for-loop within a task group. A mapping needs be used.
    # A for-loop with a mutable state is not already provided by airflow and would need to be created by us.
    # One could create one using the dynamic mapping of airflow on a range and passing the variables by the context
    # Not sure how good the UX would be in this case.
    emt_outputs = calculate_energy_and_volume.expand(atoms_json=scaled_structures)
    return fit_eos_model(emt_outputs)


with DAG(
    dag_id='eos_workflow',
    params={
        'atoms': {
            'numbers': [29],
            'positions': [[0., 0., 0.]],
            'cell':[[0. , 1.8, 1.8],
                    [1.8, 0. , 1.8],
                    [1.8, 1.8, 0. ]],
            'pbc': [ True,  True,  True],
        },
        'scales': Param([0.95, 0.97], type="array"),
        'run_relax': True,
    },
    render_template_as_native_obj = True
) as eos_dag:

    eos_taskgroup("{{ params.atoms }}",
                   "{{ params.scales }}",
                   "{{ params.run_relax }}")


    # NOTE: You cannot access the returned tuple from a task within a dag or task_group

    #@task
    #def foo():
    #    return 5, 2
    #@task
    #def foo2(a, b):
    #    pass
    #arg1, arg2 = foo() # DOES NOT WORK

    #You have to do even

    #@task
    #def foo():
    #    return 5, 2
    #@task
    #def foo2(args):
    #    args[0], args[1] = args
    #output = foo()
    #foo2(output)

    #or you can do this

    #@task
    #def foo():
    #    return {"a": 5, "b": 2}
    #@task
    #def foo2(a, b):
    #    pass
    #output = foo()
    #foo2(output["a"], output["b"])

    # NOTE: You cannot use pythonic if/for/while-logic within a task_group arguments are only resolved within tasks

    # This does not work properly:
    #@task_group
    #def eos_taskgroup(atoms: str, scales: list, run_relax: bool = True):
    #    """The complete EOS workflow as a task group."""
    #    if run_relax:
    #        atoms = relax_structure(atoms)
    # So only can do these within tasks. Airflow provides airflow representations for if/map logic.
    # The map logic can be extended by custom task groups to for/while logic but needs to be properly investigated.

    # NOTE One cannot use the dynamic task mapping in test mode. Running this file with python will fail, but running throuhg the API server works.

    # NOTE: One cannot use the dynamic task mapping with jinja template arguments, so one would need to process these with a task to get xcom arguments


    # NOTE: I used this time the argument render_template_as_native_obj=True resolves the jinja templates to python objects and not strings

    # NOTE: I used context manager `with DAG` instead of the decorator `@dag` just becasue I like the syntax more.

if __name__ == "__main__":
    eos_dag.test()
