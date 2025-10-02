# Migration Guide: AiiDA CalcJob to Airflow

This guide shows how to migrate existing AiiDA CalcJob code to work with Airflow while maintaining backward compatibility.

## Overview

The Airflow-compatible `PwCalculation` wraps the AiiDA `PwCalculation` to work with Airflow's task execution model while preserving the familiar builder pattern.

## Key Differences

| Aspect | AiiDA (Original) | Airflow (New) |
|--------|------------------|---------------|
| Execution | `run.get_node(builder)` | `PwCalculation.from_builder(...)` |
| Runtime | Immediate execution | DAG scheduling |
| Results | Returns immediately | Stored in XCom/local files |
| Dependencies | Implicit (process graph) | Explicit (task dependencies) |

## Migration Example

### Original AiiDA Code

```python
from aiida.engine import run
from aiida.orm import Dict, KpointsData, StructureData, load_code, load_group
from aiida import load_profile
from ase.build import bulk

load_profile()

# Build the calculation
code = load_code('pw-7.3@thor')
builder = code.get_builder()

structure = StructureData(ase=bulk('Si', 'fcc', 5.43))
builder.structure = structure

pseudo_family = load_group('SSSP/1.3/PBEsol/efficiency')
builder.pseudos = pseudo_family.get_pseudos(structure=structure)

cutoff_wfc, cutoff_rho = pseudo_family.get_recommended_cutoffs(
    structure=structure,
    unit='Ry'
)

parameters = Dict({
    'CONTROL': {'calculation': 'scf'},
    'SYSTEM': {
        'ecutwfc': cutoff_wfc,
        'ecutrho': cutoff_rho,
    }
})
builder.parameters = parameters

kpoints = KpointsData()
kpoints.set_kpoints_mesh([2, 2, 2])
builder.kpoints = kpoints

builder.metadata.options = {
    'resources': {'num_machines': 1},
    'max_wallclock_seconds': 1800,
    'withmpi': False,
}

# Execute
results, node = run.get_node(builder)
print(f'Calculation: {node.process_class}<{node.pk}> {node.process_state.value}')
print(f'Results: {results}')
```

### Migrated Airflow Code

```python
from datetime import datetime
from airflow import DAG
from aiida.orm import Dict, KpointsData, StructureData, load_code, load_group
from aiida import load_profile
from ase.build import bulk

from airflow_provider_aiida.calculations import PwCalculation

load_profile()

with DAG(
    dag_id='qe_pw_scf',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
) as dag:

    # SAME AS BEFORE: Build using AiiDA builder pattern
    code = load_code('pw-7.3@thor')
    builder = code.get_builder()

    structure = StructureData(ase=bulk('Si', 'fcc', 5.43))
    builder.structure = structure

    pseudo_family = load_group('SSSP/1.3/PBEsol/efficiency')
    builder.pseudos = pseudo_family.get_pseudos(structure=structure)

    cutoff_wfc, cutoff_rho = pseudo_family.get_recommended_cutoffs(
        structure=structure,
        unit='Ry'
    )

    parameters = Dict({
        'CONTROL': {'calculation': 'scf'},
        'SYSTEM': {
            'ecutwfc': cutoff_wfc,
            'ecutrho': cutoff_rho,
        }
    })
    builder.parameters = parameters

    kpoints = KpointsData()
    kpoints.set_kpoints_mesh([2, 2, 2])
    builder.kpoints = kpoints

    builder.metadata.options = {
        'resources': {'num_machines': 1},
        'max_wallclock_seconds': 1800,
        'withmpi': False,
    }

    # NEW: Instead of run.get_node(), create TaskGroup
    # Once we replace the engine, this will eventually will be backward compatible. 
    # Like the usual `engine.run(builder)`
    pw_calc = PwCalculation.from_builder(
        builder=builder,
        group_id='pw_scf_calculation',
        machine='thor',
        local_workdir='/tmp/airflow/pw_scf',
        remote_workdir='/tmp/pw_scf_remote'
    )
```

## What Changes?

### Minimal Changes Required

1. **Import**: Change from `aiida.engine.run` to `airflow_provider_aiida.calculations.PwCalculation`
2. **Wrap in DAG**: Add DAG context manager
3. **Execution**: Replace `run.get_node(builder)` with `PwCalculation.from_builder(builder, ...)`

### What Stays the Same?

- ✅ All builder setup code (structure, parameters, kpoints, etc.)
- ✅ AiiDA data types (StructureData, Dict, KpointsData)
- ✅ Input validation logic
- ✅ File generation (handled internally)

## How It Works

### Architecture

```
AiiDA Builder → PwCalculation(CalcJobTaskGroup) → Airflow Tasks
     ↓                          ↓
   Same API              Internal Tasks:
                         1. prepare   → Generate inputs using AiiDA logic
                         2. upload    → Upload to remote
                         3. submit    → Submit job
                         4. update    → Monitor job
                         5. receive   → Download results
                         6. parse     → Parse outputs using AiiDA parser
```

### Internal Process

1. **Prepare Phase**:
   - Uses AiiDA's `prepare_for_submission()` method
   - Generates input files using original AiiDA logic
   - Extracts file upload/download lists

2. **Execution Phase**:
   - Airflow handles scheduling and execution
   - Tasks run on Airflow workers
   - SSH transport via TransportQueue

3. **Parse Phase**:
   - Uses AiiDA's parser (e.g., `quantumespresso.pw`)
   - Returns results in compatible format

## Accessing Results

### In AiiDA (Original)
```python
results, node = run.get_node(builder)
energy = results['output_parameters'].get_dict()['energy']
```

### In Airflow (New)
```python
# Results are available via XCom or by reading the parse task output
# Option 1: Access via downstream task
@task
def process_results(**context):
    ti = context['task_instance']
    results = ti.xcom_pull(task_ids='pw_scf_calculation.parse')
    exit_status, output_dict = results
    if exit_status == 0:
        energy = output_dict.get('energy')
        return energy
    else:
        raise ValueError(f"Calculation failed: {output_dict.get('error')}")

# Option 2: Chain with task dependencies
energy_task = process_results()
pw_calc >> energy_task
```

## Advanced: Custom CalcJobs

To create your own Airflow-compatible CalcJob:

```python
from airflow_provider_aiida.taskgroups.calcjob import CalcJobTaskGroup
from aiida_quantumespresso.calculations.your_calc import YourCalculation as AiiDAYourCalc

class YourCalculation(CalcJobTaskGroup):

    def __init__(self, group_id, machine, local_workdir, remote_workdir,
                 builder=None, **kwargs):
        self.builder = builder
        super().__init__(group_id, machine, local_workdir, remote_workdir, **kwargs)

    @classmethod
    def from_builder(cls, builder, group_id, machine,
                     local_workdir, remote_workdir, **kwargs):
        return cls(group_id, machine, local_workdir, remote_workdir,
                   builder=builder, **kwargs)

    def prepare(self, **context):
        # Use AiiDA's prepare_for_submission logic
        temp_calc = AiiDAYourCalc(inputs=self.builder._inputs(prune=True))
        # ... generate inputs, build submission script
        # Push to XCom: to_upload_files, to_receive_files, submission_script
        pass

    def parse(self, local_workdir, **context):
        # Use AiiDA's parser
        # Return (exit_status, results_dict)
        pass
```

## Benefits

1. **Backward Compatibility**: Existing AiiDA code works with minimal changes
2. **Airflow Features**: Get scheduling, monitoring, retries, etc.
3. **Scalability**: Airflow's distributed execution model
4. **Unified Workflow**: Combine with other Airflow operators
5. **Familiar API**: Keep using AiiDA builders and data types

## Limitations

1. **Async Execution**: Results not immediately available (use XCom)
2. **DAG Definition**: Must wrap in DAG context
3. **Storage**: Files managed by Airflow (local/remote workdirs)

## Next Steps

1. Start with simple CalcJobs (like `PwCalculation`)
2. Test with your existing builder code
3. Extend to WorkChains using Airflow's task dependencies
4. Leverage Airflow's features (sensors, branches, etc.)
