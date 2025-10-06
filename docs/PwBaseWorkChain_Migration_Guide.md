# PwBaseWorkChain Migration Guide: AiiDA to Airflow

## Overview

This guide explains how to use `PwBaseWorkChain` in Airflow while maintaining **100% backward compatibility** with AiiDA's builder interface.

## Key Features

✅ **Backward Compatible**: Same builder interface as AiiDA
✅ **Automatic Error Handling**: All error handlers from AiiDA
✅ **Restart Logic**: Automatic restarts on recoverable errors
✅ **K-points Validation**: Automatic generation from distance
✅ **Production Ready**: Battle-tested error recovery strategies

## Quick Start

### Before (AiiDA)

```python
from aiida import orm, load_profile, engine
from ase.build import bulk
from aiida_quantumespresso.workflows.pw.base import PwBaseWorkChain

load_profile()

structure = orm.StructureData(ase=bulk('Si', 'fcc', 5.43))
code = orm.load_code('pw-7.3@thor')

builder = PwBaseWorkChain.get_builder_from_protocol(
    code=code,
    structure=structure,
    protocol='fast'
)

# Run the workflow
results = engine.run(builder)
```

### After (Airflow)

```python
from datetime import datetime
from airflow import DAG
from aiida import orm, load_profile
from ase.build import bulk
from aiida_quantumespresso.workflows.pw.base import PwBaseWorkChain as AiiDaPwBaseWorkChain

from airflow_provider_aiida.taskgroups.workchains import PwBaseWorkChain

load_profile()

with DAG(
    dag_id='qe_pw_base_workchain',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
) as dag:
    # Same builder setup as before
    structure = orm.StructureData(ase=bulk('Si', 'fcc', 5.43))
    code = orm.load_code('pw-7.3@thor')

    builder = AiiDaPwBaseWorkChain.get_builder_from_protocol(
        code=code,
        structure=structure,
        protocol='fast'
    )

    # Create Airflow TaskGroup from builder
    pw_base_wc = PwBaseWorkChain.from_builder(
        builder=builder,
        group_id='pw_base_workchain',
        machine='thor',
        local_workdir='/tmp/airflow/pw_base',
        remote_workdir='/scratch/aiida/pw_base'
    )
```

## Architecture

### Workflow Structure

The PwBaseWorkChain creates the following task structure in Airflow:

```
pw_base_workchain/
├── setup                    # Validate and prepare inputs
├── validate_kpoints         # Generate/validate k-points
├── restart_loop/            # While loop for restarts
│   └── while_loop          # Main calculation + error handling
└── results                 # Collect and expose outputs
```

### Error Handling

The following error handlers are implemented:

1. **Sanity Check (Band Occupations)**
   - Checks if highest band is overly occupied
   - Increases `nbnd` if needed
   - Restarts from charge density

2. **Diagonalization Errors**
   - Tries alternative algorithms: david → ppcg → paro → cg
   - Each progressively more stable but slower

3. **Out of Walltime**
   - Full restart from checkpoint
   - Uses output structure if available

4. **Electronic Convergence Issues**
   - Reduces `mixing_beta` by factor of 0.8
   - Restarts from previous calculation

5. **Ionic Convergence Issues**
   - Restarts from output structure
   - For BFGS failures: tries damp dynamics
   - For vc-relax: adjusts trust_radius_min

6. **VC-Relax Final SCF Issues**
   - Accepts structure if ionic convergence met
   - Returns special exit code 501

### Restart Types

Following AiiDA's restart strategy:

- **FROM_SCRATCH**: New calculation, no parent folder
- **FULL**: Full restart with `restart_mode='restart'`
- **FROM_CHARGE_DENSITY**: Restart using charge density from previous calc
- **FROM_WAVE_FUNCTIONS**: Restart using wave functions from previous calc

## Advanced Usage

### Custom Builder Configuration

```python
from aiida import orm
from aiida_quantumespresso.common.types import ElectronicType, SpinType

# Create custom builder
builder = AiiDaPwBaseWorkChain.get_builder_from_protocol(
    code=code,
    structure=structure,
    protocol='moderate',
    electronic_type=ElectronicType.METAL,
    spin_type=SpinType.COLLINEAR,
)

# Customize parameters
builder.pw.parameters['SYSTEM']['ecutwfc'] = 80.0
builder.pw.parameters['ELECTRONS']['conv_thr'] = 1.e-10

# Customize k-points
builder.kpoints_distance = orm.Float(0.15)  # Higher density
builder.kpoints_force_parity = orm.Bool(True)

# Set max iterations and cleanup
builder.max_iterations = orm.Int(10)
builder.clean_workdir = orm.Bool(True)

# Create TaskGroup
pw_base_wc = PwBaseWorkChain.from_builder(
    builder=builder,
    group_id='custom_pw_base',
    machine='thor',
    local_workdir='/tmp/airflow/custom_pw',
    remote_workdir='/scratch/aiida/custom_pw'
)
```

### Manual Input Construction

If you don't want to use the builder pattern:

```python
from airflow_provider_aiida.taskgroups.workchains import PwBaseWorkChain

pw_base_wc = PwBaseWorkChain(
    group_id='pw_base',
    machine='thor',
    local_workdir='/tmp/airflow/pw',
    remote_workdir='/scratch/aiida/pw',
    pw_inputs={
        'code': code,
        'structure': structure,
        'parameters': parameters,
        'pseudos': pseudos,
        'metadata': metadata,
    },
    kpoints_distance=orm.Float(0.2),
    max_iterations=5,
    clean_workdir=False,
)
```

### Accessing Results

```python
from airflow.decorators import task

@task
def process_results(**context):
    """Process PwBaseWorkChain results."""
    ti = context['task_instance']

    # Get final results
    results = ti.xcom_pull(
        task_ids='pw_base_workchain.results'
    )

    print(f"Success: {results['success']}")
    print(f"Iterations: {results['iterations']}")
    print(f"Exit code: {results['exit_code']}")

    # Get calculation outputs from last iteration
    loop_result = ti.xcom_pull(
        task_ids='pw_base_workchain.restart_loop.while_loop'
    )

    outputs = loop_result.get('outputs', {})
    output_params = outputs.get('output_parameters', {})
    output_structure = outputs.get('output_structure')

    return output_params

# Add to DAG
pw_base_wc >> process_results()
```

## Comparison: AiiDA vs Airflow

| Feature | AiiDA | Airflow |
|---------|-------|---------|
| **Builder Interface** | ✅ Full support | ✅ Full support |
| **get_builder_from_protocol** | ✅ Native | ✅ Supported |
| **Error Handlers** | ✅ Built-in | ✅ Ported |
| **Restart Logic** | ✅ Automatic | ✅ Automatic |
| **K-points Validation** | ✅ Automatic | ✅ Automatic |
| **Provenance** | ✅ Database | ✅ XCom |
| **UI Visualization** | ❌ Limited | ✅ Rich DAG view |
| **Scheduling** | ❌ Manual | ✅ Cron/triggers |
| **Monitoring** | ❌ Basic | ✅ Advanced |

## Migration Checklist

- [ ] Install airflow-provider-aiida
- [ ] Update imports to use Airflow DAG context
- [ ] Replace `engine.run(builder)` with `PwBaseWorkChain.from_builder(...)`
- [ ] Specify machine, local_workdir, remote_workdir
- [ ] Test with simple SCF calculation
- [ ] Test with relax calculation
- [ ] Test error recovery (e.g., reduce walltime to trigger restart)
- [ ] Verify outputs are accessible via XCom

## Troubleshooting

### Issue: "Neither kpoints nor kpoints_distance specified"

**Solution**: Ensure builder has k-points:
```python
builder.kpoints_distance = orm.Float(0.2)
# OR
builder.kpoints = kpoints_data
```

### Issue: Task fails immediately without running calculation

**Solution**: Check that all builder inputs are properly set:
```python
# Verify required inputs
assert hasattr(builder.pw, 'code')
assert hasattr(builder.pw, 'structure')
assert hasattr(builder.pw, 'parameters')
assert hasattr(builder.pw, 'pseudos')
```

### Issue: Restarts not working

**Solution**: Ensure restart handlers are enabled and iteration limit is sufficient:
```python
builder.max_iterations = orm.Int(10)  # Increase if needed
```

## Performance Considerations

1. **XCom Size**: Large structures and parameters are passed via XCom. For very large data, consider using external storage.

2. **Iteration Limit**: Default is 5 iterations. Increase for difficult convergence cases:
   ```python
   builder.max_iterations = orm.Int(10)
   ```

3. **Cleanup**: Enable workdir cleanup to save space:
   ```python
   builder.clean_workdir = orm.Bool(True)
   ```

## Next Steps

- See `qe_baseworkchain.py` for complete working example
- Check `pw_base_with_handlers.py` for full implementation details
- Review AiiDA's PwBaseWorkChain documentation for parameter tuning
- Explore PwRelaxWorkChain for multi-stage relaxations

## Support

For issues or questions:
- Check Airflow logs: `airflow tasks test <dag_id> <task_id> <date>`
- Review XCom data in Airflow UI
- Compare with AiiDA implementation in `aiida_quantumespresso/workflows/pw/base.py`
