# Automatic DAG Generation System

## Overview

System that generates **physical DAG files** for all AiiDA plugins during installation.

**Result**: 30+ AiiDA plugins → 30+ physical DAG `.py` files

## Installation & Generation

### Generate DAG Files

```bash
# Run this after installing new AiiDA plugins
python3 scripts/generate_dags.py
```

This creates physical `.py` files in: `src/airflow_provider_aiida/auto_generated_dags/`

### Install Package

```bash
pip install -e .
```

## Files

### Generated (30+ files)
```
src/airflow_provider_aiida/auto_generated_dags/
├── __init__.py
├── aiida_calcjob_core_arithmetic_add.py
├── aiida_calcjob_core_stash.py
├── aiida_workchain_core_arithmetic_multiply_add.py
└── ... (27+ more)
```

### Source Files
```
scripts/generate_dags.py          # Generator script
pyproject.toml                     # Entry point configuration
```

## Usage

### After Installing New AiiDA Plugin

```bash
# 1. Install new plugin
pip install aiida-new-plugin

# 2. Regenerate DAG files
python3 scripts/generate_dags.py

# 3. Restart Airflow
```

### Trigger a DAG

```python
from aiida import load_profile
from aiida.orm import load_code, Int
from aiida.calculations.arithmetic.add import ArithmeticAddCalculation

load_profile()
code = load_code('bash@localhost')
inputs = {'code': code, 'x': Int(5), 'y': Int(3)}
process = ArithmeticAddCalculation(inputs=inputs)
process._save_checkpoint()

# Trigger: airflow dags trigger aiida_calcjob_core_arithmetic_add \
#          --conf '{"node_pk": <process.node.pk>}'
```

## Benefits

✅ **Fast**: DAGs pre-generated, no startup penalty
✅ **Persistent**: Physical files on disk
✅ **Maintainable**: Regenerate when needed
✅ **Standard**: Normal Python files, standard Airflow workflow
