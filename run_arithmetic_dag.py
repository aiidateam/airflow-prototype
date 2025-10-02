import subprocess
from pathlib import Path
import json
import os

# Set AIRFLOW__CORE__DAGS_FOLDER to include example_dags
dag_folder = str(Path(__file__).parent / 'src' / 'airflow_provider_aiida' / 'example_dags')
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = dag_folder

# Create directories
Path('/tmp/airflow/local_workdir').mkdir(parents=True, exist_ok=True)
Path('/tmp/airflow/remote_workdir').mkdir(parents=True, exist_ok=True)

# Configuration
conf = {
    'machine': 'localhost',
    'local_workdir': '/tmp/airflow/local_workdir',
    'remote_workdir': '/tmp/airflow/remote_workdir',
    'add_x': 10,
    'add_y': 5,
    'multiply_x': 7,
    'multiply_y': 3,
}

# Run DAG using CLI
cmd = ['airflow', 'dags', 'test', 'arithmetic_add_multiply', '--conf', json.dumps(conf)]
subprocess.run(cmd)
