from pathlib import Path
import os
from airflow.models import DagBag
from airflow.utils.state import DagRunState
from datetime import datetime

# Set AIRFLOW__CORE__DAGS_FOLDER to include example_dags
dag_folder = str(
    Path(__file__).parent / "src" / "airflow_provider_aiida" / "example_dags"
)
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = dag_folder

# Create directories
Path("/tmp/airflow/local_workdir").mkdir(parents=True, exist_ok=True)
Path("/tmp/airflow/remote_workdir").mkdir(parents=True, exist_ok=True)

# Configuration
conf = {
    "machine": "localhost",
    "local_workdir": "/tmp/airflow/local_workdir",
    "remote_workdir": "/tmp/airflow/remote_workdir",
    "add_x": 10,
    "add_y": 5,
    "multiply_x": 7,
    "multiply_y": 3,
}

# Run DAG using Python API
dagbag = DagBag(dag_folder=dag_folder)
dag = dagbag.get_dag("arithmetic_add_multiply")
dag.test(run_conf=conf)
