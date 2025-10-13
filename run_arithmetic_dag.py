from pathlib import Path
import os

# Set AIRFLOW__CORE__DAGS_FOLDER to include example_dags
dag_folder = str(
    Path(__file__).parent / "src" / "airflow_provider_aiida" / "example_dags"
)
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = dag_folder

# Import AFTER setting the environment variable
from airflow.models import DagBag

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

# Run DAG using test mode (bypasses serialization requirement)
dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
dag = dagbag.get_dag("arithmetic_add_multiply")

# Use test mode with execution_date to avoid serialization issues
from datetime import datetime
dag.test(
    run_conf=conf,
    use_executor=False  # Run tasks sequentially in the same process
)
