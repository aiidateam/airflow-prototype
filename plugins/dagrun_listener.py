import sqlite3
import os
import logging
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState

logger = logging.getLogger(__name__)

# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), 'dagrun_tracking.db')

def _init_database():
    """Initialize the SQLite database and create the dagrun table if it doesn't exist."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS dagrun_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dag_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    run_type TEXT,
                    state TEXT,
                    execution_date TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    external_trigger BOOLEAN,
                    conf TEXT,
                    event_type TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            logger.info(f"DagRun tracking database initialized at {DB_PATH}")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")

# AIIDA INTEGRATION - OPTIONAL
AIIDA_AVAILABLE = False
try:
    from aiida import load_profile, orm
    from aiida.common.links import LinkType
    load_profile()
    AIIDA_AVAILABLE = True
    logger.info("AiiDA integration enabled")
except Exception as e:
    logger.info(f"AiiDA integration disabled: {e}")

def _should_integrate_dag_with_aiida(dag_run: DagRun) -> bool:
    if not AIIDA_AVAILABLE:
        return False
    dag_tags = getattr(dag_run.dag, 'tags', [])
    return 'aiida' in dag_tags

def _store_airflow_params_as_aiida_inputs(node, params):
    """Store Airflow parameters as AiiDA data nodes and link them as inputs."""
    for key, value in params.items():
        if isinstance(value, (str, int, float, bool)):
            if isinstance(value, str):
                aiida_data = orm.Str(value)
            elif isinstance(value, int):
                aiida_data = orm.Int(value)
            elif isinstance(value, float):
                aiida_data = orm.Float(value)
            elif isinstance(value, bool):
                aiida_data = orm.Bool(value)
            
            try:
                node.base.links.add_incoming(aiida_data, link_type=LinkType.INPUT_CALC, link_label=key)
            except ValueError:
                pass # Link already exists

            aiida_data.store()


def _create_calcjob_node_from_task(task_instance: TaskInstance, parent_workchain_node: orm.WorkChainNode) -> orm.CalcJobNode:
    """Create an AiiDA CalcJobNode from an Airflow CalcJobTaskOperator instance."""
    node = orm.CalcJobNode()
    node.label = f"airflow_calcjob_{task_instance.task_id}"
    node.description = f"CalcJob from Airflow task {task_instance.task_id}"

    node.base.extras.set('airflow_dag_id', task_instance.dag_id)
    node.base.extras.set('airflow_run_id', task_instance.run_id)
    node.base.extras.set('airflow_task_id', task_instance.task_id)

    node.set_process_type('airflow.calcjob.CalcJobTaskOperator')
    node.set_process_state('finished')
    node.set_exit_status(0)
    
    # Get all inputs from the `prepare` task
    to_upload_files = task_instance.xcom_pull(task_ids='prepare', key='to_upload_files') or {}
    submission_script = task_instance.xcom_pull(task_ids='prepare', key='submission_script')
    
    # Store submission script as a Str node
    if submission_script:
        script_node = orm.Str(submission_script)
        node.base.links.add_incoming(script_node, link_type=LinkType.INPUT_CALC, link_label='submission_script')
        script_node.store()
    
    # Create input files as FolderData or SinglefileData
    # NOTE: This part needs more robust handling for file content
    if to_upload_files:
        # For simplicity, we create a single FolderData node
        folder_node = orm.FolderData()
        for filename, content_path in to_upload_files.items():
            # NOTE: this assumes content_path is a local path to the file
            if os.path.exists(content_path):
                folder_node.put_object_from_file(content_path, filename)
        node.base.links.add_incoming(folder_node, link_type=LinkType.INPUT_CALC, link_label='input_folder')
        folder_node.store()
    
    # Store outputs
    to_receive_files = task_instance.xcom_pull(task_ids='prepare', key='to_receive_files') or {}
    local_workdir = task_instance.dag_run.conf.get('local_workdir') if task_instance.dag_run.conf else None

    if to_receive_files and local_workdir:
        from pathlib import Path
        for remote_filename, local_filename in to_receive_files.items():
            local_file_path = Path(local_workdir) / local_filename
            if local_file_path.exists():
                output_file = orm.SinglefileData(file=str(local_file_path))
                output_file.label = f"output_{local_filename}"
                output_file.store()
                output_file.base.links.add_incoming(node, link_type=LinkType.CREATE, link_label=f"output_{local_filename.replace('.', '_')}")
    
    
    # Link to parent WorkChainNode
    if parent_workchain_node:
        node.base.links.add_incoming(parent_workchain_node, link_type=LinkType.CALL_CALC, link_label=task_instance.task_id)

    node.store()
    
    logger.info(f"Created CalcJobNode {node.pk} for task {task_instance.task_id}")
    return node


def _create_workchain_node_from_dag(dag_run: DagRun):
    """Create a WorkChainNode from a successful Airflow DAG run."""
    if not AIIDA_AVAILABLE:
        return
    
    # Create the WorkChainNode for the entire DAG
    workchain_node = orm.WorkChainNode()
    workchain_node.label = f"airflow_dag_{dag_run.dag_id}"
    workchain_node.description = f"Workflow from Airflow DAG {dag_run.dag_id}"

    workchain_node.base.extras.set('airflow_dag_id', dag_run.dag_id)
    workchain_node.base.extras.set('airflow_run_id', dag_run.run_id)

    # Store DAG parameters and config as inputs to the WorkChain
    dag_params = getattr(dag_run.dag, 'params', {})
    _store_airflow_params_as_aiida_inputs(workchain_node, dag_params)
    
    dag_conf = getattr(dag_run, 'conf', {})
    _store_airflow_params_as_aiida_inputs(workchain_node, dag_conf)
    
    workchain_node.set_process_state('finished')
    workchain_node.set_exit_status(0)
    workchain_node.store()
    
    # Process each task in the DAG
    task_instances = dag_run.get_task_instances()
    
    for ti in task_instances:
        # Check if the task is an instance of CalcJobTaskOperator
        # Note: We can check based on task_id or a specific attribute
        if 'calcjob' in ti.task_id.lower() and ti.state == 'success':
            _create_calcjob_node_from_task(ti, workchain_node)
        # You can add similar logic for other task types here
    
    logger.info(f"Created WorkChainNode {workchain_node.pk} for DAG {dag_run.dag_id}")


def _store_dagrun_event(dagrun: DagRun, event_type: str):
    """Store dagrun event information to SQLite database."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            run_type_str = dagrun.run_type.value if hasattr(dagrun.run_type, 'value') else str(dagrun.run_type) if dagrun.run_type else None
            state_str = dagrun.state.value if hasattr(dagrun.state, 'value') else str(dagrun.state) if dagrun.state else None
            execution_date = getattr(dagrun, 'execution_date', None)
            start_date = getattr(dagrun, 'start_date', None)
            end_date = getattr(dagrun, 'end_date', None)
            external_trigger = getattr(dagrun, 'external_trigger', False)
            conf = getattr(dagrun, 'conf', {})

            data_tuple = (
                dagrun.dag_id,
                dagrun.run_id,
                run_type_str,
                state_str,
                execution_date.isoformat() if execution_date else None,
                start_date.isoformat() if start_date else None,
                end_date.isoformat() if end_date else None,
                external_trigger,
                str(conf) if conf else '{}',
                event_type,
                datetime.now().isoformat()
            )

            cursor.execute('''
                INSERT INTO dagrun_events (
                    dag_id, run_id, run_type, state, execution_date,
                    start_date, end_date, external_trigger, conf,
                    event_type, event_timestamp, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_tuple + (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))

            conn.commit()
            logger.info(f"Stored {event_type} event for DAG run {dagrun.dag_id}/{dagrun.run_id}")
    except Exception as e:
        logger.error(f"Failed to store dagrun event: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

# Initialize database on import
_init_database()

class DagRunListener:
    """Class-based DAG run listener."""

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
        """Called when a DAG run enters the running state."""
        logger.info(f"DAG run started: {dag_run.dag_id}/{dag_run.run_id}")
        _store_dagrun_event(dag_run, 'running')

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        """Called when a DAG run completes successfully."""
        logger.info(f"DAG run succeeded: {dag_run.dag_id}/{dag_run.run_id}")
        _store_dagrun_event(dag_run, 'success')
        
        if _should_integrate_dag_with_aiida(dag_run):
            logger.info(f"Creating WorkChainNode for DAG {dag_run.dag_id}")
            _create_workchain_node_from_dag(dag_run)
        else:
            logger.info(f"Skipping AiiDA integration for DAG {dag_run.dag_id}")

# Create listener instance
dagrun_listener = DagRunListener()

class DagRunTrackingPlugin(AirflowPlugin):
    name = "dagrun_tracking_plugin"
    listeners = [dagrun_listener]
