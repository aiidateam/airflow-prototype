import sqlite3
import os
import logging
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.models import DagRun
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

def _create_calcjob_outputs(node, task_instance):
    """Create output files for the CalcJob node."""
    # Get the result file from the local workdir
    to_receive_files = task_instance.xcom_pull(task_ids='prepare', key='to_receive_files') or {}
    local_workdir = task_instance.dag_run.conf.get('local_workdir') if task_instance.dag_run.conf else None
    
    if to_receive_files and local_workdir:
        from pathlib import Path
        
        for remote_filename, local_filename in to_receive_files.items():
            local_file_path = Path(local_workdir) / local_filename
            
            if local_file_path.exists():
                # Create SinglefileData for the output
                output_file = orm.SinglefileData(file=str(local_file_path))
                output_file.label = f"output_{remote_filename}"
                output_file.store()
                
                # Link as output
                output_file.base.links.add_incoming(node, link_type=LinkType.CREATE, link_label=f"output_{remote_filename.replace('.', '_')}")
                
                # Also try to parse the result as an integer if it's the result file
                if 'result' in local_filename.lower():
                    try:
                        result_value = int(local_file_path.read_text().strip())
                        result_int = orm.Int(result_value)
                        result_int.label = "sum"
                        result_int.store()
                        result_int.base.links.add_incoming(node, link_type=LinkType.CREATE, link_label='sum')
                    except:
                        pass  # Couldn't parse as integer

def _create_calcjob_nodes_for_dag(dag_run: DagRun):
    """Create CalcJobNodes for calcjob tasks after DAG completion."""
    if not AIIDA_AVAILABLE:
        return
    
    task_instances = dag_run.get_task_instances()
    
    for ti in task_instances:
        if 'calcjob' in ti.task_id.lower() and ti.state == 'success':
            node = orm.CalcJobNode()
            node.label = f"airflow_calcjob_{ti.task_id}"
            node.description = f"CalcJob from Airflow task {ti.task_id}"
            
            node.base.extras.set('airflow_dag_id', ti.dag_id)
            node.base.extras.set('airflow_run_id', ti.run_id)
            node.base.extras.set('airflow_task_id', ti.task_id)

            node.set_process_type('airflow.calcjob.CalcJobTaskOperator')
            
            # Store DAG run config (machine, workdir, etc)
            if hasattr(ti.dag_run, 'conf') and ti.dag_run.conf:
                for key, value in ti.dag_run.conf.items():
                    if isinstance(value, (str, int, float, bool)):
                        if isinstance(value, str):
                            aiida_data = orm.Str(value)
                        elif isinstance(value, int):
                            aiida_data = orm.Int(value)
                        elif isinstance(value, float):
                            aiida_data = orm.Float(value)
                        elif isinstance(value, bool):
                            aiida_data = orm.Bool(value)
                        
                        aiida_data.store()
                        node.base.links.add_incoming(aiida_data, link_type=LinkType.INPUT_CALC, link_label=key)
            
            # ADD: Store DAG params (x, y, sleep, etc)
            dag_params = getattr(dag_run.dag, 'params', {})
            # import ipdb; ipdb.set_trace()
            for key, param in dag_params.items():
                # Get the actual resolved value, not the Param object
                # TODO: Check Param implementation, also has `value` attribute and `dump` method
                if hasattr(param, 'resolve'):
                    value = param.resolve()
                else:
                    value = param
                
                if isinstance(value, (str, int, float, bool)):
                    if isinstance(value, str):
                        aiida_data = orm.Str(value)
                    elif isinstance(value, int):
                        aiida_data = orm.Int(value)
                    elif isinstance(value, float):
                        aiida_data = orm.Float(value)
                    elif isinstance(value, bool):
                        aiida_data = orm.Bool(value)
                    
                    aiida_data.store()
                    try:
                        node.base.links.add_incoming(aiida_data, link_type=LinkType.INPUT_CALC, link_label=key)
                    except ValueError:
                        pass
            
            # Ensure proper CalcJob setup
            # import ipdb; ipdb.set_trace()
            node.set_process_state('finished')
            node.set_exit_status(0)
            node.store()

            _create_calcjob_outputs(node, ti)
            
            logger.info(f"Created CalcJobNode {node.pk} for task {ti.task_id}")


def _store_dagrun_event(dagrun: DagRun, event_type: str):
    """Store dagrun event information to SQLite database."""
    try:
        logger.info(f"[DEBUG] Starting to store {event_type} event")
        logger.info(f"[DEBUG] DB_PATH: {DB_PATH}")
        logger.info(f"[DEBUG] dagrun.dag_id: {dagrun.dag_id}")
        logger.info(f"[DEBUG] dagrun.run_id: {dagrun.run_id}")

        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Test if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dagrun_events'")
            table_exists = cursor.fetchone()
            logger.info(f"[DEBUG] Table exists: {table_exists is not None}")

            # Safely extract values - some might be strings, some might be enums
            run_type_str = dagrun.run_type.value if hasattr(dagrun.run_type, 'value') else str(dagrun.run_type) if dagrun.run_type else None
            state_str = dagrun.state.value if hasattr(dagrun.state, 'value') else str(dagrun.state) if dagrun.state else None

            # Safely extract date attributes
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

            logger.info(f"[DEBUG] Data tuple: {data_tuple}")

            cursor.execute('''
                INSERT INTO dagrun_events (
                    dag_id, run_id, run_type, state, execution_date,
                    start_date, end_date, external_trigger, conf,
                    event_type, event_timestamp, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_tuple + (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))

            conn.commit()
            logger.info(f"[SUCCESS] Stored {event_type} event for DAG run {dagrun.dag_id}/{dagrun.run_id}")

            # Verify insertion
            cursor.execute("SELECT COUNT(*) FROM dagrun_events")
            count = cursor.fetchone()[0]
            logger.info(f"[DEBUG] Total events in DB: {count}")

    except Exception as e:
        logger.error(f"[ERROR] Failed to store dagrun event: {e}")
        import traceback
        logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")

# Initialize database on import
_init_database()

class DagRunListener:
    """Class-based DAG run listener."""

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
        """Called when a DAG run enters the running state."""
        logger.info(f"[CLASS LISTENER] DAG run started: {dag_run.dag_id}/{dag_run.run_id}")
        _store_dagrun_event(dag_run, 'running')

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        """Called when a DAG run completes successfully."""
        logger.info(f"[CLASS LISTENER] DAG run succeeded: {dag_run.dag_id}/{dag_run.run_id}")
        _store_dagrun_event(dag_run, 'success')
        
        # ADD DEBUG LOGGING:
        logger.info(f"[DEBUG] Checking AiiDA integration for DAG {dag_run.dag_id}")
        logger.info(f"[DEBUG] DAG tags: {getattr(dag_run.dag, 'tags', [])}")
        
        if _should_integrate_dag_with_aiida(dag_run):
            logger.info(f"[DEBUG] Creating CalcJob nodes for DAG {dag_run.dag_id}")
            _create_calcjob_nodes_for_dag(dag_run)
        else:
            logger.info(f"[DEBUG] Skipping AiiDA integration for DAG {dag_run.dag_id}")

    # @hookimpl
    # def on_dag_run_success(self, dag_run: DagRun, msg: str):
    #     """Called when a DAG run completes successfully."""
    #     logger.info(f"[CLASS LISTENER] DAG run succeeded: {dag_run.dag_id}/{dag_run.run_id}")
    #     _store_dagrun_event(dag_run, 'success')
    #
    #     if _should_integrate_dag_with_aiida(dag_run):
    #         _create_calcjob_nodes_for_dag(dag_run)

    #@hookimpl
    #def on_dag_run_failed(self, dag_run: DagRun, msg: str):
    #    """Called when a DAG run fails."""
    #    logger.info(f"[CLASS LISTENER] DAG run failed: {dag_run.dag_id}/{dag_run.run_id}")
    #    _store_dagrun_event(dag_run, 'failed')

# Create listener instance
dagrun_listener = DagRunListener()

class DagRunTrackingPlugin(AirflowPlugin):
    name = "dagrun_tracking_plugin"
    listeners = [dagrun_listener]
