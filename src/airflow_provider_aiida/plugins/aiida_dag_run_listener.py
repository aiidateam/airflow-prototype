import sqlite3
import os
import logging
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.models import DagRun, XCom
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
                    dag_output TEXT,
                    event_type TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            logger.info(f"DagRun tracking database initialized at {DB_PATH}")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")

def _get_dag_output(dagrun: DagRun) -> str:
    """Retrieve DAG output from XCom if available."""
    try:
        # Look for XCom with key 'dag_output' from any task in this DAG run
        from airflow.models import XCom
        from airflow.utils.session import provide_session

        @provide_session
        def _query_xcom(session=None):
            xcom_value = session.query(XCom).filter(
                XCom.dag_id == dagrun.dag_id,
                XCom.run_id == dagrun.run_id,
                XCom.key == 'dag_output'
            ).first()
            return xcom_value.value if xcom_value else None

        output = _query_xcom()
        logger.info(f"[DEBUG] Retrieved DAG output: {output}")
        return str(output) if output else '{}'

    except Exception as e:
        logger.warning(f"Failed to retrieve DAG output: {e}")
        return '{}'

def _get_dag_output_safe(dag_id: str, run_id: str) -> str:
    """Safely retrieve DAG output from XCom using dag_id and run_id strings."""
    try:
        # Look for XCom with key 'dag_output' from any task in this DAG run
        from airflow.models import XCom
        from airflow.utils.session import provide_session

        @provide_session
        def _query_xcom(session=None):
            xcom_value = session.query(XCom).filter(
                XCom.dag_id == dag_id,
                XCom.run_id == run_id,
                XCom.key == 'dag_output'
            ).first()
            return xcom_value.value if xcom_value else None

        output = _query_xcom()
        logger.info(f"[DEBUG] Retrieved DAG output: {output}")
        return str(output) if output else '{}'

    except Exception as e:
        logger.warning(f"Failed to retrieve DAG output: {e}")
        return '{}'

def _store_dagrun_event(dagrun: DagRun, event_type: str):
    """Store dagrun event information to SQLite database."""
    try:
        # IMPORTANT: Extract all needed attributes FIRST to avoid DetachedInstanceError
        # This must happen before any database operations
        dag_id = dagrun.dag_id
        run_id = dagrun.run_id
        run_type_str = dagrun.run_type.value if hasattr(dagrun.run_type, 'value') else str(dagrun.run_type) if dagrun.run_type else None
        state_str = dagrun.state.value if hasattr(dagrun.state, 'value') else str(dagrun.state) if dagrun.state else None
        execution_date = getattr(dagrun, 'execution_date', None)
        start_date = getattr(dagrun, 'start_date', None)
        end_date = getattr(dagrun, 'end_date', None)
        external_trigger = getattr(dagrun, 'external_trigger', False)
        conf = getattr(dagrun, 'conf', {})

        logger.info(f"[DEBUG] Starting to store {event_type} event")
        logger.info(f"[DEBUG] DB_PATH: {DB_PATH}")
        logger.info(f"[DEBUG] dag_id: {dag_id}")
        logger.info(f"[DEBUG] run_id: {run_id}")

        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Test if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dagrun_events'")
            table_exists = cursor.fetchone()
            logger.info(f"[DEBUG] Table exists: {table_exists is not None}")

            # Get DAG output for completed DAGs (pass extracted values instead of dagrun object)
            dag_output = _get_dag_output_safe(dag_id, run_id) if event_type in ['success', 'failed'] else '{}'

            data_tuple = (
                dag_id,
                run_id,
                run_type_str,
                state_str,
                execution_date.isoformat() if execution_date else None,
                start_date.isoformat() if start_date else None,
                end_date.isoformat() if end_date else None,
                external_trigger,
                str(conf) if conf else '{}',
                dag_output,
                event_type,
                datetime.now().isoformat()
            )

            logger.info(f"[DEBUG] Data tuple: {data_tuple}")

            cursor.execute('''
                INSERT INTO dagrun_events (
                    dag_id, run_id, run_type, state, execution_date,
                    start_date, end_date, external_trigger, conf, dag_output,
                    event_type, event_timestamp, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_tuple + (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))

            conn.commit()
            logger.info(f"[SUCCESS] Stored {event_type} event for DAG run {dag_id}/{run_id}")

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

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
        """Called when a DAG run fails."""
        logger.info(f"[CLASS LISTENER] DAG run failed: {dag_run.dag_id}/{dag_run.run_id}")
        _store_dagrun_event(dag_run, 'failed')

# Create listener instance
dag_run_listener = DagRunListener()

class AiidaDagRunListener(AirflowPlugin):
    name = "aiida_dag_run_listener"
    listeners = [dag_run_listener]
