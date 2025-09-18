# Custom XCom Backend Plugin
import json
import logging
import sqlite3
import os
from typing import Any, TYPE_CHECKING
from datetime import datetime

from airflow.models.xcom import BaseXCom
from airflow.utils.json import XComEncoder, XComDecoder

if TYPE_CHECKING:
    from airflow.models import XCom

logger = logging.getLogger(__name__)


class LoggingXComBackend(BaseXCom):
    """
    Custom XCom backend that logs all XCom operations to a separate SQLite database.

    This backend extends the default XCom functionality by:
    1. Logging all serialize/deserialize operations
    2. Tracking XCom usage statistics
    3. Storing operation metadata in a separate database
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ensure_logging_db()

    @staticmethod
    def _ensure_logging_db():
        """Ensure the logging database exists and has the required table with proper schema."""
        db_path = os.path.join(os.path.dirname(__file__), 'xcom_operations.db')

        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # Create the table if it doesn't exist
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS xcom_operations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        operation_type TEXT NOT NULL,
                        dag_id TEXT,
                        task_id TEXT,
                        run_id TEXT,
                        key TEXT,
                        map_index INTEGER,
                        value_size INTEGER,
                        timestamp TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Check if new columns exist and add them if missing
                cursor.execute("PRAGMA table_info(xcom_operations)")
                columns = [row[1] for row in cursor.fetchall()]

                if 'value_preview' not in columns:
                    cursor.execute('ALTER TABLE xcom_operations ADD COLUMN value_preview TEXT')
                    logger.info("Added value_preview column to xcom_operations table")

                if 'actual_value' not in columns:
                    cursor.execute('ALTER TABLE xcom_operations ADD COLUMN actual_value TEXT')
                    logger.info("Added actual_value column to xcom_operations table")

                if 'value_type' not in columns:
                    cursor.execute('ALTER TABLE xcom_operations ADD COLUMN value_type TEXT')
                    logger.info("Added value_type column to xcom_operations table")

                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to initialize XCom logging database: {e}")

    @staticmethod
    def _log_operation(operation_type: str, **kwargs):
        """Log an XCom operation to the database."""
        # Skip logging if we're in a deferred task context to avoid interference
        try:
            import threading
            current_thread = threading.current_thread()
            if hasattr(current_thread, 'name') and 'defer' in current_thread.name.lower():
                return
        except:
            pass

        # Ensure database exists first
        LoggingXComBackend._ensure_logging_db()

        db_path = os.path.join(os.path.dirname(__file__), 'xcom_operations.db')

        try:
            with sqlite3.connect(db_path, timeout=1.0) as conn:  # Add timeout to prevent blocking
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO xcom_operations
                    (operation_type, dag_id, task_id, run_id, key, map_index, value_size, value_preview, actual_value, value_type, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    operation_type,
                    kwargs.get('dag_id'),
                    kwargs.get('task_id'),
                    kwargs.get('run_id'),
                    kwargs.get('key'),
                    kwargs.get('map_index'),
                    kwargs.get('value_size', 0),
                    kwargs.get('value_preview'),
                    kwargs.get('actual_value'),
                    kwargs.get('value_type'),
                    datetime.now().isoformat()
                ))
                conn.commit()
        except Exception as e:
            # Silently fail to avoid disrupting normal operations
            pass

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> str:
        """Serialize XCom value to JSON string with logging."""
        try:
            # Serialize the value using the default JSON encoder
            serialized_value = json.dumps(value, cls=XComEncoder, ensure_ascii=False)

            # Skip logging for deferred tasks or if logging is disabled
            try:
                # Create preview of the value for display (truncated if too long)
                value_str = str(value)
                value_preview = value_str if len(value_str) <= 100 else value_str[:97] + "..."

                # Store the actual value (truncated if extremely large to avoid database issues)
                actual_value = value_str if len(value_str) <= 1000 else value_str[:997] + "..."

                # Get the type of the value
                value_type = type(value).__name__

                # Log the operation with actual values (non-blocking)
                LoggingXComBackend._log_operation(
                    operation_type='serialize',
                    dag_id=dag_id,
                    task_id=task_id,
                    run_id=run_id,
                    key=key,
                    map_index=map_index,
                    value_size=len(serialized_value),
                    value_preview=value_preview,
                    actual_value=actual_value,
                    value_type=value_type
                )
            except Exception:
                # If logging fails, continue with serialization
                pass

            return serialized_value

        except Exception as e:
            logger.error(f"Failed to serialize XCom value: {e}")
            # Fall back to default serialization
            return BaseXCom.serialize_value(
                value,
                key=key,
                task_id=task_id,
                dag_id=dag_id,
                run_id=run_id,
                map_index=map_index
            )

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        """Deserialize XCom value from JSON string with logging."""
        try:
            # Get the value from the XCom result
            if result.value is None:
                return None

            # Deserialize using the default JSON decoder
            deserialized_value = json.loads(result.value, cls=XComDecoder)

            # Log the operation - handle both XCom and XComResult objects
            dag_id = getattr(result, 'dag_id', None)
            task_id = getattr(result, 'task_id', None)
            run_id = getattr(result, 'run_id', None)
            key = getattr(result, 'key', None)
            map_index = getattr(result, 'map_index', None)

            # Create preview of the deserialized value
            value_str = str(deserialized_value)
            value_preview = value_str if len(value_str) <= 100 else value_str[:97] + "..."
            actual_value = value_str if len(value_str) <= 1000 else value_str[:997] + "..."
            value_type = type(deserialized_value).__name__

            LoggingXComBackend._log_operation(
                operation_type='deserialize',
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                key=key,
                map_index=map_index,
                value_size=len(str(result.value)) if result.value else 0,
                value_preview=value_preview,
                actual_value=actual_value,
                value_type=value_type
            )

            logger.info(
                f"XCom deserialized: dag_id={dag_id}, task_id={task_id}, "
                f"key={key}"
            )

            return deserialized_value

        except Exception as e:
            logger.error(f"Failed to deserialize XCom value: {e}")
            # Fall back to default deserialization
            return BaseXCom.deserialize_value(result)

    def orm_deserialize_value(self) -> Any:
        """
        Deserialize method for ORM XCom object reconstruction.

        This method is used when viewing XCom listings in the UI and should be
        lightweight to avoid unnecessary operations.
        """
        try:
            if self.value is None:
                return None
            return json.loads(self.value, cls=XComDecoder)
        except Exception:
            # Fall back to the parent implementation
            return super().orm_deserialize_value()

    @staticmethod
    def purge(xcom: "XCom", session) -> None:
        """
        Purge method called when XCom records are deleted.

        We can add custom cleanup logic here if needed.
        """
        try:
            # Log the operation
            LoggingXComBackend._log_operation(
                operation_type='purge',
                dag_id=xcom.dag_id,
                task_id=xcom.task_id,
                run_id=xcom.run_id,
                key=xcom.key,
                map_index=xcom.map_index
            )

            logger.info(
                f"XCom purged: dag_id={xcom.dag_id}, task_id={xcom.task_id}, "
                f"key={xcom.key}"
            )

        except Exception as e:
            logger.warning(f"Failed to log XCom purge operation: {e}")

        # Call the parent purge method to handle the actual deletion
        BaseXCom.purge(xcom, session)


def get_xcom_stats() -> dict:
    """
    Utility function to get XCom operation statistics.

    Returns a dictionary with operation counts and other metrics.
    """
    db_path = os.path.join(os.path.dirname(__file__), 'xcom_operations.db')

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Get operation counts
            cursor.execute('''
                SELECT operation_type, COUNT(*)
                FROM xcom_operations
                GROUP BY operation_type
            ''')
            operation_counts = dict(cursor.fetchall())

            # Get total operations
            cursor.execute('SELECT COUNT(*) FROM xcom_operations')
            total_operations = cursor.fetchone()[0]

            # Get operations by DAG
            cursor.execute('''
                SELECT dag_id, COUNT(*)
                FROM xcom_operations
                WHERE dag_id IS NOT NULL
                GROUP BY dag_id
                ORDER BY COUNT(*) DESC
                LIMIT 10
            ''')
            top_dags = dict(cursor.fetchall())

            return {
                'total_operations': total_operations,
                'operation_counts': operation_counts,
                'top_dags_by_operations': top_dags
            }

    except Exception as e:
        logger.error(f"Failed to get XCom stats: {e}")
        return {'error': str(e)}


def get_recent_xcom_operations(limit: int = 50) -> list:
    """
    Get recent XCom operations with detailed information.

    Returns a list of recent operations for display in the UI.
    """
    db_path = os.path.join(os.path.dirname(__file__), 'xcom_operations.db')

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, operation_type, dag_id, task_id, run_id, key,
                       map_index, value_size, value_preview, actual_value, value_type, timestamp, created_at
                FROM xcom_operations
                ORDER BY created_at DESC
                LIMIT ?
            ''', (limit,))

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            return [dict(zip(columns, row)) for row in rows]

    except Exception as e:
        logger.error(f"Failed to get recent XCom operations: {e}")
        return []


def get_xcom_values_from_airflow() -> list:
    """
    Get actual XCom values from our custom database.

    This function reads the XCom values that were captured during runtime
    by our custom XCom backend.
    """
    db_path = os.path.join(os.path.dirname(__file__), 'xcom_operations.db')

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT dag_id, task_id, run_id, key, map_index,
                       actual_value, value_type, timestamp, operation_type
                FROM xcom_operations
                WHERE actual_value IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 20
            ''')

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            xcom_data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                xcom_data.append({
                    'dag_id': row_dict.get('dag_id', 'Unknown'),
                    'task_id': row_dict.get('task_id', 'Unknown'),
                    'run_id': row_dict.get('run_id', 'Unknown'),
                    'key': row_dict.get('key', 'Unknown'),
                    'map_index': row_dict.get('map_index', -1),
                    'value': row_dict.get('actual_value', 'No value'),
                    'value_type': row_dict.get('value_type', 'Unknown'),
                    'timestamp': row_dict.get('timestamp', 'Unknown'),
                    'operation_type': row_dict.get('operation_type', 'Unknown')
                })

            return xcom_data

    except Exception as e:
        logger.error(f"Failed to get XCom values from custom database: {e}")
        return [{'error': f"Could not read from custom database: {e}"}]
