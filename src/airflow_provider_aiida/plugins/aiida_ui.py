from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import sqlite3
import os
import html
from typing import List, Dict

# Import our custom XCom backend utilities
try:
    from airflow_provider_aiida.xcom_backends.aiida import get_xcom_stats, get_recent_xcom_operations, get_xcom_values_from_airflow
except ImportError:
    try:
        from airflow_provider_aiida.xcom_backends.aiida import get_xcom_stats, get_recent_xcom_operations, get_xcom_values_from_airflow
    except ImportError:
        def get_xcom_stats():
            return {"error": "Custom XCom backend not available"}
        def get_recent_xcom_operations():
            return []
        def get_xcom_values_from_airflow():
            return []

def plugin_macro():
    return "ok"

def get_dagrun_events() -> List[Dict]:
    """Read DAG run events from the SQLite database."""
    db_path = os.path.join(os.path.dirname(__file__), 'dagrun_tracking.db')

    if not os.path.exists(db_path):
        return []

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, dag_id, run_id, run_type, state,
                       execution_date, start_date, end_date,
                       external_trigger, event_type, event_timestamp, created_at, conf
                FROM dagrun_events
                ORDER BY created_at DESC
                LIMIT 100
            ''')

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"Error reading database: {e}")
        return []

def get_dagrun_stats() -> Dict:
    """Get summary statistics from the DAG run events."""
    db_path = os.path.join(os.path.dirname(__file__), 'dagrun_tracking.db')

    if not os.path.exists(db_path):
        return {"total_events": 0, "running": 0, "success": 0, "failed": 0}

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Get total events
            cursor.execute("SELECT COUNT(*) FROM dagrun_events")
            total_events = cursor.fetchone()[0]

            # Get events by type
            cursor.execute('''
                SELECT event_type, COUNT(*)
                FROM dagrun_events
                GROUP BY event_type
            ''')
            event_counts = dict(cursor.fetchall())

            return {
                "total_events": total_events,
                "running": event_counts.get("running", 0),
                "success": event_counts.get("success", 0),
                "failed": event_counts.get("failed", 0)
            }
    except Exception as e:
        print(f"Error reading database stats: {e}")
        return {"total_events": 0, "running": 0, "success": 0, "failed": 0}

app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """DAG Run Events Dashboard"""
    stats = get_dagrun_stats()

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>DAG Run Events Dashboard</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
            .header {{ color: #1f77b4; border-bottom: 2px solid #1f77b4; padding-bottom: 10px; }}
            .content {{ margin-top: 20px; }}
            .card {{ background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-top: 20px; }}
            .stat-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }}
            .stat-number {{ font-size: 2em; font-weight: bold; color: #1f77b4; }}
            .stat-label {{ color: #666; margin-top: 5px; }}
            .nav-link {{ display: inline-block; margin: 10px 20px 10px 0; padding: 10px 15px; background: #1f77b4; color: white; text-decoration: none; border-radius: 5px; }}
            .nav-link:hover {{ background: #0f5a8a; color: white; text-decoration: none; }}
            .success {{ color: #28a745; }}
            .failed {{ color: #dc3545; }}
            .running {{ color: #ffc107; }}
        </style>
    </head>
    <body>
        <h1 class="header">DAG Run Events Dashboard</h1>
        <div class="content">
            <div class="card">
                <h3>DAG Run Event Tracking</h3>
                <p>This dashboard shows statistics and events from the DAG run listener.</p>
                <a href="/plugins/aiida/events" class="nav-link">View Event Table</a>
                <a href="/plugins/aiida/xcom-stats" class="nav-link">XCom Statistics</a>
                <a href="/" class="nav-link">← Back to Airflow</a>
            </div>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-number">{stats['total_events']}</div>
                    <div class="stat-label">Total Events</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number running">{stats['running']}</div>
                    <div class="stat-label">Running</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number success">{stats['success']}</div>
                    <div class="stat-label">Success</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number failed">{stats['failed']}</div>
                    <div class="stat-label">Failed</div>
                </div>
            </div>
            <div class="card">
                <h3>Database Information</h3>
                <p>DAG run events are stored in a separate SQLite database at: <code>plugins/dagrun_tracking.db</code></p>
                <p>Events are automatically captured when DAG runs start, succeed, or fail.</p>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/events", response_class=HTMLResponse)
async def events_table():
    """Display DAG run events in a table format"""
    events = get_dagrun_events()

    # Build table rows
    table_rows = ""
    if events:
        for event in events:
            state_class = ""
            if event['event_type'] == 'success':
                state_class = "success"
            elif event['event_type'] == 'failed':
                state_class = "failed"
            elif event['event_type'] == 'running':
                state_class = "running"

            # Format conf for display
            conf_value = event.get('conf') or ''
            conf_display = conf_value if conf_value and conf_value != '{}' else 'None'
            if conf_display != 'None' and len(conf_display) > 50:
                conf_display = conf_display[:47] + '...'

            # HTML escape the conf display to prevent issues with special characters
            conf_display_escaped = html.escape(conf_display) if conf_display else 'None'

            table_rows += f"""
            <tr>
                <td>{html.escape(event['dag_id'])}</td>
                <td>{html.escape(event['run_id'])}</td>
                <td><span class="{state_class}">{html.escape(event['event_type'])}</span></td>
                <td>{html.escape(event['state']) if event['state'] else 'N/A'}</td>
                <td>{html.escape(event['execution_date'][:19]) if event['execution_date'] else 'N/A'}</td>
                <td>{html.escape(event['event_timestamp'][:19]) if event['event_timestamp'] else 'N/A'}</td>
                <td>{'Yes' if event['external_trigger'] else 'No'}</td>
                <td><code style="background: #f1f1f1; padding: 2px 4px; border-radius: 3px; font-size: 0.9em;">{conf_display_escaped}</code></td>
            </tr>
            """
    else:
        table_rows = '<tr><td colspan="8" style="text-align: center; font-style: italic;">No events found. Trigger some DAG runs to see data here.</td></tr>'

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>DAG Run Events Table</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
            .header {{ color: #1f77b4; border-bottom: 2px solid #1f77b4; padding-bottom: 10px; }}
            .content {{ margin-top: 20px; }}
            .card {{ background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f8f9fa; font-weight: bold; }}
            tr:hover {{ background-color: #f5f5f5; }}
            .nav-link {{ display: inline-block; margin: 10px 20px 10px 0; padding: 10px 15px; background: #1f77b4; color: white; text-decoration: none; border-radius: 5px; }}
            .nav-link:hover {{ background: #0f5a8a; color: white; text-decoration: none; }}
            .success {{ color: #28a745; font-weight: bold; }}
            .failed {{ color: #dc3545; font-weight: bold; }}
            .running {{ color: #ffc107; font-weight: bold; }}
            .table-container {{ overflow-x: auto; }}
        </style>
    </head>
    <body>
        <h1 class="header">DAG Run Events</h1>
        <div class="content">
            <div class="card">
                <h3>Event History</h3>
                <p>Complete history of DAG run events captured by the listener (last 100 events).</p>
                <a href="/plugins/aiida/dashboard" class="nav-link">← Back to Dashboard</a>
                <a href="/" class="nav-link">← Back to Airflow</a>

                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>DAG ID</th>
                                <th>Run ID</th>
                                <th>Event Type</th>
                                <th>State</th>
                                <th>Execution Date</th>
                                <th>Event Time</th>
                                <th>External Trigger</th>
                                <th>Configuration</th>
                            </tr>
                        </thead>
                        <tbody>
                            {table_rows}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/xcom-stats", response_class=HTMLResponse)
async def xcom_stats():
    """Display XCom backend statistics"""
    stats = get_xcom_stats()

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>XCom Backend Statistics</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
            .header {{ color: #1f77b4; border-bottom: 2px solid #1f77b4; padding-bottom: 10px; }}
            .content {{ margin-top: 20px; }}
            .card {{ background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-top: 20px; }}
            .stat-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }}
            .stat-number {{ font-size: 2em; font-weight: bold; color: #1f77b4; }}
            .stat-label {{ color: #666; margin-top: 5px; }}
            .nav-link {{ display: inline-block; margin: 10px 20px 10px 0; padding: 10px 15px; background: #1f77b4; color: white; text-decoration: none; border-radius: 5px; }}
            .nav-link:hover {{ background: #0f5a8a; color: white; text-decoration: none; }}
            .error {{ color: #dc3545; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f8f9fa; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h1 class="header">XCom Backend Statistics</h1>
        <div class="content">
            <div class="card">
                <h3>Custom XCom Backend Monitoring</h3>
                <p>Statistics from the custom logging XCom backend showing operation counts and usage patterns.</p>
                <a href="/plugins/aiida/xcom-values" class="nav-link">View XCom Values</a>
                <a href="/plugins/aiida/dashboard" class="nav-link">← Back to Dashboard</a>
                <a href="/" class="nav-link">← Back to Airflow</a>
            </div>"""

    if "error" in stats:
        html_content += f"""
            <div class="card">
                <h3 class="error">Error</h3>
                <p class="error">{stats['error']}</p>
            </div>"""
    else:
        # Operation counts
        operation_counts = stats.get('operation_counts', {})
        html_content += f"""
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-number">{stats.get('total_operations', 0)}</div>
                    <div class="stat-label">Total Operations</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{operation_counts.get('serialize', 0)}</div>
                    <div class="stat-label">Serialize Operations</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{operation_counts.get('deserialize', 0)}</div>
                    <div class="stat-label">Deserialize Operations</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{operation_counts.get('purge', 0)}</div>
                    <div class="stat-label">Purge Operations</div>
                </div>
            </div>

            <div class="card">
                <h3>Top DAGs by XCom Operations</h3>
                <table>
                    <thead>
                        <tr>
                            <th>DAG ID</th>
                            <th>Operation Count</th>
                        </tr>
                    </thead>
                    <tbody>"""

        top_dags = stats.get('top_dags_by_operations', {})
        if top_dags:
            for dag_id, count in top_dags.items():
                html_content += f"""
                        <tr>
                            <td>{html.escape(dag_id)}</td>
                            <td>{count}</td>
                        </tr>"""
        else:
            html_content += """
                        <tr>
                            <td colspan="2" style="text-align: center; font-style: italic;">No operations recorded yet</td>
                        </tr>"""

        html_content += """
                    </tbody>
                </table>
            </div>"""

    html_content += """
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/xcom-values", response_class=HTMLResponse)
async def xcom_values():
    """Display actual XCom values being passed between tasks"""
    xcom_values = get_xcom_values_from_airflow()
    recent_operations = get_recent_xcom_operations(limit=20)

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>XCom Values & Parameters</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
            .header {{ color: #1f77b4; border-bottom: 2px solid #1f77b4; padding-bottom: 10px; }}
            .content {{ margin-top: 20px; }}
            .card {{ background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .nav-link {{ display: inline-block; margin: 10px 20px 10px 0; padding: 10px 15px; background: #1f77b4; color: white; text-decoration: none; border-radius: 5px; }}
            .nav-link:hover {{ background: #0f5a8a; color: white; text-decoration: none; }}
            .error {{ color: #dc3545; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; font-size: 0.9em; }}
            th {{ background-color: #f8f9fa; font-weight: bold; }}
            tr:hover {{ background-color: #f5f5f5; }}
            .value-cell {{ max-width: 300px; word-wrap: break-word; font-family: monospace; background: #f8f9fa; }}
            .operation-cell {{ font-weight: bold; }}
            .serialize {{ color: #007bff; }}
            .deserialize {{ color: #28a745; }}
            .purge {{ color: #dc3545; }}
            .type-badge {{
                background: #e9ecef;
                color: #495057;
                padding: 2px 6px;
                border-radius: 3px;
                font-size: 0.8em;
                font-weight: bold;
                font-family: monospace;
            }}
            .tab-container {{ margin: 20px 0; }}
            .tab-button {{ display: inline-block; padding: 10px 20px; margin: 0 5px; background: #e9ecef; border: none; cursor: pointer; border-radius: 5px 5px 0 0; }}
            .tab-button.active {{ background: #007bff; color: white; }}
            .tab-content {{ display: none; }}
            .tab-content.active {{ display: block; }}
        </style>
        <script>
            function showTab(tabName) {{
                // Hide all tab contents
                const contents = document.querySelectorAll('.tab-content');
                contents.forEach(content => content.classList.remove('active'));

                // Remove active class from all buttons
                const buttons = document.querySelectorAll('.tab-button');
                buttons.forEach(button => button.classList.remove('active'));

                // Show selected tab and activate button
                document.getElementById(tabName).classList.add('active');
                document.querySelector(`[onclick="showTab('${{tabName}}')"]`).classList.add('active');
            }}
        </script>
    </head>
    <body>
        <h1 class="header">XCom Values & Parameters</h1>
        <div class="content">
            <div class="card">
                <h3>XCom Data Inspection</h3>
                <p>View actual parameters and values being passed between tasks via XCom.</p>
                <a href="/plugins/aiida/xcom-stats" class="nav-link">← Back to XCom Stats</a>
                <a href="/plugins/aiida/dashboard" class="nav-link">← Back to Dashboard</a>
                <a href="/" class="nav-link">← Back to Airflow</a>
            </div>

            <div class="tab-container">
                <button class="tab-button active" onclick="showTab('xcom-values')">Current XCom Values</button>
                <button class="tab-button" onclick="showTab('operations-log')">Operations Log</button>
            </div>

            <div id="xcom-values" class="tab-content active">
                <div class="card">
                    <h3>Recent XCom Values (Last 20)</h3>
                    <p>Actual values stored in Airflow's XCom system:</p>

                    <div style="overflow-x: auto;">
                        <table>
                            <thead>
                                <tr>
                                    <th>DAG ID</th>
                                    <th>Task ID</th>
                                    <th>Run ID</th>
                                    <th>Key</th>
                                    <th>Map Index</th>
                                    <th>Type</th>
                                    <th>Value</th>
                                    <th>Timestamp</th>
                                </tr>
                            </thead>
                            <tbody>"""

    if xcom_values and not any('error' in str(val) for val in xcom_values):
        for xcom in xcom_values:
            html_content += f"""
                                <tr>
                                    <td>{html.escape(str(xcom.get('dag_id', 'N/A')))}</td>
                                    <td>{html.escape(str(xcom.get('task_id', 'N/A')))}</td>
                                    <td>{html.escape(str(xcom.get('run_id', 'N/A'))[:20])}...</td>
                                    <td>{html.escape(str(xcom.get('key', 'N/A')))}</td>
                                    <td>{html.escape(str(xcom.get('map_index', 'N/A')))}</td>
                                    <td><span class="type-badge">{html.escape(str(xcom.get('value_type', 'Unknown')))}</span></td>
                                    <td class="value-cell">{html.escape(str(xcom.get('value', 'N/A')))}</td>
                                    <td>{html.escape(str(xcom.get('timestamp', 'N/A'))[:19]) if xcom.get('timestamp') else 'N/A'}</td>
                                </tr>"""
    else:
        error_msg = "No XCom values found or error accessing database"
        if xcom_values and 'error' in str(xcom_values[0]):
            error_msg = str(xcom_values[0].get('error', error_msg))

        html_content += f"""
                                <tr>
                                    <td colspan="8" style="text-align: center; font-style: italic; color: #dc3545;">{html.escape(error_msg)}</td>
                                </tr>"""

    html_content += """
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <div id="operations-log" class="tab-content">
                <div class="card">
                    <h3>XCom Operations Log (Last 20)</h3>
                    <p>Backend operations logged by our custom XCom backend:</p>

                    <div style="overflow-x: auto;">
                        <table>
                            <thead>
                                <tr>
                                    <th>Operation</th>
                                    <th>DAG ID</th>
                                    <th>Task ID</th>
                                    <th>Key</th>
                                    <th>Size (bytes)</th>
                                    <th>Timestamp</th>
                                </tr>
                            </thead>
                            <tbody>"""

    if recent_operations:
        for op in recent_operations:
            op_class = op.get('operation_type', '')
            html_content += f"""
                                <tr>
                                    <td class="operation-cell {op_class}">{html.escape(str(op.get('operation_type', 'N/A')))}</td>
                                    <td>{html.escape(str(op.get('dag_id', 'N/A')))}</td>
                                    <td>{html.escape(str(op.get('task_id', 'N/A')))}</td>
                                    <td>{html.escape(str(op.get('key', 'N/A')))}</td>
                                    <td>{html.escape(str(op.get('value_size', 'N/A')))}</td>
                                    <td>{html.escape(str(op.get('timestamp', 'N/A'))[:19]) if op.get('timestamp') else 'N/A'}</td>
                                </tr>"""
    else:
        html_content += """
                                <tr>
                                    <td colspan="6" style="text-align: center; font-style: italic;">No operations logged yet. Try running some DAGs with XCom.</td>
                                </tr>"""

    html_content += """
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

app_with_metadata = {
    "app": app,
    "url_prefix": "/plugins/aiida",   # avoid collisions with /api/v1
    "name": "AiiDAPlugin",
}

class AiidaUI(AirflowPlugin):
    name = "aiida_ui"
    macros = [plugin_macro]
    fastapi_apps = [app_with_metadata]

    appbuilder_menu_items = [
        {
            "name": "DAG Run Events",
            "href": "/plugins/aiida/dashboard"
        }
    ]
