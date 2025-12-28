"""Utilities for viewing Airflow logs from AiiDA process nodes."""
from __future__ import annotations

import os
from pathlib import Path


def get_airflow_log_path_from_process(process_pk: int) -> Path:
    """
    Get the Airflow log directory for an AiiDA process task.

    Args:
        process_pk: The primary key of the AiiDA process node

    Returns:
        Path to the task log directory containing attempt logs

    Note:
        This returns the task directory containing logs like:
        - attempt=1.log (worker)
        - attempt=1.log.trigger1.log (triggerer)
        - attempt=2.log (worker retry)
    """
    from airflow.configuration import conf
    from aiida.orm import load_node
    from airflow_provider_aiida.aiida_core import load_profile
    from airflow_provider_aiida.utils.airflow_control import dag_id_from_process, get_dag_run_id

    # Load profile
    load_profile()

    # Load the process node
    node = load_node(process_pk)

    # Get DAG ID from process class
    process_class = node.process_class
    if not process_class:
        raise ValueError(f"Process node {process_pk} does not have a process class")

    dag_id = dag_id_from_process(process_class)

    # Get DAG run ID from node extras
    dag_run_id = get_dag_run_id(node)
    if not dag_run_id:
        raise ValueError(
            f"Process node {process_pk} does not have a DAG run ID. "
            "This node may not be managed by Airflow."
        )

    # Get base log folder from Airflow config
    base_log_folder = os.path.expanduser(
        conf.get_mandatory_value("logging", "BASE_LOG_FOLDER")
    )

    # Build the log path including task_id
    # TODO: Make task_id configurable or derive it from the process
    # For now, hardcoding the common pattern: {dag_id}.step_until_terminate
    task_id = f"{dag_id}.step_until_terminate"

    # Airflow log structure: {base_log_folder}/dag_id={dag_id}/run_id={dag_run_id}/task_id={task_id}/
    log_dir = Path(base_log_folder) / f"dag_id={dag_id}" / f"run_id={dag_run_id}" / f"task_id={task_id}"

    return log_dir


def list_log_files(log_dir: Path) -> list[Path]:
    """
    List all log files in the log directory.

    Args:
        log_dir: Path to the task log directory

    Returns:
        List of paths to log files (attempt=*.log and attempt=*.log.trigger*.log)
    """
    if not log_dir.exists():
        return []

    # Find all attempt logs (both worker and triggerer)
    # Pattern: attempt=*.log and attempt=*.log.trigger*.log
    all_logs = list(log_dir.glob("attempt=*"))

    # Sort by attempt number, then by type (worker before triggerer)
    def sort_key(path: Path) -> tuple:
        name = path.name
        # Extract attempt number
        try:
            attempt_num = int(name.split('attempt=')[1].split('.')[0])
        except (IndexError, ValueError):
            attempt_num = 0

        # Worker logs (attempt=N.log) come before triggerer logs (attempt=N.log.trigger*.log)
        is_trigger = '.trigger' in name
        return (attempt_num, is_trigger, name)

    return sorted(all_logs, key=sort_key)


def get_log_source(log_file: Path) -> str:
    """
    Determine the source of a log file (scheduler, triggerer, etc.).

    Args:
        log_file: Path to the log file

    Returns:
        Source identifier (e.g., 'SCHEDULER', 'TRIGGERER')
    """
    # Check filename for trigger pattern
    # Worker logs: attempt=N.log
    # Triggerer logs: attempt=N.log.triggerX.log
    filename = log_file.name

    if '.trigger' in filename:
        # Extract trigger number if present
        # e.g., attempt=1.log.trigger1.log -> TRIGGERER1
        try:
            trigger_part = filename.split('.trigger')[1].split('.')[0]
            return f'TRIGGERER{trigger_part}'
        except (IndexError, ValueError):
            return 'TRIGGERER'
    else:
        # Worker log
        return 'SCHEDULER'


def format_json_log_line(json_obj: dict, source: str = '') -> str:
    """
    Format a JSON log entry into a readable line.

    Args:
        json_obj: Parsed JSON log object
        source: Source identifier (e.g., 'SCHEDULER', 'TRIGGERER')

    Returns:
        Formatted log line
    """
    # Common Airflow log fields
    timestamp = json_obj.get('asctime') or json_obj.get('timestamp') or json_obj.get('time', '')
    level = json_obj.get('levelname') or json_obj.get('level', '')
    message = json_obj.get('message') or json_obj.get('msg', '')
    logger_name = json_obj.get('name') or json_obj.get('logger', '')

    # Format the line
    parts = []
    if timestamp:
        parts.append(f"[{timestamp}]")

    # Add source column
    if source:
        parts.append(f"[{source}]")

    if level:
        # Add indicators for different levels
        level_indicators = {
            'DEBUG': '🔍',
            'INFO': 'ℹ️ ',
            'WARNING': '⚠️ ',
            'ERROR': '❌',
            'CRITICAL': '🔥',
        }
        indicator = level_indicators.get(level, '')
        parts.append(f"{indicator}{level}")
    if logger_name:
        parts.append(f"({logger_name})")

    formatted = ' '.join(parts)
    if formatted:
        formatted += ': '
    formatted += message

    # Add any extra fields that might be interesting
    extra_fields = []
    for key, value in json_obj.items():
        if key not in ['asctime', 'timestamp', 'time', 'levelname', 'level',
                       'message', 'msg', 'name', 'logger', 'pathname', 'filename',
                       'module', 'lineno', 'funcName', 'process', 'processName',
                       'thread', 'threadName', 'taskName', 'exc_info', 'exc_text',
                       'stack_info', 'created', 'msecs', 'relativeCreated']:
            extra_fields.append(f"{key}={value}")

    if extra_fields:
        formatted += ' | ' + ', '.join(extra_fields)

    return formatted


def parse_log_entries(log_file: Path, source: str) -> list[tuple]:
    """
    Parse log entries from a file and extract timestamp for sorting.

    Args:
        log_file: Path to the log file
        source: Source identifier (e.g., 'SCHEDULER', 'TRIGGERER')

    Returns:
        List of tuples: (timestamp_obj, formatted_line, raw_line)
    """
    import json
    from datetime import datetime

    entries = []

    try:
        with open(log_file, 'r') as f:
            content = f.read()
    except Exception as e:
        return [(None, f"Error reading {log_file.name}: {e}", "")]

    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue

        timestamp_obj = None
        formatted_line = ""

        # Try to parse as JSON
        try:
            json_obj = json.loads(line)
            formatted_line = format_json_log_line(json_obj, source)

            # Extract timestamp for sorting
            timestamp_str = (json_obj.get('asctime') or
                           json_obj.get('timestamp') or
                           json_obj.get('time', ''))
            if timestamp_str:
                # Try to parse the timestamp
                for fmt in ['%Y-%m-%d %H:%M:%S,%f', '%Y-%m-%d %H:%M:%S',
                           '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']:
                    try:
                        timestamp_obj = datetime.strptime(timestamp_str.split('.')[0].split(',')[0], fmt)
                        break
                    except ValueError:
                        continue

        except (json.JSONDecodeError, ValueError):
            # Not JSON, keep as-is
            formatted_line = f"[{source}] {line}"

        entries.append((timestamp_obj, formatted_line, line))

    return entries


def merge_and_sort_logs(log_files: list[Path]) -> list[str]:
    """
    Merge logs from multiple files and sort by timestamp.

    Args:
        log_files: List of log file paths

    Returns:
        List of formatted log lines, sorted by timestamp
    """
    all_entries = []

    for log_file in log_files:
        source = get_log_source(log_file)
        entries = parse_log_entries(log_file, source)
        all_entries.extend(entries)

    # Sort by timestamp (None timestamps go to the end)
    all_entries.sort(key=lambda x: (x[0] is None, x[0] if x[0] is not None else ''))

    # Return just the formatted lines
    return [entry[1] for entry in all_entries]


def display_logs_with_pager(log_files: list[Path], process_pk: int, merged: bool = True):
    """
    Display log files in a pager (like less), formatting JSON logs.

    Args:
        log_files: List of log file paths
        process_pk: The process PK (for display)
        merged: If True, merge and sort logs by timestamp; if False, show separately
    """
    import click

    if not log_files:
        click.echo("No log files to display.")
        return

    # Build the content to display
    content_lines = []
    content_lines.append(f"{'=' * 80}")
    content_lines.append(f"Airflow Logs for AiiDA Process {process_pk}")
    if merged:
        content_lines.append(f"Merged and sorted by timestamp")
    content_lines.append(f"{'=' * 80}")
    content_lines.append("")

    if merged:
        # Merge all logs and sort by timestamp
        content_lines.append("Log sources:")
        for log_file in log_files:
            source = get_log_source(log_file)
            content_lines.append(f"  [{source}] {log_file}")
        content_lines.append("")
        content_lines.append(f"{'─' * 80}\n")

        merged_lines = merge_and_sort_logs(log_files)
        content_lines.extend(merged_lines)
    else:
        # Display each log file separately
        for i, log_file in enumerate(log_files, 1):
            content_lines.append(f"\n{'─' * 80}")
            content_lines.append(f"Log {i}/{len(log_files)}: {log_file.name}")
            content_lines.append(f"Path: {log_file}")
            content_lines.append(f"{'─' * 80}\n")

            source = get_log_source(log_file)
            entries = parse_log_entries(log_file, source)
            for _, formatted_line, _ in entries:
                content_lines.append(formatted_line)

            content_lines.append("")

    # Use click's pager for nice scrolling
    click.echo_via_pager('\n'.join(content_lines))
