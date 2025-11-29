#!/usr/bin/env python
"""
Check status of Airflow test services.

This script is designed to be run via hatch:
    hatch run hatch-test.py3.11:status-services
"""

import sys
import os
import argparse
import time


def format_status(status_dict: dict) -> str:
    """Format status dictionary as a readable table."""
    lines = []

    # Header
    lines.append("=" * 100)
    lines.append(f"Daemon Status - Session: {status_dict['session']}")
    lines.append("=" * 100)

    # Supervisor info
    lines.append("\nSupervisor Process:")
    supervisor = status_dict.get('supervisor')
    if supervisor:
        if 'error' in supervisor:
            lines.append(f"  Error: {supervisor['error']}")
        else:
            lines.append(f"  PID: {supervisor['pid']}")
            lines.append(f"  Status: {supervisor['status']}")
            lines.append(f"  Started: {time.ctime(supervisor['started'])}")
            lines.append(f"  Log: {supervisor['log']}")
    else:
        lines.append("  No supervisor info available")

    # Services table
    lines.append("\n" + "-" * 100)
    lines.append("Services:")
    lines.append("-" * 100)

    # Check if there are any errors at the top level
    if 'error' in status_dict:
        lines.append(f"\nError: {status_dict['error']}")
        return "\n".join(lines)

    services = status_dict.get('services', {})
    if not services:
        lines.append("\nNo services configured")
        return "\n".join(lines)

    # Table header
    lines.append(f"\n{'Service':<30} {'Type':<10} {'Workers':<8} {'PID':<10} {'Status':<10} {'Failures':<10}")
    lines.append("-" * 100)

    # Table rows
    for service_name, service_info in services.items():
        svc_type = service_info['type']

        if svc_type == 'service':
            # Non-worker service (single instance)
            pid = str(service_info['pid']) if service_info['pid'] is not None else "-"
            status = service_info['status'] if service_info['status'] is not None else "ERROR"
            failures = str(service_info['failures']) if service_info['failures'] is not None else "-"

            if service_info.get('error'):
                status = f"ERROR: {service_info['error']}"

            num_workers = service_info.get('num_workers', '-')
            lines.append(f"{service_name:<30} {svc_type:<10} {num_workers:<8} {pid:<10} {status:<10} {failures:<10}")

        elif svc_type == 'worker':
            # Worker service (multiple instances)
            for worker_num, worker_info in service_info['workers'].items():
                worker_str = f"#{worker_num}"
                pid = str(worker_info['pid']) if worker_info['pid'] is not None else "-"
                status = worker_info['status'] if worker_info['status'] is not None else "ERROR"
                failures = str(worker_info['failures']) if worker_info['failures'] is not None else "-"

                if worker_info.get('error'):
                    status = f"ERROR: {worker_info['error']}"

                lines.append(f"{service_name:<30} {svc_type:<10} {worker_str:<8} {pid:<10} {status:<10} {failures:<10}")

    lines.append("\n" + "=" * 100)

    return "\n".join(lines)


def check_status(profile_name: str):
    """Check and display status of Airflow daemon and services."""
    from airflow_provider_aiida.aiida_core.engine.daemon.airflow_daemon import AirflowDaemon

    try:
        # Load AiiDA profile
        from airflow_provider_aiida.aiida_core import load_profile
        aiida_profile = load_profile(profile_name)

        # Get status dictionary
        status_dict = AirflowDaemon(aiida_profile).status()

        # Format and print
        print(format_status(status_dict))

        return 0

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


def main():
    """Parse arguments and check status."""
    parser = argparse.ArgumentParser(
        description="Check status of Airflow test services",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '--profile-name',
        default=os.getenv('AIIDA_PROFILE'),
        help='AiiDA profile name (default: from AIIDA_PROFILE env)'
    )

    args = parser.parse_args()
    return check_status(args.profile_name)


if __name__ == "__main__":
    sys.exit(main())
