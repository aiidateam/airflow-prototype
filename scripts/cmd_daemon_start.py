#!/usr/bin/env python
"""
Start Airflow services for testing using the new Airflow Daemon.

This script is designed to be run via hatch:
    hatch run hatch-test.py3.11:start-airflow-services
"""

import sys
import os
import argparse


def start_services(profile_name: str, num_sync_workers: int, num_async_workers: int, foreground: bool):
    """Start all Airflow services using the daemon."""
    from airflow_provider_aiida.aiida_core.engine.daemon.airflow_daemon import AirflowDaemon

    print("=== Starting Airflow Test Services ===\n")
    print(f"Sync workers (scheduler): {num_sync_workers}")
    print(f"Async workers (triggerer): {num_async_workers}\n")

    try:
        # Load AiiDA profile
        from airflow_provider_aiida.aiida_core import load_profile
        aiida_profile = load_profile(profile_name)
        AirflowDaemon(aiida_profile).start(
            num_workers=num_sync_workers,
            num_triggerers=num_async_workers,
            foreground=foreground
        )

        return 0

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


def main():
    """Start Airflow services."""
    parser = argparse.ArgumentParser(
        description="Start Airflow test services using daemon architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start with default (1 sync worker, 1 async worker)
  python cmd_daemon_start.py

  # Start with 2 sync workers and 3 async workers
  python cmd_daemon_start.py 2 3

  # Start with 4 sync and 4 async workers, with specific profile
  python cmd_daemon_start.py 4 4 --profile-name myprofile

  # Start in foreground mode with defaults
  python cmd_daemon_start.py --foreground
        """
    )

    parser.add_argument(
        'num_sync_workers',
        type=int,
        nargs='?',
        default=1,
        help='Number of sync workers (scheduler parallelism, default: 1)'
    )

    parser.add_argument(
        'num_async_workers',
        type=int,
        nargs='?',
        default=1,
        help='Number of async workers (triggerer instances, default: 1)'
    )

    parser.add_argument(
        '--foreground', '-f',
        action='store_true',
        help='Run daemon in foreground (default: background)'
    )

    parser.add_argument(
        '--profile-name',
        default=os.getenv('AIIDA_PROFILE'),
        help='AiiDA profile name (default: from AIIDA_PROFILE env)'
    )

    args = parser.parse_args()

    if args.num_sync_workers < 1:
        parser.error("Number of sync workers must be at least 1")
    if args.num_async_workers < 1:
        parser.error("Number of async workers must be at least 1")

    return start_services(
        args.profile_name,
        args.num_sync_workers,
        args.num_async_workers,
        args.foreground
    )


if __name__ == "__main__":
    sys.exit(main())
