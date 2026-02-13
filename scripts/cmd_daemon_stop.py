#!/usr/bin/env python
"""
Stop Airflow daemon and all its services.

This script is designed to be run via hatch:
    hatch run hatch-test.py3.11:stop-airflow-services
"""

import sys
import os
import signal
import time
import argparse
from pathlib import Path


def stop_services(profile_name: str):
    """Stop the Airflow daemon and all its services."""
    from airflow_provider_aiida.aiida_core.engine.daemon.airflow_daemon import AirflowDaemon

    try:
        # Load AiiDA profile
        from airflow_provider_aiida.aiida_core import load_profile
        aiida_profile = load_profile(profile_name)
        AirflowDaemon(aiida_profile).stop()

        return 0

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


def main():
    """Parse arguments and stop services."""
    parser = argparse.ArgumentParser(
        description="Stop Airflow test services",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '--profile-name',
        default=os.getenv('AIIDA_PROFILE'),
        help='AiiDA profile name (default: from AIIDA_PROFILE env)'
    )

    args = parser.parse_args()
    return stop_services(args.profile_name)


if __name__ == "__main__":
    sys.exit(main())
