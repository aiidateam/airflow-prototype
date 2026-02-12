#!/usr/bin/env python
"""
Unpause all DAGs from a specific bundle.

This script uses Airflow's Python API to directly update the database,
avoiding issues with DAG import errors that break JSON output.

Usage:
    python scripts/cmd_unpause_dags.py --bundle aiida_dags
"""

import sys
import os
import argparse


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Unpause all DAGs from a specific bundle",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '--bundle',
        required=True,
        help='DAG bundle name (e.g., aiida_dags)'
    )

    return parser.parse_args()


def setup_airflow_environment():
    """Load AiiDA profile and set up Airflow environment variables."""
    from aiida import load_profile
    from airflow_provider_aiida.utils.profile import from_profile_create_airflow_env

    # Load the default AiiDA profile
    profile = load_profile()

    # Get Airflow environment variables from the profile
    airflow_env = from_profile_create_airflow_env(profile)

    # Update current environment with Airflow settings
    os.environ.update(airflow_env)

    return profile


def unpause_dags_by_bundle(bundle_name: str) -> int:
    """
    Unpause all DAGs belonging to the specified bundle.

    Args:
        bundle_name: The bundle name to filter by

    Returns:
        Number of DAGs unpaused
    """
    # Import Airflow modules after environment is set up
    from airflow.settings import Session
    from airflow.models.dag import DagModel

    session = Session()

    try:
        # Query all DAGs with the specified bundle that are currently paused
        paused_dags = session.query(DagModel).filter(
            DagModel.bundle_name == bundle_name,
            DagModel.is_paused == True
        ).all()

        if not paused_dags:
            print(f"No paused DAGs found for bundle '{bundle_name}'")
            return 0

        # Unpause each DAG
        count = 0
        for dag in paused_dags:
            dag.is_paused = False
            count += 1
            print(f"Unpaused DAG: {dag.dag_id}")

        session.commit()
        print(f"\nSuccessfully unpaused {count} DAG(s) from bundle '{bundle_name}'")
        return count

    except Exception as e:
        session.rollback()
        print(f"Error unpausing DAGs: {e}", file=sys.stderr)
        raise
    finally:
        session.close()


def main():
    """Main function."""
    try:
        args = parse_arguments()

        # Set up Airflow environment from AiiDA profile
        print("Loading AiiDA profile and configuring Airflow environment...")
        setup_airflow_environment()
        print("✓ Environment configured\n")

        # Unpause DAGs
        unpause_dags_by_bundle(args.bundle)
        return 0
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
