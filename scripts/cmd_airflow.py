#!/usr/bin/env python
"""
Run Airflow CLI commands using an AiiDA profile's configuration.

This script loads an AiiDA profile (default or specified), extracts the Airflow environment
configuration, and executes the provided Airflow CLI command with those settings.

Example usage:
    python scripts/cmd_airflow.py db migrate
    python scripts/cmd_airflow.py dags list
    python scripts/cmd_airflow.py scheduler
    python scripts/cmd_airflow.py --profile-name test scheduler
"""

import sys
import os
import subprocess
from airflow_provider_aiida.aiida_core import load_profile


def main():
    """Main function."""
    # Extract --profile-name argument if present, keeping other args for Airflow
    profile_name = None
    airflow_args = []

    i = 0
    args = sys.argv[1:]
    while i < len(args):
        if args[i] == '--profile-name':
            if i + 1 < len(args):
                profile_name = args[i + 1]
                i += 2  # Skip both --profile-name and its value
            else:
                print("Error: --profile-name requires a value")
                return 1
        else:
            airflow_args.append(args[i])
            i += 1

    if not airflow_args:
        print("Usage: {} [--profile-name PROFILE] [airflow_command] [airflow_args...]".format(sys.argv[0]))
        print("\nOptions:")
        print("  --profile-name PROFILE    Use specified AiiDA profile (default: default profile)")
        print("\nExamples:")
        print("  {} db migrate                      # Initialize/migrate Airflow database".format(sys.argv[0]))
        print("  {} dags list                       # List all DAGs".format(sys.argv[0]))
        print("  {} dags reserialize -B aiida_dags  # Reserialize DAGs".format(sys.argv[0]))
        print("  {} scheduler                       # Start the scheduler".format(sys.argv[0]))
        print("  {} triggerer                       # Start the triggerer".format(sys.argv[0]))
        print("  {} --profile-name test scheduler   # Start scheduler with 'test' profile".format(sys.argv[0]))
        return 1

    try:
        # Load the AiiDA profile
        from airflow_provider_aiida.aiida_core import load_profile
        load_profile(profile_name)


        # Build the airflow command
        airflow_command = ['airflow'] + airflow_args

        print(f"Running: {' '.join(airflow_command)}")
        print("=" * 60)
        print()

        # Execute the airflow command with the configured environment
        result = subprocess.run(
            airflow_command,
            env=os.environ
        )

        # Return the same exit code as the airflow command
        return result.returncode

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
