#!/usr/bin/env python
"""
Teardown AiiDA test profile and clean up databases.

This script is designed to be run via hatch:
    hatch run hatch-test.py3.11:teardown-profile
"""

import sys
import os
import argparse
from airflow_provider_aiida.aiida_core.manage.configuration.config import delete_aiida_profile


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Teardown AiiDA and Airflow test profiles and clean up databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script will:
  1. Verify the profile has Airflow support
  2. Drop both aiida-<profile> and airflow-<profile> databases
  3. Drop the PostgreSQL user
  4. Remove the AiiDA profile configuration

Database connection info (host, port) is extracted from the profile's storage config.

WARNING: This will permanently delete all data in the profile!
        """
    )

    parser.add_argument(
        '--profile-name',
        default=os.getenv('AIIDA_PROFILE', 'test'),
        help='AiiDA profile name (default: from AIIDA_PROFILE env or test)'
    )

    parser.add_argument(
        '--postgres-user',
        default=os.getenv('POSTGRES_USER', 'postgres'),
        help='PostgreSQL admin user with privileges to drop databases and users (default: from POSTGRES_USER env or postgres)'
    )

    parser.add_argument(
        '--postgres-password',
        default=os.getenv('POSTGRES_PASSWORD', 'postgres'),
        help='PostgreSQL admin user password (default: from POSTGRES_PASSWORD env or postgres)'
    )

    return parser.parse_args()


def main():
    """Main function."""
    try:
        args = parse_arguments()
        delete_aiida_profile(
            profile_name=args.profile_name,
            pg_admin_user=args.postgres_user,
            pg_admin_password=args.postgres_password
        )


    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
