#!/usr/bin/env python
"""
Setup AiiDA profile using the same PostgreSQL.
"""

import sys
import os
import argparse
from airflow_provider_aiida.aiida_core.manage.configuration.config import create_aiida_profile 


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Setup AiiDA and Airflow profiles using PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '--postgres-host',
        default=os.getenv('POSTGRES_HOST', '127.0.0.1'),
        help='PostgreSQL server hostname (default: from POSTGRES_HOST env or 127.0.0.1)'
    )

    parser.add_argument(
        '--postgres-port',
        type=int,
        default=int(os.getenv('POSTGRES_HOST_PORT', '5432')),
        help='PostgreSQL server port (default: from POSTGRES_HOST_PORT env or 5434)'
    )

    parser.add_argument(
        '--postgres-user',
        default=os.getenv('POSTGRES_USER', 'postgres'),
        help='PostgreSQL admin user with CREATEDB and CREATEROLE privileges (default: from POSTGRES_USER env or postgres)'
    )

    parser.add_argument(
        '--postgres-password',
        default=os.getenv('POSTGRES_PASSWORD', 'postgres'),
        help='PostgreSQL admin user password (default: from POSTGRES_PASSWORD env or postgres)'
    )

    parser.add_argument(
        '--aiida-password',
        default=os.getenv('AIIDA_PASSWORD', 'password'),
        help='PostgreSQL aiida user password (default: just password)'
    )

    parser.add_argument(
        '--profile-name',
        default=os.getenv('AIIDA_PROFILE', 'presto'),
        help='AiiDA profile name (default: presto)'
    )

    return parser.parse_args()

def main():
    """Main function."""
    try:
        args = parse_arguments()

        profile_parameters = {
            'pg_host': args.postgres_host,
            'pg_port': args.postgres_port,
            'pg_admin_user': args.postgres_user,
            'pg_admin_password': args.postgres_password,
            'profile_name': args.profile_name,
            'overwrite': True
        }

        profile = create_aiida_profile(**profile_parameters)

        if profile is None:
            return 1

        return 0

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
