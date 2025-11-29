"""Commands for managing AiiDA profiles with Airflow integration."""
import os
import click
from aiida.cmdline.commands.cmd_presto import get_default_presto_profile_name, DEFAULT_PROFILE_NAME_PREFIX
from aiida.manage.configuration import get_config_option

@click.command('presto')
@click.option(
    '-p',
    '--profile-name',
    default=lambda: get_default_presto_profile_name(),
    show_default=True,
    help=f'Name of the profile. By default, a unique name starting with `{DEFAULT_PROFILE_NAME_PREFIX}` is '
    'automatically generated.',
)
# NOTE: for now no --use-postgres flag because it is always postgres
@click.option('--postgres-hostname', type=str, default='localhost', help='The hostname of the PostgreSQL server.')
@click.option('--postgres-port', type=int, default=5432, help='The port of the PostgreSQL server.')
@click.option(
    '--postgres-username',
    type=click.STRING,
    default=lambda: os.getenv('POSTGRES_USER', 'postgres'),
    help='The username of the PostgreSQL user that is authorized to create new databases.',
)
@click.option(
    '--postgres-password',
    type=str,
    required=False,
    help='The password of the PostgreSQL user that is authorized to create new databases.',
)
@click.option(
    '--postgres-password',
    type=click.STRING,
    default=lambda: os.getenv('POSTGRES_PASSWORD', 'postgres'),
    help='PostgreSQL admin user password (default: from POSTGRES_PASSWORD env or postgres)'
)
def airdi_presto(profile_name, postgres_hostname, postgres_port, postgres_username, postgres_password):
    """
    Set up a new AiiDA profile with Airflow integration.

    This command will:
    - Create PostgreSQL users and databases for AiiDA and Airflow
    - Create and configure AiiDA profile
    - Initialize Airflow database
    - Set up directory structure for Airflow
    """
    from airflow_provider_aiida.aiida_core.manage.configuration.config import create_aiida_profile

    try:
        create_aiida_profile(
            pg_host=postgres_hostname,
            pg_port=postgres_port,
            pg_admin_user=postgres_username,
            pg_admin_password=postgres_password,
            profile_name=profile_name,
            overwrite=True
        )

    except Exception as e:
        click.secho(f"\n✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()
