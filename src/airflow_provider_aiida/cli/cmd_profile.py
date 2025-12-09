"""Commands for managing AiiDA profiles with Airflow integration."""
import os
import click


@click.group('profile')
def profile_cmd():
    """
    Manage AiiDA profiles with Airflow integration.

    Commands for setting up, configuring, and managing AiiDA profiles
    that include Airflow database and configuration.
    """
    pass


@profile_cmd.command('setup')
@click.option(
    '--profile-name',
    type=click.STRING,
    default=lambda: os.getenv('AIIDA_PROFILE', 'presto'),
    help='AiiDA profile name (default: from AIIDA_PROFILE env or presto)'
)
@click.option(
    '--postgres-host',
    type=click.STRING,
    default=lambda: os.getenv('POSTGRES_HOST', '127.0.0.1'),
    help='PostgreSQL server hostname (default: from POSTGRES_HOST env or 127.0.0.1)'
)
@click.option(
    '--postgres-port',
    type=click.INT,
    default=lambda: int(os.getenv('POSTGRES_HOST_PORT', '5432')),
    help='PostgreSQL server port (default: from POSTGRES_HOST_PORT env or 5432)'
)
@click.option(
    '--postgres-user',
    type=click.STRING,
    default=lambda: os.getenv('POSTGRES_USER', 'postgres'),
    help='PostgreSQL admin user with CREATEDB and CREATEROLE privileges (default: from POSTGRES_USER env or postgres)'
)
@click.option(
    '--postgres-password',
    type=click.STRING,
    default=lambda: os.getenv('POSTGRES_PASSWORD', 'postgres'),
    help='PostgreSQL admin user password (default: from POSTGRES_PASSWORD env or postgres)'
)
def profile_setup(profile_name, postgres_host, postgres_port, postgres_user, postgres_password):
    """
    Set up a new AiiDA profile with Airflow integration.

    This command will:
    - Create PostgreSQL users and databases for AiiDA and Airflow
    - Create and configure AiiDA profile
    - Initialize Airflow database
    - Set up directory structure for Airflow
    """
    from airflow_provider_aiida.utils.profile import create_aiida_profile

    try:
        profile = create_aiida_profile(
            pg_host=postgres_host,
            pg_port=postgres_port,
            pg_admin_user=postgres_user,
            pg_admin_password=postgres_password,
            profile_name=profile_name,
            overwrite=True
        )

        if profile is None:
            raise click.Abort()

    except Exception as e:
        click.secho(f"\n✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()


@profile_cmd.command('delete')
@click.option(
    '--profile-name',
    type=click.STRING,
    default=lambda: os.getenv('AIIDA_PROFILE', 'test'),
    help='AiiDA profile name (default: from AIIDA_PROFILE env or test)'
)
@click.option(
    '--postgres-user',
    type=click.STRING,
    default=lambda: os.getenv('POSTGRES_USER', 'postgres'),
    help='PostgreSQL admin user with privileges to drop databases and users (default: from POSTGRES_USER env or postgres)'
)
@click.option(
    '--postgres-password',
    type=click.STRING,
    default=lambda: os.getenv('POSTGRES_PASSWORD', 'postgres'),
    help='PostgreSQL admin user password (default: from POSTGRES_PASSWORD env or postgres)'
)
def profile_delete(profile_name, postgres_user, postgres_password):
    """
    Delete an AiiDA profile and its databases.

    This script will:
      1. Verify the profile has Airflow support
      2. Drop both aiida-<profile> and airflow-<profile> databases
      3. Drop the PostgreSQL user
      4. Remove the AiiDA profile configuration

    WARNING: This will permanently delete all data in the profile!
    """
    from airflow_provider_aiida.utils.profile import delete_aiida_profile

    try:
        delete_aiida_profile(
            profile_name=profile_name,
            pg_admin_user=postgres_user,
            pg_admin_password=postgres_password
        )

    except Exception as e:
        click.secho(f"\n✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()


@profile_cmd.command('list')
def profile_list():
    """List all available AiiDA profiles."""
    from aiida.manage.configuration import get_config

    try:
        config = get_config()
        profiles = config.profile_names

        if not profiles:
            click.echo("No profiles found.")
            return

        click.echo("Available profiles:")
        default_profile = config.default_profile_name
        for profile_name in profiles:
            if profile_name == default_profile:
                click.echo(f"  * {profile_name} (default)")
            else:
                click.echo(f"    {profile_name}")

    except Exception as e:
        click.secho(f"✗ Error listing profiles: {e}", fg='red', err=True)
        raise click.Abort()
