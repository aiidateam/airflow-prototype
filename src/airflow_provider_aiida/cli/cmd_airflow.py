"""Airflow CLI wrapper command for airdi."""
import sys
import os
import subprocess
import click


@click.command('airflow', context_settings=dict(
    ignore_unknown_options=True,
    allow_interspersed_args=False,
))
@click.argument('airflow_args', nargs=-1, type=click.UNPROCESSED)
def airdi_airflow(airflow_args):
    """
    Run Airflow CLI commands using the default AiiDA profile's configuration.

    This command loads the default AiiDA profile, extracts the Airflow environment
    configuration, and executes the provided Airflow CLI command with those settings.

    Examples:
        airdi airflow db migrate
        airdi airflow dags list
        airdi airflow dags reserialize -B aiida_dags
        airdi airflow scheduler
        airdi airflow triggerer
    """
    if not airflow_args:
        click.echo("Usage: airdi airflow [airflow_command] [airflow_args...]")
        click.echo("\nExamples:")
        click.echo("  airdi airflow db migrate                      # Initialize/migrate Airflow database")
        click.echo("  airdi airflow dags list                       # List all DAGs")
        click.echo("  airdi airflow dags reserialize -B aiida_dags  # Reserialize DAGs")
        click.echo("  airdi airflow scheduler                       # Start the scheduler")
        click.echo("  airdi airflow triggerer                       # Start the triggerer")
        sys.exit(1)

    try:
        # Load the default AiiDA profile
        from airflow_provider_aiida.aiida_core import load_profile

        # sets AIRFLOW_HOME in environment 
        load_profile()

        airflow_command = ['airflow'] + list(airflow_args)

        # Execute the airflow command with the configured environment
        result = subprocess.run(
            airflow_command,
            env=os.environ,
            # Don't capture output - let it stream to stdout/stderr
        )

        # Return the same exit code as the airflow command
        sys.exit(result.returncode)

    except Exception as e:
        click.secho(f"\n✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
