"""Command line interface for AiiDA-Airflow integration."""
import click


@click.group(context_settings={'help_option_names': ['-h', '--help']})
@click.version_option(None, '-v', '--version', message='AiiDA-Airflow %(version)s')
def airdi():
    """
    airdi - Command line interface for AiiDA-Airflow integration.

    This CLI provides tools to manage AiiDA profiles with Airflow integration,
    control Airflow daemon services, and interact with DAGs.
    """
    pass


# Register command groups
from airflow_provider_aiida.cli.cmd_presto import airdi_presto
from airflow_provider_aiida.cli.cmd_services import airdi_services
from airflow_provider_aiida.cli.cmd_status import airdi_status
from airflow_provider_aiida.cli.cmd_airflow import airdi_airflow

airdi.add_command(airdi_presto)
airdi.add_command(airdi_services)
airdi.add_command(airdi_status)
airdi.add_command(airdi_airflow)
