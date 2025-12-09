"""Status command for airdi - shows AiiDA and Airflow daemon status."""
import enum
import sys
import click

from aiida.common.exceptions import CorruptStorage, IncompatibleStorageSchema, UnreachableStorage
from aiida.common.log import override_log_level


class ServiceStatus(enum.IntEnum):
    """Describe status of services for 'airdi status' command."""

    UP = 0
    ERROR = 1
    WARNING = 2
    DOWN = 3


STATUS_SYMBOLS = {
    ServiceStatus.UP: {
        'color': 'green',
        'string': '\u2714',
    },
    ServiceStatus.ERROR: {
        'color': 'red',
        'string': '\u2718',
    },
    ServiceStatus.WARNING: {
        'color': 'yellow',
        'string': '\u23fa',
    },
    ServiceStatus.DOWN: {
        'color': 'red',
        'string': '\u2718',
    },
}


class ExitCode(enum.IntEnum):
    """Exit codes for airdi status command."""

    SUCCESS = 0
    CRITICAL = 1


@click.command('status')
@click.option('--print-traceback', is_flag=True, help='Print full traceback on exceptions')
def airdi_status(print_traceback):
    """Print status of AiiDA and Airflow services."""
    from airflow_provider_aiida.aiida_core import load_profile
    load_profile() 

    from aiida import __version__
    from aiida.manage.configuration.settings import AiiDAConfigDir
    from aiida.manage.manager import get_manager

    exit_code = ExitCode.SUCCESS
    configure_directory = AiiDAConfigDir.get()

    print_status(ServiceStatus.UP, 'version', f'AiiDA v{__version__}')
    print_status(ServiceStatus.UP, 'config', configure_directory)

    manager = get_manager()

    try:
        profile = manager.get_profile()

        if profile is None:
            print_status(ServiceStatus.WARNING, 'profile', 'no profile configured yet')
            click.echo(
                'Run `airdi presto` to setup a profile with Airflow integration.'
            )
            return

        print_status(ServiceStatus.UP, 'profile', profile.name)

    except Exception as exc:
        message = 'Unable to read AiiDA profile'
        print_status(ServiceStatus.ERROR, 'profile', message, exception=exc, print_traceback=print_traceback)
        sys.exit(ExitCode.CRITICAL)  # stop here - without a profile we cannot access anything

    # Check the backend storage
    storage_head_version = None
    try:
        with override_log_level():  # temporarily suppress noisy logging
            storage_cls = profile.storage_cls
            storage_head_version = storage_cls.version_head()
            storage_backend = storage_cls(profile)
    except UnreachableStorage as exc:
        message = "Unable to connect to profile's storage."
        print_status(ServiceStatus.DOWN, 'storage', message, exception=exc, print_traceback=print_traceback)
        exit_code = ExitCode.CRITICAL
    except IncompatibleStorageSchema:
        message = (
            f'Storage schema version is incompatible with the code version {storage_head_version!r}. '
            'Run `verdi storage migrate` to solve this.'
        )
        print_status(ServiceStatus.DOWN, 'storage', message)
        exit_code = ExitCode.CRITICAL
    except CorruptStorage as exc:
        message = 'Storage is corrupted.'
        print_status(ServiceStatus.DOWN, 'storage', message, exception=exc, print_traceback=print_traceback)
        exit_code = ExitCode.CRITICAL
    except Exception as exc:
        message = "Unable to instantiate profile's storage."
        print_status(ServiceStatus.ERROR, 'storage', message, exception=exc, print_traceback=print_traceback)
        exit_code = ExitCode.CRITICAL
    else:
        message = str(storage_backend)
        print_status(ServiceStatus.UP, 'storage', message)

    # Broker is not needed with Airflow - removed broker check

    # Getting the Airflow daemon status
    try:
        from airflow_provider_aiida.aiida_core.engine.daemon.airflow_daemon import AirflowDaemon

        daemon = AirflowDaemon(profile)
        status_dict = daemon.status()

        # Check supervisor status
        supervisor = status_dict.get('supervisor')
        if not supervisor or 'error' in supervisor:
            error_msg = supervisor.get('error', 'Unknown error') if supervisor else 'No supervisor info'
            print_status(ServiceStatus.WARNING, 'services', f'Services are not running ({error_msg})')
        else:
            supervisor_status = supervisor['status']

            if supervisor_status != 'RUNNING':
                print_status(ServiceStatus.ERROR, 'services', f'Services stopped')
                exit_code = ExitCode.CRITICAL
            else:
                # Count services
                services = status_dict.get('services', {})
                if not services:
                    print_status(ServiceStatus.WARNING, 'services', f'Services are running but no services configured')
                else:
                    # Count running services/workers
                    total_instances = 0
                    running_instances = 0

                    for service_name, service_info in services.items():
                        if service_info['type'] == 'service':
                            total_instances += 1
                            if service_info.get('status') == 'ALIVE':
                                running_instances += 1
                        elif service_info['type'] == 'worker':
                            workers = service_info.get('workers', {})
                            total_instances += len(workers)
                            running_instances += sum(1 for w in workers.values() if w.get('status') == 'ALIVE')

                    if running_instances == total_instances:
                        print_status(ServiceStatus.UP, 'services', f'Services running ({running_instances}/{total_instances} services active)')
                    elif running_instances > 0:
                        print_status(ServiceStatus.WARNING, 'services', f'Services running {running_instances}/{total_instances} services active)')
                    else:
                        print_status(ServiceStatus.ERROR, 'services', f'Services running but no services are active')
                        exit_code = ExitCode.CRITICAL

    except Exception as exception:
        message = 'Error getting services status'
        print_status(ServiceStatus.ERROR, 'services', message, exception=exception, print_traceback=print_traceback)
        exit_code = ExitCode.CRITICAL

    # Note: click does not forward return values to the exit code
    if exit_code != ExitCode.SUCCESS:
        sys.exit(exit_code)


def print_status(status, service, msg='', exception=None, print_traceback=False):
    """Print status message.

    Includes colored indicator.

    :param status: a ServiceStatus code
    :param service: string for service name
    :param msg:  message string
    """
    symbol = STATUS_SYMBOLS[status]
    click.secho(f" {symbol['string']} ", fg=symbol['color'], nl=False)
    click.echo(f"{service + ':':12s} {msg}")

    if exception is not None:
        click.secho(f'{type(exception).__name__}: {exception}', fg='red', err=True)

    if print_traceback:
        import traceback
        traceback.print_exc()
