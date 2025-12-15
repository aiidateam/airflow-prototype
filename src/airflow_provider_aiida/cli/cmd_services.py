"""Commands for managing Airflow services."""
import click


@click.group('services')
def airdi_services():
    """
    Manage Airflow services.

    Commands for starting, stopping, and monitoring Airflow services
    (scheduler, triggerer, dag-processor, api-server).
    """
    pass


@airdi_services.command('start')
@click.argument(
    'num_workers',
    type=int,
    default=1,
    required=False
)
@click.argument(
    'num_triggerers',
    type=int,
    default=1,
    required=False
)
@click.option(
    '-f', '--foreground',
    is_flag=True,
    help='Run daemon in foreground (default: background)'
)
# TODO rename to services_start and so on
def daemon_start(num_workers, num_triggerers, foreground):
    """
    Start Airflow daemon services.

    Arguments:
        NUM_WORKERS: Number of sync workers (scheduler parallelism, default: 1)
        NUM_TRIGGERERS: Number of async workers (triggerer instances, default: 1)

    Examples:
        airdi services start
        airdi services start 2 3
        airdi services start --foreground
        airdi services start 4 4 --foreground
    """
    from airflow_provider_aiida.aiida_core import load_profile
    from airflow_provider_aiida.aiida_core.engine.daemon.airflow_daemon import AirflowDaemon

    try:
        # Load profile
        click.echo("Loading AiiDA profile...")
        aiida_profile = load_profile()
        click.echo(f"✓ Using profile: {aiida_profile.name}\n")

        click.echo("=== Starting Airflow Services ===\n")
        click.echo(f"Sync workers (scheduler): {num_workers}")
        click.echo(f"Async workers (triggerer): {num_triggerers}")

        mode = "foreground" if foreground else "background"
        click.echo(f"Mode: {mode}\n")

        # Create and start daemon
        daemon = AirflowDaemon(aiida_profile)
        daemon.start(
            num_workers=num_workers,
            num_triggerers=num_triggerers,
            foreground=foreground
        )

        if not foreground:
            click.secho("✓ Daemon started successfully", fg='green', bold=True)
            click.echo()
            click.echo("Use 'airdi services status' to check service status")
            click.echo("Use 'airdi services stop' to stop all services")

    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()


@airdi_services.command('stop')
def daemon_stop():
    """
    Stop Airflow daemon services.

    Gracefully stops all running Airflow services and the daemon process.

    Examples:
        airdi services stop
    """
    from airflow_provider_aiida.aiida_core import load_profile
    from airflow_provider_aiida.aiida_core.engine.daemon.airflow_daemon import AirflowDaemon

    try:
        # Load profile
        click.echo("Loading AiiDA profile...")
        aiida_profile = load_profile()
        click.echo(f"✓ Using profile: {aiida_profile.name}\n")

        click.echo("Stopping Airflow daemon...")

        # Stop daemon
        daemon = AirflowDaemon(aiida_profile)
        daemon.stop()

        click.secho("✓ Daemon stopped successfully", fg='green', bold=True)

    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()


@airdi_services.command('status')
@click.option('--debug', is_flag=True, help='Show detailed debug information (logs, start times, etc.)')
def daemon_status(debug):
    """
    Show status of Airflow daemon services.

    Displays the current state of the daemon and all managed services,
    including PID, status, and health information.

    Examples:
        airdi services status
        airdi services status --debug
    """
    from airflow_provider_aiida.aiida_core import load_profile
    from airflow_provider_aiida.aiida_core.engine.daemon.airflow_daemon import AirflowDaemon
    import time

    try:
        # Load profile
        aiida_profile = load_profile()

        # Get status
        daemon = AirflowDaemon(aiida_profile)
        status_dict = daemon.status()

        # Display header
        click.echo("=" * 100)
        click.echo(f"Daemon Status - Session: {status_dict['session']}")
        click.echo("=" * 100)

        # Supervisor info
        click.echo("\nSupervisor Process:")
        supervisor = status_dict.get('supervisor')
        if supervisor:
            if 'error' in supervisor:
                click.secho(f"  Error: {supervisor['error']}", fg='red')
            else:
                click.echo(f"  PID: {supervisor['pid']}")
                status_color = 'green' if supervisor['status'] == 'RUNNING' else 'red'
                click.secho(f"  Status: {supervisor['status']}", fg=status_color)
                if debug:
                    click.echo(f"  Started: {time.ctime(supervisor['started'])}")
                    click.echo(f"  Log: {supervisor['log']}")
        else:
            click.echo("  No supervisor info available")

        # Services table
        click.echo("\n" + "-" * 100)
        click.echo("Services:")
        click.echo("-" * 100)

        # Check for errors
        if 'error' in status_dict:
            click.secho(f"\nError: {status_dict['error']}", fg='red')
            return

        services = status_dict.get('services', {})
        if not services:
            click.echo("\nNo services configured")
            return

        # Table header
        if debug:
            click.echo(f"\n{'Service':<30} {'Type':<10} {'Workers':<8} {'PID':<10} {'Status':<10} {'Failures':<10}")
            click.echo("-" * 100)
        else:
            click.echo(f"\n{'Service':<30} {'Type':<10} {'Workers':<8} {'PID':<10} {'Status':<10} {'Failures':<10}")
            click.echo("-" * 100)

        # Table rows
        for service_name, service_info in services.items():
            svc_type = service_info['type']

            if svc_type == 'service':
                # Non-worker service (single instance)
                pid = str(service_info['pid']) if service_info['pid'] is not None else "-"
                status = service_info['status'] if service_info['status'] is not None else "ERROR"
                failures = str(service_info['failures']) if service_info['failures'] is not None else "-"
                num_workers = service_info.get('num_workers', '-')

                if service_info.get('error'):
                    status = f"ERROR: {service_info['error']}"

                # Color status
                status_color = 'green' if status == 'ALIVE' else 'red'
                status_str = click.style(status, fg=status_color)

                click.echo(f"{service_name:<30} {svc_type:<10} {num_workers:<8} {pid:<10} {status_str:<19} {failures:<10}")

                if debug:
                    # Show additional details
                    if service_info.get('started'):
                        click.echo(f"  {'Started:':<28} {time.ctime(service_info['started'])}")
                    if service_info.get('last_check'):
                        click.echo(f"  {'Last Check:':<28} {time.ctime(service_info['last_check'])}")
                    if service_info.get('output_log'):
                        click.echo(f"  {'Output Log:':<28} {service_info['output_log']}")
                    if service_info.get('command'):
                        click.echo(f"  {'Command:':<28} {service_info['command']}")
                    click.echo()  # Blank line between services

            elif svc_type == 'worker':
                # Worker service (multiple instances)
                for worker_num, worker_info in service_info['workers'].items():
                    worker_str = f"#{worker_num}"
                    pid = str(worker_info['pid']) if worker_info['pid'] is not None else "-"
                    status = worker_info['status'] if worker_info['status'] is not None else "ERROR"
                    failures = str(worker_info['failures']) if worker_info['failures'] is not None else "-"

                    if worker_info.get('error'):
                        status = f"ERROR: {worker_info['error']}"

                    # Color status
                    status_color = 'green' if status == 'ALIVE' else 'red'
                    status_str = click.style(status, fg=status_color)

                    click.echo(f"{service_name:<30} {svc_type:<10} {worker_str:<8} {pid:<10} {status_str:<19} {failures:<10}")

                    if debug:
                        # Show additional details for worker
                        if worker_info.get('started'):
                            click.echo(f"  {'Started:':<28} {time.ctime(worker_info['started'])}")
                        if worker_info.get('last_check'):
                            click.echo(f"  {'Last Check:':<28} {time.ctime(worker_info['last_check'])}")
                        if worker_info.get('output_log'):
                            click.echo(f"  {'Output Log:':<28} {worker_info['output_log']}")
                        if service_info.get('command'):
                            click.echo(f"  {'Command:':<28} {service_info['command']}")
                        click.echo()  # Blank line between workers

        click.echo("\n" + "=" * 100)

    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()


@airdi_services.command('restart')
@click.argument(
    'num_workers',
    type=int,
    default=1,
    required=False
)
@click.argument(
    'num_triggerers',
    type=int,
    default=1,
    required=False
)
def daemon_restart(num_workers, num_triggerers):
    """
    Restart Airflow daemon services.

    Stops the daemon if running, then starts it again.

    Arguments:
        NUM_WORKERS: Number of sync workers (scheduler parallelism, default: 1)
        NUM_TRIGGERERS: Number of async workers (triggerer instances, default: 1)

    Examples:
        airdi services restart
        airdi services restart 2 3
    """
    ctx = click.get_current_context()

    # Stop daemon
    click.echo("Stopping daemon...")
    ctx.invoke(daemon_stop)

    click.echo()

    # Start daemon
    click.echo("Starting daemon...")
    ctx.invoke(daemon_stop)
    ctx.invoke(daemon_start, num_workers=num_workers, num_triggerers=num_triggerers, foreground=False)
