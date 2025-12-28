"""Commands for managing AiiDA processes in Airflow."""
import click


@click.group('process')
def airdi_process():
    """
    Manage AiiDA processes in Airflow.

    Commands for pausing (marking as failed) and playing (clearing/resuming)
    DAG runs associated with AiiDA process nodes.
    """
    pass


@airdi_process.command('stop')
@click.argument('pk', type=int)
def process_pause(pk):
    """
    Stop the Airflow DAG run for an AiiDA process.

    This marks the DAG run associated with the process node as failed,
    effectively pausing its execution in Airflow.

    Arguments:
        PK: The primary key (PK) of the AiiDA process node

    Examples:
        airdi process pause 123
        airdi process pause 456
    """
    from airflow_provider_aiida.aiida_core import load_profile
    from airflow_provider_aiida.utils.airflow_control import mark_aiida_process_dag_run_failed, get_dag_run_id
    from aiida.orm import load_node

    try:
        # Load profile
        load_profile()

        # Load the process node
        click.echo(f"Loading process node {pk}...")
        try:
            node = load_node(pk)
        except Exception as e:
            click.secho(f"✗ Error: Could not load node with PK {pk}: {e}", fg='red', err=True)
            raise click.Abort()

        # Get the DAG run ID from node attributes

        dag_run_id = get_dag_run_id(node)
        if dag_run_id is None:
            click.secho(
                f"✗ Error: Node {pk} does not have a DAG run ID attribute. "
                f"This node may not be managed by Airflow.",
                fg='red',
                err=True
            )
            raise click.Abort()

        click.echo(f"DAG run ID: {dag_run_id}")

        # Get process class name for DAG ID
        process_type = node.process_class
        if not process_type:
            click.secho(f"✗ Error: Node {pk} does not have a process type", fg='red', err=True)
            raise click.Abort()

        click.echo(f"Process type: {process_type}")
        click.echo(f"\nMarking DAG run as failed...")

        # Mark the DAG run as failed
        # TODO check success
        mark_aiida_process_dag_run_failed(process_type, dag_run_id)
        node.pause()
        node.store()


        click.secho(f"✓ Successfully paused process {pk}", fg='green', bold=True)
        click.echo(f"  DAG run '{dag_run_id}' has been marked as failed")

    except click.Abort:
        raise
    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()


@airdi_process.command('continue')
@click.argument('pk', type=int)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Show what would be cleared without actually clearing'
)
@click.option(
    '--only-failed',
    is_flag=True,
    help='Only clear failed tasks'
)
def process_play(pk, dry_run, only_failed):
    """
    Continues the Airflow DAG run for an AiiDA process.

    This clears the DAG run associated with the process node, allowing it
    to be re-run or resumed in Airflow.

    Arguments:
        PK: The primary key (PK) of the AiiDA process node

    Examples:
        airdi process play 123
        airdi process play 456 --dry-run
        airdi process play 789 --only-failed
    """
    from airflow_provider_aiida.aiida_core import load_profile
    from airflow_provider_aiida.utils.airflow_control import clear_aiida_process_dag_run, get_dag_run_id
    from aiida.orm import load_node

    try:
        # Load profile
        load_profile()

        # Load the process node
        click.echo(f"Loading process node {pk}...")
        try:
            node = load_node(pk)
        except Exception as e:
            click.secho(f"✗ Error: Could not load node with PK {pk}: {e}", fg='red', err=True)
            raise click.Abort()

        # Get the DAG run ID from node attributes
        dag_run_id = get_dag_run_id(node)
        if dag_run_id is None:
            click.secho(
                f"✗ Error: Node {pk} does not have a DAG run ID attribute. "
                f"This node may not be managed by Airflow.",
                fg='red',
                err=True
            )
            raise click.Abort()

        click.echo(f"DAG run ID: {dag_run_id}")

        # Get process class name for DAG ID
        process_type = node.process_class
        
        if not process_type:
            click.secho(f"✗ Error: Node {pk} does not have a process type", fg='red', err=True)
            raise click.Abort()

        click.echo(f"Process type: {process_type}")

        if dry_run:
            click.echo(f"\n[DRY RUN] Clearing DAG run...")
        else:
            click.echo(f"\nClearing DAG run...")

        # Clear the DAG run
        # TODO check success
        result = clear_aiida_process_dag_run(
            process_type,
            dag_run_id,
            dry_run=dry_run,
            only_failed=only_failed
        )
        node.unpause()

        if dry_run:
            click.secho(f"✓ [DRY RUN] Would clear process {pk}", fg='yellow', bold=True)
            click.echo(f"  DAG run '{dag_run_id}' would be cleared")
            if result:
                click.echo(f"  Tasks that would be cleared: {len(result)}")
        else:
            click.secho(f"✓ Successfully cleared process {pk}", fg='green', bold=True)
            click.echo(f"  DAG run '{dag_run_id}' has been cleared and can be re-run")

    except click.Abort:
        raise
    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()


@airdi_process.command('workerlog')
@click.argument('pk', type=int)
@click.option(
    '--separate',
    is_flag=True,
    help='Show logs separately instead of merged by timestamp'
)
@click.option(
    '--list-only',
    is_flag=True,
    help='Only list log files without displaying content'
)
def process_workerlog(pk, separate, list_only):
    """
    View Airflow worker and triggerer logs for an AiiDA process.

    This displays the Airflow execution logs (from scheduler and triggerer)
    for a given AiiDA process node. By default, logs are merged and sorted
    chronologically.

    Arguments:
        PK: The primary key (PK) of the AiiDA process node

    Examples:
        airdi process workerlog 123
        airdi process workerlog 456 --separate
        airdi process workerlog 789 --list-only
    """
    from airflow_provider_aiida.aiida_core import load_profile
    load_profile()

    from airflow_provider_aiida.utils.process_logs import (
        get_airflow_log_path_from_process,
        list_log_files,
        display_logs_with_pager,
        get_log_source
    )

    try:
        # Get the log path
        log_path = get_airflow_log_path_from_process(pk)

        if list_only:
            click.echo(f"Log directory for process {pk}:")
            click.echo(f"  {log_path}")

        # List all log files in the directory
        log_files = list_log_files(log_path)

        if log_files:
            if list_only:
                click.echo(f"\nFound {len(log_files)} log file(s):")
                for log_file in log_files:
                    source = get_log_source(log_file)
                    click.echo(f"  [{source}] {log_file}")
                click.echo("\nTip: Remove '--list-only' flag to view log contents in a pager")
                if len(log_files) > 1:
                    click.echo("     Logs will be merged and sorted by timestamp by default")
                    click.echo("     Use '--separate' to view logs separately")
            else:
                # Display logs in pager
                display_logs_with_pager(log_files, pk, merged=not separate)
        else:
            if log_path.exists():
                click.secho(f"✗ No log files found in the directory.", fg='yellow')
                if list_only:
                    click.echo(f"  Directory: {log_path}")
            else:
                click.secho(
                    f"✗ Log directory does not exist yet.",
                    fg='yellow'
                )
                if list_only:
                    click.echo(f"  Expected directory: {log_path}")
                click.echo("  Logs may not have been generated or the DAG run hasn't started.")

    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()
