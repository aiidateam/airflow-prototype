import signal
import subprocess
import sys
import time
from pathlib import Path


class TriggererService:
    """Manages multiple Airflow triggerer processes."""

    def __init__(self, num_triggerers: int, airflow_home: str | None = None):
        self.num_triggerers = num_triggerers
        self.airflow_home = airflow_home
        self.processes: list[subprocess.Popen] = []
        self.base_port = self._get_base_port()

    def _get_base_port(self) -> int:
        """Get the base trigger log server port from airflow.cfg."""
        from airflow.configuration import AirflowConfigParser

        config = AirflowConfigParser()

        if self.airflow_home:
            config_file = Path(self.airflow_home) / 'airflow.cfg'
            if config_file.exists():
                config.read(str(config_file))

        # Get port from config, default to 8794
        return config.getint('logging', 'trigger_log_server_port', fallback=8794)

    def _start_triggerer(self, worker_num: int) -> subprocess.Popen:
        """Start a single triggerer process with a unique port."""
        # TODO check if port available if not add +1
        port = self.base_port + worker_num

        # Build environment with unique port
        import os
        env = os.environ.copy()
        env['AIRFLOW__LOGGING__TRIGGER_LOG_SERVER_PORT'] = str(port)

        if self.airflow_home:
            env['AIRFLOW_HOME'] = self.airflow_home

        print(f"Starting triggerer #{worker_num} on port {port}")

        # Start triggerer process - output goes to terminal stdout/stderr
        process = subprocess.Popen(
            ['airflow', 'triggerer'],
            env=os.environ | env,
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True
        )

        return process

    def start(self):
        """Start all triggerer processes."""
        print(f"Starting {self.num_triggerers} triggerer(s)")
        print(f"Base port: {self.base_port}")
        print()

        # Start all triggerers
        for i in range(self.num_triggerers):
            # TODO check if port available if not add +1
            process = self._start_triggerer(i)
            self.processes.append(process)
            # Small delay to avoid startup race conditions
            time.sleep(0.5)

        print()
        print(f"Successfully started {len(self.processes)} triggerer(s)")
        print("Press Ctrl+C to stop all triggerers")

    def stop(self):
        """Stop all triggerer processes."""
        print("\nStopping all triggerers...")

        for i, process in enumerate(self.processes):
            if process.poll() is None:  # Still running
                print(f"Stopping triggerer #{i} (PID {process.pid})")
                process.terminate()

        # Wait for graceful shutdown (max 10 seconds)
        for process in self.processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print(f"Force killing triggerer (PID {process.pid})")
                process.kill()

        print("All triggerers stopped")

    def monitor(self):
        """Monitor triggerer processes and restart if they crash."""
        try:
            while True:
                # Check if any process has died
                for i, process in enumerate(self.processes):
                    if process.poll() is not None:  # Process has exited
                        returncode = process.returncode
                        print(f"\nTriggerer #{i} died with exit code {returncode}")
                        print(f"Restarting triggerer #{i}...")

                        # Restart the process
                        new_process = self._start_triggerer(i)
                        self.processes[i] = new_process

                # Sleep before next check
                time.sleep(5)

        except KeyboardInterrupt:
            print("\nReceived interrupt signal")
            self.stop()

    def run(self):
        """Run the supervisor - start processes and monitor them."""
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        # Start all triggerers
        self.start()

        # Monitor them
        self.monitor()

    def _signal_handler(self, signum, frame):
        """Handle termination signals."""
        print(f"\nReceived signal {signum}")
        self.stop()
        sys.exit(0)




def main():
    """Main entry point."""
    from airflow_provider_aiida.aiida_core import load_profile
    load_profile()

    import argparse
    import os
    parser = argparse.ArgumentParser(
        description='Supervise multiple Airflow triggerer processes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start 3 triggerers with default airflow home
  python triggerer_supervisor.py 3

  # Start 5 triggerers with custom airflow home
  python triggerer_supervisor.py 5 --airflow-home /path/to/airflow

  # Start 2 triggerers (they will use ports 8794 and 8795 by default)
  python triggerer_supervisor.py 2

The script will:
  1. Read the base port from airflow.cfg (default: 8794)
  2. Start N triggerers on ports: base_port, base_port+1, base_port+2, ...
  3. Redirect all output to logs/triggerers_stdout.log and logs/triggerers_stderr.log
  4. Monitor processes and restart them if they crash
  5. Gracefully stop all triggerers when receiving Ctrl+C or SIGTERM
        """
    )

    parser.add_argument(
        'num_triggerers',
        type=int,
        nargs='?',
        default=None,
        help='Number of triggerer processes to start (default: from AIRFLOW__CORE__ASYNC_PARALLELISM env var, or 1)'
    )

    args = parser.parse_args()

    # Determine num_triggerers: CLI arg > env var > default (1)
    if args.num_triggerers is not None:
        num_triggerers = args.num_triggerers
    elif (env_value := os.environ.get("AIRFLOW__CORE__ASYNC_PARALLELISM")) is not None:
        num_triggerers = int(env_value)
    else:
        raise ValueError("Value for number of triggerers was not provided and AIRFLOW__CORE__ASYNC_PARALLELISM is not set.")

    if num_triggerers < 1:
        parser.error("Number of triggerers must be at least 1")

    # Create and run supervisor
    supervisor = TriggererService(
        num_triggerers=num_triggerers,
        airflow_home=os.environ.get("AIRFLOW_HOME"),
    )

    supervisor.run()

if __name__ == '__main__':
    main()
