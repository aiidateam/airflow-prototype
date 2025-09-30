"""SSH Hook for AiiDA transport."""

from typing import Optional
from pathlib import Path

from airflow.hooks.base import BaseHook
from aiida.transports.plugins.ssh_async import AsyncSshTransport


class SSHHook(BaseHook):
    """
    Hook to interact with remote servers via SSH using AiiDA's AsyncSshTransport.

    :param conn_id: Connection ID to use for SSH connection (default: 'ssh_default')
    :param machine: Machine/computer name (overrides conn_id if provided)
    """

    conn_name_attr = 'conn_id'
    default_conn_name = 'ssh_default'
    conn_type = 'ssh'
    hook_name = 'SSH (AiiDA)'

    def __init__(self, conn_id: str = default_conn_name, machine: Optional[str] = None):
        super().__init__()
        self.conn_id = conn_id
        self.machine = machine or conn_id
        self._transport = None

    def get_conn(self):
        """Get the SSH transport connection."""
        if self._transport is None:
            self._transport = AsyncSshTransport(machine=self.machine)
        return self._transport

    def upload_file(self, local_path: str, remote_path: str):
        """
        Upload a file to the remote server.

        :param local_path: Local file path
        :param remote_path: Remote file path
        """
        transport = self.get_conn()
        with transport.open() as connection:
            connection.putfile(Path(local_path).absolute(), Path(remote_path))

    def download_file(self, remote_path: str, local_path: str):
        """
        Download a file from the remote server.

        :param remote_path: Remote file path
        :param local_path: Local file path
        """
        transport = self.get_conn()
        with transport.open() as connection:
            connection.getfile(Path(remote_path), Path(local_path))

    def execute_command(self, command: str, workdir: Optional[str] = None):
        """
        Execute a command on the remote server.

        :param command: Command to execute
        :param workdir: Working directory for command execution
        :return: Tuple of (exit_code, stdout, stderr)
        """
        transport = self.get_conn()
        with transport.open() as connection:
            return connection.exec_command_wait(command, workdir=workdir)

    def test_connection(self):
        """Test the SSH connection."""
        try:
            transport = self.get_conn()
            with transport.open() as connection:
                # Simple test - check if we can open connection
                return True, "Connection successful"
        except Exception as e:
            return False, str(e)
