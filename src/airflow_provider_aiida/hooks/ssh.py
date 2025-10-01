"""SSH Hook for AiiDA transport - provides connection parameters for UI."""

from typing import Any, Optional

from airflow.hooks.base import BaseHook


class SSHHook(BaseHook):
    """
    Hook to provide SSH connection parameters from Airflow UI.

    This hook does NOT perform actual SSH operations - it only retrieves
    connection parameters from Airflow's connection metadata. The actual
    transport operations are handled by TransportQueue via triggers/operators.

    :param conn_id: Connection ID to use for SSH connection (default: 'ssh_default')
    :param machine: Machine/computer name (overrides conn_id if provided)
    """

    conn_name_attr = 'conn_id'
    default_conn_name = 'ssh_default'
    conn_type = 'aiida_ssh'
    hook_name = 'SSH (AiiDA)'

    def __init__(self, conn_id: str = default_conn_name, machine: Optional[str] = None):
        super().__init__()
        self.conn_id = conn_id
        self.machine = machine or conn_id

    def get_conn(self):
        """
        Get connection parameters from Airflow metadata.

        Returns the Airflow Connection object containing SSH parameters.
        """
        return BaseHook.get_connection(self.conn_id)

    def get_connection_params(self) -> dict:
        """
        Get SSH connection parameters as a dictionary.

        :return: Dictionary with connection parameters (host, port, username, etc.)
        """
        conn = self.get_conn()
        return {
            'hostname': conn.host or 'localhost',
            'port': conn.port or 22,
            'username': conn.login or 'airflow',
            **conn.extra_dejson
        }

    def test_connection(self):
        """Test that the connection parameters can be retrieved."""
        try:
            params = self.get_connection_params()
            return True, f"Connection parameters retrieved: {params.get('hostname')}"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """
        Returns custom field behaviour for the Airflow UI.

        This defines which fields are shown/hidden and provides placeholders.
        """
        return {
            "hidden_fields": ['schema', 'extra'],
            "relabeling": {
                'login': 'Username',
                'host': 'Hostname',
            },
            "placeholders": {
                'login': 'SSH username (default: current user)',
                'password': 'SSH password (leave empty for key-based auth)',
                'host': 'Remote host address (e.g., localhost or 192.168.1.1)',
                'port': '22',
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """
        Returns custom widgets for additional connection fields in the UI.

        This can be used to add extra fields specific to SSH connections.
        """
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__ssh__key_file": StringField(
                lazy_gettext('Private Key File'),
                widget=BS3TextFieldWidget(),
                description="Path to SSH private key file (optional)"
            ),
            "extra__ssh__timeout": StringField(
                lazy_gettext('Connection Timeout'),
                widget=BS3TextFieldWidget(),
                description="SSH connection timeout in seconds (default: 30)"
            ),
        }
