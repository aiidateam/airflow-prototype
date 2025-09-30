"""
Unittest module to test Hooks.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_sample.TestSampleHook

"""

# Import Hook
from airflow_provider_aiida.hooks.ssh import SSHHook



# Mock the `conn_sample` Airflow connection


def test_ssh_hook():
    SSHHook()
