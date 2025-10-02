__version__ = "0.0.1dev0"

def get_provider_info():
    return {
        "package-name": "airflow-provider-aiida",  # Required
        "name": "AiiDA Provider",  # Required
        "description": "AiiDA framework for scientific workflows",  # Required
        "operators": [
            {
                "integration-name": "AiiDA CalcJob",
                "python-modules": ["airflow_provider_aiida.operators.calcjob"],
            }
        ],
        "triggers": [
            {
                "integration-name": "AiiDA CalcJob Async",
                "python-modules": ["airflow_provider_aiida.triggers.async_calcjob"],
            }
        ],
        "hooks": [
            {
                "integration-name": "SSH (AiiDA)",
                "python-modules": ["airflow_provider_aiida.hooks.ssh"],
            }
        ],
        "connection-types": [
            {
                "connection-type": "aiida_ssh",
                "hook-class-name": "airflow_provider_aiida.hooks.ssh.SSHHook",
                "hook-name": "SSH (AiiDA)"
            }
        ],
        "plugins": [
            {
                "name": "dagrun_tracking_plugin",
                "plugin-class": "airflow_provider_aiida.plugins.dagrun_listener.DagRunTrackingPlugin",
            },
            {
                "name": "aiida_plugin",
                "plugin-class": "airflow_provider_aiida.plugins.custom_ui.AirflowTestPlugin",
            }
        ],
        "versions": [__version__],  # Required
    }
