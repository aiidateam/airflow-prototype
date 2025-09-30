__version__ = "0.0.1dev0"

def get_provider_info():
    return {
        "package-name": "airflow-provider-aiida",  # Required
        "name": "AiiDA Provider",  # Required
        "description": "An Apache Airflow provider for AiiDA.",  # Required
        #"operators": #TODO
        #"triggers": #TODO
        "hooks": [
            {
                "integration-name": "SSH",
                "python-modules": ["aiida_provider.hooks.ssh"],
            }
        ],
        "connection-types": [
            {
                "connection-type": "ssh",
                "hook-class-name": "aiida_provider.hooks.ssh.SSHHook",
                "hook-name": "SSH"
            }
        ],
        "versions": [__version__],  # Required
    }
