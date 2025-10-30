__version__ = "0.0.1dev0"

def get_provider_info():
    return {
        "package-name": "airflow-provider-aiida",  # Required
        "name": "AiiDA Provider",  # Required
        "description": "An Apache Airflow provider for AiiDA.",  # Required
        #"operators": #TODO
        #"triggers": #TODO
        "versions": [__version__],  # Required
    }
