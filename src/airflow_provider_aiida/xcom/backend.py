from airflow.sdk.execution_time.xcom import BaseXCom

class AiidaBackend(BaseXCom):
    # TODO this should add logic serialize so it can be tracked in the provenance graph
    pass

