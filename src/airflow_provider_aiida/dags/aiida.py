from airflow import DAG
from airflow.models.param import Param


def AiidaDAG(**kwargs):
    kwargs['params'].update({
        # TODO move to nested transport params
        "machine": Param("localhost", type="string", section="Submission config"),
        "remote_workdir": Param("/tmp/airflow/remote_workdir", type="string", section="Submission config"),
        "local_workdir": Param("/tmp/airflow/local_workdir", type="string", section="Submission config"),
        })
    return DAG(**kwargs)
