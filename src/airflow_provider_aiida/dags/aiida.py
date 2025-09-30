# TODO imports


def AiidaDAG(**kwargs):
    kwargs['params'].update({
        # TODO move to nested transport params
        "machine": Param("localhost", type="string", section="Submission config"),
        "remote_workdir": Param("/Users/alexgo/code/airflow/remote_workdir", type="string", section="Submission config"),
        "local_workdir": Param("/Users/alexgo/code/airflow/local_workdir", type="string", section="Submission config"),
        })
    return DAG(**kwargs)
