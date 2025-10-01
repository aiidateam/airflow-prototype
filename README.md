# Airflow Provider for AiiDA

⚠️ **DEVELOPMENT STATUS** ⚠️

This package is currently under active development and should be considered **experimental**.

- The API is unstable and subject to change without notice
- Breaking changes may occur between versions
- Features may be incomplete or contain bugs
- No guarantees are provided regarding functionality, stability, or backwards compatibility
- Not recommended for production use at this time

Use at your own risk. Contributions and feedback are welcome!

---

### Installation

For now we only support Python 3.11.

```bash
pip install .
```

### Development

For project management we use [hatch](https://hatch.pypa.io/latest/install/).

You might need to serialize the dags in the `dags_example` manually before running the tests.
```
hatch run test:airflow dags reserialize
```
Then
```shell
# Runs all tests
hatch test
 # Runs test_calcjob test
hatch run test:pytest tests/operators/test_calcjob.py
```

Run formatting
```shell
hatch fmt
```
Create a dev environment
```shell
# Only project dependencies
hatch shell
# Including test dependencies
hatch -e test shell
```

You can check if it was loaded correctly with
```
airflow providers get airflow-provider-aiida
```

### Using the AiiDA DAG Bundle

To automatically load DAGs from the `aiida.dags` entry point, configure the DAG bundle in your `airflow.cfg`:

```ini
[dag_processor]
dag_bundle_config_list = [
    {
      "name": "aiida_dags",
      "classpath": "airflow_provider_aiida.bundles.aiida_dag_bundle.AiidaDagBundle",
      "kwargs": {}
    }
]
```

See [docs/dag_bundle.md](docs/dag_bundle.md) for more details on how to use the DAG bundle and contribute your own workflows.

You need to create a `localhost` connection 
```
airflow connections add localhost --conn-host localhost --conn-login alexgo --conn-type aiida_ssh
```
You can add new dags by putting it into the `dags_example` submodule.

The webserver can be accessed on `http://localhost:8080`
The authentication information are written in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`
You can access the plugin http://localhost:8080/plugins/aiida/dashboard


### Production

For production runs we use the fixed list of
```
pip install "apache-airflow==3.1.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.11.txt"
```
