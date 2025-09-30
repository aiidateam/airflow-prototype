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

Run tests
```shell
# Runs all tests
hatch test
 # Runs test_calcjob test
hatch run test:run tests/operators/test_calcjob.py
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

The webserver can be accessed on `http://localhost:8080`
The authentication information are written in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`
You can access the plugin http://localhost:8080/plugins/aiida/dashboard


### Production

For production runs we use the fixed list of
```
pip install "apache-airflow==3.1.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.11.txt"
```
