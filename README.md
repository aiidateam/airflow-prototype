### To install:

```bash
pip install "apache-airflow==3.0.6" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.`x`.txt" # where `x` is your python version
pip install asyncssh
pip install apache-airflow-providers-fab
export AIRFLOW_HOME=~/airflow/

git clone git@github.com:agoscinski/calclava.git
cp calclava/plugins calclava/dags -r ~/airflow/
```

The webserver can be accessed on `http://localhost:8080`
The authentication information are written in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`
You can access the plugin http://localhost:8080/plugins/aiida/dashboard

