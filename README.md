# [IN DEVELOPMENT]
### To install:

```bash
pip install "apache-airflow==3.1.0rc2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.`x`.txt" # where `x` is your python version
pip install asyncssh
pip install apache-airflow-providers-fab
export AIRFLOW_HOME=~/airflow/

git clone https://github.com/aiidateam/airflow-prototype & cd airflow-prototype
cp plugins dags -r $AIRFLOW_HOME
```

You may need `aiida-core` along with. Please install `airflow-dev` branch:
```bash
git clone https://github.com/aiidateam/aiida-core & cd aiida-core
git checkout airflow-dev
pip install -e . 
```

The webserver can be accessed on `http://localhost:8080`
The authentication information are written in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`
You can access the plugin http://localhost:8080/plugins/aiida/dashboard

