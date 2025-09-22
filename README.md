
```bash
pip install "apache-airflow==3.0.6" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.9.txt"
pip install asyncssh
export AIRFLOW_HOME=<this-directory>
airflow standalone
```

The webserver can be accessed on http://localhost:8080. 
The authentication information are written in `simple_auth_manager_passwords.json.generated`3
You can access the plugin http://localhost:8080/plugins/aiida/dashboard
