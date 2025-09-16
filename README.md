pip install "apache-airflow==3.0.6" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.9.txt"
pip install asyncssh
export AIRFLOW_HOME=<this-directory>
airflow standalone
