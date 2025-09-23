### To install:

```bash
pip install "apache-airflow==3.0.6" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.`x`.txt" # where `x` is your python version
pip install asyncssh
pip install apache-airflow-providers-fab
export AIRFLOW_HOME=~/airflow/

git clone git@github.com:agoscinski/calclava.git
cp calclava/plugins calclava/dags -r ~/airflow/
```


#### Known conflicts
At the moment, if you decide to install the latest aiida-core as well, you may faces these conflicts:

(on python 3.12)
```bash
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
apache-airflow-core 3.0.6 requires importlib-metadata>=7.0; python_version >= "3.12", but you have importlib-metadata 6.11.0 which is incompatible.
apache-airflow-core 3.0.6 requires sqlalchemy[asyncio]<2.0,>=1.4.49, but you have sqlalchemy 2.0.43 which is incompatible.
apache-airflow-task-sdk 1.0.6 requires psutil>=6.1.0, but you have psutil 5.9.8 which is incompatible.
flask-appbuilder 4.6.3 requires SQLAlchemy<1.5, but you have sqlalchemy 2.0.43 which is incompatible.
```
