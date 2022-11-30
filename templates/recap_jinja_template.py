from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_mesh_team",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["data_mesh_team@aautoscout24.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG("{{ dag_id }}",
         default_args=default_args,
         schedule_interval="{{ schedule_interval }}",
         catchup={{ catchup or False }}, tags=['workshop']) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="echo 'extract from {{ input }}'"
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="echo 'transform from {{ input }}'"
    )

    load = BashOperator(
        task_id="load",
        bash_command="echo 'load to {{ output }}'"
    )

extract >> transform >> load
