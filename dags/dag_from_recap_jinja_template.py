from airflow import DAG
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.decorators import task

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


with DAG("dag_from_recap_jinja_template",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False, tags=['workshop']) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="echo 'extract from s3://as24-data/raw/mydag/'"
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="echo 'transform from s3://as24-data/raw/mydag/'"
    )

    load = BashOperator(
        task_id="load",
        bash_command="echo 'load to s3://as24-data/processed/mydag'"
    )

extract >> transform >> load