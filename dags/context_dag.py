from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

from datawario.dag_templates import EmrCluster

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

with DAG("context_dag", default_args=default_args, schedule_interval=timedelta(1)) as dag:
    with EmrCluster("my_emr", dag) as emr:
        emr.add_job("first_job")
        emr.add_job("second_job")

    op2 = BashOperator(task_id=f"end_of_dag", bash_command=f"echo 'end of dag'")

    emr.get_emr_flow() >> op2


