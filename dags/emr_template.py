from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from datawario.emr_cluster import EmrCluster
import datawario.emr_cluster as e

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

with DAG("emr_api_example", default_args=default_args, schedule_interval=timedelta(1)) as dag:
    with EmrCluster(dag) as emr:
        emr.add_spark_steps(e.SPARK_PI_STEPS)

    op2 = BashOperator(task_id=f"end_of_dag", bash_command=f"echo 'log end of dag'")

    emr.get_emr_flow() >> op2
