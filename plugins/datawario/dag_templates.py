
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


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


def create_dag(name):
    with DAG(name, default_args=default_args) as dag:
        hello = BashOperator(task_id="1st_task", bash_command="echo 'hello world 1'" )
        hello2 = BashOperator(task_id="2nd_task", bash_command="echo 'hello world 2'" )

    hello >> hello2
    return dag


class EmrCluster:

    def __init__(self, name, dag):
       self.name = name
       self.dag = dag
       self.flow = None

    def __enter__(self):
        print(f'starting emr with {self.name}.')
        self.flow = BashOperator(task_id=f"{self.name}_start", bash_command=f"echo 'start emt with {self.name}'", dag=self.dag)
        return self

    def add_job(self, job_name):
        self.flow = self.flow >> BashOperator(task_id=f"{job_name}", bash_command=f"echo 'executing job with {job_name}'", dag=self.dag )
        self.flow = self.flow >> BashOperator(task_id=f"wait_for_{job_name}", bash_command=f"echo 'sensor on job {job_name}'", dag=self.dag )


    def get_emr_flow(self):
        return self.flow

    def __exit__(self, exc_type, exc_value, exc_traceback):
        print(f'shutting down emr {self.name}.')
        self.flow = self.flow >> BashOperator(task_id=f"{self.name}_stop", bash_command=f"echo 'terminating emr {self.name}'", dag=self.dag)
        return False

