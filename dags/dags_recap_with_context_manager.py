from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
                 'depends_on_past': False,
                 'email': ['airflow@example.com'],
                 'email_on_failure': False,
                 'email_on_retry': False,
                 'retries': 1,
                  'retry_delay': timedelta(minutes=5),
               }

with DAG('dags_recap_context_manager',
         default_args=default_args,
         description='A context manager tutorial DAG',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2021, 1, 1),
         catchup=False,
         tags=['workshop']
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
        retries=3
    )

    t3 = BashOperator(
        task_id='say_hello',
        bash_command='echo "hello"'
    )

    t1 >> t2 >> t3
    # OR
    #t1 >> [t2 >> t3]
