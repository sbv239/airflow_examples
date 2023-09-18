"""
DAG for task 13
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
        'hw_t-togyzbaev_13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['t-togyzbaev']
) as dag:
    startml_desc_task = PythonOperator(
        task_id="startml_desc",
        python_callable=lambda: print("StartML is a starter course for ambitious people")
    )
    not_startml_desc_task = PythonOperator(
        task_id="not_startml_desc",
        python_callable=lambda: print("Not a startML course, sorry")
    )


    def branch_func():
        if Variable.get("is_startml") == "True":
            return startml_desc_task.task_id
        else:
            return not_startml_desc_task.task_id


    branch = BranchPythonOperator(
        task_id="branc_func",
        python_callable=branch_func
    )

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> branch >> [startml_desc_task, not_startml_desc_task] >> end
