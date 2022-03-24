import os
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import requests
import json

conn_id="startml_feed"

def get_variable():
    from airflow.models import Variable
    print(Variable.get("is_startml"))


with DAG(
        # Название таск-дага
        'hw_12_i-rjazanov-6',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='This is from DAG for Ryazanov to Task 12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task_12']

) as dag:

    def startml_desc_func():
        print("StartML is a starter course for ambitious people")

    def not_startml_desc():
        print("Not a startML course, sorry")

    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc_func)

    t2= PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc)

    start = DummyOperator(task_id='before_branching')
    finish = DummyOperator(task_id='after_branching')

    def func_branch():
        from airflow.models import Variable
        variable = Variable.get("is_startml")
        if variable is True:
            return "startml_desc"
        else:
            return "not_startml_desc"

    branch_task = BranchPythonOperator(
        task_id='determine_course',
        python_callable=func_branch,
        trigger_rule="all_done",
        dag=dag)

    start >> branch_task >> [t1, t2] >> finish