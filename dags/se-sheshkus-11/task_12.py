from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


with DAG(
    'task_12_se-sheshkus-11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='Less_11_task_12_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['hw_12_se-sheshkus-11'],
) as dag:

    def choosing_task():
        if Variable.get("is_startml"):
            return "startml_desc"
        else:
            return "not_startml_desc"

    def startml_desc():
        print("StartML is a starter course for ambitious people")

    def not_startml_desc():
        print("Not a startML course, sorry")


    t1 = DummyOperator(
        task_id='start'
    )

    t2 = BranchPythonOperator(
        task_id='choosing_task',
        python_callable=choosing_task,
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc,
    )

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc,
    )

    t5 = DummyOperator(
        task_id='end',
        trigger_rule='none_failed_or_skipped'
    )

    t1 >> t2 >> [t3, t4] >> t5


