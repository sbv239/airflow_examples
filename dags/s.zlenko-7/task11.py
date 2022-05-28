"""
Task 11: https://lab.karpov.courses/learning/84/module/1049/lesson/10040/29383/139296/
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

with DAG(
    'hw_11_s.zlenko-7',
    default_args = default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    def return_variable():
        from airflow.models import Variable
        print(Variable.get('is_startml'))

    t1 = PythonOperator(
        task_id='return_var',
        python_callable=return_variable
    )

    t1