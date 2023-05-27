"""
Lesson KC Airflow
Task 12
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def get_airflow_var():
    from airflow.models import Variable
    is_startml_var = Variable.get("is_startml")
    print(is_startml_var)


with DAG(
    'Task_12',
    # DAG dafault parameters
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    tags=['i-nechushkin-20_Task_12'],
) as dag:

    t = PythonOperator(
        task_id='get_airflow_var',
        python_callable=get_airflow_var,
    )

    t
