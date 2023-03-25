from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def xcom_task1(ti):
    return "Airflow tracks everything"


def xcom_task2(ti):
    print(ti.xcom_pull(key="return_value", task_ids='xcom_pusher'))


with DAG(
        'hw_10_a-pichugin-23',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        start_date=datetime.now(),
        catchup=False,
        tags=['hw_10_a-pichugin-23'],
) as dag:
    t1 = PythonOperator(
        task_id='xcom_pusher',
        python_callable=xcom_task1
    )

    t2 = PythonOperator(
        task_id='xcom_puller',
        python_callable=xcom_task2
    )

    t1 >> t2