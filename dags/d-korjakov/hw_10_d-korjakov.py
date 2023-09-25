from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


def python_operator_one():
    return "Airflow tracks everything"


def python_operator_two(ti):
    xcom_pull = ti.xcom_pull(
        key='return_value',
        task_ids='push_data',
    )
    return print(xcom_pull)


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('hw_10_d-korjakov',
         default_args=default_args,
         description='DAG with PythonOperator and PythonOperator',
         start_date=datetime(2023, 9, 24),
         schedule_interval=timedelta(days=1)
         ) as dag:
    t1 = PythonOperator(
        task_id='push_data',
        python_callable=python_operator_one,
    )
    t2 = PythonOperator(
        task_id='pull_data',
        python_callable=python_operator_two,
    )

    t1 >> t2
