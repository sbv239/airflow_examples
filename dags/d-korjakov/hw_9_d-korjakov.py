from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


def python_operator_put(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test',
    )


def python_operator_pull(ti):
    xcom_pull = ti.xcom_pull(
        key='sample_xcom_key',
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

with DAG('hw_9_d-korjakov',
         default_args=default_args,
         description='DAG with "for" BashOperator and PythonOperator',
         start_date=datetime(2023, 9, 24),
         schedule_interval=timedelta(days=1)
         ) as dag:
    t1 = PythonOperator(
        task_id='push_data',
        python_callable=python_operator_put,
    )
    t2 = PythonOperator(
        task_id='pull_data',
        python_callable=python_operator_pull,
    )

    t1 >> t2
