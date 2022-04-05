from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def push_xcom(key, value, ti):
    ti.xcom_push(key=key, value=value)


def pull_xcom(key, ti):
    value = ti.xcom_pull(
        key=key,
        task_ids='push_data_to_xcom'
    )
    print(f'[{key}] = {value}')


with DAG(
        dag_id='a-kalmykov-dag-8',
        default_args=default_args,
        description='Dag 8 Kalmykov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 4),
        catchup=False,
        tags=['a-kalmykov'],
) as dag:
    t1 = PythonOperator(
        task_id='push_data_to_xcom',
        python_callable=push_xcom,
        op_kwargs={'key': 'sample_xcom_key', 'value': 'xcom test'}
    )

    t2 = PythonOperator(
        task_id='pull_data_from_xcom',
        python_callable=pull_xcom,
        op_kwargs={'key': 'sample_xcom_key'}
    )

    t1 >> t2
