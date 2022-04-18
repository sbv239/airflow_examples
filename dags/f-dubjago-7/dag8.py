"""
XCom 1
"""
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def put_data(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test',
    )


def get_and_print_data(ti):
    data = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='put',
    )
    print(data)


with DAG(
        'f-dubjago-7_dag8',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 10),
        tags=['df'],
) as dag:
    put = PythonOperator(
        task_id='put',
        python_callable=put_data,
    )

    get_and_print = PythonOperator(
        task_id='get_and_print',
        python_callable=get_and_print_data,
    )

    put >> get_and_print
