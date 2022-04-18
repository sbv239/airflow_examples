"""
XCom 2
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
    return 'Airflow tracks everything'


def get_data(ti):
    data = ti.xcom_pull(
        key='return_value',
        task_ids='put',
    )
    print(data)


with DAG(
        'f-dubjago-7_dag9',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 10),
        tags=['df'],
) as dag:
    put = PythonOperator(
        task_id='put',
        python_callable=put_data,
    )

    get = PythonOperator(
        task_id='get',
        python_callable=get_data,
    )

    put >> get
