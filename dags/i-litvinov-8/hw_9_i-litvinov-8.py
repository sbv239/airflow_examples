from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
from textwrap import dedent


def push_to_xcom(ti):
    return 'Airflow tracks everything'


def pull_from_xcom(ti):
    pull_res = ti.xcom_pull(
        key="return_value",
        task_ids='pushing_to_xcom',
    )
    print(f'pull_res = {pull_res}')


with DAG(
        'hw_8_i-litvinov-8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='hw_8_i-litvinov-8',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 16),
        catchup=False,
        tags=['i-litvinov-8']
) as dag:
    t1 = PythonOperator(
        task_id='pushing_to_xcom',
        python_callable=push_to_xcom
    )
    t2 = PythonOperator(
        task_id='pulling_from_xcom',
        python_callable=pull_from_xcom
    )

    t1 >> t2

