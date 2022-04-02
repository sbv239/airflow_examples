from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def set_xcom_func():
    return 'Airflow tracks everything'


def get_xcom_func(ti):
    value = ti.xcom_pull(
        key='return_value',
        task_ids='set_xcom'
    )
    print(value)


with DAG(
    'task_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='DAG for task 9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['DP HW9']
) as dag:
    t1 = PythonOperator(
        task_id='set_xcom',
        python_callable=set_xcom_func
    )

    t2 = PythonOperator(
        task_id='get_xcom',
        python_callable=get_xcom_func
    )

    t1 >> t2
