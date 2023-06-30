from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

ID = "hw_7_n-chistjakov_"

def set_data(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def get_data(ti):
    print(
        ti.xcom_pull(
            key='sample_xcom_key',
            task_ids=ID + '1'
        )
    )

def print_info(ts,  run_id, **kwargs):
    print(ts)
    print(run_id)
    print(f'task number is: {kwargs["task_number"]}'    )

with DAG(
    'hw_7_n-chistjakov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description="Second task",
    start_date=datetime(2023, 6, 30),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["task_03"],
) as dag:

    pyth_1 = PythonOperator(
        task_id=ID + "1",
        python_callable=set_data
    )

    pyth_2 = PythonOperator(
        task_id=ID + "2",
        python_callable=get_data
    )

    pyth_1 >> pyth_2