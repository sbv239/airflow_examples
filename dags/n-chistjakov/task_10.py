from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

ID = "hw_10_n-chistjakov_"

def dummy_func(ti):
    return "Airflow tracks everything"

def get_data(ti):
    print(
        ti.xcom_pull(
            key='return_value',
            task_ids=ID + '1'
        )
    )

def print_info(ts,  run_id, **kwargs):
    print(ts)
    print(run_id)
    print(f'task number is: {kwargs["task_number"]}'    )

with DAG(
    'hw_10_n-chistjakov',
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
    tags=["task_10"],
) as dag:

    pyth_1 = PythonOperator(
        task_id=ID + "1",
        python_callable=dummy_func
    )

    pyth_2 = PythonOperator(
        task_id=ID + "2",
        python_callable=get_data
    )

    pyth_1 >> pyth_2