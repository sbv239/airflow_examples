from datetime import timedelta, datetime
from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_ds(ds):
    print(ds)

with DAG(
    'hw_1_a-vdovenko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    description="Lesson 11 home work 1",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022,1,1),
    catchup=False,
    tags=['a-vdovenko'],
) as dag:
    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd',
    )

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_ds,
    )

    t1 >> t2
