from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime


def print_ds_from_python(ds, **kwargs):
    print(ds)
    return 'Function to print ds date is working OK'

with DAG(
    'o-chikin_task2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description='task2_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 16),
    catchup=False,
    tags=['Oleg_Chikin_DAG']
) as dag:

    t1 = BashOperator(
        task_id='print_bash_pwd',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_ds_date_from_python',
        python_callable=print_ds_from_python,
    )

    t1 >> t2
