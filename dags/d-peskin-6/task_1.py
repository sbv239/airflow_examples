from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'task_1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='DAG for task 1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 27),
    catchup=False,
    tags=['DP HW1']
) as dag:
    t1 = BashOperator(task_id = "Bash_operator_task", bash_command = "pwd ")


    def print_ds(ds):
        print(ds)
        return  'Ok'

    t2 = PythonOperator(task_id = "Python_operator_task", python_callable = print_ds)

    t1 >> t2
