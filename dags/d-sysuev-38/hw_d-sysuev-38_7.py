from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_d-sysuev-38_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 25),
    catchup=False,
    tags=["hw_7"]
) as dag:

    def print_task_number(task_number, ts, run_id):
        print (f"task number is: {task_number}")
        print(ts)
        print(run_id)
        return "it's working"

    for i in range (20):
        t2 = PythonOperator(
        task_id=f'print_task_number_{i}',
        python_callable=print_task_number,
        op_kwargs = {'task_number':i}
        )
