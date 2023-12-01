from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'hw_7_m-statsenko',
        default_args=default_args,
        description='HW 7',
        start_date=datetime(2023, 11, 30),
        catchup=False,
        tags=['HW 7']
) as dag:
    def print_number(ts, run_id, **kwarg):
        print(f"task number is: {kwarg[task_number]}")
        print("ts:", ts)
        print("run_id:", run_id)


    for i in range(20):
        task_p = PythonOperator(
            task_id='print_number_task_' + str(i),
            python_callable=print_number,
            op_kwargs={'task_number': i}
        )
