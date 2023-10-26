from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG('hw_d-shestak_7',
         default_args=default_args,
         description='hw_d-shestak_7',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_7_d-shestak']
         ) as dag:
    def print_task(task_number, ts, run_id):
        print(ts)
        print(run_id)
        print(f'task number is: {task_number}')
        return 'string for log'

    for i in range(20):
        PythonOperator(
            task_id=f'task_{i}',
            python_callable=print_task,
            op_kwargs={'task_number': i}
        )
    
