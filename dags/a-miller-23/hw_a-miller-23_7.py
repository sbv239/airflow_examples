from airflow import  DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
'hw_a-miller-23_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 8, 19),
        catchup=False,
        tags=['example'],

) as dag:

    def print_task(task_number, ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        return (f"task number is: {task_number}")

    for i in range(20):
        task = PythonOperator(task_id = f"python_task_{i}",
                              python_callable=print_task,
                              op_kwargs={"task_number": f'{i}'},)

    task
