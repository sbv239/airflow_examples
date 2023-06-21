from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'task_r-izutov_3',


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},

    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 20),
    catchup=False,
    tags=['izutov']
) as dag:
    def print_task_number(ts, run_id, task_number):
        print(f"ts: {ts}")
        print(f"run_id: {run_id}")
        print(f"task number: {task_number}")

    for i in range(30):
        if i < 10:
            bash_task = BashOperator(
                task_id=f'BashOperator_{i}',
                bash_command=f'echo {i}'
            )

        else:
            python_task = PythonOperator(
                task_id=f'PythonOperator_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )


    # bash_task >> python_task