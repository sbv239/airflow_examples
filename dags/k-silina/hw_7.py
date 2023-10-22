from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    'hw_k-silina_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw5_k_silina',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    for i in range(10):
        task_bash = BashOperator(
            task_id = f'print_{i}',
            bash_command = f"echo {i}",
        )
    def print_context(task_number, ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(f"ts = {ts}, run_id = {run_id}")
        return 'Whatever you return gets printed in the logs'

    for i in range(20):
        task_python = PythonOperator(
            task_id='print_number_' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )
    task_bash >> task_python