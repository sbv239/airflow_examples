from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    'ignatev_dag_02',
    default_args=default_args,
    start_date=datetime(2022, 4, 15),
    max_active_runs=1,
    schedule_interval=timedelta(days = 1),
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )

    def print_task(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_' + str(i),
            python_callable=print_task,
            op_kwargs={'task_number':i}
        )