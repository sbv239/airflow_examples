from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'k-kavitsjan-18_task_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='task_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['k-kavitsjan-18'],
    ) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f"date_{i}",
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)}
        )


    def print_task_id(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(f"ts is: {ts}")
        print(f"run_id is: {run_id}")


    for i in range(20):
        t2 = PythonOperator(
            task_id='Python_Task_' + str(i),
            python_callable=print_task_id,
            op_kwargs={'task_number': i, 'ts': '{{ ts }}', 'run_id': '{{ run_id }}'}
        )

    t1 >> t2