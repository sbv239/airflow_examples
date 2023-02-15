from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'hm_6_i-li',
    default_args={
        'dependes_on_past': False,
        'email' : ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2023,2,13)
) as dag:
    def print_task(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(ts, run_id)


    for task in range(10):
        t = BashOperator(
            env={'NUMBER': task},
            task_id='task_' + str(task),
            bash_command='echo $NUMBER'
        )
    for task in range(20):
        t = PythonOperator(
            task_id='task' + str(task + 10),
            python_callable=print_task,
            op_kwargs={'task_number': task}
        )
