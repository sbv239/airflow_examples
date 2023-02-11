from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent


with DAG(
    'hw_7_e-morzavchenkova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 11),
    catchup=False,
    tags=['example'],
) as dag:
    def print_context(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts, run_id)


    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id='print_pwd' + str(i),
                bash_command=f'echo {i}',
            )
        else:
            t2 = PythonOperator(
                task_id='print_the_context' + str(i),
                python_callable=print_context,
                op_kwargs={'task_number': i}
            )

    t1 >> t2