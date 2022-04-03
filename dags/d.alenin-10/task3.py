from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


with DAG(
    'hw_3_d.alenin-10',
    default_args=default_args,
    description='Simple first dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    t0 = BashOperator(
        task_id=f"print_0",
        bash_command=f"echo 0"
    )

    for i in range(1, 10):
        t = BashOperator(
            task_id=f"print_{i}",
            bash_command=f"echo {i}"
        )
        t0 >> t

    def print_task_number(task_number):
        print(f"task number is: {task_number}")

    t1 = PythonOperator(
        task_id='print_date',
        python_callable=print_task_number,
        op_kwargs={'task_number': 10}
    )

    for i in range(11, 30):
        t = PythonOperator(
            task_id='print_date',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )
        t1 >> t
