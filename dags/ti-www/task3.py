from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    "ti-www_task3",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="A simple DAG for task 2",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 19),
    catchup=False,
    tags=["ti-www"],
) as dag:

    for i in range(10):

        t1 = BashOperator(
            task_id="my_echo_loop_" + str(i),
            bash_command=f"echo {i}",
        )
    
    def print_task_num(task_number):
        print(f"task number is: {task_number}")

    for i in range(10, 30):
        
        t2 = PythonOperator(
            task_id="my_print_task_" + str(i),
            python_callable=print_task_num,

            op_kwargs={'task_number': i},
        )

    t1 >> t2