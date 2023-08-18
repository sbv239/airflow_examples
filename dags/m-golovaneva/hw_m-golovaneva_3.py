from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    "hw_m-golovaneva_task3",

    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
    },
    description='my DAG for 2d task Lecture 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,

    tags=['task3_L11']
) as dag:

    for i in range(10):
        task_BO = BashOperator(
            task_id= f"task_{i}",
            bash_command=f"echo {i}"
        )

    def print_t_number(task_number:int):
        print(f"task number is: {task_number}")

    for i in range(20):
        task_PO = PythonOperator(
            task_id= f"Python_task_{i}",
            python_callable=print_t_number,
            op_kwargs={"t_number":i}
        )

    task_BO >> task_PO