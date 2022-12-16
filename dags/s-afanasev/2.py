from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_2_s-afanasev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 1, 1),
) as dag:

    def print_task_number(task_number):
    """Распечатать номер задачи"""
    print(f"task number is: {task_number}")


    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id="print_number",
                bash_command=f"echo{i}"
            )
        else:
            t2 = PythonOperator(
        task_id='print_task_number',
        python_callable=print_task_number,
        op_kwargs={'task_number': i},
    )

    t1 >> t2