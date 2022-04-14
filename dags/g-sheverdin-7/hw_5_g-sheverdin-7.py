import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'g-sheverdin-7_task05',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='g-sheverdin-7_DAG_task05',
    start_date=datetime(2022, 2, 10),
    catchup=False,
    tags=['g-sheverdin-7-task05'],
) as dag:
    def print_task(num):
        print(f"task number is: {num}")

    for i in range(30):
        if i < 10:
            os.environ['NUMBER'] = str(i)
            bash_op = BashOperator(
                task_id='print_task'+str(i),
                bash_command="echo '{}'".format(os.environ['NUMBER'])
            )
        else:
            python_op = PythonOperator(
                task_id='task_number_' + str(i),
                python_callable=print_task,
                op_kwargs={'num': i}
            )

    bash_op >> python_op
