from datetime import datetime, timedelta
from textwrap import dedent

from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator




with DAG(
    "more_arguments",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    description= "first try of starting DAG's",
    schedule_interval= timedelta(days=1),
    start_date= datetime(2023, 4, 17),
    catchup= False,
    tags=['idk7']
) as dag:

    for i in range(10):

        t1 = BashOperator(
            task_id='bash_' + str(i),
            bash_command= f"echo $NUMBER",
            env = {"NUMBER": i}
        )
    def print_task_number(ts, run_id, task_number):
        print(ts, run_id)
        return f"Something == {task_number}"

    for j in range(20):

        t2 = PythonOperator(
            task_id='python_task_' + str(j),
            python_callable=print_task_number,
            op_kwargs={'task_number': j}
        )

    t1 >> t2