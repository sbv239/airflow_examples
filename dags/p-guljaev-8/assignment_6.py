"""
##Assignment 6 DAG documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
        'gul_assignment_6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Some description',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 28),
        catchup=False,
        tags=['gul_dag_6']
) as dag:
    for i in range(10):
        b_task = BashOperator(
            task_id='bash_task_' + str(i),
            bash_command=f'echo {i}'
        )


    def print_task_number(task_number, ts, run_id):
        print(f'task number={task_number}, ts={ts}, run_id={run_id}')
        return "If you see this message the task was successful"


    for j in range(20):
        p_task = PythonOperator(
            task_id='python_task_' + str(j),
            python_callable=print_task_number,
            op_kwargs={'task_number': j}
        )
    b_task >> p_task

    dag.doc_md = __doc__
