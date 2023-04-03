from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_context(ts, run_id, **kwargs):
    print("task number is: {kw}".format(kw=kwargs['task_number']))
    print(ts)
    print(run_id)

with DAG(
    'tasks',
    description='Args in DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 31),
    catchup=False,
    tags=['task7'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_num_' + str(i),
            bash_command=f"echo {i}",
        )
        
    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='print_num_tasks_' + str(i),
            dag=dag,
            python_callable=print_context,
            op_kwargs={'task_number': i}
        )
