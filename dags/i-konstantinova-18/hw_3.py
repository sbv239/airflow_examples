from datetime import datetime, timedelta

from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'task_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='task_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['i-konstantinova-18'],
    ) as dag:

    for i in range(10):
        task1 = BashOperator(
            task_id=f"date_{i}",
            bash_command=f"echo {i}"
        )

    def print_task_context(ds, **kwargs):
        print(kwargs)
        print(f"task number is: {kwargs.get('task_number')}")
        print(ds)
        return 'Somw text'

    for i in range(20):
        task2 = PythonOperator(
            task_id=f"task_con{i}",
            python_callable=print_task_context,
            op_kwargs={"task_number": i},
        )

    dag.doc_md=__doc__

    task1 >> task2