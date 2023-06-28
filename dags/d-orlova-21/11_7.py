from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG (
    'hw_d-orlova-21_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'dag for lesson 11.7',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 6, 28),
    catchup = False
) as dag:
    for i in range(10):
        task_1 = BashOperator(
            task_id = 'bash'+str(i),
            bash_command = f"echo {i}"
        )
    def task_number(task_number, ts, run_id, **kwargs): #здась добавила новые аргументы по заданию 11.7
        print (f'task number is: {task_number}')
        print(ts)
        print(run_id)

    for i in range(10, 30):
        task_2 = PythonOperator(
            task_id = 'task'+str(i),
            python_callable = task_number,
            op_kwargs={'task_number': int(i)} #здесь ничего не поменяла с задания 11.3
        )

    task_1.doc_md = dedent(
        """
        ###**Task Documentation**
        _Attribute 'doc_md' is for markdown_
        """
    )

    task_2.doc_md = dedent(
        """
        `for i in range(10, 30)
            print(f'task_number is {i}')`
        """
    )

    task_1 >> task_2