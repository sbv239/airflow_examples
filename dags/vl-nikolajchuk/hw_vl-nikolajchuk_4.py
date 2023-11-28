from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_vl-nikolajchuk_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description = 'Dag for hw3',
        schedule_interval=timedelta(days=1),
        start_date = datetime(2023, 11, 27),
        catchup=False,
        tags=['hw_3']
) as dag:
    def print_task(task, **kwargs):
        print(kwargs)
        print(task)

    for i in range(10):
        t1 = BashOperator(
            task_id=f'new_bash{i}',
            bash_command=f'echo{i}'
        )
    for i in range(20):
        t2 = PythonOperator(
            task_id=f'task_{i}',
            python_callable=print_task,
            op_kwargs={'task number is': f'task_{i}'}
        )
    t1.doc_md = dedent(
            """
        #### **Task Generator** `task_id=f"random_bash{i}"`
        *Creates tasks from 1 to 10*
        """
        )

    t2.doc_md = dedent(
            """
        #### **Task Printer** `task_id=f"task_{i}"`
        *Prints tasks by id*
        """
        )

