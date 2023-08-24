from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_iter(task_number, **kwargs):
    print(kwargs)
    return f'task number is: {task_number}'


with DAG(
        'hw_n-shishkin_4',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_n-shishkin_4'],
) as dag:
    for i in range(30):
        if i < 10:
            t = BashOperator(
                task_id='bash_' + str(i + 1),
                bash_command=f'echo {i + 1}'
            )
        else:
            t = PythonOperator(
                task_id='python_' + str(i),
                python_callable=print_iter,
                op_kwargs={'task_number': i},
            )
        t.doc_md = dedent("""
        ### Task Documentation  
        **This task prints number of task**  
        If _i<10_ than use `BashOperator`  
        else use `PythonOperator`  
        """)
