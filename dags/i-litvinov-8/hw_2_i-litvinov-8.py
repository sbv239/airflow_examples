'''
# Task 2 Documentation

В прошлом примере все задачи в `DAG` были объявлены **явно**.
Однако это не единственный способ задать DAG: можно использовать всю силу цикла *for* для объявления задач.
'''
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
from textwrap import dedent


def task_id_print(task_number):
    return f"task number is: {task_number}"


with DAG(
        'hw_2_i-litvinov-8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='2_Task_DAG_i-litvinov-8',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 16),
        catchup=False,
        tags=['i-litvinov-8']
) as dag:

    for i in range(30):
        if i < 10:
            t = BashOperator(
                    task_id=f'task_{i}',
                    bash_command=f"echo {i}",
                    dag=dag
            )
        else:
            t = PythonOperator(
                    task_id=f'task_{i}',
                    python_callable=task_id_print,
                    op_kwargs={'task_number': i}
            )
        t.doc_md = dedent(
            """
            # Task Documentation
            
            `This` **is** *task*
            """
        )

    dag.doc_md = __doc__

