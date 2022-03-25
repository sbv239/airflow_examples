from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_v-kovalev-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_3'],
) as dag:

    def task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(30):
        if i <= 10:
            t1 = BashOperator(
                task_id='bash_operator_' + str(i),
                bash_command=f"echo {i}",
            )
            t1.doc_md = dedent(
                f"""\
            #### Task Documentation
            This **task** *starts* bash_command = `echo {i}`
            """
            )
        else:
            t2 = PythonOperator(
                task_id='python_operator_' + str(i),
                python_callable=task_number,
                op_kwargs={'task_number': i}
            )
            t2.doc_md = dedent(
                f"""\
            #### Task Documentation
            This **task** *starts* `python_operator_{i}`
            """
            )