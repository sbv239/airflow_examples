from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

def print_num(ds, **kwargs):
    print(f'task number is: {kwargs["number"]}')

with DAG(
    'hw_al-pivovarov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now()
) as dag:
    for i in range(10):
        t1 = BashOperator(task_id=f'echo_{i}', bash_command=f'echo {i}')
    for i in range(20):
        t2 = PythonOperator(task_id=f'print_{i}', python_callable=print_num, op_kwargs={'number': i})
    t1.doc_md = dedent("""
        ####Documentation for t1 
        with `code`, **bold** and *italic* text
    """)
    t2.doc_md = dedent("""
        ####Documentation for t2 
        with `code`, **bold** and *italic* text
    """)
    t1 >> t2