from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_d-ajmuratov-26_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    catchup=False,
    tags=['HW 3']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_the_context' + str(i),
            env={'NUMBER': str(i)},
            bash_command=f"echo $NUMBER"
        )
        t1.doc_md = dedent("""
            `code`, **bold text**, *cursive*
            
            new line
                           
            # Header
        """)
        t1

    def print_numbers(ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        print(f"task number is: {kwargs.get('task_number')}")

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_number' + str(i),
            python_callable=print_numbers,
            op_kwargs={'task_number': i},
        )
        t2.doc_md = dedent("""
            `code`, **bold text**, *cursive*
            
            new line
                           
            # Header
        """)
        t2
