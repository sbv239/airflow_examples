from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
}

with DAG(
    'hw_4_d-iarastov-17',
    default_args = default_args,
    start_date=datetime(2023, 2, 1),
    description = "For loop in tasks iteration"
) as dag:
    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id=f'print_in_bash_{i}',
                bash_command=f"echo line number {i}"
                )
            t1.doc_md = dedent(
            f"""
    
                # This is BashOperator
                **some bold text**
                *with some italic text*
                'and a code line echo {i}`
            """)

        else:
            def print_task_number(ds, **kwargs):
                task_number = kwargs['task_number']
                print(f"task number is: {task_number}")


            t2 = PythonOperator(
                task_id=f'task_number_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
                )
            t2.doc_md = dedent(
                f"""
                    # This is PythonOperator
                    **some bold text**
                    *with some italic text*
                    'and a code line task number is: {i}`
                """
            )
    t1>>t2


