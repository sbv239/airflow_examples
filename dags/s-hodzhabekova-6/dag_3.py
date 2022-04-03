"""
HW 3 s-hodzhabekova-6 
dag documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_3_s-hodzhabekova-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    def print_context(ds, **kwargs):
        tn = kwargs['task_number']
        print("task number is: {tn}")

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id='print_bash_'+str(i),
                bash_command=f"echo {i}",
            )

        else:
            t2 = PythonOperator(
                task_id='print_ds_'+str(i),
                python_callable=print_context,
                op_kwargs={'task_number': i}
            )
    
    t1.doc_md = dedent(
        """\
        ### First Task Documentation
        task 1 runs `BashOperator` operator
        print **i** *from 0 to 9*
    """
    )

    t2.doc_md = dedent(
        """\
        ### Second Task Documentation
        task 2 calls `PythonOperator` *print_context* 
        which prints **task_number** *from 10 to 29*
    """
    )
