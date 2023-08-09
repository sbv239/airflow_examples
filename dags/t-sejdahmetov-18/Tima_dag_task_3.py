"""
simple dag
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'Tima_dag_task_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='1_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 6),
    catchup=False,
    tags=['tima'],
) as dag:

    run_this_first = DummyOperator(task_id = 'start_dag')
    run_after_bash = DummyOperator(task_id = 'wait_bush_commands')
    run_at_the_end = DummyOperator(task_id = 'stop')

    for i in range(10):
        bush_task = BashOperator(
            task_id = f'task_number_{i}',
            bash_command =f"echo{i}"
        )
        run_this_first >> bush_task>> run_after_bash
        bush_task.doc_md = dedent(
            f"""\
            #### Tash {i}  doc
            TEST DOC
            """
        )
    def print_context(ts,run_id,**kwargs):
        print(kwargs)
        print(f"task_number_is:{kwargs.get('task_number')}")
        print(run_id)
        print(ts)
        return 'Hello!'

    for i in range(20):
        python_task = PythonOperator(
            task_id = f'print_the_context_{i}',
            python_callable = print_context,
            op_kwargs = {"task_number" : i}
        )
        run_after_bash >> python_task >> run_at_the_end