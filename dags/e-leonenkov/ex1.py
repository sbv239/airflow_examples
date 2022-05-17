"""
First DAG documentation
This DAG consists of Bash and Python tasks
Bash: print the current directory
Python: get `ds` and print text
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'First DAG',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple DAG with two tasks',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example1'],
) as dag:

    t1 = BashOperator(
        task_id='print_dir',
        bash_command='pwd',
    )


    def print_context(ds, **kwargs):
        print(ds)
        return "Start time is returned!"


    t2 = PythonOperator(
        task_id='get ds',
        depends_on_past=False,  # переопределили настройку из DAG
        bash_command='sleep 5',
        retries=3,  # тоже переопределили retries (было 1)
    )
    t1.doc_md = dedent(
        """\
    #### Task 1 Documentation
    Print the current directory
    """
    )

    t2.doc_md = dedent(
        """\
    #### Task 2 Documentation
    Get `ds` and print text
    """
    )

    dag.doc_md = __doc__

    t1 >> t2