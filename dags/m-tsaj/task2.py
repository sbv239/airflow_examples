from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
        'first_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='First dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
        tags=['example'],
) as dag:
    t1 = BashOperator(
        task_id='print_working_directory',
        bash_command='pwd',
    )

    t1.doc_md = dedent(
        """
        #### Task 1 documentation
        This task executes the `pwd` bash command
        """
    )


    def print_ds(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='print_the_ds',
        python_callable=print_ds,
    )

    t2.doc_md(
        """
        #### Task 2 documentation
        This task prints the **ds** variable
        """
    )

    dag.doc_md(
        """
        #### first_dag documentation
        A simple dag featuring a bash- and a python- operators
        """
    )

    t1 >> t2
