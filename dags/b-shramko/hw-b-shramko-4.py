from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('hw-b-shramko-2',
         default_args={
             'depends_on_past': False,
             'email': ['airflow@example.com'],
             'email_on_failure': False,
             'email_on_retry': False,
             'retries': 1,
             'retry_delay': timedelta(minutes=5),
         },
         start_date=datetime(2022, 1, 1),
         catchup=False
         ) as dag:
    t1 = BashOperator(
        task_id='print_dir',
        bash_command='pwd'
    )

    t1.doc_md = dedent(
        """
        # My doc
        Inline `code`
        # h1 Heading
        *This is italic text*
        **This is bold text**
        """
    )


    def print_context(ds, **kwargs):
        print(ds)
        print("One more print by SBV")


    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_context
    )
