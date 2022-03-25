import os
from datetime import timedelta, datetime
from textwrap import dedent

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_5_t-volkov-5',
    default_args=default_args,
    description='God bless my creature',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    for i in range(10):
        os.environ['NUMBER'] = str(i)
        t1 = BashOperator(
            task_id='looping_bash_operator_with_NUMBER_' + str(i),
            bash_command='echo $NUMBER'
        )
        t1.doc_md = dedent(
            """\
        #### Task 1 Documentation
        **current** _bash_ command loops `echo $NUMBER`

        """
        )  
   
        t1
