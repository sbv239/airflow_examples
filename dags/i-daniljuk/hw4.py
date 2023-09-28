"""
PythonOperator and BashOperator test
"""
from datetime import datetime, timedelta
# from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
    'hw_4_i-daniljuk',
    # Параметры по умолчанию для тасок
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    description='templated DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['templated'],
) as dag:
    
    i = 0
    while i < 10:
        task_bash = BashOperator(
            task_id='echo_for_' + str(i),
            bash_command=f"echo {i}"
        )
        task_bash.doc_md = """\
            #Documentation
            **task_bash** makes echo *ten* times
            contains function `echo {i}`
        """
        i += 1
