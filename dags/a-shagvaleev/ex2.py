from datetime import datetime, timedelta
# from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'ex2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 6, 19),
) as dag:
    t1 = BashOperator(
        task_id = "simple_bash_task",
        bash_command = 'pwd'
    )


    def ds_func(ds):
        """Simple example for PythonOperator"""
        print(ds)
        return "There simple text"

    t2 = PythonOperator(
        task_id = "simple_python_task",
        python_callable = ds_func
    )

    t1 >> t2