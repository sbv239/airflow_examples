from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    "hw_al-shirshov_2",
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date=datetime.now()    
) as dag:

    t1=BashOperator(
        task_id = "run_pwd",
        bash_command = "pwd"
    )

    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)

    t2 = PythonOperator(
        task_id = "print_the_context",
        python_callable=print_context
    )

    t1 >> t2