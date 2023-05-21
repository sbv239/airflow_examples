from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def push_data(ti):
    return "Airflow tracks everything"

def pull_data(ti):
    result = ti.xcom_pull(
        key = "return_value",
        task_ids = "push_value"
    )




with DAG(
    "hw_al-shirshov_10",
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date=datetime(2023,5,20),
    catchup=True   
) as dag:
    t1 = PythonOperator(
        task_id = "push_value",
        python_callable = push_data
    )

    t2 = PythonOperator(
        task_id = "pull_data",
        python_callable = pull_data
    )

    t1 >> t2
