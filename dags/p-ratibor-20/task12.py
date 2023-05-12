from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'ratibor_task12',
    start_date=datetime(2023, 5, 11),
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:

    def print_variable():
        from airflow.models import Variable
        print(Variable.get("is_startml"))

    xcom_setter = PythonOperator(
        task_id='print_is_startml',
        python_callable=print_variable
    )
