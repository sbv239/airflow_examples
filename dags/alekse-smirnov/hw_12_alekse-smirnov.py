from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_variable_is_startml():
    is_startml = Variable.get("is_startml")
    print(is_startml)
    

with DAG(
    'hw_alekse-smirnov_12',
    default_args=default_args,
    description='DAG for Lesson #11 Task #12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 16),
    catchup=False,
    tags=['alekse-smirnov'],
) as dag:

    print_var_task = PythonOperator(
        task_id="print_variable_is_startml_task",
        python_callable=print_variable_is_startml
    )
