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


def get_variable():
    print(Variable.get("is_startml"))


with DAG(
        "hw_12_n-efremov",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 22),
        catchup=False,
        tags=['n-efremov'],
) as dag:
    task_1 = PythonOperator(
        task_id='print_variable',
        python_callable=get_variable,
    )
    task_1
