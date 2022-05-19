from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.models import Variable


def print_var():
    is_startml = Variable.get('is_startml')
    print(is_startml)


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


with DAG(
    'lesson11_s-kosouhov_task_11',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 10),
    catchup=False,
    tags=['task_11'],
) as dag:

    task_1 = PythonOperator(
        task_id = f"task_print_var",
        python_callable=print_var,
    )

    task_1
    