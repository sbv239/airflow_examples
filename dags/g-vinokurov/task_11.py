from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable


def print_func():
    var = Variable.get("is_startml")
    print (var)

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('hw_g-vinokurov_11',
    default_args=default_args,
        description='DAG in task_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 29),
        catchup=False,
) as dag:

    operator = PythonOperator(
        task_id='user_by_likes',
        python_callable=print_func,
    )

    operator