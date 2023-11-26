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

with DAG(
    'hw_kamilahmadov_12',
    default_args=default_args,
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_12"]
) as dag:

    def print_t1(**kwargs):
        is_startml = Variable.get("is_startml")
        print(is_startml)

    t1 = PythonOperator(
        task_id="t1_value",
        python_callable=print_t1,
    )

    t1
