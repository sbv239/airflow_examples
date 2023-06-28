from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
    'hw_12_m-sazonov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='hw_12_m-sazonov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 27),
    catchup=False,
) as dag:

    def print_variable():
        res = Variable.get("is_startml")
        print(res)

    t1 = PythonOperator(
        task_id='print_variable',
        python_callable=print_variable,
    )

    t1

