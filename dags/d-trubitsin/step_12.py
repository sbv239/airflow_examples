from airflow import DAG

from datetime import timedelta, datetime

from airflow.operators.python import PythonOperator
from airflow.models import Variable


def get_variable(ti):
    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
    'hw_d-trubitsin_12',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 26),
    catchup=False,
    tags=['d-trubitsin_12'],
) as dag:

    t1 = PythonOperator(
        task_id='gat_variable',
        python_callable=get_variable,
    )
