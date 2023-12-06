from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get('is_startml')
    print(is_startml)

with DAG(
    'm-mihail-24_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example_7'],
) as dag:
    p12 = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable
    )