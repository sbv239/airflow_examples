from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def print_variable():
    print(Variable.get('is_startml'))
    return 'print is_startml'


with DAG(
        'task_12_dm-antonov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 4, 23),
        catchup=False,
        tags=['task_12']
) as dag:
    t1 = PythonOperator(
        task_id='print_variable',
        python_callable=print_variable
    )
