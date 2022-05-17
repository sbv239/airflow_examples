from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
from textwrap import dedent
from psycopg2.extras import RealDictCursor


def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get('is_startml')
    print(is_startml)
    return is_startml




with DAG(
        'hw_10_i-litvinov-8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='hw_10_i-litvinov-8',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 16),
        catchup=False,
        tags=['i-litvinov-8']
) as dag:
    t1 = PythonOperator(
        task_id='variable_example',
        python_callable=get_variable
    )

    t1

