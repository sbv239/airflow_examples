from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


with DAG(
        'hw_11_a-donskoj-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG in 11 step',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 25),
        catchup=False,
        tags=['task 11'],
) as dag:
    def get_variable():
        from airflow.models import Variable

        result = Variable.get('is_startml')
        print(result)

    t1 = PythonOperator(
        task_id='print_value',
        python_callable=get_variable
    )

    t1