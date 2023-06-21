from airflow.providers.postgres.operators.postgres import PostgresHook
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def valiable_get():
    is_startml = Variable.get('is_startml')
    print(is_startml)


with DAG(
        'hw_e-stepanjan_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Connection to Postgre base',
        start_date=datetime(2023, 6, 20),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['e-stepanjan', 'step_12']
) as dag:
    t1 = PythonOperator(
        task_id='variables',
        python_callable=valiable_get
    )
