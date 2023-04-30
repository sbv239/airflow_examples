from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_garachev_12_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_12'],
) as dag:
    

    def get_user():
        print(Variable.get("is_startml"))

    t1 = PythonOperator(
        task_id='hw_12_garachev',
        python_callable=get_user
    )

    t1