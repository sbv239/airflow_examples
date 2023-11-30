from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_g-hristov_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 28),
    catchup=False,
) as dag:

    def get_variable():
        from airflow.models import Variable
        is_startml= Variable.get("is_startml")
        print(is_startml)
        return is_startml



    taskpopu = PythonOperator(
        task_id='hw_g-hristov_12_PO',
        python_callable=get_variable,
    )
