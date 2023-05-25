from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.models import Variable


with DAG(

    'hw_12_r-muratov-9',

    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    schedule_interval=timedelta(days=1),

    start_date=datetime(2023,5,20),

    catchup=False
    ) as dag:

    def get_var():
        is_startml = Variable.get("is_startml")
        print(is_startml)
    
    task1 = PythonOperator(
        task_id='get_var_is_startml',
        python_callable=get_var
    )

    task1
