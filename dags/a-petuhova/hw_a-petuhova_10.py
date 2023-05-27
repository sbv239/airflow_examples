from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def print_variables():
    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
        'a-petuhova_step12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Tasks for step 12 a-petuhova',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 26),
        catchup=False,
        tags=['a-petuhova'],
) as dag:
    task_1 = PythonOperator(
      task_id="user_like",
      python_callable=print_variables
    )