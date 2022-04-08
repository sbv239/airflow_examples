from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable


with DAG(
        'hw_11_k.alekseev-9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Eleventh DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['T1'],
) as dag:

    def var():
        is_startml = Variable.get("is_startml")
        print(is_startml)

    t1 = PythonOperator(
        task_id='var',
        python_callable=var,
    )