from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime

with DAG(
    '11_11_martsinyuk',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=7),
    start_date=datetime(2022, 9, 21),
    catchup=False,
) as dag:
    def get_var():
        print(Variable.get("is_startml"))

    t1 = PythonOperator(
        task_id="Variables",
        python_callable=get_var
    )