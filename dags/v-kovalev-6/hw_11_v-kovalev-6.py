from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable


with DAG(
        'hw_11_v-kovalev-6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='First DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_11'],
) as dag:

    def get_airflow_var():
        is_startml = Variable.get("is_startml")
        print(is_startml)

    t1 = PythonOperator(
        task_id='get_airflow_var',
        python_callable=get_airflow_var,
    )