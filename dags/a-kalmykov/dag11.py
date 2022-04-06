from airflow.providers.postgres.operators.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def pull_xcom():
    print(Variable.get("is_startml"))


with DAG(
        dag_id='a-kalmykov-dag-11',
        default_args=default_args,
        description='Dag 11 Kalmykov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 4),
        catchup=False,
        tags=['a-kalmykov'],
) as dag:
    t = PythonOperator(task_id='print_startml',
                       python_callable=pull_xcom)
    t
