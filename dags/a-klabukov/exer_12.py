from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python_operator import PythonOperator

def get_var():
    from airflow.models import Variable

    is_prod = Variable.get("is_startml")
    print(is_prod)


with DAG(
          'a-klabukov_hw_12',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
          },
          description='A simple tutorial DAG',
          schedule_interval=timedelta(days=1),
          start_date=datetime(2022, 1, 1),
          catchup=False,
          tags=['example'],
) as dag:


  task_for_get = PythonOperator(
    task_id = 'task_id_for_get_max',
    python_callable = get_var,

  )