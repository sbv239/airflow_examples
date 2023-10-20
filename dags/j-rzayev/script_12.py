from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def print_variables():
    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
        'j-rzayev_task_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 20),
        catchup=False,
        tags=['task_12', 'lesson_11', 'j-rzayev'],
) as dag:
    task_1 = PythonOperator(
      task_id="user_w_like",
      python_callable=print_variables
    )
