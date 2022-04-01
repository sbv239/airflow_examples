from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

with DAG(
    'task_11_grjaznov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='task_11_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False,
    tags=['hw_11_a-grjaznov-5'],
) as dag:

    def func():
        print(Variable.get('is_startml'))

    t1 = PythonOperator(
        task_id='print_variable',
        python_callable=func
    )

    t1