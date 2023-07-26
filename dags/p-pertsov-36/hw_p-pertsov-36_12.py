from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")  # необходимо передать имя, заданное при создании Variable
    # теперь в is_startml лежит значение Variable
    print(is_startml)

with DAG(
    'hw_p-pertsov-36_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for p-pertsov-36 task 12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 22),
    catchup=False,
    tags=['pavelp_hw_12'],
) as dag:
    t1 = PythonOperator(
        task_id='return_variable',
        python_callable=get_variable,
    )

t1