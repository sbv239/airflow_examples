from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
def print_var():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)
with DAG(
    'hw_s-majkova_12',
    default_args = {
    'depens_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
    },
    description = 'first_dynamic_DAG',
    schedule_interval=timedelta(days=1),
    start_date = datetime(2023, 11, 25),
    catchup = False,
    tags = ['hw_12'],
) as dag:
    t1 = PythonOperator(
        task_id = 'is_startml',
        python_callable = print_var
    )
t1