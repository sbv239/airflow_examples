from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def get_variable():
    print(Variable.get('is_startml'))

with DAG(
    'hw_12_a-korotaeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG', schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 13),
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='get_var', python_callable=get_variable)

    t1