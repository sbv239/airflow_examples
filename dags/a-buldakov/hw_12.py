from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from textwrap import dedent


def read_variable():
    print(Variable.get("is_startml"))

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'hw_12_a-buldakov',
    default_args = default_args,
    start_date = datetime.now(),
    tags=['a-buldakov']
)

t1 = PythonOperator(
    task_id='variable_task',
    python_callable=read_variable,
    dag = dag
)

t1