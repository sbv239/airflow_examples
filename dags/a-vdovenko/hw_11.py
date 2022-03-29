from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python import PythonOperator

def variable():
    from airflow.models import Variable
    print(Variable.get('is_startml'))


with DAG(
    'hw_11_a-vdovenko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="Lesson 11 home work 11",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-vdovenko'],
) as dag:
    task = PythonOperator(
        task_id = "print_variable",
        python_callable = variable
    )
    task