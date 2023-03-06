from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.models import Variable

def get_variable():
    startml = Variable.get("is_startml")
    print(startml)

           
with DAG(
    'hw11_12_r-safarov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Task_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_11_r-safarov']
) as dag:
    t1 = PythonOperator(
        task_id="print_variable",
        python_callable=get_variable
    )

t1