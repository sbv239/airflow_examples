from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

Variable = 'is_startml'

def print_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")  
    print(is_startml)
    
with DAG(
    'hw_12_r-nazmiev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 1),
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='retrieve_top_liked_user',
        python_callable=print_variable,
    )
    task
