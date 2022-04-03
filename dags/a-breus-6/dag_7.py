from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta

with DAG(
    'task_11_breus',

    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    'retry_delay': timedelta(minutes=5), 
    },

    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:

    def print_var():
        
        is_startml = Variable.get("is_startml")
        print(is_startml)

    print_variable = PythonOperator(
        task_id = 'print_var',
        python_callable=print_var
    )
        