from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def variables():
    is_startml = Variable.get("is_startml")
    return print(is_startml)

with DAG(
    'hw_ni-nikitina_12', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Twelfth Task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 29),
    catchup=False
) as dag:
    
    t1 = PythonOperator(
        task_id='variables_basics', 
        python_callable=variables
    )
