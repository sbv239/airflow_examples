from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def get_var():
    print(Variable.get('is_startml'))

with DAG(
    'a-igumnov_task_12',

    default_args = {
    'depends_on_past' : False,
    'email': ['airflow@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
    },
    description = 'hw_12_a-igumnov',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 1, 1),
    catchup = False,
    tags = ['hw_12_a-igumnov_variable']

) as dag:
    
    t1 = PythonOperator(
        task_id = 'variable',
        python_callable = get_var
    )

    t1
