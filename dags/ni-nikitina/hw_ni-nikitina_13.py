from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

def branch():
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def first():
    return print("StartML is a starter course for ambitious people")

def second():
    return print("Not a startML course, sorry")

with DAG(
    'hw_ni-nikitina_13', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Thirteenth Task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 29),
    catchup=False
) as dag:

    t0 = BranchPythonOperator(
        task_id='BranchingOperator', 
        python_callable=branch
    )

    t1 = PythonOperator(
        task_id='startml_desc', 
        python_callable=first
    )

    t2 = PythonOperator(
        task_id='not_startml_desc', 
        python_callable=second
    )

    start = DummyOperator(
                task_id='start'
        )

    stop = DummyOperator(
                task_id='stop'
        )

    start >> t0 >> [t1, t2] >> stop