import pwd
from airflow import DAG
from airlow.operators.bash import BashOpertor
from airlow.operators.python import PythonOpertor

def print_ds(ds):
    print(ds)
    return "good"

with DAG(
    'task1', 
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='study DAG',
    tags=['task_muzalev']
) as dag:
    t1 = BashOpertor(
        task_id='your directory',
        bash_command='pwd'
    )

    t2 = PythonOpertor(
        task_id='print DS'
        python_callable=print_ds
    )

    t1>>t2