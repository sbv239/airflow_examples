from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_ds(ds, **kwargs):
    print(ds, 'any other message')

with DAG(
    'hw_al-pivovarov_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now()
) as dag:
    t1 = BashOperator(task_id='pwd', bash_command='pwd')
    t2 = PythonOperator(task_id='print_ds', python_callable=print_ds)