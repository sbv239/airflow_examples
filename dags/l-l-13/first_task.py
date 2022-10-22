from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def printi(ds):
    return print(ds)


with DAG(
    'First task',
    default_args={
        'depends_on_past':False,
        'email':['norgello.lenia@yandex.ru'],
        'email_on_failure':True,
        'email_on_retry':True,
        'retries':3,
        'retry_delay':timedelta(minutes=3)
    },
    description='first task in lesson №11',
    schedule_interval=timedelta(days=3650),
    start_date=datetime(2022, 10, 20),
    catchup=False
) as dag:
    m1=BashOperator(
        task_id='command pwd',
        bash_command='pwd',
)

    m2=PythonOperator(
        task_id='logical date',
        python_callable=printi)
m1>>m2