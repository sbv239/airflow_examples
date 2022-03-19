from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'kittinwork',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 20),
    'retries': 0
}

dag = DAG(
    'hello_StartML',
    default_args=default_args,
    schedule_interval='00 12 * * 1'
)


def hello():
    print('Hello, StartML!')


t1 = PythonOperator(
    task_id='task1',
    python_callable=hello,
    dag=dag
)
