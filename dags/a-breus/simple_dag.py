from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def task_1():
    print('Task 1')


def task_2():
    print('Task 2')


def task_3():
    print('Task 3')


default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG('simple_dag_3',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 1)
        ) as dag:


    t1 = PythonOperator(task_id='task_1', python_callable=task_1)
    t2 = PythonOperator(task_id='task_2', python_callable=task_2)
    t3 = PythonOperator(task_id='task_3', python_callable=task_3)


    t1 >> t2 >> t3