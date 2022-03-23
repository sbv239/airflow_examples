from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def push(ti):
    return "Airflow tracks everything"


def pull(ti):
    t = ti.xcom_pull(key='return_value', task_ids='push_data')
    print(t)


with DAG('hw_9_vorotnikov', default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}, start_date=datetime(2022, 3, 20), catchup=False) as dag:
    t1 = PythonOperator(task_id='push_data', python_callable=push)
    t2 = PythonOperator(task_id='pull_data', python_callable=pull)
    t1 >> t2
