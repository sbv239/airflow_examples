from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def push_xcom(ti):
    ti.xcom_push(key='sample_xcom_key', value='xcom test')

def pull_xcom(ti):
    result = ti.xcom_pull(key='sample_xcom_key', task_ids='push_xcom')
    print(result)

with DAG(
    'hw_al-pivovarov_9',
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
    t1 = PythonOperator(task_id='push_xcom', python_callable=push_xcom)
    t2 = PythonOperator(task_id='pull_xcom', python_callable=pull_xcom)
    t1 >> t2