from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def push(ti):
    ti.xcom_push(key='sample_xcom_key', value='xcom test')

def pull(ti):
    result = ti.xcom_pull(key='sample_xcom_key', task_ids = 'push_data')
    print(result)


with DAG(
    'hw_8_a-vdovenko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="Lesson 11 home work 8",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-vdovenko'],
) as dag:
    t1 = PythonOperator(
        task_id = 'push_data',
        python_callable = push
    )
    t2 = PythonOperator(
        task_id = 'pull_data', 
        python_callable = pull
    )
    t1 >> t2