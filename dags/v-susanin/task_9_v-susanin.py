from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

def push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def pull(ti):
    xcom_test = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_xcom'
    )
    print(xcom_test)

with DAG(
        'v-susanin_task_9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='v-susanin_DAG_task_9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 26),
        catchup=False,
        tags=['DAG_task_9'],
) as dag:
    first = PythonOperator(
        task_id='push_xcom',
        python_callable=push
    )
    second = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull
    )
    first >> second
