from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def push_xcom( ti):
    ti.xcom_push(
        key = "sample_xcom_key",
        value = "xcom test"
    )

def pull_xcom(ti):
    print (ti.xcom_pull(
        key = "sample_xcom_key",
        task_ids = 'push_xcom'
    ))



with DAG(
    'hw_efelagereva_9',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = 'a dag with a simple xcom',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 8, 21)
) as dag:
    t1 = PythonOperator(
        task_id = 'push_xcom',
        python_callable = push_xcom,
        provide_context = True
    )
    t2 = PythonOperator(
        task_id = 'pull_xcom',
        python_callable = pull_xcom,
        provide_context = True
    )
    t1 >> t2
