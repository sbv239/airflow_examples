from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def return_string():
    return "Airflow tracks everything"

def pull_string(ti):
    print(ti.xcom_pull(
        key = 'return_value',
        task_ids = 'return_string'
    ))

with DAG(
    'hw_efelagereva_10',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = 'a simple dag',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 8, 21)
) as dag:
    t1 = PythonOperator(
        task_id = 'return_string',
        python_callable = return_string
    )
    t2 = PythonOperator(
        task_id = 'pull_string',
        python_callable = pull_string,
        provide_context = True
    )
    t1 >> t2
