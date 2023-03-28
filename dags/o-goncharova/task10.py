from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def push_data():
    return "Airflow tracks everything"
def pull_data(ti):
    output = ti.xcom_pull(
        key='return_value',
        task_ids='push'
    )

with DAG(
    'dag_task_10-o-goncharova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    start_date = datetime(2023, 3, 27)
) as dag:
    t1 = PythonOperator(
        task_id='push',
        python_callable=push_data
    )
    t2 = PythonOperator(
        task_id='pull',
        python_callable=pull_data
    )
    t1 >> t2