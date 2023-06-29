from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def xcom_push(ti):
    return 'Airflow tracks everything'

def xcom_pull(ti):
    test = ti.xcom_pull(
        key='return_value', #ключ return_value предполагает неявную передачу данных через XCOM
        task_ids='get_data'
    )
    print(test)

# Default settings applied to all tasks
with DAG (
    'hw_d-orlova-21_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'dag for lesson 11.10',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 6, 28),
    catchup = False
) as dag:

    get_data = PythonOperator(
        task_id = 'get_data',
        python_callable=xcom_push
    )
    analyse_data = PythonOperator(
        task_id = 'analyze_data',
        python_callable=xcom_pull
    )

    get_data >> analyse_data