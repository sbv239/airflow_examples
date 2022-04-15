from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def task_1(ti):
    return 'Airflow tracks everything'


def task_2(ti):
    xcom_key = ti.xcom_pull(
        key='return_value',
        task_ids='a_loshkarev_9_1'
    )

    print(xcom_key)


with DAG(
    'aloshkarev_task_9',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 12),
    catchup=False,
) as dag:
    t1 = task = PythonOperator(
            task_id='a_loshkarev_9_1',
            python_callable=task_1
        )

    t2 = task = PythonOperator(
            task_id='a_loshkarev_9_2',
            python_callable=task_2
        )

    t1 >> t2
