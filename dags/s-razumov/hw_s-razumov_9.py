from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def get_sample_xcom_key(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def analyze_sample_xcom_key(ti):
    xcom_value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='xcom_test'
    )
    print(xcom_value)


with DAG(
        'hw_s-razumov_9',
        default_args={
            # Если прошлые запуски упали, надо ли ждать их успеха
            'depends_on_past': False,
            # Кому писать при провале
            'email': ['airflow@example.com'],
            # А писать ли вообще при провале?
            'email_on_failure': False,
            # Писать ли при автоматическом перезапуске по провалу
            'email_on_retry': False,
            # Сколько раз пытаться запустить, далее помечать как failed
            'retries': 1,
            # Сколько ждать между перезапусками
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        start_date=datetime(2023, 1, 1),
        tags=['hw_s-razumov_9'],
) as dag:

    t1 = PythonOperator(
        task_id="xcom_add",
        python_callable=get_sample_xcom_key,
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=analyze_sample_xcom_key,
    )

    t1 >> t2