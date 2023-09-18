"""
DAG for task 9
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_t-togyzbaev_9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['t-togyzbaev']
) as dag:
    def put_in(ti):
        ti.xcom_push(key="sample_xcom_key", value="xcom test")

    def print_value(ti):
        val = ti.xcom_pull(key="sample_xcom_key", task_ids="put_value")
        print("valud in xcom: ", val)

    t1 = PythonOperator(
        task_id="put_value",
        python_callable=put_in
    )
    t2 = PythonOperator(
        task_id="print_value",
        python_callable=print_value
    )

    t1 >> t2
