from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_8_a-donskoj-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG in 9 step',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 25),
        catchup=False,
        tags=['task 9'],
) as dag:
    def set_xcom_func(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    t1 = PythonOperator(
        task_id='set_xcom',
        python_callable=set_xcom_func,
    )

    def get_xcom_func(ti):
        value_read = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='set_xcom'
        )
        print(value_read)

    t2 = PythonOperator(
        task_id='get_xcom',
        python_callable=get_xcom_func,
    )

    t1 >> t2