"""
XCom: explicit
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'hw_8_e-leonenkov',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='The DAG transmits data through XCom explicitly',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['ex8'],
) as dag:

    def test_xcom_push(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )


    def test_xcom_pull(ti):
        ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='test_xcom_push'
        )


    t1 = PythonOperator(
        task_id='test_xcom_push',
        python_callable=test_xcom_push
    )

    t2 = PythonOperator(
        task_id='test_xcom_pull',
        python_callable=test_xcom_pull
    )


    t1 >> t2