"""
XCom: implicit
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'hw_9_e-leonenkov',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='The DAG transmits data through XCom implicitly',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['ex9'],
) as dag:

    def test_xcom_push(ti):
        return 'Airflow tracks everything'


    def test_xcom_pull(ti):
        ti.xcom_pull(
            key='return_value',
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