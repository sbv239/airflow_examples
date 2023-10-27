from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'hw_a-kramarenko_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='lesson 11 task 10 DAG',
    start_date=datetime(2023, 10, 26),
) as dag:

    def pushing_xcom(ti):
        return 'Airflow tracks everything'

    task1 = PythonOperator(
        task_id='pushing_xcom',
        python_callable=pushing_xcom,
    )

    def pulling_xcom(ti):
        print(ti.xcom_pull(
            key='return_value',
            task_ids='pushing_xcom',
        ))

    task2 = PythonOperator(
        task_id='pulling_xcom',
        python_callable=pulling_xcom,
    )
    task1>>task2