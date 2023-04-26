"""
step_10 DAG
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator


with DAG(
    'hw_r-shahvaly_10',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for step_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['r-shahvaly'],
) as dag:

        def push_xcom(ti):
            return "Airflow tracks everything"

        def get_xcom(ti):
            result = ti.xcom_pull(
                key="return_value",
                task_ids="PythonOperator1"
            )
            print(result)

        task1 = PythonOperator(
            task_id="PythonOperator1",
            python_callable=push_xcom,
        )

        task2 = PythonOperator(
            task_id="PythonOperator2",
            python_callable=get_xcom,
        )

        task1 >> task2