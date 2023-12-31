from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        "hw_9_s-kim",
        description="Homework 9",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 8, 1),
        catchup=True,
        tags=["s-kim"],
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
) as dag:
    def save_to_xcom(ti):
        ti.xcom_push(key="sample_xcom_key",
                     value="xcom test")

    def print_xcom(ti):
        xcom_value = ti.xcom_pull(key="sample_xcom_key",
                                  task_ids="save_to_xcom")

    t1 = PythonOperator(
        task_id="save_to_xcom",
        python_callable=save_to_xcom
    )

    t2 = PythonOperator(
        task_id="print_xcom",
        python_callable=print_xcom
    )

    t1 >> t2