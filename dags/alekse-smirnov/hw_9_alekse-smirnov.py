from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def send_xcom_test(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def receive_xcom_test(ti):
    value = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="xcom_test_sender_task"
    )
    print(value)


with DAG(
    'hw_alekse-smirnov_9',
    default_args=default_args,
    description='DAG for Lesson #11 Task #9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 15),
    catchup=False,
    tags=['alekse-smirnov'],
) as dag:

    sender_task = PythonOperator(
        task_id="xcom_test_sender_task",
        python_callable=send_xcom_test
    )
    
    receiver_task = PythonOperator(
        task_id="xcom_test_receiver_task",
        python_callable=receive_xcom_test
    )

    sender_task >> receiver_task
