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


def send_xcom_return_value():
    return "Airflow tracks everything"


def receive_xcom_return_value(ti):
    value = ti.xcom_pull(
        key="return_value",
        task_ids="xcom_return_value_sender"
    )
    print(value)


with DAG(
    'hw_alekse-smirnov_10',
    default_args=default_args,
    description='DAG for Lesson #11 Task #10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 15),
    catchup=False,
    tags=['alekse-smirnov'],
) as dag:

    sender_task = PythonOperator(
        task_id="xcom_return_value_sender",
        python_callable=send_xcom_return_value
    )
    
    receiver_task = PythonOperator(
        task_id="xcom_return_value_receiver",
        python_callable=receive_xcom_return_value
    )

    sender_task >> receiver_task
