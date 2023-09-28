from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Function to push value to XCom
def push_to_xcom(ti, **kwargs):
    key = 'sample_xcom_key'
    value = 'xcom test'
    ti.xcom_push(key=key, value=value)

# Function to pull value from XCom and print it
def pull_from_xcom(ti, **kwargs):
    key = 'sample_xcom_key'
    value = ti.xcom_pull(key=key, task_ids='push_value_task')
    print(f"Value from XCom with key '{key}': {value}")

# Define the DAG, its schedule, and set it to run
dag = DAG(
    'hw_e-ansperi_9',
    default_args=default_args,
    description='A DAG with two PythonOperators working with XCom',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False
)

# Task using PythonOperator to push value to XCom
push_value_task = PythonOperator(
    task_id='push_value_task',
    python_callable=push_to_xcom,
    provide_context=True,
    dag=dag
)

# Task using PythonOperator to pull value from XCom and print it
pull_value_task = PythonOperator(
    task_id='pull_value_task',
    python_callable=pull_from_xcom,
    provide_context=True,
    dag=dag
)

# Set task order
push_value_task >> pull_value_task
