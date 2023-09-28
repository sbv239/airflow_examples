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

# Function to return a string
def push_string_to_xcom(**kwargs):
    return "Airflow tracks everything"

# Function to pull the string from XCom and print it
def pull_string_from_xcom(ti, **kwargs):
    value = ti.xcom_pull(key='return_value', task_ids='push_string_task')
    print(f"Value from XCom: {value}")

# Define the DAG, its schedule, and set it to run
dag = DAG(
    'hw_e-ansperi_10',
    default_args=default_args,
    description='A DAG with two PythonOperators demonstrating implicit XCom',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False
)

# Task using PythonOperator to push a string to XCom
push_string_task = PythonOperator(
    task_id='push_string_task',
    python_callable=push_string_to_xcom,
    provide_context=True,
    dag=dag
)

# Task using PythonOperator to pull the string from XCom and print it
pull_string_task = PythonOperator(
    task_id='pull_string_task',
    python_callable=pull_string_from_xcom,
    provide_context=True,
    dag=dag
)

# Set task order
push_string_task >> pull_string_task
