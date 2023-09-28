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

# Define the Python function that will be run by the PythonOperator
def print_hello():
    print("Hello, World!")

# Define the DAG, its schedule, and set it to run
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='An example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False
)

# Define the task using a PythonOperator
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)
