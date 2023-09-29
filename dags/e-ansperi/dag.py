from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

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

# Function to retrieve and print the value of the Variable 'is_startml'
def print_variable_value(**kwargs):
    variable_value = Variable.get('is_startml')
    print(f"The value of the variable 'is_startml' is: {variable_value}")

# Define the DAG, its schedule, and set it to run
dag = DAG(
    'hw_e-ansperi_12',
    default_args=default_args,
    description='A DAG with a PythonOperator to print the value of the Variable is_startml',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False
)

# Task using PythonOperator to retrieve and print the Variable's value
print_variable_task = PythonOperator(
    task_id='print_variable_task',
    python_callable=print_variable_value,
    provide_context=True,
    dag=dag
)
