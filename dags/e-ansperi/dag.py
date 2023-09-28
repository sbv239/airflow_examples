from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

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

# Updated Python function for the PythonOperator
def print_task_details(ts, run_id, task_number, **kwargs):
    print(f"Timestamp: {ts}")
    print(f"Run ID: {run_id}")
    print(f"Task Number: {task_number}")

# Define the DAG, its schedule, and set it to run
dag = DAG(
    'hw_e-ansperi_7',
    default_args=default_args,
    description='A DAG with tasks declared using a for loop and updated PythonOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False
)

# First 10 tasks using BashOperator
for i in range(10):
    bash_task = BashOperator(
        task_id=f'bash_task_{i}',
        bash_command=f'echo {i}',
        dag=dag
    )

# Next 20 tasks using updated PythonOperator
for i in range(10, 30):
    python_task = PythonOperator(
        task_id=f'python_task_{i}',
        python_callable=print_task_details,
        op_kwargs={'task_number': i},
        provide_context=True,
        dag=dag
    )
