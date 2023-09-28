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

# Define the Python function for the PythonOperator
def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

# Define the DAG, its schedule, and set it to run
dag = DAG(
    'HW_2_e-ansperi',
    default_args=default_args,
    description='A refined DAG with BashOperator and PythonOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False
)

# Define the task using a BashOperator
bash_task = BashOperator(
    task_id='print_working_directory',
    bash_command='pwd',
    dag=dag
)

# Define the task using a PythonOperator
python_task = PythonOperator(
    task_id='print_ds_and_context',
    python_callable=print_context,
    provide_context=True,  # This is required to access the ds variable and other context
    dag=dag
)

# Set the task order
bash_task >> python_task
