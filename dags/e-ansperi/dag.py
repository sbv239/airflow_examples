from datetime import datetime, timedelta
from airflow import DAG
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

# Define the templated command
templated_command = '''
for i in {0..4}; do
    echo "Value of ts: {{ ts }}"
    echo "Value of run_id: {{ run_id }}"
done
'''

# Define the DAG, its schedule, and set it to run
dag = DAG(
    'hw_e-ansperi_5',
    default_args=default_args,
    description='A DAG with a single BashOperator using templated command',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False
)

# Task using BashOperator with templated command
bash_task = BashOperator(
    task_id='print_ts_and_run_id',
    bash_command=templated_command,
    dag=dag
)
