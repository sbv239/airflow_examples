from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_date(ds, **kwargs):
    print("Current Date:", ds)

default_args = {
    'owner': 'kimsanova',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hw_b-kimsanova-22_1',  
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

bash_task = BashOperator(
    task_id='print_working_directory',
    bash_command='pwd',
    dag=dag,
)

python_task = PythonOperator(
    task_id='print_date_task',
    python_callable=print_date,
    provide_context=True,
    dag=dag,
)

bash_task >> python_task  # Set the dependency order

