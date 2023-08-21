from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_date(ds, **kwargs):
    """
    This function prints the current date received as a parameter.

    :param ds: The execution date in 'YYYY-MM-DD' format.
    :type ds: str
    :param kwargs: Additional keyword arguments.
    """
    print(f"The execution date is: {ds}")

default_args = {
    'owner': 'kimsanova',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hw_b-kimsanova-22_3',  
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Creating BashOperator tasks
bash_task = BashOperator(
    task_id='print_working_directory',
    bash_command='pwd',
    dag=dag,
)

# Creating PythonOperator tasks
python_task = PythonOperator(
    task_id='print_date_task',
    python_callable=print_date,
    provide_context=True,
    dag=dag,
)

# Set the dependency order
bash_task >> python_task
