from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_context():
    is_startml = Variable.get('is_startml')  # необходимо передать имя, заданное при создании Variable
    # теперь в is_startml лежит значение Variable
    print(is_startml)
    # return is_startml


with DAG(
    'r-kutuzov-1_dag_12-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Airflow lesson step 12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=['r-kutuzov-1_step_12'],
) as dag:
    
    task = PythonOperator(
        task_id=f'get_variable_with_Python', 
        python_callable=print_context,  # какую python команду выполнить в этом таске
    )

    task
