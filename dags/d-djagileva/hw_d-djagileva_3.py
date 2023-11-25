from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_d-djagileva_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }, 
    description='hw_d-djagileva_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_d-djagileva_2']
) as dag:

    for i in range(10):
        task = BashOperator(
            task_id='cycle',  
            bash_command=f"echo {i}",  
        )

    def print_context( task_number):
        print(task_number)
        return f'task number is: {task_number}'

    for i in range(20,31):
        task = PythonOperator(
            task_id = 'task' + str(i),  
            python_callable = print_context,
            op_kwargs = {'task_number': i},
        )
