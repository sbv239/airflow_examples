from datetime import timedelta, datetime



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator



def print_range_number(task_number):
    print(f"task number is:{task_number}")



with DAG(
    'hw_d-oreshnikov_02',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date=datetime(2023, 1, 1)
) as dag:
    
    for i in range(10):
    
        t1 = BashOperator(
            task_id = f'print_echo_{i}',
            bash_command = f'echo {i}'
        )

    for i in range(20):

        t2 = PythonOperator(
            task_id = f'print_range_number_{i}',
            python_callable= print_range_number,
            op_kwargs={'task_number' : i}
        )
t1 >> t2