from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta,datetime

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def tusk_number2(task_number):
    return  f"task number is: {task_number}"

with DAG(
'il_shestov_task_3',
default_args = default_args,
schedule_interval= timedelta(days=1),
start_date = datetime(2023,4,13),
catchup = False
) as dag:
    for i in range(10):
        t1 = BashOperator(
        task_id ='print' + str(i),
        bash_command = f'echo{i}'
        )
    for task_number in range(20):
        t2 = PythonOperator(
        task_id = 'print_2_'+ str(task_number),
        python_callable = tusk_number2,
        op_kwargs = {'tusk_number':task_number}
        )

t1 >> t2