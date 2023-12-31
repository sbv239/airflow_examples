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

def task_number2(ts,run_id,task_number):
    print(ts)
    print(run_id)
    return  f"task number is: {task_number}"

with DAG(
'il_shestov_task_7',
default_args = default_args,
schedule_interval= timedelta(days=1),
start_date = datetime(2023,4,12),
catchup = False
) as dag:
    for task_number in range(20):
        t2 = PythonOperator(
        task_id = 'print_2_'+ str(task_number),
        python_callable = task_number2,
        op_kwargs = {'task_number':task_number}
        )

t2