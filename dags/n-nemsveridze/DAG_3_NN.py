from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
with DAG (
    'task2NN',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description = 'ten times bash twenty times py',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023,3,21,22,30,20),
    catchup=False,
    tags=['NNtask3'],
) as dag:
    for i in range(10):
        task1=BashOperator(task_id='Ten times + str(i)',
                           bash_command='f"echo{i}')
    def cycle_twenty(task_number):
        return print(f"task number is: {task_number}")

    for j in range(20):
        task2=PythonOperator(task_id='Twenty times + str(j)',
                             python_callable = cycle_twenty,
                             op_kwargs={'task_number':int(j)})
    task1 >> task2




