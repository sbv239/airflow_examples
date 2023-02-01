from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


def print_context(task_number):
    print(f'task number is: {task_number}')
    # return 'Whatever you return gets printed in the logs'


with DAG(
    'r-kutuzov-1_dag_6-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Airflow lesson step 6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=['r-kutuzov-1_step_6'],
) as dag:

    # Генерируем таски в цикле - так тоже можно
    for i in range(5):
        
        task = BashOperator(
            task_id=f'print_task_num_{i}_with_Bash',  # id, будет отображаться в интерфейсе
            env={"NUMBER": i},  # задает переменные окружения
            bash_command="echo $NUMBER",  # какую bash команду выполнить в этом таске
        )

        task
