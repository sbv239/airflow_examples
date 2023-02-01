from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_context(task_number):
    print(f'task number is: {task_number}')
    # return 'Whatever you return gets printed in the logs'


with DAG(
    'r-kutuzov-1_dag_3-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Airflow lesson step 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=['r-kutuzov-1_step_3'],
) as dag:

    # Генерируем таски в цикле - так тоже можно
    for i in range(30):
        
        if i < 10:
            task = BashOperator(
                task_id=f'print_task_num_{i}_with_Bash',  # id, будет отображаться в интерфейсе
                bash_command=f'echo task # {i}',  # какую bash команду выполнить в этом таске
            )
        
            task
        
        else:
            task = PythonOperator(
                task_id=f'print_task_num_{i}_with_Python',  # в id можно делать все, что разрешают строки в python
                python_callable=print_context,
                # передаем в аргумент с названием random_base значение float(i) / 10
                op_kwargs={'task_number': i},
            )
        
            task
