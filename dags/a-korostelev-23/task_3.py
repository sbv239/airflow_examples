from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

login = 'a-korostelev-23'

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(f'hw_{login}_3', default_args=default_args, start_date=datetime(2023, 8, 13)) as dag:
    for i in range(10):
        bash_tasks = BashOperator(task_id='task_bash_'+str(i),
                                  bash_command=f'echo {i}')
        
    def func_for_po(task_number):
        print(f'task number is: {task_number}')
    
    for j in range(20):
        python_tasks = PythonOperator(task_id='task_python_'+str(j),
                                      python_callable=func_for_po,
                                      op_kwargs={'task_number': j})
        
    bash_tasks >>  python_tasks
    
    
    

