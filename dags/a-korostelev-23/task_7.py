"""
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
Here is example of code: `bash -c pwd`
Here is example of cursive: *some text with cursive*
Here is example of bold: **some bold text**
#tst
"""

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

with DAG(f'hw_{login}_7', default_args=default_args, start_date=datetime(2023, 8, 13)) as dag:
        
    def func_for_po(ts, run_id, **kwargs):
        print(f" printing ts: {ts}")
        print(f" printing runid: {run_id}")
        print(f"task number is: {kwargs['task_number']}")
    
    for j in range(20):
        python_tasks = PythonOperator(task_id='task_python_'+str(j),
                                      python_callable=func_for_po,
                                      op_kwargs={'task_number': j})
        
    python_tasks
    
    
    
    

