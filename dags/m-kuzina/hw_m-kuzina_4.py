from airflow import DAG 
from datetime import timedelta, datetime
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_number(task_number):
    print (f'task number is: {task_number}')



with DAG(
    'hw_m-kuzina_3', 
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    
    start_date=datetime(2023, 9, 19),
    catchup=False 
) as dag:
    
    
    for i in range(10):
        task = BashOperator(
            task_id='use_i' + str(i),
            bash_command=f"echo {i}"
        )
        
    task.doc_md = dedent(
        """
        Эта функция *выполняет команду* `print (i)`
                
        """
    )
    
    
       
        
    for i in range(20):
        task_python = PythonOperator(
        task_id = 'print_num_task' + str(i),
        python_callable=print_task_number,
        op_kwargs={'task_number': i}
        )
        
    task_python.doc_md = dedent(
        """  
        Эта функция **Python** также выводит на экран значение `i`
        # Задействуя функцию print_task_number
        
        """
    )
     
        
    task >> task_python