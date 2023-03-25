from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'm-zinovenkov_task_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='m-zinovenkov_task_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 16),
        catchup=False,
        tags=['m-zinovenkov_task_2']
) as dag:
    for i in range(10):
        task_1 = BashOperator(
            task_id = f'task_{i}',
            bash_command = f"echo {i}"
        )
        
        task_1.doc_md = dedent(
            """
            ## Документация
            Первые **10 задач** типа *BashOperator*.  
            Выполняется команда, использующая переменную цикла `"f"echo {i}"`   
            """
        )


    def print_task_number(task_number):
        print(f'task number is:  {task_number}')

    for i in range(20):
        task_2 = PythonOperator(
            task_id = f'task_python_{i}',
            python_callable = print_task_number,
            op_kwargs={'task_number' : i}
        )   

        task_2.doc_md = dedent(
            """
            ## Документация
            Последние **20 задач** типа *PythonOperator*.  
            Выполняется команда, использующая переменную цикла `"f"echo {i}"`   
            """
        )

    task_1 >> task_2