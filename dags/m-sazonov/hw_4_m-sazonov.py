from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_4_m-sazonov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='hw_4_m-sazonov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 27),
    catchup=False,
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'hw_3_sazonov_taks_{i}',
            bash_command=f'echo {i}',
        )
        t1.doc_md = dedent(
            """
            ## Документация
            Первые **10 задач** типа *BashOperator*.  
            Выполняется команда, использующая переменную цикла `"f"echo {i}"`   
            """
        )

    def tasks(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t2 = PythonOperator(
            task_id = f'task_number_is_{i}',
            python_callable=tasks,
            op_kwargs={'task_number': i},
        )
        t2.doc_md = dedent(
            """
            ## Документация
            Следующие **20 задач** типа *PythonOperator*.  
            Выполняется команда, использующая переменную цикла `"f"task number {i}"`   
            """
        )

    t1 >> t2