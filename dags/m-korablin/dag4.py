"""
DAG doc
Cycles 'for i in range(10)' and 'for i in range(20)'
with **BashOperator** *and* **PythonOperator**
"""
from airflow import DAG                         #импорты всякие
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_3_m-korablin', #уникально имя
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='my_first_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['VanDarkholme'], #тэг
) as dag:

    for i in range(10):
        tB = BashOperator(
            task_id='tB'+str(i),
            bash_command=f"echo {i}" # вывод номера в цикле
        )
    


    def print_number_task(task_number): #сама функция
        print(f'task number is: {task_number}')

    for i in range(20):
        tP = PythonOperator( #часть с PythonOperator
            task_id='tP'+str(i),
            python_callable=print_number_task,
            op_kwargs={'task_number': i}
        )
    tP.doc_md = dedent(
        """\
           # Task Documentation
           ###`code`
           ###*italic*
           ###**bold**
           """
    )

    tB >> tP
    