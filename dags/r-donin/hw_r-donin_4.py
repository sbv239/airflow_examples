"""
Добавьте к вашим задачам из прошлого задания документацию. 
В документации обязательно должны быть элементы кода (заключены в кавычки `code`), 
    полужирный текст и текст курсивом, а также абзац (объявляется через решетку).

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
    'hw_r-donin_3',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Exercise dag from step 2',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 9, 29),
    catchup = False,
    tags = ['startml', 'airflow', 'r-donin']
) as dag:
    
    for i in range(10):
        t_bash = BashOperator(
            task_id = 'step_3_task_' + str(i),
            bash_command = f"echo {i}"
        )
    
    t_bash.doc_md = dedent(
        """
        ### Документация к задаче **t_bash**
        Задача возвращает номер итерации *i* с помощью команды:
        `
        f"echo {i}"
        `
        """
    ) 
       
    def print_task_num(task_number):
        print(f'task number is: {task_number}')
    for i in range(10,30):
        t_py = PythonOperator(
            task_id = 'step_3_task_' + str(i),
            python_callable = print_task_num,
            op_kwargs = {'task_number': i}
        )

    t_py.doc_md = dedent(
        """
        ### Документация к задаче **t_py**
        Задача печатает текст с номером задачи *task_number*.
        Осуществляется через объявленную функцию:
        `
        def print_task_num(task_number):
        print(f'task number is: {task_number}')
        `
        `f"echo {i}"`
        """
    ) 

    dag.doc_md = __doc__
    
    t_bash >> t_py