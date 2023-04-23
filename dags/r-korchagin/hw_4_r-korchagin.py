"""
`code`
#Fat
*italic*
**bold**
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'HW3_rkorch',
    # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    # Описание DAG (не тасок, а самого DAG)
    description='HW3',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:


    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id='hw3_templ_bash_r-korchagin'+str(i),
                bash_command = f'echo {i}'
            )

        else:
            def print_task(task_number):
                print(f"task number is: {task_number}")
            t2 = PythonOperator(
                task_id='print_task_r-korchagin' + str(i),
                python_callable=print_task,
                op_kwargs={'task_number': i})

    t1.doc_md = dedent(
        '''
        `code`
    
        *italic*
    
        **bold**
    
        #Fat
    
        ''')

    t2.doc_md = dedent(
        '''
        `code`
    
        *italic*
    
        **bold**
    
        #Fat
    
        ''')