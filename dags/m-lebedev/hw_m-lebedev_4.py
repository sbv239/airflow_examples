'''
Хорошая практика, которую я никогда не запомню, это внятно описывать происходящее в комментарии.
Домашнее задание 4: написать такой комментарий.
'''

from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_number(task_number, **kwargs):
    print(f'task number is: {task_number}')

with DAG(
    'hw_m-lebedev_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework: 4, login: m-lebedev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 23),
    catchup=False,
    tags=['m-lebedev'],
) as dag:

    for i in range(1, 11):
        bash_task = BashOperator(
            task_id= f'bash_print_task_{i}',
            bash_command=f'echo {i}',
        )
        
    for i in range(11, 31):
        puthon_task =  PythonOperator(
            task_id= f'python_print_task_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
        
    dag.doc_md = __doc__
        
    bash_task.doc_md = dedent(
        '''\
        ### Описание простой задачи по выполнению bash
        *Это 4 задание*, [ссылка на него](https://lab.karpov.courses/learning/255/module/2542/lesson/22966/65951/306727/)
        Нужно еще написать текст **жирным**.
        А команда для печати впеременной в ***bash*** `f'echo {i}'`
        '''
        )
        
    puthon_task.doc_md = dedent(
        '''\
        ### Описание простой задачи по выполнению python кода
        *Это 4 задание*, [ссылка на него](https://lab.karpov.courses/learning/255/module/2542/lesson/22966/65951/306727/)
        Нужно еще написать текст **жирным**.
        А в функцию переменная отправляется через `op_kwargs={'key': i}'`
        '''
        )        