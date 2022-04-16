# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 15:43:41 2022

@author: Vera
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    'HM_2',
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
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 15),
    # Запустить за старые даты относительно сегодня
) as dag:
    # t1 - это операторы (они формируют таски, а таски формируют даг)
    for i in range(10):
        t1 = BashOperator(
            task_id='Bash_op_' + str(i), # в id можно делать все, что разрешают строки в python
            bash_command=dedent(f"echo {i}"), # какую bash команду выполнить в этом таске
        )
    def print_text(ts, run_id, **kwargs):
        """Пример PythonOperator"""
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print("task number is: {task_number}")
        print(ts)
        print(run_id)
    for i in range(20):
        t2 = PythonOperator(
            task_id='Python_Op_' + str(i), # нужен task_id, как и всем операторам
            python_callable=print_text,# свойственен только для PythonOperator - передаем саму функцию
            op_kwargs={'task_number': i},
            provide_context=True,    
            dag=dag
        )

        
        
        
