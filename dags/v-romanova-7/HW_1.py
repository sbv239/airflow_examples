# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 15:27:00 2022

@author: Vera
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

with DAG(
    'HM_1',
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
    t1 = BashOperator(
        task_id='pwd_command', # id, будет отображаться в интерфейсе
        bash_command='pwd ', # какую bash команду выполнить в этом таске
    )
    def print_context(ds, **kwargs):
        """Пример PythonOperator"""
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'Какой-то текст'
    t2 = PythonOperator(
        task_id='print_the_context', # нужен task_id, как и всем операторам
        python_callable=print_context # свойственен только для PythonOperator - передаем саму функцию
    )

    












