# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 11:36:39 2022

@author: Vera
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    'HM_4',
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
    templated_command = dedent(
        """
        {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{run_id}}"
        {% endfor %}
        """
    ) # поддерживается шаблонизация через Jinja
    t1 = BashOperator(
        task_id="bash_ts",
        bash_command=templated_command, # обратите внимание на пробел в конце!
        dag=dag
    )  
        
        
        
        