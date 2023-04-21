# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 16:01:30 2023

@author: user
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 16:05:43 2023

@author: user
"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tutorial',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='task_2',
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['task_2'],
    ) as dag:
    
        t_bash = BashOperator(
            task_id='print_pwd',  # id, будет отображаться в интерфейсе
            bash_command='pwd',  # выполняем комманду pwd
            dag=dag,
        )
        
        def print_context(ds):
            print(ds)

            
        t_python = PythonOperator(
            task_id='print_ds',
            python_callable=print_context,
            dag=dag,
        )
        
        # Указывается последовательность задач
        t_bash >> t_python