#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 04:07:30 2023

@author: yupar
"""

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from textwrap import dedent

with DAG(
    'documentation',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "documentation" (hw_3 with documentation)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_4'],
) as dag:

    def print_task_num(task_number):
        print(f'task number is: {task_number}')

    chain([BashOperator(
            task_id='task_' + str(i),
            bash_command=f'echo {i}',
            doc_md=dedent(
                f"""
                bash tasks\
                `code`\
                **bold**\
                *italic*
                # paragraph {i}
                """ + __doc__)
        ) for i in range(1, 11)]
        + [PythonOperator(
            task_id='task_' + str(i),
            python_callable=print_task_num,
            op_kwargs={'task_number': i},
            doc_md=dedent(
                f"""
                py tasks\
                `code`\
                **bold**\
                *italic*
                # paragraph {i}
                """ + __doc__)
        ) for i in range(11, 31)])

    if __name__ == "__main__":
        dag.test()