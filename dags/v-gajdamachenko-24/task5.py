#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: vitinho-pc
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
}


def print_task_number(task_number):
    print(f"task number is: {task_number}")


with DAG(
    "hw_5_v-gajdamachenko-24",
    description="v-gajdamachenko-24, L11 task 5",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 29),
    catchup=False,
    tags=["v-gajdamachenko-24"],
) as dag:
    
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id}}"
    {% endfor %}
    """
    )
        
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )