#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: vitinho-pc
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
}


def print_task_number(task_number, ts, run_id, **kwargs):
    print(f"task number is: {task_number}")
    print(run_id)
    print(ts)


with DAG(
    "hw_v-gajdamachenko-24_7",
    description="v-gajdamachenko-24, L11 Task 7",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 27),
    catchup=False,
    tags=["v-gajdamachenko-24"],
) as dag:
    for i in range(30):
        if i < 10:
            task = BashOperator(task_id=f"bash_task_{i}", bash_command=f"echo {i}")
        else:
            task = PythonOperator(
                task_id=f"python_task_{i}",
                python_callable=print_task_number,
                op_kwargs={"task_number": i},
            )
        task
