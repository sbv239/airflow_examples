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
    "retry_delay": timedelta(minutes=5), 
}


def print_task_number(task_number, ts, run_id, **kwargs):
    print(f"task number is: {task_number}")
    print(run_id)
    print(ts)


with DAG(
    "hw_v-gajdamachenko-24_10",
    description="v-gajdamachenko-24, L11 Task 10",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=["v-gajdamachenko-24"],
) as dag:
    
    def xcom_push():
        return "Airflow tracks everything"

    def xcom_pull(ti):
        tmp = ti.xcom_pull(
            key='return_value',
            task_ids='xcom_push')

    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=xcom_push
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull
    )

    t1 >> t2
