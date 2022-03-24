#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta


date = "{{ ds }}"
def return_date(date):
    return date

with DAG(
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}) as dag:
    task1 = PythonOperator(
    task_id='print_the_date',
    python_callable=return_date,
    dag=dag
)
    task2 = BashOperator(
    task_id="pwd",
    bash_command="pwd ", 
    dag=dag
)
    task2 >> task1