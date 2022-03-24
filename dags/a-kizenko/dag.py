#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.bash import PythonOperator


date = "{{ ds }}"
def return_date(date):
    return date

with DAG() as dag:
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