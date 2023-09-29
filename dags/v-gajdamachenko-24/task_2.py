#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 11:42:44 2023

@author: vitinho-pc
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def print_date(ds, **kwargs):
    print(f"Date:{ds}")

with DAG (
    'hw_v-gajdamachenko-24_1',
    description='v-gajdamachenko-24, lesson 11 task 2',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 26),
    catchup=False,
    tags=['v-gajdamachenko'],
) as dag:

    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd',
    )

    t2 = PythonOperator(
        task_id = 'print_date',
        python_callable=print_date,
    )

    t1 >> t2
