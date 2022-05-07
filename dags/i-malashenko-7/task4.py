"""
Test documentation
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

import requests
import json

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
with DAG(
    'hw_4_i-malashenko-7',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 3, 20),
    catchup=False,
    tags=['example'],
) as dag:

    templated_command=dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id='bash_template',
        bash_command = templated_command
    )

    