"""
Добавьте к вашим задачам из прошлого задания документацию. 
В документации обязательно должны быть элементы кода (заключены в кавычки `code`), 
    полужирный текст и текст курсивом, а также абзац (объявляется через решетку).

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
    'hw_r-donin_5',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Exercise dag from step 2',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 9, 29),
    catchup = False,
    tags = ['startml', 'airflow', 'r-donin']
) as dag:
    
    template_comm = dedent(
        '''
        {% for i in range(5) %}
            echo "{{ts}}"
            echo "{{run_id}}"
        {% endfor %}
        '''
    )
    
    template_task = BashOperator(
        task_id = 'task_step_5',
        bash_command = template_comm
    )

    dag.doc_md = __doc__
    
    