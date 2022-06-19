from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'a-shagvaleev_ex2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 6, 19),
) as dag:
        templated_command = dedent(
                """
                {% for i in range(5) %}
                        echo "{{ ts }}"
                        echo "{{ run_id }}"
                {% endfor %}
            """
        )

        t1 = BashOperator(
                task_id="bash_task",
                bash_command=templated_command
        )
        t1.doc_md = dedent(
                """
                # Документация
        
                _текст курсивом_
                
                *ещё один текст курсивом*
                
                **полужирный текст**
                
                __ещё полужирный текст__
                
                `x = 5`
                """
        )

        t1