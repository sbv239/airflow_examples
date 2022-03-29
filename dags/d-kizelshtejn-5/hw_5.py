from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'hw_5_d-kizelshtejn-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_5']
) as dag:
    print_ts_run_id = dedent(
        """ 
        {% for i in range(5) %}
            echo "{{ ts }}" 
        {% endfor %} 
        echo "{{ run_id }}" 
        """
    )

    t1 = BashOperator(
        task_id='print_ts_and_run_id_',
        bash_command=print_ts_run_id
    )

    t1.doc_md = dedent(
        """
        ## Создаем __DAG__ _**типа `BashOperator`**_
        этот оператор использует шаблонизированную команду __`print_ts_run_id`__
        """
    )

    t1
