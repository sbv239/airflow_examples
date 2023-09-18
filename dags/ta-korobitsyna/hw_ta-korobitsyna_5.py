from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_ta-korobitsyna_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
        },
    description='hw_5_ta-korobitsyna',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 18),
    catchup=False,
    tags=['hw_ta-korobitsyna_5'],
) as dag:

    print_st = dedent ("""
                       
                    {% for i in range(5) %}
                        echo "{{ ts }}" 
                        echo "{{ run_id }}"
                    {% endfor %}

    """)
    

    t1 = BashOperator(
        task_id=f'id_ts_5cl',  # id, будет отображаться в интерфейсе
        bash_command=print_st,  # какую bash команду выполнить в этом таске 
        )
