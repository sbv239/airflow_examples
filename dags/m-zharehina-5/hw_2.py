"""
hw_1_m-zharehina-5
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_2_m_zharehina_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='hw_2_m_zharehina_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 26),
    catchup=False,
    tags=['hw_2_m_zharehina_5'],
    ) as dag:
    
    templated_command = dedent(
    """
    {% for i in range(10) %}
        print(f"echo {i}")
    {% endfor %}
    """
    )
    
    t1 = BashOperator(
        task_id='hw_2_m_zharehina_5',
        depends_on_past=False,
        bash_command=templated_command,
    )
    
    def print_var(i):
        print(i)
    
    for i in range(5):
        task = PythonOperator(
            task_id='hw_2_m_zharehina_5_print_i_' + str(i),
            python_callable=print_var,
            op_kwargs={'i': int(i)},
        )
        task_id
    