"""
My second DAG
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_5_r-nazmiev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='A simple practice DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,7,17),
    catchup=False,
    tags=['example'],
) as dag:
            
    sample_command = dedent(
        """
        {% for i in range(5) %}
        echo {{ts}}
        echo {{run_id}}
        {% endfor %}
        """
        
    )        
    t1 = BashOperator(
        task_id = "sample_command",
        bash_command =  sample_command,
            
        )
        
