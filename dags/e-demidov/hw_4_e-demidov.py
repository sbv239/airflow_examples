from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

with DAG(
    'hw_4_e-demidov',
    default_args={
        'depends_on_past': False,
        'email': ['yevgeniy.demidov@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_4_e-demidov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 16),
    catchup=False,
    tags=['e-demidov'],
) as dag:
    
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )  
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )
    t3