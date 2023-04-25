from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_5_al-savelev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='test_11_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    catchup=False,
    tags=['hw_5_al-savelev']
) as dag:
    
    temp_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{run_id}}"
        """
    )

    t1 = BashOperator(
        task_id='hw_5_al-savelev_bash',
        bash_command=temp_command)
    
    t1