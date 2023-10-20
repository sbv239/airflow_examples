from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    'Print_bash_for',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Task5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 18),
    catchup=False,
    tags=['task_5', 'lesson_11']
)

templated_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
)

t1 = BashOperator(
    task_id='hw_j-rzayev_1',
    bash_command=templated_command,
    dag=dag,
)

t1
