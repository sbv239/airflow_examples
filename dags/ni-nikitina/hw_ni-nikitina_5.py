from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_ni-nikitina_5', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Second Task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 29),
    catchup=False
) as dag:
    command_template = dedent(
        '''
        {% for i in range(5) %}
        echo ts: {{ ts }}
        echo run_id: {{ run_id }}
        {% endfor %}
        '''
    )
    t1 = BashOperator(
        task_id='hw_5_nn_bo',
        bash_command=command_template
    )