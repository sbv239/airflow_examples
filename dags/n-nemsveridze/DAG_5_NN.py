from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent


with DAG (
    'task5NN',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description = 'ten times bash twenty times py',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023,3,21,22,30,20),
    catchup=False,
    tags=['NNtask3'],
) as dag:
    templated = dedent("""
    {% for i in range(5) %}
    echo {{ts}}
    echo {{run_id}}
    {% endfor %}
    """)

    task1=BashOperator(task_id='Tamplated',
                       bash_command=templated)
