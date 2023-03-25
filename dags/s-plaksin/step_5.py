from datetime import timedelta, datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_5_s-plaksin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Templating',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 22),
        catchup=False,
        tags=['hw_5'],
) as dag:
    templated_command = dedent(
        '''
        {% for i in range(5) %}
            echo '{{ts}}'
            echo '{{run_id}}'
        {% endfor %}
        '''
    )

    task = BashOperator(
        task_id='templated',
        bash_command=templated_command,
    )
