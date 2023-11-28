from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent
from airflow.operators.bash import BashOperator
#from airflow.operators.python import PythonOperator

with DAG(
    'hw_vl-nikolajchuk_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description = 'Dag for hw5',
        schedule_interval=timedelta(days=1),
        start_date = datetime(2023, 11, 27),
        catchup=False,
        tags=['hw_5']
) as dag:
    templated_command = dedent(
        '''
        {% for i in range(5)%}
            echo '{{ ts }}'
            echo '{{ run_id }}'
        {% endfor %}
        '''
    )

    task = BashOperator(
        task_id = 'print_ts_&_run_id',
        bash_command = templated_command
    )

