from airflow import DAG

from datetime import timedelta, datetime
from textwrap import dedent

from airflow.operators.bash import BashOperator

with DAG(
    'hw_d-trubitsin_5',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 23),
    catchup=False,
    tags=['d-trubitsin_3'],
) as dag:

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo {{ts}}
            echo {{run_id}}
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id='bash_task_1',
        bash_command=templated_command,
        dag=dag
    )
