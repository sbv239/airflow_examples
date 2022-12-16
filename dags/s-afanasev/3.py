from datetime import timedelta, datetime
from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_3_s-afanasev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        start_date=datetime(2022, 1, 1),
) as dag:

    template = dedent(
        """
        {% for i in range(5) %}
            echo {{ ts }}
            echo {{ run_id }}
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id='template_example',
        bash_command=template,
    )

    t1