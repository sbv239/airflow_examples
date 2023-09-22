from datetime import datetime, timedelta
from textwrap import dedent

from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator



with DAG(
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},

        start_date=datetime(2023, 9, 21),
        dag_id="hw_3_a-ratushnyj",
        schedule_interval=timedelta(days=1),
        tags=['hw-3'],
        # Описание DAG (не тасок, а самого DAG)

) as dag:

    templated_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )

    t1 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )
    t1
