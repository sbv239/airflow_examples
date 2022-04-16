from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
    'G-Ivanov-task4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='First task in the lesson on Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        """
    )
    t1 = BashOperator(
        task_id="print_ts",
        bash_command=templated_command,
        dag=dag,
    )

    t1
