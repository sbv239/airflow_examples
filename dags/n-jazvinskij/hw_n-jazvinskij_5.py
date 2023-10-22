from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_11_ex_5-n-jazvinskij',
    default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
    description = 'hw_11_ex_5',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 10, 21),
    catchup = False,
) as dag:
    shablon_command = dedent(
        """
        {% for i in range(5) %}
        echo "{{ts}}"
        echo "{{run_id}}"
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id = 'Bash_task',
        bash_command = shablon_command
    )

    t1