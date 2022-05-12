from datetime import timedelta
from textwrap import dedent

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    "task_4",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": days_ago(2),
    },
    catchup=False,
) as dag:

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo {{ ts }}
            echo {{ run_id }}
        {% endfor %}
        """
    )
    BashOperator(task_id=f"jinja_task", bash_command=templated_command, dag=dag)
