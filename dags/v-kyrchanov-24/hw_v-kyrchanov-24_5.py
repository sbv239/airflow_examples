from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(year=2023, month=9, day=30),
}

dag = DAG(
    dag_id="templated_dag",
    default_args=default_args,
    description="DAG with templated BashOperator",
    schedule_interval="@daily",
)
bash_task = BashOperator(
    task_id="bash_task",
    bash_command="""
    {% for i in range(5) %}
        echo "For i = {{i}}, ts = {{ ts }}, run_id = {{ run_id }}"
    {% endfor %}
    """,
    dag=dag,
)
