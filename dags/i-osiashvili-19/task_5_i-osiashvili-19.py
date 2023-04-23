from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from textwrap import dedent


with DAG(
        'les_11_task_5_i-osiashvili-19',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 4, 22),
        schedule_interval=timedelta(days=1),
) as dag:
    template_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ts}}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )
    task_1 = BashOperator(
        task_id="printing_ts_and_run_id",
        bash_command=template_command,
    )

