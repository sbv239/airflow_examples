from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    "hw_4_v-baev-7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='templates in DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=5, day=3),
    catchup=False,
    tags=['hw_4'],
) as dag:
        task_bash = BashOperator(
        task_id='temlated',
        bash_command=temlated_command
        )

templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ts}}"
        {% endfor %}
        echo "{{run_id}}"    
    """
    )