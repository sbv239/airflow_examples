from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

with DAG(
    'hw_4_j-gladkov-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = "dynamic tasks",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 22),
    catchup = False,
    tags = ['hw_4'],
) as dag:

    templ_comma = dedent(
        """
        {% for i in range(5) %}
            echo "{{ts}}"
        {% endfor %}
        echo "{{run_id}}"
        """
    )

    alone_task = BashOperator(
        task_id = 'template_printer',
        bash_command = templ_comma,
    )
    
    alone_task
