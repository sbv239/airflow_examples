from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_garachev_5_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_5'],
) as dag:
    
    command = dedent(
        """
        {% for i in range(5) %}
            echo {{ ts }}
            echo {{ run_id }}
        {% endfor %}
        """)
    for i in range(5):
        t1 = BashOperator(
            task_id=f"hw_5_garachev_{i}_bash",
            bash_command=command
        )
        t1

