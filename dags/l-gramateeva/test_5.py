
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.python_operator import PythonOperator
from airflow import DAG


from airflow.operators.bash import BashOperator
with DAG(
    'hw_l-gramateeva_5',
    default_args={
    	'depends_on_past': False,
        'email': ['airflow@example.com'],
    	'email_on_failure': False,
    	'email_on_retry': False,
    	'retries': 1,
   	    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=['l-gramateeva'],
) as dag:
    template_command = dedent("""
        {% for i in range(1, 5) %}
        echo "ts={{ ts }} run_id={{ run_id }}"
        {% endfor %}
    """)
    t1 = BashOperator(
        task_id=f'hw_5_l-gramateeva',  # id, будет отображаться в интерфейсе
        bash_command=template_command,
    )
            