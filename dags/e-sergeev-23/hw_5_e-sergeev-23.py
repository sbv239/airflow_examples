from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
#from airflow.operators.python import PythonOperator

templeted_command=dedent('''
{% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id}}"
    {% endfor %}




''')
with DAG(
    "hw_5_e-sergeev-23",
    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

    },
    schedule_interval=timedelta(days=1),
    start_date=datetime.now()-timedelta(days=1),
    catchup=False



) as dag:
    t1=BashOperator(
        task_id='Jinja_test',
        bash_command=templeted_command
    )