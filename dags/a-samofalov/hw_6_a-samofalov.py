from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'HW_3_a-samofalov',
    # Параметры по умолчанию для тасок
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description='A simple tutorial DAG june 2023',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    tags=['hw_6_a-samofalov'],
) as dag:
    for i in range(10):
        bash_task = BashOperator(
            task_id='bash_print_' + str(i),  # в id можно делать все, что разрешают строки в python
            bash_command="echo $NUMBER", env={'NUMBER': str(i)})