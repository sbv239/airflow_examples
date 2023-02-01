from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


def print_context(task_number):
    print(f'task number is: {task_number}')
    # return 'Whatever you return gets printed in the logs'


with DAG(
    'r-kutuzov-1_dag_5-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Airflow lesson step 5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=['r-kutuzov-1_step_5'],
) as dag:
    
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )  # поддерживается шаблонизация через Jinja
    # https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#concepts-jinja-templating

    task = BashOperator(
        task_id='print_ts_and_run_id_with_Bash_for_loop',
        depends_on_past=False,
        bash_command=templated_command,
    )

    task
